package logica

import (
	"bytes"
	crand "crypto/rand"
	"encoding/hex"
	"fmt"
	mrand "math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"context"

	"kademlia-nft/common"
	pb "kademlia-nft/proto/kad"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ===== Config locali (puoi portarli in ENV) =====
const (
	joinDialTimeout     = 1200 * time.Millisecond
	joinRPCTimeout      = 1500 * time.Millisecond
	joinMaxInflightPing = 12 // pings concorrenti
)

// --- helpers locali ---

func addrOf(name string) string {
	// Semplice risoluzione "nodeX" -> "nodeX:8000"
	// Se già è "host:port", va bene così.
	if strings.Contains(name, ":") {
		return name
	}
	return fmt.Sprintf("%s:%d", name, 8000)
}

func uniqKeepOrder(ss []string) []string {
	seen := make(map[string]struct{}, len(ss))
	out := make([]string, 0, len(ss))
	for _, s := range ss {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

func randomID160() []byte {
	b := make([]byte, 20)
	_, _ = crand.Read(b)
	return b
}

// prende un pb.Node e restituisce un nome "chiamabile" (nodeX o host:port)
func dialableName(n *pb.Node) string {
	if n == nil {
		return ""
	}
	if h := strings.TrimSpace(n.GetHost()); h != "" {
		return h
	}
	if id := strings.TrimSpace(n.GetId()); id != "" {
		return id
	}
	return ""
}

// Ping(remote) con FromId=selfName. Se OK, TouchContact(remote).
func RpcPing(ctx context.Context, selfName, remote string) error {
	ap := addrOf(remote)

	dctx, cancel := context.WithTimeout(ctx, joinDialTimeout)
	defer cancel()
	conn, err := grpc.DialContext(dctx, ap, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return err
	}
	defer conn.Close()

	cli := pb.NewKademliaClient(conn)

	rctx, cancel2 := context.WithTimeout(ctx, joinRPCTimeout)
	defer cancel2()

	_, err = cli.Ping(rctx, &pb.PingReq{
		From: &pb.Node{Id: selfName, Host: selfName, Port: 8000},
	})
	if err == nil {
		_ = TouchContactByName(remote) // aggiorna LRU locale (salva kbucket.json)
	}
	return err
}

// FIND_NODE emulata via LookupNFT(Key=target). Se OK, TouchContact(remote) e ritorna nomi chiamabili.
func RpcFindNodeNames(ctx context.Context, selfName, remote string, target []byte) ([]string, error) {
	ap := addrOf(remote)

	dctx, cancel := context.WithTimeout(ctx, joinDialTimeout)
	defer cancel()
	conn, err := grpc.DialContext(dctx, ap, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	cli := pb.NewKademliaClient(conn)

	rctx, cancel2 := context.WithTimeout(ctx, joinRPCTimeout)
	defer cancel2()

	resp, err := cli.LookupNFT(rctx, &pb.LookupNFTReq{
		FromId: selfName,
		Key:    &pb.Key{Key: target}, // usiamo "Key" come target per FIND_NODE
	})
	if err != nil {
		return nil, err
	}

	_ = TouchContactByName(remote) // ha risposto → vivo

	out := make([]string, 0, len(resp.GetNearest()))
	for _, n := range resp.GetNearest() {
		if nm := dialableName(n); nm != "" {
			out = append(out, nm)
		}
	}
	return uniqKeepOrder(out), nil
}

// ============== FUNZIONE RICHIESTA ==============

// JoinAndExpandLite:
//  1. prende seed dal seeder, li pinga → Live[]
//  2. per iters ondate: da ogni vivo fa 2 FIND_NODE (target=self, target=random), raccoglie prospects,
//     ne pinga un sottoinsieme, e ripete con i nuovi vivi.
//
// Stop aggiuntivi: no prospects, niente da pingare, "no gain" (nessun nuovo contatto aggiunto).
func JoinAndExpandLite(ctx context.Context, seederAddr, selfName string, alpha, seedSample, iters int) error {
	if alpha <= 0 {
		alpha = 2
	}
	if seedSample <= 0 {
		seedSample = 2 * kCapacity
	}
	if iters <= 0 {
		iters = 2
	}
	mrand.Seed(time.Now().UnixNano())

	// 1) prendo i seed dal seeder
	seeds, err := GetNodeListIDs(seederAddr, selfName)
	if err != nil {
		return err
	}
	clean := make([]string, 0, len(seeds))
	seen := map[string]struct{}{}
	for _, s := range seeds {
		s = strings.TrimSpace(s)
		if s == "" || s == selfName {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		clean = append(clean, s)
	}
	mrand.Shuffle(len(clean), func(i, j int) { clean[i], clean[j] = clean[j], clean[i] })
	if len(clean) > seedSample {
		clean = clean[:seedSample]
	}

	// 2) ping iniziale dei seed → popola RT via TouchContactByName
	live := pingMany(ctx, selfName, clean, joinMaxInflightPing)
	if len(live) == 0 {
		return nil // nessuno vivo ora; riproverai più tardi
	}

	selfRaw := common.Sha1ID(selfName)
	selfHex := strings.ToLower(hex.EncodeToString(selfRaw))

	for it := 0; it < iters; it++ {
		if len(live) == 0 {
			break
		}

		// --- snapshot prima (nuovo formato) ---
		beforeSet := kbSnapSet(kBucketPath)

		// --- scoperta tramite "FIND_NODE" emulata ---
		type prospect struct {
			Name  string
			Score []byte // distanza al target per ordinamento
		}
		prospects := make([]prospect, 0, 256)
		sem := make(chan struct{}, alpha)
		var mu sync.Mutex

		targets := [][]byte{selfRaw, randomID160()}

		for _, src := range live {
			for _, tgt := range targets {
				src, tgt := src, tgt
				sem <- struct{}{}
				go func() {
					defer func() { <-sem }()
					names, err := RpcFindNodeNames(ctx, selfName, src, tgt)
					if err != nil {
						return
					}
					mu.Lock()
					for _, nm := range names {
						if nm == "" || nm == selfName {
							continue
						}
						peerID := common.Sha1ID(nm)
						peerHex := strings.ToLower(hex.EncodeToString(peerID))
						if peerHex == selfHex {
							continue
						}
						if _, ok := beforeSet[peerHex]; ok {
							continue // già presente prima
						}
						// score = XOR(peerID, tgt)
						sc := make([]byte, 20)
						for i := 0; i < 20; i++ {
							sc[i] = peerID[i] ^ tgt[i]
						}
						prospects = append(prospects, prospect{Name: nm, Score: sc})
					}
					mu.Unlock()
				}()
			}
		}
		// drain: attendo tutte le goroutine
		for i := 0; i < cap(sem); i++ {
			sem <- struct{}{}
		}

		if len(prospects) == 0 {
			break
		}

		// ordina per distanza crescente al target
		sort.SliceStable(prospects, func(i, j int) bool {
			return bytes.Compare(prospects[i].Score, prospects[j].Score) < 0
		})

		// evita ping su nodi già noti (snapshot "now")
		nowSet := kbSnapSet(kBucketPath)
		toPing := make([]string, 0, 2*kCapacity)
		for _, p := range prospects {
			if len(toPing) >= 2*kCapacity {
				break
			}
			nm := p.Name
			hx := strings.ToLower(hex.EncodeToString(common.Sha1ID(nm)))
			if _, ok := nowSet[hx]; ok {
				continue
			}
			toPing = append(toPing, nm)
		}
		toPing = uniqKeepOrder(toPing)
		if len(toPing) == 0 {
			break
		}

		// ping mirato → se rispondono entrano via TouchContactByName
		live = pingMany(ctx, selfName, toPing, joinMaxInflightPing)

		// criterio di arresto: nessun nuovo contatto aggiunto
		afterSet := kbSnapSet(kBucketPath)
		added := 0
		for h := range afterSet {
			if _, ok := beforeSet[h]; !ok {
				added++
			}
		}
		if added == 0 {
			break
		}
	}

	return nil
}

// -------- helpers per la RT NUOVA --------

// snapshot degli SHA presenti in tutti i bucket del file strutturato
func kbSnapSet(path string) map[string]struct{} {
	set := make(map[string]struct{}, 128)
	rt, err := loadRT(path) // <-- la tua loadRT del nuovo formato
	if err != nil || rt.Buckets == nil {
		return set
	}
	for _, list := range rt.Buckets {
		for _, e := range list {
			h := strings.ToLower(strings.TrimSpace(e.SHA))
			if len(h) == 40 {
				set[h] = struct{}{}
			}
		}
	}
	return set
}
