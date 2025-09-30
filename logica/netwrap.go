package logica

import (
	"bytes"
	crand "crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	mrand "math/rand"
	"os"
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

var (
	rtMu sync.Mutex // <-- nome corretto
)

// struttura per il salvataggio
type KBucketFile struct {
	NodeID    string   `json:"node_id"`
	BucketHex []string `json:"bucket_hex"`
	SavedAt   string   `json:"saved_at"`
}

type PeerEntry struct {
	Name string `json:"name"` // es. "node7"
	SHA  string `json:"sha"`  // sha1 in hex del peer
	Dist string `json:"dist"` // distanza XOR self^peer (hex)
}

type RoutingTableFile struct {
	NodeID       string                 `json:"self_node"`
	BucketSize   int                    `json:"bucket_size"`
	HashBits     int                    `json:"hash_bits"` // 160 per SHA1
	SavedAt      string                 `json:"saved_at"`
	Buckets      map[string][]PeerEntry `json:"buckets"` // "0".."159" -> peers
	NonEmptyInfo int                    `json:"non_empty_buckets"`
}

var (
	// Esporta questi se vuoi settarli dal main:
	KBucketPath string
	KCapacity   int
	kHashBits   = 160
)

func SetKBucketGlobals(path string, capacity int) {
	KBucketPath = path
	KCapacity = capacity
}

func EnsureKBucketFile(path, selfName string) error {
	rtMu.Lock()
	defer rtMu.Unlock()

	// Backup best-effort se esiste
	if _, err := os.Stat(path); err == nil {
		_ = os.Rename(path, path+".bak."+time.Now().UTC().Format("20060102T150405Z"))
	}

	rt := &RoutingTableFile{
		NodeID:       selfName, // JSON: "self_node"
		BucketSize:   kCapacity,
		HashBits:     kHashBits,
		SavedAt:      time.Now().UTC().Format(time.RFC3339),
		Buckets:      map[string][]PeerEntry{},
		NonEmptyInfo: 0,
	}

	return saveRTAtomic(path, rt)
}

func saveRTAtomic(path string, rt *RoutingTableFile) error {
	rt.SavedAt = time.Now().UTC().Format(time.RFC3339)
	j, _ := json.MarshalIndent(rt, "", "  ")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, j, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path) // rename è atomico su stessa FS
}

func countNonEmpty(m map[string][]PeerEntry) int {
	n := 0
	for _, v := range m {
		if len(v) > 0 {
			n++
		}
	}
	return n
}

func loadRT(path string) (*RoutingTableFile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var rt RoutingTableFile
	if err := json.Unmarshal(b, &rt); err != nil {
		return nil, err
	}
	if rt.Buckets == nil {
		rt.Buckets = map[string][]PeerEntry{}
	}
	if rt.BucketSize <= 0 {
		rt.BucketSize = kCapacity
	}
	return &rt, nil
}

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

	// 1) prendo i seed dal seeder → pulisco, mescolo, taglio e me ne rimangono seedSample
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

	// 2) ping iniziale dei seed → popola RT via TouchContactByName, Ritorna i nodi vivi
	live := pingMany(ctx, selfName, clean, joinMaxInflightPing)
	if len(live) == 0 {
		return nil
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

						sc, err := common.XOR(peerID, tgt)
						if err != nil {
							return
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

// Viene fatto il ping di ogni nodo in targets e restituisce la lista di quelli che vanno a buon fine
func pingMany(ctx context.Context, selfName string, targets []string, maxInflight int) []string {
	if maxInflight <= 0 {
		maxInflight = 8
	}
	sem := make(chan struct{}, maxInflight)
	var mu sync.Mutex
	live := make([]string, 0, len(targets))

	for _, tgt := range targets {
		tgt := tgt
		sem <- struct{}{}
		go func() {
			defer func() { <-sem }()

			addr := fmt.Sprintf("%s:%d", tgt, 8000) // adatta se hai un mapping nome→host:port
			cctx, cancel := context.WithTimeout(ctx, 1*time.Second)
			defer cancel()

			conn, err := grpc.DialContext(cctx, addr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithBlock(),
			)
			if err != nil {
				return
			}
			defer conn.Close()

			client := pb.NewKademliaClient(conn)
			res, err := client.Ping(cctx, &pb.PingReq{
				From: &pb.Node{Id: selfName},
			})
			if err != nil || !res.GetOk() {
				return
			}

			// ⬇️ QUI: aggiorna il TUO bucket con il peer che hai pingato con successo
			_ = TouchContactByName(tgt)

			mu.Lock()
			live = append(live, tgt)
			mu.Unlock()
		}()
	}
	for i := 0; i < cap(sem); i++ {
		sem <- struct{}{}
	}
	return live
}
