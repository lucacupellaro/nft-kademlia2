package ui

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"kademlia-nft/common"
	"kademlia-nft/logica"

	"sort"

	pb "kademlia-nft/proto/kad"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Pair: esa = SHA1 hex dell'ID, hash = ID/alias del nodo (es. "node7")
func Check(value string, list []Pair) string {
	val := strings.ToLower(strings.TrimSpace(value))
	for _, v := range list {
		if strings.ToLower(strings.TrimSpace(v.esaSha1)) == val {

			return strings.TrimSpace(v.name)
		}
	}
	return "NOTFOUND"
}

// --- util esterne che già hai altrove ---
// ResolveStartHostPort(name) (string, error)
// Check(idHex, reverse []Pair) string
// common.Sha1ID(string) []byte
// common.XOR(a,b []byte) ([]byte, error)

// ====================== TIPI E UTIL ======================

type cand struct {
	idHex string // ID in hex (40 char)
	addr  string // host:port risolto
	label string // per logging (nome nodo o host)
	dist  []byte // XOR(target, id)
}

// ordina la shortlist per distanza crescente (tie-break su idHex)
func sortShort(short []cand) {
	sort.Slice(short, func(i, j int) bool {
		if c := bytes.Compare(short[i].dist, short[j].dist); c != 0 {
			return c < 0
		}
		return short[i].idHex < short[j].idHex
	})
}

// taglia la shortlist a K elementi
func trimToK(short []cand, K int) []cand {
	if len(short) > K {
		return short[:K]
	}
	return short
}

// distanza minima corrente nella shortlist (nil se vuota)
func getClosest(short []cand) []byte {
	if len(short) == 0 {
		return nil
	}
	return short[0].dist
}

// restituisce fino a k candidati **non interrogati** (in ordine)
func takeTopUnqueried(short []cand, queried map[string]bool, k int) []cand {
	out := make([]cand, 0, k)
	for _, c := range short {
		if len(out) >= k {
			break
		}
		if queried[c.idHex] {
			continue
		}
		out = append(out, c)
	}
	return out
}

// costruisce candidati da pb.Node; se host/port manca, usa reverse-map per risolvere.
// skipID: ID hex da escludere (es. self)
func buildFromNearest(nearest []*pb.Node, skipID string, reverse []Pair, target []byte) []cand {
	seen := make(map[string]bool, len(nearest))
	out := make([]cand, 0, len(nearest))

	for _, n := range nearest {
		id := strings.ToLower(strings.TrimSpace(n.GetId()))
		if len(id) != 40 || id == skipID || seen[id] {
			continue
		}
		seen[id] = true

		// endpoint
		var addr, label string
		host := strings.TrimSpace(n.GetHost())
		port := int(n.GetPort())
		if host != "" && port > 0 {
			addr = fmt.Sprintf("%s:%d", host, port)
			label = host
		} else {
			if name := Check(id, reverse); name != "NOTFOUND" {
				if hp, e := ResolveStartHostPort(name); e == nil && hp != "" {
					addr = hp
					label = name
				}
			}
		}
		if addr == "" {
			continue
		}
		if label == "" {
			if host != "" {
				label = host
			} else if len(id) >= 8 {
				label = id[:8]
			} else {
				label = id
			}
		}

		idBytes, err := hex.DecodeString(id)
		if err != nil || len(idBytes) != len(target) {
			continue
		}
		d, err := common.XOR(target, idBytes)
		if err != nil {
			continue
		}

		out = append(out, cand{
			idHex: id,
			addr:  addr,
			label: label,
			dist:  d,
		})
	}
	return out
}

// unisce nuovi candidati nella shortlist, con dedup, sort e trim a K
func mergeIntoShort(short []cand, add []cand, K int, idsInShort map[string]bool) ([]cand, int) {
	added := 0
	for _, c := range add {
		if idsInShort[c.idHex] {
			continue
		}
		idsInShort[c.idHex] = true
		short = append(short, c)
		added++
	}
	if added > 0 {
		sortShort(short)
		short = trimToK(short, K)
	}
	return short, added
}

// effettua una LookupNFT (FIND_VALUE) su un singolo candidato
func rpcLookup(ctx context.Context, addr string, target []byte) (*pb.LookupNFTRes, error) {
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	client := pb.NewKademliaClient(conn)
	resp, err := client.LookupNFT(ctx, &pb.LookupNFTReq{
		FromId: "CLI",
		Key:    &pb.Key{Key: target},
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ====================== ENTRYPOINT ======================

// LookupNFTOnNodeByNameAlpha: orchestratore della lookup iterativa α-parallela.
func LookupNFTOnNodeByNameAlpha(startNode string, reverse []Pair, nftName string, alpha int, maxRounds int) (round int, found bool, err error) {
	// parametri
	if alpha <= 0 {
		alpha = 3
	}
	if maxRounds <= 0 {
		maxRounds = 30
	}
	K := logica.RequireIntEnv("BUCKET_SIZE", 5)
	if K <= 0 {
		K = 20
	}

	// target = SHA1(nftName)
	target := common.Sha1ID(nftName)

	// risoluzione nodo di partenza
	hostPort, e := ResolveStartHostPort(startNode)
	if e != nil || hostPort == "" {
		return 0, false, fmt.Errorf("risoluzione %q fallita: %w", startNode, e)
	}
	selfHex := strings.ToLower(hex.EncodeToString(common.Sha1ID(startNode)))

	// strutture per la shortlist
	short := make([]cand, 0, 64)
	idsInShort := map[string]bool{}
	queried := map[string]bool{selfHex: true}

	// --- Hop 1: chiedi al nodo di partenza ---
	fmt.Printf("🔎 Hop 1: cerco '%s' su %s (%s)\n", nftName, startNode, hostPort)
	{
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		resp, rpcErr := rpcLookup(ctx, hostPort, target)
		cancel()
		if rpcErr != nil {
			return 0, false, fmt.Errorf("RPC su %s: %w", startNode, rpcErr)
		}
		if resp.GetFound() {
			fmt.Printf("✅ Trovato su nodo %s\n", resp.GetHolder().GetId())
			fmt.Printf("Contenuto JSON:\n%s\n", string(resp.GetValue().GetBytes()))
			return 1, true, nil
		}
		add := buildFromNearest(resp.GetNearest(), selfHex, reverse, target)
		short, _ = mergeIntoShort(short, add, K, idsInShort)
	}

	prevClosest := getClosest(short)

	// --- Round successivi (α-parallel) ---
	for round = 2; round <= maxRounds; round++ {
		next := takeTopUnqueried(short, queried, alpha)
		if len(next) == 0 {
			fmt.Printf("✖ non trovato dopo %d round (α=%d)\n", round-1, alpha)
			return round - 1, false, nil
		}

		for _, c := range next {
			queried[c.idHex] = true
		}
		names := make([]string, 0, len(next))
		for _, c := range next {
			names = append(names, c.label)
		}
		fmt.Printf("🔎 Hop %d (α=%d): interrogo in parallelo: %s\n", round, alpha, strings.Join(names, ", "))

		type result struct {
			found   bool
			holder  *pb.Node
			value   []byte
			nearest []*pb.Node
			err     error
		}
		out := make(chan result, len(next))

		// lancia α RPC in parallelo
		for _, c := range next {
			go func(c cand) {
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				resp, rpcErr := rpcLookup(ctx, c.addr, target)
				if rpcErr != nil {
					out <- result{err: rpcErr}
					return
				}
				if resp.GetFound() {
					out <- result{found: true, holder: resp.GetHolder(), value: resp.GetValue().GetBytes()}
					return
				}
				out <- result{nearest: resp.GetNearest()}
			}(c)
		}

		addedAny := 0
		for i := 0; i < len(next); i++ {
			r := <-out
			if r.err != nil {
				fmt.Printf("   ⚠️ errore batch: %v\n", r.err)
				continue
			}
			if r.found {
				fmt.Printf("✅ Trovato (via batch) su nodo %s\n", r.holder.GetId())
				fmt.Printf("Contenuto JSON:\n%s\n", string(r.value))
				return round, true, nil
			}
			add := buildFromNearest(r.nearest, selfHex, reverse, target)
			short, _ = mergeIntoShort(short, add, K, idsInShort)
			addedAny += len(add)
		}

		// stop condizione: nessun miglioramento del più vicino
		newClosest := getClosest(short)
		progress := (prevClosest == nil) || (newClosest != nil && bytes.Compare(newClosest, prevClosest) < 0)
		prevClosest = newClosest

		if !progress && addedAny == 0 {
			future := takeTopUnqueried(short, queried, alpha)
			if len(future) == 0 {
				fmt.Println("ℹ️ Nessun progresso e nessun non-interrogato — stop.")
				return round, false, nil
			}
		}
	}

	fmt.Printf("⛔ limite di round (%d) raggiunto, non trovato.\n", maxRounds)
	return maxRounds, false, nil
}
