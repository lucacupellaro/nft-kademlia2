package test

import (
	"context"
	"encoding/hex"
	"fmt"
	"kademlia-nft/common"
	"kademlia-nft/internal/ui"
	pb "kademlia-nft/proto/kad"
	"log"
	"math"
	"math/rand"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// test concorrente con 5 client
func ConcurrentLookupTest(nodes []string, reverse []ui.Pair, nftName string, maxHops int) {
	fmt.Printf("\n🚀 Avvio test concorrente con 5 client sullo stesso nodo\n")
	if len(nodes) == 0 {
		log.Println("Nessun nodo disponibile")
		return
	}

	// scegli nodo partenza (diverso da node1 se possibile)
	start := RandomNode(nodes)
	fmt.Printf("Userò il nodo %s come punto di partenza\n", start)

	var wg sync.WaitGroup
	wg.Add(5)

	for i := 1; i <= 5; i++ {
		go func(id int) {
			defer wg.Done()
			hops, found, err := LookupNFTOnNodeByNameStats(start, reverse, nftName, maxHops)
			if err != nil {
				fmt.Printf("❌ Client %d errore: %v\n", id, err)
				return
			}
			if found {
				fmt.Printf("✅ Client %d trovato in %d hop\n", id, hops)
			} else {
				fmt.Printf("⚠️ Client %d non ha trovato l’NFT\n", id)
			}
		}(i)
	}

	wg.Wait()
	fmt.Println("🏁 Test concorrente completato")
}

func RandomNode(nodes []string) string {
	choices := []string{}
	for _, n := range nodes {
		if !strings.EqualFold(strings.TrimSpace(n), "node1") {
			choices = append(choices, n)
		}
	}
	if len(choices) == 0 {
		return nodes[0]
	}
	return choices[rand.Intn(len(choices))]
}

// ======================================================================
// Lookup “greedy” (la tua), invariata: ritorna hop e found
// ======================================================================
func LookupNFTOnNodeByNameStats(startNode string, str []ui.Pair, nftName string, maxHops int) (hops int, found bool, err error) {
	if maxHops <= 0 {
		maxHops = 15
	}

	nftID20 := common.Sha1ID(nftName)

	visitedIDs := make(map[string]bool)
	visitedNames := make(map[string]bool)

	startNameLC := strings.ToLower(strings.TrimSpace(startNode))
	startIDHex := strings.ToLower(hex.EncodeToString(common.Sha1ID(startNode)))
	visitedNames[startNameLC] = true
	visitedIDs[startIDHex] = true

	hostPort, err := ui.ResolveStartHostPort(startNode)
	if err != nil {
		return 0, false, fmt.Errorf("risoluzione %q fallita: %w", startNode, err)
	}
	currentLabel := startNode

	for hop := 0; hop < maxHops; hop++ {
		hops = hop + 1
		fmt.Printf("🔎 Hop %d: cerco '%s' con id %x (%s)\n", hops, nftName, nftID20, currentLabel)

		conn, err := grpc.Dial(hostPort, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return hops, false, fmt.Errorf("dial fallito %s: %w", hostPort, err)
		}
		client := pb.NewKademliaClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		resp, rpcErr := client.LookupNFT(ctx, &pb.LookupNFTReq{
			FromId: "CLI",
			Key:    &pb.Key{Key: nftID20},
		})
		cancel()
		_ = conn.Close()
		if rpcErr != nil {
			return hops, false, fmt.Errorf("RPC fallita su %s: %w", currentLabel, rpcErr)
		}

		if resp.GetFound() {
			return hops, true, nil
		}

		nearest := resp.GetNearest()
		if len(nearest) == 0 {
			return hops, false, nil
		}

		type cand struct {
			idHex string
			addr  string
			name  string
			label string
		}
		usabili := make([]cand, 0, len(nearest))

		for _, n := range nearest {
			id := strings.ToLower(strings.TrimSpace(n.GetId()))
			if id == "" {
				continue
			}
			if visitedIDs[id] {
				continue
			}

			var addr, label, name string
			host := strings.TrimSpace(n.GetHost())
			port := int(n.GetPort())
			if host != "" && port > 0 {
				addr = fmt.Sprintf("%s:%d", host, port)
				label = host
			} else {
				if nm := ui.Check(id, str); nm != "NOTFOUND" {
					name = nm
					if hp, e := ui.ResolveStartHostPort(nm); e == nil {
						addr = hp
						label = nm + " (fallback)"
					}
				}
			}

			if name != "" && visitedNames[strings.ToLower(name)] {
				continue
			}
			if addr == "" {
				continue
			}

			usabili = append(usabili, cand{
				idHex: id,
				addr:  addr,
				name:  name,
				label: label,
			})
		}

		if len(usabili) == 0 {
			return hops, false, nil
		}

		ids := make([]string, len(usabili))
		for i, c := range usabili {
			ids[i] = c.idHex
		}

		bestID, selErr := ui.SceltaNodoPiuVicino(nftID20, ids)
		if selErr != nil {
			bestID = ids[0]
		}
		bestID = strings.ToLower(strings.TrimSpace(bestID))

		var next cand
		for _, c := range usabili {
			if c.idHex == bestID {
				next = c
				break
			}
		}
		if next.addr == "" {
			if nm := ui.Check(bestID, str); nm != "NOTFOUND" {
				if hp, e := ui.ResolveStartHostPort(nm); e == nil {
					next.addr = hp
					next.name = nm
					next.label = nm + " (fallback2)"
				}
			}
		}
		if next.addr == "" {
			return hops, false, nil
		}

		visitedIDs[bestID] = true
		if next.name == "" {
			if nm := ui.Check(bestID, str); nm != "NOTFOUND" {
				next.name = nm
			}
		}
		if next.name != "" {
			visitedNames[strings.ToLower(next.name)] = true
		}

		hostPort = next.addr
		if next.label != "" {
			currentLabel = next.label
		} else if next.name != "" {
			currentLabel = next.name
		} else {
			if len(bestID) > 8 {
				currentLabel = bestID[:8]
			} else {
				currentLabel = bestID
			}
		}
	}

	return maxHops, false, nil
}

func MeanStd(data []int) (float64, float64) {
	n := len(data)
	if n == 0 {
		return 0, 0
	}
	var sum float64
	for _, v := range data {
		sum += float64(v)
	}
	mean := sum / float64(n)

	var sq float64
	for _, v := range data {
		d := float64(v) - mean
		sq += d * d
	}
	std := math.Sqrt(sq / float64(n))
	return mean, std
}
