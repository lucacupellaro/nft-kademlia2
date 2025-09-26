package ui

import (
	"context"
	"fmt"
	"kademlia-nft/common"

	pb "kademlia-nft/proto/kad"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

/*
func LookupNFTOnNodeByName(startNode string, str []Pair, nftName string, maxHops int) error {
	if maxHops <= 0 {
		maxHops = 15
	}

	nftID20 := common.Sha1ID(nftName)
	visited := make(map[string]bool)
	current := startNode

	for hop := 0; hop < maxHops; hop++ {
		if visited[current] {
			// già visto: non ha senso riprovarlo
			break
		}
		visited[current] = true

		hostPort, err := resolveStartHostPort(current)
		if err != nil {
			return fmt.Errorf("risoluzione %q fallita: %w", current, err)
		}

		fmt.Printf("🔎 Hop %d: cerco '%s' su %s (%s)\n", hop+1, nftName, current, hostPort)

		conn, err := grpc.Dial(hostPort, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("dial fallito %s: %w", hostPort, err)
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
			return fmt.Errorf("RPC fallita su %s: %w", current, rpcErr)
		}

		if resp.GetFound() {
			fmt.Printf("✅ Trovato su nodo %s\n", resp.GetHolder().GetId())
			fmt.Printf("Contenuto JSON:\n%s\n", string(resp.GetValue().GetBytes()))
			return nil
		}

		nearest := resp.GetNearest()
		if len(nearest) == 0 {
			fmt.Println("✖ NFT non trovato e nessun nodo vicino restituito — arresto.")
			return nil
		}

		// Estrai gli ID utili e filtra già i visitati
		candidates := make([]string, 0, len(nearest))
		fmt.Println("… nodi vicini suggeriti:")
		for _, n := range nearest {
			id := n.GetId()
			if id == "" {
				id = n.GetHost()
			}
			if id == "" {
				continue
			}
			fmt.Printf("   - %s (%s:%d)\n", id, n.GetHost(), n.GetPort())
			if !visited[id] {
				candidates = append(candidates, id)
			}
		}

		if len(candidates) == 0 {
			fmt.Println("✖ Nessun vicino non visitato disponibile — arresto.")
			return nil
		}

		fmt.Printf("candidati: %s,", candidates)
		best, err := sceltaNodoPiuVicino(nftID20, candidates)

		strBest := check(best, str)

		if err != nil {
			fmt.Printf("⚠️  Impossibile scegliere il nodo più vicino: %v — prendo il primo candidato.\n", err)
			best = candidates[0]
		}

		fmt.Printf("➡️  Prossimo nodo scelto: %s\n", strBest)
		current = strBest
	}

	fmt.Printf("⛔ Max hop (%d) raggiunto senza trovare '%s'.\n", maxHops, nftName)
	return nil
}
*/

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

// --------------------------------------------------
func LookupNFTOnNodeByName(startNode string, str []Pair, nftName string, maxHops int) error {
	if maxHops <= 0 {
		maxHops = 15
	}

	nftID20 := common.Sha1ID(nftName) // []byte(20)
	visitedIDs := make(map[string]bool)

	// Hop 0: risolvi SOLO lo startNode
	hostPort, err := ResolveStartHostPort(startNode)
	if err != nil {
		return fmt.Errorf("risoluzione %q fallita: %w", startNode, err)
	}
	currentLabel := startNode // per log

	for hop := 0; hop < maxHops; hop++ {
		fmt.Printf("🔎 Hop %d: cerco '%s' su %s (%s)\n", hop+1, nftName, currentLabel, hostPort)

		conn, err := grpc.Dial(hostPort, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("dial fallito %s: %w", hostPort, err)
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
			return fmt.Errorf("RPC fallita su %s: %w", currentLabel, rpcErr)
		}

		if resp.GetFound() {
			fmt.Printf("✅ Trovato su nodo %s\n", resp.GetHolder().GetId())
			fmt.Printf("Contenuto JSON:\n%s\n", string(resp.GetValue().GetBytes()))
			return nil
		}

		nearest := resp.GetNearest()
		if len(nearest) == 0 {
			fmt.Println("✖ NFT non trovato e nessun nodo vicino restituito — arresto.")
			return nil
		}

		// Prepara candidati usabili: (id, addr, label) con fallback via check()
		type cand struct {
			id    string
			addr  string // host:port
			label string // per log: host o "nodeX" o id short
		}
		usabili := make([]cand, 0, len(nearest))

		fmt.Println("… nodi vicini suggeriti:")
		for _, n := range nearest {
			id := strings.TrimSpace(n.GetId())
			if id == "" {
				continue // senza ID non posso fare XOR
			}
			lcID := strings.ToLower(id)

			var addr, label string
			host := strings.TrimSpace(n.GetHost())
			port := int(n.GetPort())
			if host != "" && port > 0 {
				addr = fmt.Sprintf("%s:%d", host, port)
				label = host
			} else {
				// Fallback: mappa ID -> "nodeX" usando la tua tabella 'str'
				name := Check(id, str)
				if name != "NOTFOUND" {
					if hp, e := ResolveStartHostPort(name); e == nil {
						addr = hp
						label = name
					}
				}
			}

			if addr == "" {
				fmt.Printf("   - %s (endpoint mancante)\n", id)
				continue
			}
			if visitedIDs[lcID] {
				// già visitato: non lo ripropongo
				fmt.Printf("   - %s (%s) [già visitato]\n", id, addr)
				continue
			}

			if label == "" {
				// etichetta estetica di fallback
				if host != "" {
					label = host
				} else {
					if len(id) >= 8 {
						label = id[:8]
					} else {
						label = id
					}
				}
			}

			fmt.Printf("   - %s (%s)\n", id, addr)
			usabili = append(usabili, cand{id: lcID, addr: addr, label: label})
		}

		if len(usabili) == 0 {
			fmt.Println("✖ Nessun vicino utilizzabile non visitato — arresto.")
			return nil
		}

		// Seleziona il più vicino usando la TUA funzione (input: lista di ID hex)
		ids := make([]string, len(usabili))
		for i, c := range usabili {
			ids[i] = c.id
		}

		fmt.Printf("candidati: %v,", ids)
		bestID, err := SceltaNodoPiuVicino(nftID20, ids)
		if err != nil {
			fmt.Printf("⚠️  Impossibile scegliere il nodo più vicino: %v — prendo il primo candidato.\n", err)
			bestID = ids[0]
		}
		bestID = strings.ToLower(strings.TrimSpace(bestID))

		// Recupera endpoint del bestID
		var nextAddr, nextLabel string
		for _, c := range usabili {
			if c.id == bestID {
				nextAddr = c.addr
				nextLabel = c.label
				break
			}
		}
		if nextAddr == "" {
			// come ulteriore fallback, prova mapping ID->nodeX e risolvi
			if name := Check(bestID, str); name != "NOTFOUND" {
				if hp, e := ResolveStartHostPort(name); e == nil {
					nextAddr = hp
					nextLabel = name
				}
			}
		}
		if nextAddr == "" {
			fmt.Println("✖ Best candidato senza endpoint — arresto.")
			return nil
		}

		// Marca visitato per ID
		visitedIDs[bestID] = true

		// Prepara hop successivo
		hostPort = nextAddr
		if nextLabel != "" {
			currentLabel = nextLabel
		} else {
			currentLabel = bestID[:8]
		}
		fmt.Printf("➡️  Prossimo nodo scelto: %s\n", currentLabel)
	}

	fmt.Printf("⛔ Max hop (%d) raggiunto senza trovare '%s'.\n", maxHops, nftName)
	return nil
}
