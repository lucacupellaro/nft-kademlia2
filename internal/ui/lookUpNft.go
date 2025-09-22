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

// Pair: esa = SHA1 hex dell'ID, hash = ID/alias del nodo (es. "node7")
func check(value string, list []Pair) string {
	val := strings.ToLower(strings.TrimSpace(value))
	for _, v := range list {
		if strings.ToLower(strings.TrimSpace(v.esa)) == val {
			// RITORNA l'ID del nodo, non decodificare l'hex!
			return strings.TrimSpace(v.hash)
		}
	}
	return "NOTFOUND"
}
