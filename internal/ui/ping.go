package ui

import (
	"context"
	"fmt"
	"kademlia-nft/common"
	pb "kademlia-nft/proto/kad"
	"math/big"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

//AGGIORNO IL KBUCKET DEL NODO CHIAMATO

func PingNode(startNode, targetNode string, pairs []Pair) {
	targetID := common.Sha1ID(targetNode)

	hex2id := make(map[string]string, len(pairs))
	for _, p := range pairs {
		h := strings.ToLower(strings.TrimSpace(p.esa))
		id := strings.TrimSpace(p.hash)
		if h != "" && id != "" {
			hex2id[h] = id
		}
	}

	visited := map[string]bool{}
	candidates := map[string]bool{}
	addCand := func(list []string) {
		for _, n := range list {
			n = strings.TrimSpace(n)
			if n == "" {
				continue
			}
			candidates[n] = true // set
		}
	}

	//il nodo di partenza è il primo candidato
	addCand([]string{startNode})

	var bestDist *big.Int
	stagnate := 0
	const maxHops = 15

	for hop := 0; hop < maxHops; hop++ {
		// scegli il candidato non visitato più vicino al target
		var next string
		var nextD *big.Int
		for id := range candidates {
			if visited[id] {
				continue
			}
			d := common.XorDist(targetID, common.Sha1ID(id))
			if next == "" || d.Cmp(nextD) < 0 {
				next, nextD = id, d
			}
		}
		if next == "" {
			fmt.Println("✖ Nessun vicino non visitato — arresto.")
			return
		}

		fmt.Printf("🔍 Inizio PING da %s a %s (hop %d)\n", next, targetNode, hop+1)
		visited[next] = true

		// prendi il KBucket del nodo "next"
		kbRaw, err := RPCGetKBucket(next) // restituisce []string ma attualmente sono HEX token
		if err != nil {
			fmt.Printf("⚠️  GetKBucket(%s) fallita: %v\n", next, err)
			continue
		}
		fmt.Printf("KBucket (raw) di %s: %v\n", next, kbRaw)

		kbIDs := make([]string, 0, len(kbRaw))
		for _, s := range kbRaw {
			t := strings.ToLower(strings.TrimSpace(s))
			if t == "" {
				continue
			}
			if id, ok := hex2id[t]; ok {
				kbIDs = append(kbIDs, id)
				continue
			}
			if looksLikeNodeID(s) {
				kbIDs = append(kbIDs, strings.TrimSpace(s))
			}

		}

		fmt.Printf("🔎 %s ha %d vicini (IDs: %v)\n", next, len(kbIDs), kbIDs)

		// target presente?
		for _, n := range kbIDs {
			if n == targetNode {
				fmt.Printf("✅ %s conosce %s — invio Ping…\n", next, targetNode)
				if err := SendPing(next, targetNode); err != nil {
					fmt.Printf("⚠️  Ping fallito: %v\n", err)
				}
				return
			}
		}

		// accumula nuovi candidati
		addCand(kbIDs)

		// controllo progresso
		if bestDist == nil || nextD.Cmp(bestDist) < 0 {
			bestDist = nextD
			stagnate = 0
		} else {
			stagnate++
			if stagnate >= 2 {
				fmt.Println("⛔ Nessun miglioramento di distanza — arresto.")
				return
			}
		}
	}
	fmt.Println("⛔ Max hop raggiunto senza contattare il target.")
}

func looksLikeNodeID(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	// semplice check: "node" + numero
	if strings.HasPrefix(s, "node") {
		for _, r := range s[4:] {
			if r < '0' || r > '9' {
				return false
			}
		}
		return true
	}
	return false
}

func SendPing(fromID, targetName string) error {

	addr, err := resolveStartHostPort(targetName) // es: "localhost:8004"
	if err != nil {
		return err
	}

	// connessione con timeout e block (meglio feedback chiaro sulle reachability)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithReturnConnectionError(),
	)
	if err != nil {
		return fmt.Errorf("dial %s: %w", addr, err)
	}
	defer conn.Close()

	client := pb.NewKademliaClient(conn)
	resp, err := client.Ping(ctx, &pb.PingReq{
		From: &pb.Node{Id: fromID, Host: fromID, Port: 0}, // meta: Host/Port opzionali
	})
	if err != nil {
		return fmt.Errorf("Ping %s: %w", targetName, err)
	}

	fmt.Printf("PONG da %s (ok=%v, t=%d)\n", resp.GetNodeId(), resp.GetOk(), resp.GetUnixMs())
	// (opzionale) aggiorna la routing table locale di X con Y, perché ha risposto:
	// UpdateBucketLocal(targetName)

	return nil
}
