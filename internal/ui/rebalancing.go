package ui

import (
	"context"
	pb "kademlia-nft/proto/kad"

	"fmt"
	"kademlia-nft/logica"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func NFTBelongsHere(nodo string, assigned []logica.NodePick) bool {
	for _, a := range assigned {
		if a.Key == nodo { // Key contiene il nome del nodo, es: "node4"
			return true
		}
	}
	return false
}

func RebalanceNode(targetAddr string, targetID string, activeNodes []string, k int) error {

	// targetAddr :indirizzo del nodo da ribilanciare
	// targetID :ID del nodo da ribilanciare (es: "node6")
	// activeNodes :lista di nodi attivi (es: ["node1:8000", "node3:8000", ...])
	// k :numero di repliche per NFT (es: 2)
	// Recupera la lista dei nodi attivi

	dctx, dcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer dcancel()

	conn, err := grpc.DialContext(
		dctx,
		targetAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return fmt.Errorf("DIAL FALLITA verso %s: %w", targetAddr, err)
	}
	defer conn.Close()

	client := pb.NewKademliaClient(conn)

	var pbNodes []*pb.Node
	for _, n := range activeNodes {
		host, portStr, _ := strings.Cut(n, ":") // es: "node6:8000" → host="node6"
		port, _ := strconv.Atoi(portStr)

		pbNodes = append(pbNodes, &pb.Node{
			Id:   host, // <-- USA l’ID “umano” (nodeX)
			Host: host,
			Port: int32(port),
		})
	}

	//fmt.Printf("%+v\n", pbNodes)
	//fmt.Printf("%+v\n", targetID)

	req := &pb.RebalanceReq{
		TargetId: targetID, //  nodo target
		Nodes:    pbNodes,  // lista di nodi attivi
		K:        int32(k), // numero repliche
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	resp, err := client.Rebalance(ctx, req)
	if err != nil {
		return fmt.Errorf("errore chiamata Rebalance: %v", err)
	}

	fmt.Printf("✅ Rebalance completato per %s\n", targetID)
	fmt.Printf("   - NFT tenuti: %d\n", resp.Kept)
	fmt.Printf("   - NFT spostati: %d\n", resp.Moved)
	fmt.Println("   - Messaggio:", resp.Message)

	return nil
}
