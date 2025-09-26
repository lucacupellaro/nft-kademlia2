package ui

import (
	"bufio"
	"context"
	"encoding/hex"
	"regexp"
	"strconv"

	pb "kademlia-nft/proto/kad"

	"fmt"
	"kademlia-nft/common"
	"math/big"
	"os"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type MenuChoice int

const (
	MenuListNodes MenuChoice = iota + 1
	MenuShowBucket
	MenuPing
	MenuSearchNFT
	MenuAddNFT
	MenuAddNode
	MenuRebalance
	MenuRemoveNode
	MenuQuit
)

func ShowWelcomeMenu() MenuChoice {
	fmt.Print("\033[2J\033[H")
	fmt.Println(`
╔══════════════════════════════════════════════╗
║        Kademlia NFT – Console Control        ║
╚══════════════════════════════════════════════╝
Benvenuto! Seleziona un'operazione:

  1) Elenca nodi
  2) Ping (X->Y)
  3) Cerca un NFT  
  4) Aggiungi un NFT
  5) Aggiungi un nodo
  6) Rebalancing delle risorse
  7) Rimuovi un nodo
  8) Esci
`)

	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Print("Scegli [1-8]: ") // <-- coerente con 1..8
		line, _ := reader.ReadString('\n')
		line = strings.TrimSpace(line)
		switch line {
		case "1":
			return MenuChoice(1)
		case "2":
			return MenuChoice(2)
		case "3":
			return MenuChoice(3)
		case "4":
			return MenuChoice(4)
		case "5":
			return MenuChoice(5)
		case "6":
			return MenuChoice(6)
		case "7":
			return MenuChoice(7)
		case "8", "q", "Q", "exit", "quit":
			return MenuChoice(8)
		default:
			fmt.Println("Scelta non valida, riprova.")
		}
	}
}

var reNode = regexp.MustCompile(`^node(\d+)$`)

func ResolveStartHostPort(name string) (string, error) {
	name = strings.TrimSpace(strings.ToLower(name))
	if strings.HasPrefix(name, "nodo") { // supporto "nodoX"
		name = "node" + name[len("nodo"):]
	}
	m := reNode.FindStringSubmatch(name)
	if m == nil {
		return "", fmt.Errorf("nome nodo non valido: %q", name)
	}
	n, _ := strconv.Atoi(m[1])

	// Se vuoi, vincola a un massimo N letto da .env; altrimenti niente upper bound:
	if n < 1 {
		return "", fmt.Errorf("indice nodo non valido: %d", n)
	}

	// Fuori da Docker:
	return fmt.Sprintf("localhost:%d", 8000+n), nil
	// Dentro rete docker: return fmt.Sprintf("%s:%d", name, 8000), nil
}

/*
func ResolveStartHostPort(name string) (string, error) {
	name = strings.TrimSpace(strings.ToLower(name))
	// supporta sia "node3" sia "nodo3"
	if strings.HasPrefix(name, "nodo") {
		name = "node" + name[len("nodo"):]
	}
	var n int
	if _, err := fmt.Sscanf(name, "node%d", &n); err != nil || n < 1 || n > 11 {
		return "", fmt.Errorf("nome nodo non valido: %q", name)
	}

	return fmt.Sprintf("localhost:%d", 8000+n), nil
}
*/

type Pair struct {
	esaSha1 string //esAdecimale(SHA1(NODOX))
	name    string //nome del nodo (nodeX )
}

func Reverse2(nodes []string) ([]Pair, error) {

	out := make([]Pair, 0, len(nodes))
	for _, n := range nodes {

		idHex := hex.EncodeToString(common.Sha1ID(n))

		out = append(out, Pair{esaSha1: idHex, name: n})
	}
	return out, nil
}

// sceltaNodoPiuVicino: XOR distance minima tra nftID20 e ogni nodo (ID a 20 byte).
func SceltaNodoPiuVicino(nftID20 []byte, nodiVicini []string) (string, error) {
	var bestNode string
	var bestDist *big.Int

	for _, idStr := range nodiVicini {

		nidBytes := common.Sha1ID(idStr)

		distBytes := make([]byte, len(nftID20))
		for i := range nftID20 {
			distBytes[i] = nftID20[i] ^ nidBytes[i]
		}

		distInt := new(big.Int).SetBytes(distBytes)
		if bestDist == nil || distInt.Cmp(bestDist) < 0 {
			bestDist = distInt
			bestNode = idStr
		}
	}

	if bestNode == "" {
		return "", fmt.Errorf("nessun nodo valido trovato")
	}
	return bestNode, nil
}

func RPCGetKBucket(nodeAddr string) ([]string, error) {

	add, err := ResolveStartHostPort(nodeAddr)
	fmt.Printf("Risolvo %s in %s\n", nodeAddr, add)
	fmt.Printf("🔍 Recupero KBucket di %s\n", add)

	if err != nil {
		return nil, fmt.Errorf("risoluzione %q fallita: %w", nodeAddr, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, add,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return nil, fmt.Errorf("dial %s: %v", add, err)
	}
	defer conn.Close()

	client := pb.NewKademliaClient(conn)
	resp, err := client.GetKBucket(ctx, &pb.GetKBucketReq{RequesterId: "cli"})
	if err != nil {
		return nil, fmt.Errorf("rpc GetKBucket: %v", err)
	}

	// converto []*pb.Node in []string
	var res []string
	for _, n := range resp.Nodes {
		res = append(res, n.Id)
	}

	return res, nil
}
