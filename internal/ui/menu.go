package ui

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"

	pb "kademlia-nft/proto/kad"
	"path/filepath"

	"fmt"
	"kademlia-nft/common"
	"math/big"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"

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

// Restituisce i servizi Compose attivi (node1, node2, ...) del progetto.
func ListActiveComposeServices(project string) ([]string, error) {

	cmd := exec.Command("docker", "ps",
		"--filter", "label=com.docker.compose.project="+project,
		"--format", "{{.Names}}",
	)
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		return nil, err
	}

	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	services := make([]string, 0, len(lines))
	for _, name := range lines {
		name = strings.TrimSpace(name)
		if name == "" {
			continue
		}
		// Nome Compose tipico: <project>-<service>-<index>
		parts := strings.Split(name, "-")
		if len(parts) >= 3 {
			services = append(services, parts[len(parts)-2]) // prende <service> (es. "node1")
		}
	}
	return dedupe(services), nil
}

func dedupe(in []string) []string {
	seen := map[string]bool{}
	out := make([]string, 0, len(in))
	for _, s := range in {
		if !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

func resolveStartHostPort(name string) (string, error) {
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

type Pair struct {
	esa  string
	hash string
}

/*
func check(value string, list []Pair) string {

	val := strings.ToLower(strings.TrimSpace(value))
	for _, v := range list {
		if strings.ToLower(strings.TrimSpace(v.esa)) == val {
			decode, err := hexToString(v.esa)
			if err == nil {
				return decode
			}
		}
	}
	return "NOTFOUND"

}
*/

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

func Reverse2(nodes []string) ([]Pair, error) {
	//recupero dal file nodi e hash

	out := make([]Pair, 0, len(nodes))
	for _, n := range nodes {

		idHex := hex.EncodeToString(common.Sha1ID(n))

		out = append(out, Pair{esa: idHex, hash: n})
	}
	return out, nil
}

// sceltaNodoPiuVicino: XOR distance minima tra nftID20 e ogni nodo (ID a 20 byte).
func sceltaNodoPiuVicino(nftID20 []byte, nodiVicini []string) (string, error) {
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

	add, err := resolveStartHostPort(nodeAddr)
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

func xorDist(a20 []byte, b20 []byte) *big.Int {
	nb := make([]byte, len(a20))
	for i := range a20 {
		nb[i] = a20[i] ^ b20[i]
	}
	return new(big.Int).SetBytes(nb)
}

/*
	func AddNode(ctx context.Context, nodeName, seederAddr, hostPort string) error {
		cli, err := client.NewClientWithOpts(client.FromEnv)
		if err != nil {
			return err
		}
		defer cli.Close()

		// percorso assoluto della cartella locale
		hostDataPath, err := filepath.Abs("./data/" + nodeName)
		if err != nil {
			return err
		}

		// Configurazione del container
		config := &container.Config{
			Image: "kademlia-nft-node", // immagine generica che hai buildato
			Env: []string{
				"NODE_ID=" + nodeName,
				"DATA_DIR=/data",
				"SEEDER_ADDR=" + seederAddr,
			},
			ExposedPorts: nat.PortSet{
				"8000/tcp": struct{}{},
			},
		}

		hostConfig := &container.HostConfig{
			Binds: []string{
				fmt.Sprintf("%s:/data", hostDataPath), // bind mount con path assoluto
			},
			PortBindings: nat.PortMap{
				"8000/tcp": []nat.PortBinding{
					{HostIP: "0.0.0.0", HostPort: hostPort},
				},
			},
		}

		networkingConfig := &network.NetworkingConfig{
			EndpointsConfig: map[string]*network.EndpointSettings{
				"kademlia-nft_kadnet": {},
			},
		}

		// Crea il container
		resp, err := cli.ContainerCreate(ctx, config, hostConfig, networkingConfig, nil, nodeName)
		if err != nil {
			return err
		}

		// Avvia il container
		if err := cli.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{}); err != nil {
			return err
		}

		fmt.Printf("✅ Nodo %s avviato sulla porta %s\n", nodeName, hostPort)
		return nil
	}
*/
func AddNode(ctx context.Context, nodeName, seederAddr, hostPort string) error {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return err
	}
	defer cli.Close()

	// Path host per il bind mount: deve esistere
	hostDataPath, err := filepath.Abs(filepath.Join("./data", nodeName))
	if err != nil {
		return err
	}
	if err := os.MkdirAll(hostDataPath, 0o755); err != nil {
		return err
	}

	// Verifica che la network di Compose esista
	if _, err := cli.NetworkInspect(ctx, "kademlia-nft_kadnet", types.NetworkInspectOptions{}); err != nil {
		return fmt.Errorf("rete 'kademlia-nft_kadnet' non trovata: %w", err)
	}

	port := nat.Port("8000/tcp")

	config := &container.Config{
		Image: "kademlia-nft-node:latest",
		Env: []string{
			"NODE_ID=" + nodeName,
			"DATA_DIR=/data",
			"SEEDER_ADDR=" + seederAddr,
		},
		ExposedPorts: nat.PortSet{port: struct{}{}},
		Labels: map[string]string{
			"com.docker.compose.project": "kademlia-nft",
			"com.docker.compose.service": nodeName, // es. "node12"
			"com.docker.compose.version": "2",
		},
	}

	hostConfig := &container.HostConfig{
		Binds: []string{fmt.Sprintf("%s:/data", hostDataPath)},
		PortBindings: nat.PortMap{
			port: []nat.PortBinding{{HostIP: "0.0.0.0", HostPort: hostPort}},
		},
		RestartPolicy: container.RestartPolicy{Name: "unless-stopped"},
	}

	networkingConfig := &network.NetworkingConfig{
		EndpointsConfig: map[string]*network.EndpointSettings{
			"kademlia-nft_kadnet": {
				Aliases: []string{nodeName}, // es. "node12"
			},
		},
	}

	// Nome stile Compose: kademlia-nft-node12-1
	name := fmt.Sprintf("kademlia-nft-%s-1", nodeName)

	resp, err := cli.ContainerCreate(ctx, config, hostConfig, networkingConfig, nil, name)
	if err != nil {
		return err
	}

	// *** AVVIA il container ***
	if err := cli.ContainerStart(ctx, resp.ID, types.ContainerStartOptions{}); err != nil {
		return err
	}

	fmt.Printf("✅ Nodo %s (%s) avviato sulla porta %s\n", nodeName, resp.ID[:12], hostPort)
	return nil
}

func BiggerNodes(nodi []string) (string, int) {
	var maxID = -1

	for _, n := range nodi {
		if strings.HasPrefix(n, "node") {
			idStr := strings.TrimPrefix(n, "node")
			id, err := strconv.Atoi(idStr)
			if err != nil {
				continue
			}

			if id > maxID {
				maxID = id
			}
		}
	}

	// ritorna il nuovo nodo con ID incrementato
	return "node" + strconv.Itoa(maxID+1), maxID + 1
}
