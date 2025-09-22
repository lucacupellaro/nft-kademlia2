package logica

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	pb "kademlia-nft/proto/kad"
)

type NFT struct {
	Index             string
	Name              string
	Volume            string
	Volume_USD        string
	Market_Cap        string
	Market_Cap_USD    string
	Sales             string
	Floor_Price       string
	Floor_Price_USD   string
	Average_Price     string
	Average_Price_USD string
	Owners            string
	Assets            string
	Owner_Asset_Ratio string
	Category          string
	Website           string
	Logo              string

	TokenID            []byte
	AssignedNodesToken []string
}

func ReadCsv2(path string) [][]string {

	//fmt.Printf("file: %s\n", path)

	file, err := os.Open(path)
	if err != nil {
		fmt.Printf("Errore nell'aprire il file CSV: %s\n", err)
		log.Fatal(err)
	}

	defer file.Close()

	reader := csv.NewReader(file)

	records, err := reader.ReadAll()
	if err != nil {
		fmt.Printf("Errore nella lettura del file CSV: %s\n", err)
		log.Fatal(err)
	}

	fmt.Println("CSV file read successfully")
	return records
}

func NewIDFromToken(tokenID string, size int) []byte {
	b := []byte(tokenID)
	if len(b) > size {
		out := make([]byte, size)
		copy(out, b[:size])
		return out
	}
	padded := make([]byte, size)
	copy(padded, b)
	return padded
}

func Sha1ID(tokenID string) []byte {
	s := sha1.Sum([]byte(tokenID)) // [20]byte
	b := make([]byte, sha1.Size)
	copy(b, s[:])
	return b // esattamente 20 byte
}

func DecodeID(b []byte) string {
	return string(bytes.TrimRight(b, "\x00"))
}

func XOR(a, b []byte) ([]byte, error) {
	if a == nil || b == nil {
		return nil, fmt.Errorf("nil input")
	}
	if len(a) != len(b) {
		return nil, fmt.Errorf("length mismatch: %d vs %d", len(a), len(b))
	}
	out := make([]byte, len(a))
	for i := range a {
		out[i] = a[i] ^ b[i]
	}
	return out, nil
}

// confronto lessicografico: true se a < b
func LessThan(a, b []byte) bool {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		if a[i] < b[i] {
			return true
		}
		if a[i] > b[i] {
			return false
		}
	}
	// se prefissi uguali, quello più corto è “minore”
	return len(a) < len(b)
}

// Generate a list of IDs from a list of tokens or Nodes
func GenerateBytesOfAllNfts(list []string) [][]byte {
	ids := make([][]byte, len(list))
	for i, s := range list {
		ids[i] = NewIDFromToken(s, 20) // 20 bytes = 160 bit

	}
	return ids
}

func GenerateBytesOfAllNftsSHA1(list []string) [][]byte {
	ids := make([][]byte, len(list))
	for i, s := range list {
		sum := sha1.Sum([]byte(s)) // [20]byte
		b := make([]byte, 20)
		copy(b, sum[:])
		ids[i] = b
	}
	return ids
}

// Mapping tra stringhe (es. nomi/addr) e ID a 20 byte (SHA-1)
type ByteMapping struct {
	List  []string          // lista pulita (trim, dedup, ordine preservato)
	IDs   [][]byte          // ID corrispondenti (len==len(List)), 20 byte ciascuno
	ByKey map[string][]byte // lookup: key -> ID (20 byte)
	ByHex map[string]string // lookup: hex(ID) -> key (utile per log/JSON)
}

// BuildByteMappingSHA1: crea il mapping pulendo spazi ed eliminando duplicati.
func BuildByteMappingSHA1(input []string) *ByteMapping {
	seen := make(map[string]struct{}, len(input))
	out := &ByteMapping{
		List:  make([]string, 0, len(input)),
		IDs:   make([][]byte, 0, len(input)),
		ByKey: make(map[string][]byte, len(input)),
		ByHex: make(map[string]string, len(input)),
	}

	for _, raw := range input {
		key := strings.TrimSpace(raw)
		if key == "" {
			continue
		}
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}

		sum := sha1.Sum([]byte(key)) // [20]byte
		id := make([]byte, 20)
		copy(id, sum[:])

		out.List = append(out.List, key)
		out.IDs = append(out.IDs, id)
		out.ByKey[key] = id
		out.ByHex[hex.EncodeToString(id)] = key
	}
	return out
}

type NodePick struct {
	Key    string // es. "nodo1" o "node3:8000"
	SHA    []byte // 20 byte
	SHAHex string // esadecimale (40 char)
}

// key = SHA-1 (20B) dell'NFT, dir = rubrica nodi (key->SHA-1), k = quanti nodi vuoi
func ClosestNodesForNFTWithDir(key []byte, dir *ByteMapping, k int) []NodePick {
	if dir == nil || len(key) == 0 || k <= 0 || len(dir.List) == 0 {
		return nil
	}
	L := len(key)

	type cand struct {
		idx  int
		dist []byte
	}
	cands := make([]cand, 0, len(dir.List))

	// prepara distanze XOR
	for i, name := range dir.List {
		id := dir.ByKey[name]
		if id == nil || len(id) != L {
			continue
		}
		if bytes.Equal(id, key) { // skip self
			continue
		}
		d := make([]byte, L)
		for j := 0; j < L; j++ {
			d[j] = key[j] ^ id[j]
		}
		cands = append(cands, cand{idx: i, dist: d})
	}
	if len(cands) == 0 {
		return nil
	}

	// ordina per distanza XOR crescente (+ tie-break su ID grezzo)
	sort.Slice(cands, func(i, j int) bool {
		if c := bytes.Compare(cands[i].dist, cands[j].dist); c != 0 {
			return c < 0
		}
		return bytes.Compare(dir.IDs[cands[i].idx], dir.IDs[cands[j].idx]) < 0
	})

	if k > len(cands) {
		k = len(cands)
	}

	// costruisci output
	out := make([]NodePick, k)
	for n := 0; n < k; n++ {
		i := cands[n].idx
		id := dir.IDs[i]
		idCopy := make([]byte, len(id))
		copy(idCopy, id)
		out[n] = NodePick{
			Key:    dir.List[i],
			SHA:    idCopy,
			SHAHex: hex.EncodeToString(idCopy),
		}
	}
	return out
}

func AssignNFTToNodes(key []byte, nodes [][]byte, k int) [][]byte {
	if key == nil || len(key) == 0 || len(nodes) == 0 || k <= 0 {
		return nil
	}
	L := len(key)

	// 1) Dedup + filtri (len==L, !=self), copia difensiva
	uniq := make([][]byte, 0, len(nodes))
	seen := make(map[string]struct{}, len(nodes))
	self := string(key)

	for _, nid := range nodes {
		if nid == nil || len(nid) != L {
			continue
		}
		s := string(nid) // ok con byte arbitrari
		if s == self {
			continue // niente self
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		buf := make([]byte, L)
		copy(buf, nid)
		uniq = append(uniq, buf)
	}
	if len(uniq) == 0 {
		return nil
	}
	if k > len(uniq) {
		k = len(uniq)
	}

	// 2) Prepara (id, dist=key XOR id)
	type pair struct {
		id   []byte
		dist []byte
	}
	pairs := make([]pair, len(uniq))
	for i, id := range uniq {
		d := make([]byte, L)
		for j := 0; j < L; j++ {
			d[j] = key[j] ^ id[j]
		}
		pairs[i] = pair{id: id, dist: d}
	}

	// 3) Ordina per distanza XOR, tie-break su ID
	sort.Slice(pairs, func(i, j int) bool {
		if c := bytes.Compare(pairs[i].dist, pairs[j].dist); c != 0 {
			return c < 0
		}
		return bytes.Compare(pairs[i].id, pairs[j].id) < 0
	})

	// 4) Primi k
	out := make([][]byte, k)
	for i := 0; i < k; i++ {
		out[i] = pairs[i].id
	}
	return out
}

/*
// restituisce i k nodeID più vicini alla chiave (distanza XOR, ordinata crescente)
func AssignNFTToNodes(key []byte, nodes [][]byte, k int) [][]byte {
	if k <= 0 || len(nodes) == 0 {
		return nil
	}
	if k > len(nodes) {
		k = len(nodes)
	}

	type pair struct {
		id   []byte
		dist []byte
	}
	pairs := make([]pair, len(nodes))
	for i, nid := range nodes {
		pairs[i] = pair{id: nid, dist: XOR(key, nid)}
	}

	sort.Slice(pairs, func(i, j int) bool {
		return LessThan(pairs[i].dist, pairs[j].dist)
	})

	out := make([][]byte, k)
	for i := 0; i < k; i++ {
		out[i] = pairs[i].id
	}
	return out
}
*/
// StoreNFTToNodes invia lo stesso NFT a tutti i nodi indicati.
// Ritorna nil se TUTTE le store vanno a buon fine; altrimenti un error descrittivo.
func StoreNFTToNodes(nft NFT, tokenID []byte, name string, nodes []string, ttlSecs int32) error {
	if len(tokenID) == 0 {
		return errors.New("tokenID vuoto")
	}

	//tokenIDStr := fmt.Sprintf("%x.json", tokenID)
	tokenIDStr := hex.EncodeToString(tokenID)

	payload, err := json.Marshal(struct {
		TokenID string `json:"token_id"`
		Name    string `json:"name"`

		Index string `json:"index,omitempty"`

		Volume            string `json:"volume,omitempty"`
		Volume_USD        string `json:"volume_usd,omitempty"`
		Market_Cap        string `json:"market_cap,omitempty"`
		Market_Cap_USD    string `json:"market_cap_usd,omitempty"`
		Sales             string `json:"sales,omitempty"`
		Floor_Price       string `json:"floor_price,omitempty"`
		Floor_Price_USD   string `json:"floor_price_usd,omitempty"`
		Average_Price     string `json:"average_price,omitempty"`
		Average_Price_USD string `json:"average_price_usd,omitempty"`
		Owners            string `json:"owners,omitempty"`
		Assets            string `json:"assets,omitempty"`
		Owner_Asset_Ratio string `json:"owner_asset_ratio,omitempty"`
		Category          string `json:"category,omitempty"`
		Website           string `json:"website,omitempty"`
		Logo              string `json:"logo,omitempty"`
	}{
		TokenID:           tokenIDStr,
		Name:              name,
		Index:             nft.Index,
		Volume:            nft.Volume,
		Volume_USD:        nft.Volume_USD,
		Market_Cap:        nft.Market_Cap,
		Market_Cap_USD:    nft.Market_Cap_USD,
		Sales:             nft.Sales,
		Floor_Price:       nft.Floor_Price,
		Floor_Price_USD:   nft.Floor_Price_USD,
		Average_Price:     nft.Average_Price,
		Average_Price_USD: nft.Average_Price_USD,
		Owners:            nft.Owners,
		Assets:            nft.Assets,
		Owner_Asset_Ratio: nft.Owner_Asset_Ratio,
		Category:          nft.Category,
		Website:           nft.Website,
		Logo:              nft.Logo,
	})
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	// Dedup & normalizzazione address
	seen := make(map[string]struct{}, len(nodes))
	addrs := make([]string, 0, len(nodes))
	//fmt.Printf("Nodes da inviare: %d\n", len(nodes))
	for _, h := range nodes {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}
		// se manca la porta, usa 8000
		if _, _, err := net.SplitHostPort(h); err != nil {
			h = net.JoinHostPort(h, "8000")
		}
		if _, ok := seen[h]; ok {
			continue
		}
		seen[h] = struct{}{}
		addrs = append(addrs, h)
	}
	if len(addrs) == 0 {
		return errors.New("nessun nodo valido")
	}
	//fmt.Printf("n indirizzi: %d\n", len(addrs))
	var errs []string
	for _, addr := range addrs {
		conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			errs = append(errs, fmt.Sprintf("dial %s: %v", addr, err))
			continue
		}

		client := pb.NewKademliaClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		_, callErr := client.Store(ctx, &pb.StoreReq{
			From:    &pb.Node{Id: "seeder", Host: "seeder", Port: 8000},
			Key:     &pb.Key{Key: tokenID},        // *** bytes RAW (20B), niente ascii-hex ***
			Value:   &pb.NFTValue{Bytes: payload}, // unico file JSON lato server
			TtlSecs: ttlSecs,
		})
		cancel()
		_ = conn.Close()

		if callErr != nil {
			errs = append(errs, fmt.Sprintf("Store(%s): %v", addr, callErr))
			continue
		}
		//fmt.Printf("✅ NFT %s inviato a %s\n", hex.EncodeToString(tokenID), addr)
	}

	if len(errs) > 0 {
		return fmt.Errorf("alcune Store sono fallite: %s", strings.Join(errs, "; "))
	}
	return nil
}

// Se CLI su host: localhost:8000+n ; se CLI in Docker: nodeN:8000
func ResolveAddrForNode(nodeName string) (string, error) {
	name := strings.ToLower(strings.TrimSpace(nodeName))
	if strings.HasPrefix(name, "nodo") {
		name = "node" + name[len("nodo"):]
	}
	var n int
	if _, err := fmt.Sscanf(name, "node%d", &n); err != nil || n < 1 || n > 11 {
		return "", fmt.Errorf("nome nodo non valido: %q", nodeName)
	}
	if os.Getenv("CLI_IN_DOCKER") == "1" {
		return fmt.Sprintf("node%d:%d", n, 8000), nil
	}
	return fmt.Sprintf("localhost:%d", 8000+n), nil
}

// ===== Server RPC =====

func RunGRPCServer() error {
	lis, err := net.Listen("tcp", ":8000")
	if err != nil {
		return err
	}
	gs := grpc.NewServer()
	pb.RegisterKademliaServer(gs, &KademliaServer{})
	log.Println("gRPC server in ascolto su :8000")
	return gs.Serve(lis) // BLOCCA
}

func WaitReady(host string, timeout time.Duration) error {
	addr := fmt.Sprintf("%s:8000", host)
	deadline := time.Now().Add(timeout)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		_, err := grpc.DialContext(
			ctx, addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
			grpc.WithReturnConnectionError(),
		)
		cancel()
		if err == nil {
			return nil // è raggiungibile
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout aspettando %s", addr)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// Su HOST: elenca i file in ./data/<nodeID>
// (se la esegui DENTRO al container, usa base := "/data" e ignora nodeID)
func ListNodeVolumeFiles(nodeID string) ([]string, error) {
	base := "./data" // nel container metti "/data"
	dir := filepath.Join(base, nodeID)

	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var files []string
	for _, e := range entries {
		if !e.IsDir() {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)
	return files, nil
}

func HexFileNameFromName(nameBytes []byte) string {
	// pad/truncate a 20 byte e poi hex
	fixed := make([]byte, 20)
	copy(fixed, nameBytes) // se nameBytes >20 viene troncato, se <20 viene padded con 0x00
	return fmt.Sprintf("%x.json", fixed)
}

// helpers
func isHex(s string) bool {
	if len(s)%2 == 1 {
		return false
	}
	for _, c := range s {
		switch {
		case '0' <= c && c <= '9',
			'a' <= c && c <= 'f',
			'A' <= c && c <= 'F':
			continue
		default:
			return false
		}
	}
	return true
}

func RemoveNode1(nodi *[]string) {

	out := (*nodi)[:0]
	for _, s := range *nodi {
		if s != "node1" {
			out = append(out, s)
		}
	}
	*nodi = out
}
