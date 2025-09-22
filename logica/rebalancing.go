package logica

import (
	"context"
	"encoding/json"
	pb "kademlia-nft/proto/kad"
	"path/filepath"

	"fmt"

	"encoding/hex"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type TempNFT struct {
	TokenID         string `json:"token_id"`
	Name            string `json:"name"`
	Index           string `json:"index"`
	Volume          string `json:"volume"`
	VolumeUSD       string `json:"volume_usd"`
	MarketCap       string `json:"market_cap"`
	MarketCapUSD    string `json:"market_cap_usd"`
	Sales           string `json:"sales"`
	FloorPrice      string `json:"floor_price"`
	FloorPriceUSD   string `json:"floor_price_usd"`
	AveragePrice    string `json:"average_price"`
	AveragePriceUSD string `json:"average_price_usd"`
	Owners          string `json:"owners"`
	Assets          string `json:"assets"`
	OwnerAssetRatio string `json:"owner_asset_ratio"`
	Category        string `json:"category"`
	Website         string `json:"website"`
	Logo            string `json:"logo"`
}

// helper: normalizza host/porta (porta 0 o vuota -> 8000)
func sanitizeHostPort(host string, port int) (string, int) {
	host = strings.TrimSpace(host)
	if host == "" {
		host = "localhost"
	}
	if port == 0 {
		port = 8000
	}
	return host, port
}
func (s *KademliaServer) Rebalance(ctx context.Context, req *pb.RebalanceReq) (*pb.RebalanceRes, error) {
	nodo := strings.TrimSpace(req.GetTargetId())
	k := int(req.GetK())
	if k <= 0 {
		k = 2
	}

	// ---- mappa chiave-stabile -> endpoint ----
	type hostPort struct {
		Host string
		Port int
	}
	peerAddr := make(map[string]hostPort, len(req.GetNodes()))
	nodeKeys := make([]string, 0, len(req.GetNodes()))
	for _, n := range req.GetNodes() {
		key := strings.TrimSpace(n.GetId())
		if key == "" {
			key = strings.TrimSpace(n.GetHost())
		}
		if key == "" {
			continue
		}
		h, p := sanitizeHostPort(n.GetHost(), int(n.GetPort()))
		peerAddr[key] = hostPort{Host: h, Port: p}
		nodeKeys = append(nodeKeys, key)
	}

	// ---- mapping byte su chiavi stabili ----
	dir := BuildByteMappingSHA1(nodeKeys)

	mkAddr := func(key string) string {
		hp, ok := peerAddr[key]
		if !ok || hp.Host == "" || hp.Port == 0 {
			return ""
		}
		return fmt.Sprintf("%s:%d", hp.Host, hp.Port)
	}
	isUp := func(key string) bool {
		ap := mkAddr(key)
		if ap == "" {
			return false
		}
		cctx, cancel := context.WithTimeout(ctx, 600*time.Millisecond)
		defer cancel()
		conn, err := grpc.DialContext(cctx, ap, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		if err != nil {
			return false
		}
		_ = conn.Close()
		return true
	}
	hasNFT := func(key string, tokenID []byte) bool {
		ap := mkAddr(key)
		if ap == "" {
			return false
		}
		cctx, cancel := context.WithTimeout(ctx, 1200*time.Millisecond)
		defer cancel()

		fmt.Printf("🔎 Sto cercando NFT id=%s sul nodo \"%s\" (%s)...\n", hex.EncodeToString(tokenID), key, ap)
		conn, err := grpc.DialContext(cctx, ap, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		if err != nil {
			fmt.Printf("   ⚠️ dial %s: %v\n", ap, err)
			return false
		}
		defer conn.Close()

		client := pb.NewKademliaClient(conn)
		resp, err := client.LookupNFT(cctx, &pb.LookupNFTReq{
			FromId: nodo,
			Key:    &pb.Key{Key: tokenID},
		})
		if err != nil {
			fmt.Printf("   ❌ errore LookupNFT su %s: %v\n", ap, err)
			return false
		}
		if resp.GetFound() {
			fmt.Printf("   ✅ Trovato su %s\n", ap)
		} else {
			fmt.Printf("   ❌ NON trovato su %s\n", ap)
		}
		return resp.GetFound()
	}

	// ---- directory dati locale ----
	dataDir := strings.TrimSpace(os.Getenv("DATA_DIR"))
	if dataDir == "" {
		dataDir = "/data"
	}
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, fmt.Errorf("ReadDir(%s): %w", dataDir, err)
	}
	fmt.Printf("[Rebalance2] nodo=%s dir=%s files=%d k=%d\n", nodo, dataDir, len(entries), k)

	// contatori per la risposta
	var moved, kept int
	var skippedNonJSON, skippedReadErr, skippedParseErr, skippedBadToken, skippedNoAssigned int

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if !strings.HasSuffix(e.Name(), ".json") {
			skippedNonJSON++
			continue
		}

		tokenHex := strings.TrimSuffix(e.Name(), ".json")
		tokenID, decErr := hex.DecodeString(tokenHex)
		if decErr != nil || len(tokenID) == 0 {
			skippedBadToken++
			continue
		}

		path := filepath.Join(dataDir, e.Name())

		// leggo il JSON in TempNFT (serve per convert e per il nome nei log)
		var tmp TempNFT
		if b, rerr := os.ReadFile(path); rerr != nil {
			skippedReadErr++
		} else if jerr := json.Unmarshal(b, &tmp); jerr != nil {
			skippedParseErr++
		}
		nftName := tmp.Name
		if nftName == "" {
			nftName = tokenHex
		}

		// top (k+2) vicini
		assigned := ClosestNodesForNFTWithDir(tokenID, dir, k+2)
		if len(assigned) == 0 {
			skippedNoAssigned++
			continue
		}

		fmt.Printf("→ NFT %q (%s) assegnato ai più vicini (max %d): [", nftName, tokenHex, k+2)
		for i, a := range assigned {
			if i > 0 {
				fmt.Print(" ")
			}
			fmt.Print(a.Key)
		}
		fmt.Println("]")

		// stato candidati in ordine
		type cand struct {
			key      string
			up, have bool
		}
		cands := make([]cand, 0, len(assigned))
		for _, a := range assigned {
			up := isUp(a.Key)
			have := up && hasNFT(a.Key, tokenID)
			cands = append(cands, cand{key: a.Key, up: up, have: have})
		}

		// quante copie utili nei primi k up
		copies := 0
		for _, c := range cands {
			if c.up && c.have {
				copies++
				if copies >= k {
					break
				}
			}
		}

		// se mancano copie → store sui più vicini up che non hanno
		if copies < k {
			need := k - copies
			var dests []string
			for _, c := range cands {
				if need == 0 {
					break
				}
				if !c.up || c.have {
					continue
				}
				if ap := mkAddr(c.key); ap != "" {
					dests = append(dests, ap)
					need--
				}
			}
			if len(dests) > 0 {
				fmt.Printf("📦 Replico %q (%s) su: %v\n", nftName, tokenHex, dests)
				finale := convert(NFT{}, tmp, nil)
				if err := StoreNFTToNodes(finale, tokenID, nftName, dests, 24*3600); err != nil {
					fmt.Printf("❌ Replicazione NFT %q fallita (dest=%v): %v\n", nftName, dests, err)
				}
			} else {
				fmt.Printf("⚠️ Mancano %d copie ma nessun candidato up disponibile.\n", k-copies)
			}
		}

		// se QUESTO nodo è oltre il top-k (tra gli up) → rimuovo locale
		myPos := -1
		for i, c := range cands {
			if c.key == nodo {
				myPos = i
				break
			}
		}
		if myPos >= 0 {
			rankUp := 0
			for i := 0; i <= myPos; i++ {
				if cands[i].up {
					rankUp++
				}
			}
			if rankUp > k {
				fmt.Printf("🧹 Questo nodo (%s) è fuori dal top-%d per %s → Remove(%s)\n", nodo, k, tokenHex, path)
				if err := os.Remove(path); err != nil {
					fmt.Printf("⚠️ Remove(%s): %v\n", path, err)
					kept++ // fallita cancellazione → consideralo "tenuto"
					continue
				}
				moved++
				continue
			}
		}

		// se arrivo qui, il file locale resta
		kept++
	}

	msg := fmt.Sprintf(
		"[Rebalance2 nodo=%s] tenuti=%d, rimossi_local=%d. skipped: nonjson=%d read=%d parse=%d badtoken=%d noassigned=%d",
		nodo, kept, moved, skippedNonJSON, skippedReadErr, skippedParseErr, skippedBadToken, skippedNoAssigned,
	)
	return &pb.RebalanceRes{
		Moved:   int32(moved),
		Kept:    int32(kept),
		Message: msg,
	}, nil
}

/*

func (s *KademliaServer) Rebalance(ctx context.Context, req *pb.RebalanceReq) (*pb.RebalanceRes, error) {
	nodo := strings.TrimSpace(req.GetTargetId())

	k := int(req.GetK())
	if k <= 0 {
		k = 2 // fattore di replica "buono"
	}
	const inspectN = 4 // calcoliamo sempre i 4 più vicini: 1-2 da tenere, 3-4 da scartare

	// --- Rubrica chiave-stabile -> endpoint (host:port) ---
	type hostPort struct {
		Host string
		Port int
	}
	peerAddr := make(map[string]hostPort, len(req.GetNodes()))
	nodeKeys := make([]string, 0, len(req.GetNodes()))

	for _, n := range req.GetNodes() {
		key := strings.TrimSpace(n.GetId())
		if key == "" {
			key = strings.TrimSpace(n.GetHost())
		}
		if key == "" {
			continue
		}
		h, p := sanitizeHostPort(n.GetHost(), int(n.GetPort()))
		peerAddr[key] = hostPort{Host: h, Port: p}
		nodeKeys = append(nodeKeys, key)
	}

	// --- Byte mapping su chiavi stabili ---
	dir := BuildByteMappingSHA1(nodeKeys)
	fmt.Printf("[Rebalance2] ByteMapping su %d chiavi: %v\n", len(nodeKeys), nodeKeys)

	// --- helper con LOG: controlla presenza NFT su un nodo via LookupNFT ---
	hasNFT := func(addr, keyName string, tokenHex string, tokenID []byte) (bool, error) {
		cctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		fmt.Printf("🔎 Sto cercando NFT id=%s sul nodo %q (%s)...\n", tokenHex, keyName, addr)
		conn, err := grpc.DialContext(cctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		if err != nil {
			fmt.Printf("   ↪️ dial fallita verso %s: %v\n", addr, err)
			return false, fmt.Errorf("dial %s: %w", addr, err)
		}
		defer conn.Close()

		client := pb.NewKademliaClient(conn)
		resp, err := client.LookupNFT(cctx, &pb.LookupNFTReq{
			FromId: nodo,
			Key:    &pb.Key{Key: tokenID},
		})
		if err != nil {
			fmt.Printf("   ↪️ LookupNFT errore su %s: %v\n", addr, err)
			return false, err
		}
		if resp.GetFound() {
			fmt.Printf("   ✅ Trovato su %s\n", addr)
		} else {
			fmt.Printf("   ❌ NON trovato su %s\n", addr)
		}
		return resp.GetFound(), nil
	}

	// --- scan directory dati locali ---
	dataDir := strings.TrimSpace(os.Getenv("DATA_DIR"))
	if dataDir == "" {
		dataDir = "/data"
	}
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, fmt.Errorf("ReadDir(%s): %w", dataDir, err)
	}
	fmt.Printf("[Rebalance2] dirPath=%s entries=%d\n", dataDir, len(entries))

	var moved, kept int
	var skippedNonJSON, skippedReadErr, skippedParseErr, skippedBadToken, skippedNoAssigned int

	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		if strings.ToLower(filepath.Ext(e.Name())) != ".json" {
			skippedNonJSON++
			continue
		}

		path := filepath.Join(dataDir, e.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			skippedReadErr++
			fmt.Printf("⚠️ ReadFile(%s): %v\n", path, err)
			continue
		}

		var tmp TempNFT
		if err := json.Unmarshal(data, &tmp); err != nil {
			skippedParseErr++
			fmt.Printf("⚠️ Unmarshal(%s): %v\n", e.Name(), err)
			continue
		}

		// ricava tokenID: preferisci filename <hash>.json (40 hex) altrimenti tmp.TokenID
		base := strings.TrimSuffix(strings.ToLower(strings.TrimSpace(e.Name())), ".json")
		var tokenID []byte
		if len(base) == 40 {
			if b, err := hex.DecodeString(base); err == nil {
				tokenID = b
			}
		}
		if tokenID == nil {
			b, err := hex.DecodeString(strings.ToLower(strings.TrimSpace(tmp.TokenID)))
			if err != nil {
				skippedBadToken++
				fmt.Printf("⚠️ %s: token_id non valido: %v\n", e.Name(), err)
				continue
			}
			tokenID = b
		}
		tokenHex := hex.EncodeToString(tokenID)

		// --- calcola i 4 più vicini (o meno se cluster piccolo) ---
		assigned := ClosestNodesForNFTWithDir(tokenID, dir, inspectN)
		if len(assigned) == 0 {
			skippedNoAssigned++
			fmt.Printf("⚠️ %s: nessun nodo assegnato per token %q → skip\n", e.Name(), tmp.Name)
			continue
		}

		// prepara elenco (name,addr) per logging e chiamate
		type dest struct{ name, addr string }
		dests := make([]dest, 0, len(assigned))
		for _, a := range assigned {
			if hp, ok := peerAddr[a.Key]; ok {
				dests = append(dests, dest{name: a.Key, addr: fmt.Sprintf("%s:%d", hp.Host, hp.Port)})
			} else {
				dests = append(dests, dest{name: a.Key, addr: fmt.Sprintf("%s:%d", a.Key, 8000)}) // fallback
			}
		}

		// log elenco assegnati
		names := make([]string, 0, len(dests))
		for _, d := range dests {
			names = append(names, d.name)
		}
		fmt.Printf("→ NFT %q (%s) assegnato ai più vicini (max %d): %v\n", tmp.Name, tokenHex, inspectN, names)

		// --- verifica presenza sui candidati ---
		present := make([]bool, len(dests))
		for i, d := range dests {
			ok, err := hasNFT(d.addr, d.name, tokenHex, tokenID)
			if err != nil {
				// se lookup fallisce, considera non presente (replicheremo)
				present[i] = false
				continue
			}
			present[i] = ok
		}

		// --- prepara lista dei nodi (addr) dove DEVE esserci (i < k) ma manca ---
		missingAddrs := make([]string, 0, k)
		for i := 0; i < len(dests) && i < k; i++ {
			if !present[i] {
				missingAddrs = append(missingAddrs, dests[i].addr)
			}
		}

		// --- se mancano repliche sui primi k, replicale ---
		if len(missingAddrs) > 0 {
			fmt.Printf("📦 Replico %q (%s) su: %v\n", tmp.Name, tokenHex, missingAddrs)
			finale := convert(NFT{}, tmp, nil)
			if err := StoreNFTToNodes(finale, tokenID, finale.Name, missingAddrs, 24*3600); err != nil {
				fmt.Printf("❌ Replicazione NFT %q fallita (dest=%v): %v\n", tmp.Name, missingAddrs, err)
				// non proseguo con delete locale in caso di replica fallita
				kept++ // lo tengo locale per sicurezza
				continue
			}
		}

		// --- se QUESTO nodo non è tra i primi k, rimuovi la copia locale ---
		//     (cioè se questo nodo è in posizione 3 o 4 tra i più vicini, o non è tra i 4)
		myPos := -1
		for i, d := range dests {
			if d.name == nodo || d.name == peerAddr[nodo].Host {
				myPos = i
				break
			}
		}
		if myPos >= 0 && myPos >= k {
			// siamo 3°/4° tra i più vicini → elimina locale
			fmt.Printf("🧹 Questo nodo (%s) è in pos %d (>=%d). Rimuovo copia locale di %q (%s)\n", nodo, myPos+1, k, tmp.Name, tokenHex)
			if err := os.Remove(path); err != nil {
				fmt.Printf("⚠️ Remove(%s): %v\n", path, err)
				kept++ // se non sono riuscito a cancellare, risulta “tenuto”
				continue
			}
			moved++
			continue
		}

		// altrimenti lo tengo (questo nodo è tra i primi k)
		kept++
	}

	msg := fmt.Sprintf(
		"[Rebalance2 nodo=%s] tenuti=%d, rimossi_local=%d. skipped: nonjson=%d read=%d parse=%d badtoken=%d noassigned=%d",
		nodo, kept, moved, skippedNonJSON, skippedReadErr, skippedParseErr, skippedBadToken, skippedNoAssigned,
	)
	return &pb.RebalanceRes{
		Moved:   int32(moved),
		Kept:    int32(kept),
		Message: msg,
	}, nil
}



func (s *KademliaServer) Rebalance2(ctx context.Context, req *pb.RebalanceReq) (*pb.RebalanceRes, error) {
	nodo := strings.TrimSpace(req.GetTargetId())
	k := int(req.GetK())
	if k <= 0 {
		k = 2
	}

	// --- Rubrica chiave-stabile -> endpoint (host:port) + lista chiavi per mapping ---
	type hostPort struct {
		Host string
		Port int
	}
	peerAddr := make(map[string]hostPort, len(req.GetNodes()))
	nodeKeys := make([]string, 0, len(req.GetNodes())) // SOLO chiavi stabili per il mapping (no :port)

	for _, n := range req.GetNodes() {
		key := strings.TrimSpace(n.GetId())
		if key == "" {
			key = strings.TrimSpace(n.GetHost())
		}
		if key == "" {
			continue
		}
		h, p := sanitizeHostPort(n.GetHost(), int(n.GetPort()))
		peerAddr[key] = hostPort{Host: h, Port: p}
		nodeKeys = append(nodeKeys, key)
	}

	// --- Byte mapping su chiavi stabili ---
	dir := BuildByteMappingSHA1(nodeKeys)
	//ora ho il mapping tra id e SHa1 di ogni nodo
	fmt.Printf("ByteMapping costruito su chiavi: %v\n", nodeKeys)

	// --- helper: controlla presenza NFT su un nodo via LookupNFT ---
	hasNFT := func(addr string, tokenID []byte) (bool, error) {
		cctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		conn, err := grpc.DialContext(cctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		if err != nil {
			return false, fmt.Errorf("dial %s: %w", addr, err)
		}
		defer conn.Close()

		client := pb.NewKademliaClient(conn)
		resp, err := client.LookupNFT(cctx, &pb.LookupNFTReq{
			FromId: nodo,
			Key:    &pb.Key{Key: tokenID},
		})
		if err != nil {
			return false, err
		}
		return resp.GetFound(), nil
	}

	newNodi:=dir.List

	// --- leggo tutti i dati dal nodo  ---
	dataDir := strings.TrimSpace(os.Getenv("DATA_DIR"))
	if dataDir == "" {
		dataDir = "/data"
	}
	entries, err := os.ReadDir(dataDir)
	if err != nil {
		return nil, fmt.Errorf("ReadDir(%s): %w", dataDir, err)
	}
	fmt.Printf("[Rebalance] dirPath=%s entries=%d\n", dataDir, len(entries))

	for _, entry := range entries {

		for i, n := range newNodi {
		if hasNFT(n[i],[]byte(nodo)) {
			fmt.Printf("✅ Trovato su %s\n", n[i])
		}
		if i>1{
			//cancella il file sul nodo n[i]
		}
		else{

			finale := convert(NFT{}, tmp, nil)
			if err := StoreNFTToNodes(finale, tokenID, finale.Name, missingAddrs, 24*3600); err != nil {
				fmt.Printf("❌ Replicazione NFT %q fallita (dest=%v): %v\n", tmp.Name, missingAddrs, err)
				continue
			}

		}
	}

	}



}

*/

func convert(to NFT, from TempNFT, nodiSelected []string) NFT {

	fmt.Printf("ID NFT: %s\n", from.TokenID)
	tokenID, _ := hex.DecodeString(from.TokenID)

	to.TokenID = tokenID
	to.Name = from.Name
	to.Index = from.Index
	to.Volume = from.Volume
	to.Volume_USD = from.VolumeUSD
	to.Market_Cap = from.MarketCap
	to.Market_Cap_USD = from.MarketCapUSD
	to.Sales = from.Sales
	to.Floor_Price = from.FloorPrice
	to.Floor_Price_USD = from.FloorPriceUSD
	to.Average_Price = from.AveragePrice
	to.Average_Price_USD = from.AveragePriceUSD
	to.Owners = from.Owners
	to.Assets = from.Assets
	to.Owner_Asset_Ratio = from.OwnerAssetRatio
	to.Category = from.Category
	to.Website = from.Website
	to.Logo = from.Logo
	to.AssignedNodesToken = nodiSelected

	return to
}

func NFTBelongsHere(nodo string, assigned []NodePick) bool {
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
