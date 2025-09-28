package logica

import (
	"bytes"
	"encoding/hex"
	"sort"
	"sync"

	"fmt"
	"kademlia-nft/common"
	pb "kademlia-nft/proto/kad"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gogo/protobuf/proto"

	"context"
	"encoding/json"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type KademliaServer struct {
	pb.UnimplementedKademliaServer

	kbMu        sync.RWMutex // protegge kbucket.json
	rebalanceMu sync.Mutex   // opzionale: serializza Rebalance

	fileMuMap   map[string]*sync.RWMutex // lock per-file NFT (<hex>.json)
	fileMuMapMu sync.Mutex               // protegge la mappa dei lock
}

func NewKademliaServer() *KademliaServer {
	return &KademliaServer{
		fileMuMap: make(map[string]*sync.RWMutex),
	}
}

// ritorna (creandolo se serve) il lock per un certo file NFT
func (s *KademliaServer) fileLock(keyHex string) *sync.RWMutex {
	s.fileMuMapMu.Lock()
	if s.fileMuMap == nil { // <-- init difensiva
		s.fileMuMap = make(map[string]*sync.RWMutex)
	}
	mu, ok := s.fileMuMap[keyHex]
	if !ok {
		mu = &sync.RWMutex{}
		s.fileMuMap[keyHex] = mu
	}
	s.fileMuMapMu.Unlock()
	return mu
}

func (s *KademliaServer) Store(ctx context.Context, req *pb.StoreReq) (*pb.StoreRes, error) {
	dataDir := strings.TrimSpace(os.Getenv("DATA_DIR"))
	if dataDir == "" {
		dataDir = "/data" // default nel container
	}
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("creazione dir %s: %w", dataDir, err)
	}

	keyHex := strings.ToLower(hex.EncodeToString(req.GetKey().GetKey()))
	mu := s.fileLock(keyHex) // ⇦ lock per questo NFT
	mu.Lock()
	defer mu.Unlock()

	fileName := fmt.Sprintf("%x.json", req.Key.Key)
	filePath := filepath.Join(dataDir, fileName)

	if err := os.WriteFile(filePath, req.Value.Bytes, 0644); err != nil {
		return nil, fmt.Errorf("scrittura file %s: %w", filePath, err)
	}
	return &pb.StoreRes{Ok: true}, nil
}

// ======================= SERVER SIDE =======================
// RPC: LookupNFT (usa il NUOVO formato di kbucket.json)
func (s *KademliaServer) LookupNFT(ctx context.Context, req *pb.LookupNFTReq) (*pb.LookupNFTRes, error) {
	dataDir := strings.TrimSpace(os.Getenv("DATA_DIR"))
	if dataDir == "" {
		dataDir = "/data"
	}

	keyRaw := req.GetKey().GetKey()
	keyHex := strings.ToLower(hex.EncodeToString(keyRaw))
	fileName := keyHex + ".json"
	filePath := filepath.Join(dataDir, fileName)

	log.Printf("[SERVER %s] LookupNFT: keyHex='%s' → file='%s'",
		os.Getenv("NODE_ID"), keyHex, fileName)

	// 1) Presente localmente?
	{
		mu := s.fileLock(keyHex)
		mu.RLock()
		b, err := os.ReadFile(filePath)
		mu.RUnlock()
		if err == nil {
			log.Printf("[SERVER %s] TROVATO %s", os.Getenv("NODE_ID"), fileName)
			return &pb.LookupNFTRes{
				Found: true,
				Holder: &pb.Node{
					Id:   os.Getenv("NODE_ID"),
					Host: os.Getenv("NODE_ID"),
					Port: 8000,
				},
				Value: &pb.NFTValue{Bytes: b},
			}, nil
		}
	}

	// 2) Non trovato: leggi routing table NUOVA
	kbPath := filepath.Join(dataDir, "kbucket.json")
	s.kbMu.RLock()
	kbBytes, err := os.ReadFile(kbPath)
	s.kbMu.RUnlock()
	if err != nil {
		log.Printf("[SERVER %s] Nessun kbucket.json: %v", os.Getenv("NODE_ID"), err)
		return &pb.LookupNFTRes{Found: false}, nil
	}

	// Strutture per il NUOVO formato
	type PeerEntry struct {
		Name string `json:"name"`
		SHA  string `json:"sha"`  // 40 hex
		Dist string `json:"dist"` // opzionale nel file; NON ci fidiamo: ricalcoliamo
	}
	type RoutingTableFile struct {
		SelfNode        string                 `json:"self_node"`
		BucketSize      int                    `json:"bucket_size"`
		HashBits        int                    `json:"hash_bits"`
		SavedAt         string                 `json:"saved_at"`
		Buckets         map[string][]PeerEntry `json:"buckets"`
		NonEmptyBuckets int                    `json:"non_empty_buckets"`
	}

	var rt RoutingTableFile
	if err := json.Unmarshal(kbBytes, &rt); err != nil {
		log.Printf("[SERVER %s] Errore parse kbucket.json: %v", os.Getenv("NODE_ID"), err)
		return &pb.LookupNFTRes{Found: false}, nil
	}

	selfName := strings.TrimSpace(os.Getenv("NODE_ID"))
	selfHexByName := strings.ToLower(hex.EncodeToString(common.Sha1ID(selfName)))

	// helper XOR
	xor := func(a, b []byte) []byte {
		out := make([]byte, len(a))
		for i := range a {
			out[i] = a[i] ^ b[i]
		}
		return out
	}

	type cand struct {
		idHex string
		dist  []byte
		name  string
	}
	cands := make([]cand, 0, 64)

	// raccogli TUTTI i contatti da tutti i bucket
	for _, list := range rt.Buckets {
		for _, e := range list {
			hx := strings.ToLower(strings.TrimSpace(e.SHA))
			if len(hx) != 40 {
				continue
			}
			if hx == selfHexByName {
				continue
			}
			idBytes, decErr := hex.DecodeString(hx)
			if decErr != nil || len(idBytes) != len(keyRaw) {
				continue
			}
			cands = append(cands, cand{
				idHex: hx,
				dist:  xor(keyRaw, idBytes),
				name:  e.Name,
			})
		}
	}

	if len(cands) == 0 {
		log.Printf("[SERVER %s] routing table vuota/illeggibile", os.Getenv("NODE_ID"))
		return &pb.LookupNFTRes{Found: false}, nil
	}

	// ordina per distanza a key (tie-break per idHex)
	sort.Slice(cands, func(i, j int) bool {
		if c := bytes.Compare(cands[i].dist, cands[j].dist); c != 0 {
			return c < 0
		}
		return cands[i].idHex < cands[j].idHex
	})

	// opzionale: cappiamo al bucket size (K)
	K := rt.BucketSize
	if K <= 0 {
		K = 20
	}
	if len(cands) > K {
		cands = cands[:K]
	}

	nearest := make([]*pb.Node, 0, len(cands))
	for _, c := range cands {
		nearest = append(nearest, &pb.Node{
			Id:   c.idHex, // ID = SHA hex
			Host: "",      // il client potrà risolvere via reverse-map (name -> host:port)
			Port: 8000,
		})
	}

	resp := &pb.LookupNFTRes{Found: false, Nearest: nearest}
	// sanity pre-marshal
	if _, err := proto.Marshal(resp); err != nil {
		log.Printf("💥 PRE-MARSHAL FALLITO: %v", err)
		return nil, err
	}
	return resp, nil
}

/*
func (s *KademliaServer) LookupNFT(ctx context.Context, req *pb.LookupNFTReq) (*pb.LookupNFTRes, error) {
	dataDir := strings.TrimSpace(os.Getenv("DATA_DIR"))
	if dataDir == "" {
		dataDir = "/data"
	}

	// Chiave richiesta (20 byte) e sue forme per log/confronti
	keyRaw := req.GetKey().GetKey()
	keyHex := strings.ToLower(hex.EncodeToString(keyRaw))

	// Nome file locale = hex(key).json
	fileName := fmt.Sprintf("%s.json", keyHex)
	filePath := filepath.Join(dataDir, fileName)

	log.Printf("[SERVER %s] LookupNFT: keyHex='%s' → file='%s'",
		os.Getenv("NODE_ID"), keyHex, fileName)

	// 1) Presente localmente? (lettura protetta da RLock per-file)
	{
		mu := s.fileLock(keyHex)
		mu.RLock()
		b, err := os.ReadFile(filePath)
		mu.RUnlock()

		if err == nil {
			log.Printf("[SERVER %s] TROVATO %s", os.Getenv("NODE_ID"), fileName)
			return &pb.LookupNFTRes{
				Found: true,
				Holder: &pb.Node{
					Id:   os.Getenv("NODE_ID"),
					Host: os.Getenv("NODE_ID"),
					Port: 8000,
				},
				Value: &pb.NFTValue{Bytes: b},
			}, nil
		}
		// se err != nil, proseguiamo coi nearest
	}

	// 2) Non trovato: leggi kbucket.json sotto RLock (solo per l'I/O)
	kbPath := filepath.Join(dataDir, "kbucket.json")
	s.kbMu.RLock()
	kbBytes, err := os.ReadFile(kbPath)
	s.kbMu.RUnlock()
	if err != nil {
		log.Printf("[SERVER %s] Nessun kbucket.json: %v", os.Getenv("NODE_ID"), err)
		return &pb.LookupNFTRes{Found: false}, nil
	}

	// --- parsing senza lock ---
	var parsed struct {
		NodeID    string   `json:"node_id"`    // atteso in hex (40 char) o vuoto
		BucketHex []string `json:"bucket_hex"` // lista di ID in hex (40 char)
		SavedAt   string   `json:"saved_at"`
	}
	if err := json.Unmarshal(kbBytes, &parsed); err != nil {
		log.Printf("[SERVER %s] Errore parse kbucket.json: %v", os.Getenv("NODE_ID"), err)
		return &pb.LookupNFTRes{Found: false}, nil
	}

	// Calcola anche lo SHA1 del nome nodo (per sicurezza nel confronto "self")
	selfIDFromName := strings.ToLower(hex.EncodeToString(common.Sha1ID(os.Getenv("NODE_ID"))))
	selfIDFromKB := strings.ToLower(strings.TrimSpace(parsed.NodeID))

	// helper XOR
	xor := func(a, b []byte) []byte {
		out := make([]byte, len(a))
		for i := range a {
			out[i] = a[i] ^ b[i]
		}
		return out
	}

	type cand struct {
		idHex string
		dist  []byte
	}
	var cands []cand
	for i, hx := range parsed.BucketHex {
		hx = strings.TrimSpace(strings.ToLower(hx))
		if len(hx) != 40 || !isHex(hx) || !utf8.ValidString(hx) {
			log.Printf("⚠️ kbucket entry NON valida: idx=%d val=%q len=%d — SKIP", i, hx, len(hx))
			continue
		}

		// salta self (sia se uguale al NodeID del file che allo SHA1 del NODE_ID env)
		if hx == selfIDFromKB || hx == selfIDFromName {
			continue
		}

		idBytes, err := hex.DecodeString(hx)
		if err != nil || len(idBytes) != len(keyRaw) {
			continue
		}
		cands = append(cands, cand{
			idHex: hx,
			dist:  xor(keyRaw, idBytes),
		})
	}

	if len(cands) == 0 {
		log.Printf("[SERVER %s] kbucket vuoto o tutto invalido", os.Getenv("NODE_ID"))
		return &pb.LookupNFTRes{Found: false}, nil
	}

	// ORDINA per distanza XOR crescente (tie-break sull'ID hex)
	sort.Slice(cands, func(i, j int) bool {
		if c := bytes.Compare(cands[i].dist, cands[j].dist); c != 0 {
			return c < 0
		}
		return cands[i].idHex < cands[j].idHex
	})

	// Costruisci risposta (ritorna tutti: il bucket è già limitato a K lato build)
	nearest := make([]*pb.Node, 0, len(cands))
	for _, c := range cands {
		nearest = append(nearest, &pb.Node{
			Id:   c.idHex,
			Host: "",   // opzionale popolarlo se conosci mapping ID→host
			Port: 8000, // irrilevante se client risolve via mappa
		})
	}

	log.Printf("[SERVER %s] Nearest ordinati=%d (top=%s …)", os.Getenv("NODE_ID"), len(nearest), nearest[0].GetId())

	// (facoltativo) sanity pre-marshal
	resp := &pb.LookupNFTRes{Found: false, Nearest: nearest}
	if _, err := proto.Marshal(resp); err != nil {
		log.Printf("💥 PRE-MARSHAL FALLITO: %v", err)
		return nil, err
	}
	return resp, nil
}
*/

func (s *KademliaServer) GetKBucket(ctx context.Context, req *pb.GetKBucketReq) (*pb.GetKBucketResp, error) {
	dataDir := strings.TrimSpace(os.Getenv("DATA_DIR"))
	if dataDir == "" {
		dataDir = "/data"
	}

	// --- 1) leggi kbucket.json ---
	type kbucketFile struct {
		NodeID    string   `json:"node_id"`
		BucketHex []string `json:"bucket_hex"`
		SavedAt   string   `json:"saved_at"`
	}
	kbPath := filepath.Join(dataDir, "kbucket.json")

	raw, err := os.ReadFile(kbPath)
	if err != nil {
		return nil, fmt.Errorf("errore lettura %s: %w", kbPath, err)
	}

	var kb kbucketFile
	if err := json.Unmarshal(raw, &kb); err != nil {
		return nil, fmt.Errorf("errore parse kbucket.json: %w", err)
	}

	// --- 2) normalizza/valida HEX e costruisci i Node ---
	nodes := make([]*pb.Node, 0, len(kb.BucketHex))
	for _, hx := range kb.BucketHex {
		hx = strings.ToLower(strings.TrimSpace(hx))
		if hx == "" {
			continue
		}

		fmt.Printf("GetKBucket: processing hex %q\n", hx)
		// deve essere hex valido e lungo 20 byte (SHA-1)
		b, err := hex.DecodeString(hx)
		if err != nil || len(b) != 20 {
			log.Printf("GetKBucket: scarto voce non valida (hex/len): %q", hx)
			continue
		}
		// hx è ASCII -> UTF-8 valido
		nodes = append(nodes, &pb.Node{
			Id:   hx, // esadecimale, quindi sicuro
			Host: "", // se non lo sai, lascialo vuoto (UTF-8 valido)
			Port: 0,
		})
	}

	// --- 3) sanità extra: verifica UTF-8 su tutti i campi string prima di serializzare ---
	for i, n := range nodes {
		if !utf8.ValidString(n.Id) {
			log.Printf("GetKBucket: INVALID UTF-8 in nodes[%d].Id: %q", i, n.Id)
			// opzionale: converti in hex di byte grezzi, ma qui è già hex
		}
		if !utf8.ValidString(n.Host) {
			log.Printf("GetKBucket: INVALID UTF-8 in nodes[%d].Host: %q", i, n.Host)
			n.Host = ""
		}
	}

	resp := &pb.GetKBucketResp{Nodes: nodes}

	// --- 4) pre-marshal check (così il panic non arriva dal layer gRPC) ---
	if _, err := proto.Marshal(resp); err != nil {
		// log utile per capire quale campo è sporco
		log.Printf("GetKBucket pre-marshal FAILED: %v", err)
		return nil, fmt.Errorf("internal: invalid UTF-8 in response: %w", err)
	}

	return resp, nil
}

func (s *KademliaServer) Ping(ctx context.Context, req *pb.PingReq) (*pb.PingRes, error) {
	if f := req.GetFrom(); f != nil && f.GetId() != "" {
		log.Printf("[Ping] ricevuto From.Id=%q", f.GetId())
		if err := TouchContact(f.GetId()); err != nil {
			log.Printf("[Ping] TouchContact(%q) FAILED: %v", f.GetId(), err)
		} else {
			log.Printf("[Ping] TouchContact(%q) OK (bucket aggiornato)", f.GetId())
		}
	} else {
		log.Printf("[Ping] req.From mancante o vuoto: nessun update del bucket")
	}

	self := os.Getenv("NODE_ID")
	if self == "" {
		self = "unknown"
	}
	return &pb.PingRes{Ok: true, NodeId: self, UnixMs: time.Now().UnixMilli()}, nil
}

func (s *KademliaServer) UpdateBucket(ctx context.Context, req *pb.UpdateBucketReq) (*pb.UpdateBucketRes, error) {
	c := req.GetContact()
	if c == nil || c.GetId() == "" {
		return &pb.UpdateBucketRes{Ok: false}, nil
	}
	if err := TouchContact(c.GetId()); err != nil {
		return nil, err
	}
	return &pb.UpdateBucketRes{Ok: true}, nil
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
	var kept int
	var replicated, deletedLocal int
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

		// leggo il JSON in TempNFT (per nome leggibile nei log)
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
				} else {
					replicated += len(dests)
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
				deletedLocal++
				continue
			}
		}

		// se arrivo qui, il file locale resta
		kept++
	}

	msg := fmt.Sprintf(
		"[Rebalance2 nodo=%s] kept=%d, replicated=%d, deletedLocal=%d. skipped: nonjson=%d read=%d parse=%d badtoken=%d noassigned=%d",
		nodo, kept, replicated, deletedLocal, skippedNonJSON, skippedReadErr, skippedParseErr, skippedBadToken, skippedNoAssigned,
	)
	return &pb.RebalanceRes{
		Moved:   int32(replicated), // “spostati” = repliche effettuate
		Kept:    int32(kept),
		Message: msg,
	}, nil
}

func (s *KademliaServer) GetNodeList(ctx context.Context, req *pb.GetNodeListReq) (*pb.GetNodeListRes, error) {
	raw := os.Getenv("NODES")
	if raw == "" {
		log.Println("WARN: NODES env vuota nel seeder")
		return &pb.GetNodeListRes{}, nil
	}
	parts := strings.Split(raw, ",")
	out := &pb.GetNodeListRes{Nodes: make([]*pb.Node, 0, len(parts))}
	for _, name := range parts {
		out.Nodes = append(out.Nodes, &pb.Node{
			Id:   name,
			Host: name,
			Port: 8000,
		})
	}
	return out, nil
}
