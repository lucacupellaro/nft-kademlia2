package logica

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	pb "kademlia-nft/proto/kad"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/gogo/protobuf/proto"
)

func (s *KademliaServer) Store(ctx context.Context, req *pb.StoreReq) (*pb.StoreRes, error) {
	dataDir := strings.TrimSpace(os.Getenv("DATA_DIR"))
	if dataDir == "" {
		dataDir = "/data" // default nel container
	}
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("creazione dir %s: %w", dataDir, err)
	}

	fileName := fmt.Sprintf("%x.json", req.Key.Key)
	filePath := filepath.Join(dataDir, fileName)

	// (facoltativo) log utile per conferma
	//abs, _ := filepath.Abs(filePath)
	//log.Printf("✅ Salvato NFT in %s (abs=%s)", filePath, abs)

	if err := os.WriteFile(filePath, req.Value.Bytes, 0644); err != nil {
		return nil, fmt.Errorf("scrittura file %s: %w", filePath, err)
	}
	return &pb.StoreRes{Ok: true}, nil
}

func (s *KademliaServer) LookupNFT(ctx context.Context, req *pb.LookupNFTReq) (*pb.LookupNFTRes, error) {
	dataDir := strings.TrimSpace(os.Getenv("DATA_DIR"))
	if dataDir == "" {
		dataDir = "/data"
	}

	// Chiave in HEX per log e filename
	keyRaw := req.GetKey().GetKey()
	keyHex := strings.ToLower(hex.EncodeToString(keyRaw))
	fileName := HexFileNameFromName(keyRaw) // passa i bytes, NON string(keyRaw)
	filePath := filepath.Join(dataDir, fileName)

	log.Printf("[SERVER %s] LookupNFT: keyHex='%s' → file='%s'",
		os.Getenv("NODE_ID"), keyHex, fileName)

	// --- Present on this node?
	if b, err := os.ReadFile(filePath); err == nil {
		log.Printf("[SERVER %s] TROVATO %s", os.Getenv("NODE_ID"), fileName)
		resp := &pb.LookupNFTRes{
			Found: true,
			Holder: &pb.Node{
				Id:   os.Getenv("NODE_ID"), // string UTF-8
				Host: os.Getenv("NODE_ID"),
				Port: 8000,
			},
			Value: &pb.NFTValue{Bytes: b},
		}
		return resp, nil
	}

	// --- Not found: build nearest from kbucket.json
	kbPath := filepath.Join(dataDir, "kbucket.json")
	kbBytes, err := os.ReadFile(kbPath)
	if err != nil {
		log.Printf("[SERVER %s] Nessun kbucket.json: %v", os.Getenv("NODE_ID"), err)
		return &pb.LookupNFTRes{Found: false}, nil
	}

	var parsed struct {
		NodeID    string   `json:"node_id"`
		BucketHex []string `json:"bucket_hex"`
		SavedAt   string   `json:"saved_at"`
	}
	if err := json.Unmarshal(kbBytes, &parsed); err != nil {
		log.Printf("[SERVER %s] Errore parse kbucket.json: %v", os.Getenv("NODE_ID"), err)
		return &pb.LookupNFTRes{Found: false}, nil
	}

	nearest := make([]*pb.Node, 0, len(parsed.BucketHex))
	for i, hx := range parsed.BucketHex {
		hx = strings.TrimSpace(strings.ToLower(hx))

		// Deve essere esattamente 40 char esadecimali (SHA-1)
		if len(hx) != 40 || !isHex(hx) || !utf8.ValidString(hx) {
			log.Printf("⚠️ kbucket entry NON valida: idx=%d val=%q len=%d (hex=%v utf8=%v) — SKIP",
				i, hx, len(hx), isHex(hx), utf8.ValidString(hx))
			continue
		}

		nearest = append(nearest, &pb.Node{
			Id:   hx,   // HEX = ASCII/UTF-8 → OK
			Host: "",   // opzionale: popola se lo conosci, "" è valido
			Port: 8000, // non influisce sull'UTF-8
		})
	}

	log.Printf("[SERVER %s] Nearest=%d", os.Getenv("NODE_ID"), len(nearest))
	for i, n := range nearest {
		log.Printf(" nearest[%d]: id=%q host=%q port=%d (utf8 id=%v host=%v)",
			i, n.Id, n.Host, n.Port, utf8.ValidString(n.Id), utf8.ValidString(n.Host))
	}

	// Pre-marshal DIAGNOSTICO: se fallisce, stampa dove
	resp := &pb.LookupNFTRes{Found: false, Nearest: nearest}
	if _, err := proto.Marshal(resp); err != nil {
		log.Printf("💥 PRE-MARSHAL FALLITO: %v", err)
		for i, n := range nearest {
			log.Printf(" check nearest[%d]: idBytes=%x hostBytes=%x utf8(id)=%v utf8(host)=%v",
				i, []byte(n.Id), []byte(n.Host), utf8.ValidString(n.Id), utf8.ValidString(n.Host))
		}
		// Ritorna comunque un INTERNAL con messaggio chiaro nei log
		return nil, err
	}

	return resp, nil
}

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
