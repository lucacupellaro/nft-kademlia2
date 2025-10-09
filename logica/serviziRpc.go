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

	"github.com/gogo/protobuf/proto"

	"context"
	"encoding/json"
)

type KademliaServer struct {
	pb.UnimplementedKademliaServer

	kbMu sync.RWMutex // protegge kbucket.json

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

func (s *KademliaServer) LookupNFT(ctx context.Context, req *pb.LookupNFTReq) (*pb.LookupNFTRes, error) {

	if from := strings.TrimSpace(req.GetFromId()); from != "" {
		if err := TouchContactByName(from); err != nil {
			log.Printf("[LookupNFT] TouchContact(%q) FAILED: %v", from, err)
		}
	}

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

	// 2) Non trovato: leggi routing table
	kbPath := filepath.Join(dataDir, "kbucket.json")
	s.kbMu.RLock()
	kbBytes, err := os.ReadFile(kbPath)
	s.kbMu.RUnlock()
	if err != nil {
		log.Printf("[SERVER %s] Nessun kbucket.json: %v", os.Getenv("NODE_ID"), err)
		return &pb.LookupNFTRes{Found: false}, nil
	}

	var rt RoutingTableFile
	if err := json.Unmarshal(kbBytes, &rt); err != nil {
		log.Printf("[SERVER %s] Errore parse kbucket.json: %v", os.Getenv("NODE_ID"), err)
		return &pb.LookupNFTRes{Found: false}, nil
	}

	selfName := strings.TrimSpace(os.Getenv("NODE_ID"))
	selfHexByName := strings.ToLower(hex.EncodeToString(common.Sha1ID(selfName)))

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
			d, err := common.XOR(keyRaw, idBytes)
			if err != nil {
				continue
			}

			cands = append(cands, cand{
				idHex: hx,
				dist:  d,
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

func (s *KademliaServer) GetKBucket(ctx context.Context, req *pb.GetKBucketReq) (*pb.GetKBucketResp, error) {
	dataDir := strings.TrimSpace(os.Getenv("DATA_DIR"))
	if dataDir == "" {
		dataDir = "/data"
	}
	kbPath := filepath.Join(dataDir, "kbucket.json")

	// lettura semplice: i writer usano saveRTAtomic (rename), quindi è safe
	b, err := os.ReadFile(kbPath)
	if err != nil {
		return &pb.GetKBucketResp{}, nil
	}

	var rt RoutingTableFile // <<-- NUOVO FORMATO
	if err := json.Unmarshal(b, &rt); err != nil {
		log.Printf("GetKBucket: parse err: %v", err)
		return &pb.GetKBucketResp{}, nil
	}

	// Esponiamo i contatti per nome (Id=Name) così il client può contattarli.
	nodes := make([]*pb.Node, 0, 128)
	for _, list := range rt.Buckets {
		for _, e := range list {
			name := strings.TrimSpace(e.Name)
			if name == "" {
				continue
			}
			nodes = append(nodes, &pb.Node{
				Id:   name,
				Host: name,
				Port: 8000,
			})
		}
	}

	return &pb.GetKBucketResp{Nodes: nodes}, nil
}

func (s *KademliaServer) Ping(ctx context.Context, req *pb.PingReq) (*pb.PingRes, error) {
	if f := req.GetFrom(); f != nil && f.GetId() != "" {
		log.Printf("[Ping] ricevuto From.Id=%q", f.GetId())
		if err := TouchContactByName(f.GetId()); err != nil {
			log.Printf("[Ping] TouchContact(%q) FAILED: %v", f.GetId(), err)
		} else {
			log.Printf("[Ping] TouchContact(%q) OK (bucket aggiornato)", f.GetId())
		}
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
	if err := TouchContactByName(c.GetId()); err != nil {
		return nil, err
	}

	return &pb.UpdateBucketRes{Ok: true}, nil
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
