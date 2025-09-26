package logica

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"kademlia-nft/common"
	pb "kademlia-nft/proto/kad"
	"log"
	"os"
	"sort"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// struttura per il salvataggio
type KBucketFile struct {
	NodeID    string   `json:"node_id"`
	BucketHex []string `json:"bucket_hex"`
	SavedAt   string   `json:"saved_at"`
}

// SaveKBucket salva il bucket del nodo su file JSON
func SaveKBucket(nodeID string, bucket [][]byte, path string) error {

	bucketHex := make([]string, len(bucket))
	for i, b := range bucket {
		bucketHex[i] = hex.EncodeToString(b)
	}

	data := KBucketFile{
		NodeID:    nodeID,
		BucketHex: bucketHex,
		SavedAt:   time.Now().UTC().Format(time.RFC3339),
	}

	// codifica in JSON con indentazione
	jsonBytes, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}

	// scrive il file
	return os.WriteFile(path, jsonBytes, 0o644)
}

func RemoveAndSortMe(bucket [][]byte, selfId []byte) [][]byte {
	// Rimuove un nodo dal bucket
	for i := range bucket {
		if bytes.Equal(bucket[i], selfId) {
			bucket = append(bucket[:i], bucket[i+1:]...)
			break
		}
	}

	// 2. Riordina per distanza XOR dal nodo corrente (selfID)
	sort.Slice(bucket, func(i, j int) bool {
		var err1 error
		var err2 error
		distI, err1 := common.XOR(selfId, bucket[i])
		distJ, err2 := common.XOR(selfId, bucket[j])
		if err1 != nil || err2 != nil {
			log.Printf("WARN: errore calcolo distanza XOR: %v, %v", err1, err2)
			return false
		}
		return LessThan(distI, distJ)
	})

	return bucket

}

type KademliaServer struct {
	pb.UnimplementedKademliaServer
}

func GetNodeListIDs(seederAddr, requesterID string) ([]string, error) {
	// 1) connetti al seeder via gRPC
	conn, err := grpc.Dial(seederAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	// 2) crea client e manda la richiesta
	client := pb.NewKademliaClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	resp, err := client.GetNodeList(ctx, &pb.GetNodeListReq{RequesterId: requesterID})
	if err != nil {
		return nil, err
	}

	// 3) mappa la lista in []string con gli ID
	ids := make([]string, 0, len(resp.Nodes))
	for _, n := range resp.Nodes {
		ids = append(ids, n.Id)
	}
	return ids, nil
}

const (
	kBucketPath = "/data/kbucket.json"
	kCapacity   = 8
)

type kbucketFile struct {
	NodeID    string   `json:"node_id"`
	BucketHex []string `json:"bucket_hex"`
	SavedAt   string   `json:"saved_at"`
}

func loadKBucket(path string) (kb kbucketFile, _ error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return kb, err
	}
	err = json.Unmarshal(b, &kb)
	return kb, err
}

func saveKBucket(path string, kb kbucketFile) error {
	kb.SavedAt = time.Now().UTC().Format(time.RFC3339)
	j, _ := json.MarshalIndent(kb, "", "  ")
	return os.WriteFile(path, j, 0o644)
}

func touchContactHex(kb *kbucketFile, hexID string) {
	// rimuovi se presente
	out := kb.BucketHex[:0]
	for _, h := range kb.BucketHex {
		if h != hexID {
			out = append(out, h)
		}
	}
	kb.BucketHex = out

	// append in coda, con capacità
	if len(kb.BucketHex) < kCapacity {
		kb.BucketHex = append(kb.BucketHex, hexID)
		return
	}
	// bucket pieno: drop LRU (pos 0) e append nuovo
	kb.BucketHex = append(kb.BucketHex[1:], hexID)
}

func TouchContact(nodeID string) error {
	raw := common.Sha1ID(nodeID)
	hexID := hex.EncodeToString(raw)
	kb, err := loadKBucket(kBucketPath)
	if err != nil {
		return err
	}
	touchContactHex(&kb, hexID)
	return saveKBucket(kBucketPath, kb)
}
