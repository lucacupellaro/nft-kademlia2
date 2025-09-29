package logica

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"kademlia-nft/common"
	pb "kademlia-nft/proto/kad"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var (
	rtMu sync.Mutex // <-- nome corretto
)

// struttura per il salvataggio
type KBucketFile struct {
	NodeID    string   `json:"node_id"`
	BucketHex []string `json:"bucket_hex"`
	SavedAt   string   `json:"saved_at"`
}

type PeerEntry struct {
	Name string `json:"name"` // es. "node7"
	SHA  string `json:"sha"`  // sha1 in hex del peer
	Dist string `json:"dist"` // distanza XOR self^peer (hex)
}

type RoutingTableFile struct {
	NodeID       string                 `json:"self_node"`
	BucketSize   int                    `json:"bucket_size"`
	HashBits     int                    `json:"hash_bits"` // 160 per SHA1
	SavedAt      string                 `json:"saved_at"`
	Buckets      map[string][]PeerEntry `json:"buckets"` // "0".."159" -> peers
	NonEmptyInfo int                    `json:"non_empty_buckets"`
}

var (
	kHashBits = 160
)

func SaveRoutingTableJSON(nodeID string, selfSHA []byte, bucketSize int, buckets map[int][]string, path string) error {
	const hashBits = 160

	dump := RoutingTableFile{
		NodeID:     nodeID,
		BucketSize: bucketSize,
		HashBits:   hashBits,
		SavedAt:    time.Now().UTC().Format(time.RFC3339),
		Buckets:    make(map[string][]PeerEntry, len(buckets)),
	}

	nonEmpty := 0
	for idx, names := range buckets {
		if len(names) == 0 {
			continue
		}
		// taglia comunque a K per coerenza
		if len(names) > bucketSize {
			names = names[:bucketSize]
		}

		entries := make([]PeerEntry, 0, len(names))
		for _, name := range names {
			peerSHA := common.Sha1ID(name) // []byte len=20
			// XOR distance
			x := make([]byte, len(selfSHA))
			for i := range selfSHA {
				x[i] = selfSHA[i] ^ peerSHA[i]
			}
			entries = append(entries, PeerEntry{
				Name: name,
				SHA:  hex.EncodeToString(peerSHA),
				Dist: hex.EncodeToString(x),
			})
		}

		dump.Buckets[strconv.Itoa(idx)] = entries
		nonEmpty++
	}
	dump.NonEmptyInfo = nonEmpty

	jsonBytes, err := json.MarshalIndent(dump, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, jsonBytes, 0o644)
}

// SaveKBucket(Nodi piu vicini) salva il bucket del nodo su file JSON
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

var (
	// Esporta questi se vuoi settarli dal main:
	KBucketPath string
	KCapacity   int
)

func SetKBucketGlobals(path string, capacity int) {
	KBucketPath = path
	KCapacity = capacity
}

// Crea kbucket.json vuoto se non esiste (NodeID + lista vuota)

func EnsureKBucketFile(path, selfName string) error {
	rtMu.Lock()
	defer rtMu.Unlock()

	// 1) Non esiste → crea nuovo file strutturato
	if _, err := os.Stat(path); os.IsNotExist(err) {
		rt := &RoutingTableFile{
			NodeID:       selfName, // json:"self_node"
			BucketSize:   kCapacity,
			HashBits:     kHashBits,
			SavedAt:      time.Now().UTC().Format(time.RFC3339),
			Buckets:      map[string][]PeerEntry{},
			NonEmptyInfo: 0,
		}
		return saveRTAtomic(path, rt)
	}

	// 2) Esiste → leggi contenuto
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	trim := bytes.TrimSpace(raw)

	// 2a) Vecchio formato piatto? ("node_id" presente ma non "self_node")
	if bytes.Contains(trim, []byte(`"node_id"`)) && !bytes.Contains(trim, []byte(`"self_node"`)) {
		_ = os.WriteFile(path+".bak", raw, 0o644)
		rt := &RoutingTableFile{
			NodeID:       selfName,
			BucketSize:   kCapacity,
			HashBits:     kHashBits,
			SavedAt:      time.Now().UTC().Format(time.RFC3339),
			Buckets:      map[string][]PeerEntry{},
			NonEmptyInfo: 0,
		}
		return saveRTAtomic(path, rt)
	}

	// 2b) Nuovo formato ma JSON non valido → riparti pulito
	var dummy RoutingTableFile
	if err := json.Unmarshal(trim, &dummy); err != nil {
		_ = os.WriteFile(path+".corrupt", raw, 0o644)
		rt := &RoutingTableFile{
			NodeID:       selfName,
			BucketSize:   kCapacity,
			HashBits:     kHashBits,
			SavedAt:      time.Now().UTC().Format(time.RFC3339),
			Buckets:      map[string][]PeerEntry{},
			NonEmptyInfo: 0,
		}
		return saveRTAtomic(path, rt)
	}

	// 3) File valido: patcha eventuali campi mancanti/inconsistenti
	patched := false
	if dummy.NodeID == "" && selfName != "" {
		dummy.NodeID = selfName
		patched = true
	}
	if dummy.BucketSize <= 0 {
		dummy.BucketSize = kCapacity
		patched = true
	}
	if dummy.HashBits <= 0 {
		dummy.HashBits = kHashBits
		patched = true
	}
	if dummy.Buckets == nil {
		dummy.Buckets = map[string][]PeerEntry{}
		patched = true
	}
	// ricalcola numero bucket non vuoti
	nonEmpty := 0
	for _, lst := range dummy.Buckets {
		if len(lst) > 0 {
			nonEmpty++
		}
	}
	if dummy.NonEmptyInfo != nonEmpty {
		dummy.NonEmptyInfo = nonEmpty
		patched = true
	}

	if patched {
		dummy.SavedAt = time.Now().UTC().Format(time.RFC3339)
		return saveRTAtomic(path, &dummy)
	}
	return nil
}

func saveRTAtomic(path string, rt *RoutingTableFile) error {
	rt.SavedAt = time.Now().UTC().Format(time.RFC3339)
	j, _ := json.MarshalIndent(rt, "", "  ")
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, j, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path) // rename è atomico su stessa FS
}

func countNonEmpty(m map[string][]PeerEntry) int {
	n := 0
	for _, v := range m {
		if len(v) > 0 {
			n++
		}
	}
	return n
}

func loadRT(path string) (*RoutingTableFile, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var rt RoutingTableFile
	if err := json.Unmarshal(b, &rt); err != nil {
		return nil, err
	}
	if rt.Buckets == nil {
		rt.Buckets = map[string][]PeerEntry{}
	}
	if rt.BucketSize <= 0 {
		rt.BucketSize = kCapacity
	}
	return &rt, nil
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

func TouchContactByName(contactName string) error {
	if contactName == "" {
		return nil
	}

	rtMu.Lock()
	defer rtMu.Unlock()

	rt, err := loadRT(kBucketPath)
	if err != nil {
		return fmt.Errorf("loadRT: %w", err)
	}
	if rt.NodeID == "" {
		rt.NodeID = strings.TrimSpace(os.Getenv("NODE_ID"))
	}
	if rt.BucketSize <= 0 {
		rt.BucketSize = kCapacity
	}
	if rt.HashBits <= 0 {
		rt.HashBits = kHashBits // 160 per SHA-1
	}
	if rt.Buckets == nil {
		rt.Buckets = map[string][]PeerEntry{}
	}

	// non inserire te stesso
	if contactName == rt.NodeID {
		return saveRTAtomic(kBucketPath, rt)
	}

	selfSHA := common.Sha1ID(rt.NodeID)
	peerSHA := common.Sha1ID(contactName)
	peerSHAHex := strings.ToLower(hex.EncodeToString(peerSHA))

	idx, err := common.MSBIndex(selfSHA, peerSHA)
	if err != nil || idx < 0 || idx >= rt.HashBits {
		return nil
	}
	key := strconv.Itoa(idx)

	b := rt.Buckets[key]

	// rimuovi se già presente (refresh LRU)
	out := b[:0]
	for _, e := range b {
		if e.Name == contactName || strings.EqualFold(e.SHA, peerSHAHex) {
			continue
		}
		out = append(out, e)
	}
	b = out

	entry := PeerEntry{Name: contactName, SHA: peerSHAHex}
	if len(b) >= rt.BucketSize {
		// bucket pieno → drop LRU (testa) e append nuovo in coda
		b = append(b[1:], entry)
	} else {
		b = append(b, entry)
	}
	rt.Buckets[key] = b

	// ✅ nome campo corretto nella struct
	rt.NonEmptyInfo = countNonEmpty(rt.Buckets)

	return saveRTAtomic(kBucketPath, rt)
}
