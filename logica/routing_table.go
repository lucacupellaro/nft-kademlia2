package logica

import (
	"encoding/hex"
	"encoding/json"
	"math/bits"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"kademlia-nft/common"
)

type Contact struct {
	IDHex    string // 40 char hex
	Host     string // opzionale (può essere "nodeX")
	Port     int    // opzionale (8000)
	LastSeen int64
}

type bucket struct {
	k    int
	list []Contact // LRU: vecchio all’inizio, nuovo in coda
}

type RoutingTable struct {
	mu     sync.RWMutex
	selfID [20]byte
	k      int
	b      [160]*bucket
}

func NewRoutingTable(selfID [20]byte, k int) *RoutingTable {
	rt := &RoutingTable{selfID: selfID, k: k}
	for i := 0; i < 160; i++ {
		rt.b[i] = &bucket{k: k, list: make([]Contact, 0, k)}
	}
	return rt
}

// calcola indice bucket: 0 = più vicino, 159 = più lontano
func (rt *RoutingTable) bucketIndex(peerID [20]byte) int {
	var d [20]byte
	for i := 0; i < 20; i++ {
		d[i] = rt.selfID[i] ^ peerID[i]
	}
	for i := 0; i < 20; i++ {
		if d[i] != 0 {
			L := 8*i + bits.LeadingZeros8(d[i]) // 0..159 (big-endian)
			return 159 - L
		}
	}
	return -1 // stesso ID
}

func (rt *RoutingTable) Insert(idHex, host string, port int) {
	idHex = strings.ToLower(strings.TrimSpace(idHex))
	if len(idHex) != 40 {
		return
	}
	raw, err := hex.DecodeString(idHex)
	if err != nil || len(raw) != 20 {
		return
	}
	var peerID [20]byte
	copy(peerID[:], raw)

	idx := rt.bucketIndex(peerID)
	if idx < 0 {
		return
	}

	now := time.Now().Unix()
	rt.mu.Lock()
	defer rt.mu.Unlock()
	b := rt.b[idx]

	// se esiste, aggiorna e sposta in coda (LRU)
	for i, c := range b.list {
		if c.IDHex == idHex {
			c.Host, c.Port, c.LastSeen = host, port, now
			b.list = append(append([]Contact{}, b.list[:i]...), b.list[i+1:]...)
			b.list = append(b.list, c)
			return
		}
	}

	// nuovo contatto
	if len(b.list) < b.k {
		b.list = append(b.list, Contact{IDHex: idHex, Host: host, Port: port, LastSeen: now})
		return
	}
	// bucket pieno: politica semplice → rimpiazza il più vecchio
	b.list = append(b.list[1:], Contact{IDHex: idHex, Host: host, Port: port, LastSeen: now})
}

func xor(a, b []byte) []byte {
	out := make([]byte, len(a))
	for i := range a {
		out[i] = a[i] ^ b[i]
	}
	return out
}

type dc struct {
	dist []byte
	c    Contact
}

// ritorna i K contatti più vicini a target (su TUTTI i bucket)
func (rt *RoutingTable) NearestTo(target []byte, K int) []Contact {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	var all []dc
	for _, bk := range rt.b {
		for _, c := range bk.list {
			id, err := hex.DecodeString(c.IDHex)
			if err != nil || len(id) != 20 {
				continue
			}
			all = append(all, dc{dist: xor(target, id), c: c})
		}
	}
	sort.Slice(all, func(i, j int) bool {
		di, dj := all[i].dist, all[j].dist
		for k := 0; k < len(di) && k < len(dj); k++ {
			if di[k] != dj[k] {
				return di[k] < dj[k]
			}
		}
		return all[i].c.IDHex < all[j].c.IDHex
	})
	if K > len(all) {
		K = len(all)
	}
	out := make([]Contact, K)
	for i := 0; i < K; i++ {
		out[i] = all[i].c
	}
	return out
}

// tutti i contatti (deduplicati) — utile per seed/backup
func (rt *RoutingTable) AllContacts() []Contact {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	seen := map[string]bool{}
	out := []Contact{}
	for _, bk := range rt.b {
		for _, c := range bk.list {
			if !seen[c.IDHex] {
				seen[c.IDHex] = true
				out = append(out, c)
			}
		}
	}
	return out
}

func (rt *RoutingTable) FlattenIDs() []string {
	cs := rt.AllContacts()
	ids := make([]string, 0, len(cs))
	for _, c := range cs {
		ids = append(ids, c.IDHex)
	}
	return ids
}

// helper: nome → SHA1 hex
func NodeNameToIDHex(name string) string {
	id := common.Sha1ID(strings.TrimSpace(name))
	return strings.ToLower(hex.EncodeToString(id))
}

// salvataggio compatibile col tuo server (bucket_hex flatten)
func SaveKBucketFromRT(nodeName string, rt *RoutingTable, path string) error {
	type fileFmt struct {
		NodeID    string   `json:"node_id"`
		BucketHex []string `json:"bucket_hex"`
		SavedAt   string   `json:"saved_at"`
	}
	selfHex := NodeNameToIDHex(nodeName)
	ids := rt.FlattenIDs()

	// rimuovi eventuale self
	filter := make([]string, 0, len(ids))
	for _, h := range ids {
		if h != selfHex {
			filter = append(filter, h)
		}
	}

	b, _ := json.MarshalIndent(fileFmt{
		NodeID:    selfHex,
		BucketHex: filter,
		SavedAt:   time.Now().Format(time.RFC3339),
	}, "", "  ")
	return os.WriteFile(path, b, 0o644)
}
