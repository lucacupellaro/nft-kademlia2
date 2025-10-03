package logica

import (
	"context"
	"encoding/hex"
	"fmt"
	"kademlia-nft/common"
	pb "kademlia-nft/proto/kad"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	kBucketPath = "/data/kbucket.json"
)

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

// TouchContactByName ( inserisce) un contatto nella routing table
func TouchContactByName(contactName string) error {
	if contactName == "" {
		return nil
	}

	rtMu.Lock()
	defer rtMu.Unlock() //defer unlock quando in caso di return

	rt, err := loadRT(kBucketPath)
	if err != nil {
		return fmt.Errorf("loadRT: %w", err)
	}
	if rt.NodeID == "" {
		rt.NodeID = strings.TrimSpace(os.Getenv("NODE_ID"))
	}
	if rt.BucketSize <= 0 {
		rt.BucketSize = KCapacity
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

	//mi trovo l'indice globale del bucket esempio 120
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
	//vado nel bucket corretto
	rt.Buckets[key] = b

	rt.NonEmptyInfo = countNonEmpty(rt.Buckets)

	//salva tutto sul file
	return saveRTAtomic(kBucketPath, rt)
}
