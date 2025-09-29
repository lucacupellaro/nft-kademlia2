// logica/netwrap.go
package logica

import (
	"context"
	"fmt"
	"time"

	pb "kademlia-nft/proto/kad"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	dialTO = 1200 * time.Millisecond
	rpcTO  = 1500 * time.Millisecond
)

func addrOf(name string) string { return fmt.Sprintf("%s:%d", name, 8000) }

// Ping(remote) con FromId = selfName. Se OK, TouchContact(remote).
func RpcPing(ctx context.Context, selfName, remote string) error {
	ap := addrOf(remote)
	dctx, cancel := context.WithTimeout(ctx, dialTO)
	defer cancel()
	conn, err := grpc.DialContext(dctx, ap, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return err
	}
	defer conn.Close()

	cli := pb.NewKademliaClient(conn)
	rctx, cancel2 := context.WithTimeout(ctx, rpcTO)
	defer cancel2()
	_, err = cli.Ping(rctx, &pb.PingReq{From: &pb.Node{Id: selfName, Host: selfName, Port: 8000}})
	if err == nil {
		_ = TouchContact(remote) // aggiorna LRU locale
	}
	return err
}

// FIND_NODE emulata: usiamo LookupNFT(Key=target). Se OK, TouchContact(remote).
// Ritorna solo nomi dial-abili (Id/Host tipo "node7" o "host:port").
func RpcFindNodeNames(ctx context.Context, selfName, remote string, target []byte) ([]string, error) {
	ap := addrOf(remote)
	dctx, cancel := context.WithTimeout(ctx, dialTO)
	defer cancel()
	conn, err := grpc.DialContext(dctx, ap, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	cli := pb.NewKademliaClient(conn)
	rctx, cancel2 := context.WithTimeout(ctx, rpcTO)
	defer cancel2()
	resp, err := cli.LookupNFT(rctx, &pb.LookupNFTReq{FromId: selfName, Key: &pb.Key{Key: target}})
	if err != nil {
		return nil, err
	}

	_ = TouchContact(remote)

	out := make([]string, 0, len(resp.GetNearest()))
	for _, n := range resp.GetNearest() {
		if n.GetHost() != "" {
			out = append(out, n.GetHost())
			continue
		}
		if n.GetId() != "" {
			out = append(out, n.GetId())
		}
	}
	return out, nil
}
