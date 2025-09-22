package ui

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"kademlia-nft/logica"
	pb "kademlia-nft/proto/kad"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func StoreNFTToNodes2(nft logica.NFT, tokenID []byte, name string, nodes []string, ttlSecs int32) error {

	payload, _ := json.Marshal(struct {
		TokenID           string `json:"token_id"`
		Name              string `json:"name"`
		Index             string `json:"index,omitempty"`
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
		TokenID:           hex.EncodeToString(tokenID),
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

	var errs []string

	for _, nodeName := range nodes {
		if strings.TrimSpace(nodeName) == "" {
			errs = append(errs, "host vuoto")
			continue
		}

		addr, rerr := logica.ResolveAddrForNode(nodeName)
		if rerr != nil {
			errs = append(errs, fmt.Sprintf("resolve %s: %v", nodeName, rerr))
			continue
		}

		fmt.Printf("→ dial %s per salvare %q\n", addr, name)
		conn, err := grpc.Dial(addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
			grpc.WithReturnConnectionError(),
		)
		if err != nil {
			errs = append(errs, fmt.Sprintf("dial %s: %v", addr, err))
			continue
		}

		client := pb.NewKademliaClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
		_, callErr := client.Store(ctx, &pb.StoreReq{
			From:    &pb.Node{Id: "seeder", Host: "seeder", Port: 0}, // info meta, non usata
			Key:     &pb.Key{Key: tokenID},
			Value:   &pb.NFTValue{Bytes: payload},
			TtlSecs: ttlSecs,
		})
		cancel()
		_ = conn.Close()

		if callErr != nil {
			errs = append(errs, fmt.Sprintf("Store(%s): %v", nodeName, callErr))
			continue
		}

		fmt.Printf("✅  NFT inviato %q su %s\n", tokenID, nodeName)
	}

	if len(errs) > 0 {
		return fmt.Errorf("alcune Store sono fallite: %s", strings.Join(errs, "; "))
	}
	return nil
}
