package main

import (
	"context"
	"fmt"
	"kademlia-nft/logica"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func main() {

	var csvAll [][]string

	go func() {
		if err := logica.RunGRPCServer(); err != nil {
			log.Printf("gRPC server chiuso: %v", err)
		}
	}()

	// opzionale: piccolo delay per dare tempo al listener di alzarsi
	time.Sleep(400 * time.Millisecond)

	// Prende l'ID del nodo dall'ambiente
	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = "default"
	}

	fmt.Println("Avviato nodo:", nodeID)

	isSeeder := os.Getenv("SEED") == "true"

	if isSeeder {

		repFactor := logica.RequireIntEnv("REPLICATION_FACTOR", 3)

		fmt.Printf("Replication factor: %d\n", repFactor)

		fmt.Printf("sono il seeder,\nReading CSV file...\n")

		csvAll = logica.ReadCsv2("csv/NFT_Top_Collections.csv")

		fmt.Printf("NFT letti: %d\n", len(csvAll))

		// Genera gli ID per la lista di NFT
		fmt.Printf("Generating IDs for NFTs...\n")

		var colName []string
		for i, row := range csvAll {
			if i == 0 {
				continue // salta intestazione se presente
			}
			colName = append(colName, row[1])
		}

		listNFTId := logica.GenerateBytesOfAllNftsSHA1(colName)

		// recuper gli ID dei container
		rawNodes := os.Getenv("NODES")
		if rawNodes == "" {
			fmt.Println("Nessun nodo in NODES")
			return
		}

		var dir *logica.ByteMapping

		parts := strings.Split(rawNodes, ",")

		dir = logica.BuildByteMappingSHA1(parts)

		//------------creazione file-------------------------//
		base := os.Getenv("DATA_DIR")
		if base == "" {
			base = "/data"
		}

		out := filepath.Join(base, "byte_mapping.json")
		if err := os.MkdirAll(filepath.Dir(out), 0o755); err != nil {
			log.Fatalf("mkdir: %v", err)
		}
		//salvo il mapping dei nodi sul seeder 1
		if err := logica.SaveByteMappingJSON(out, dir); err != nil {
			log.Fatalf("Errore salvataggio byte_mapping.json: %v", err)
		}
		fmt.Printf("✅ File salvato in: %s\n", out)

		// Per ogni nft scorre tutti e gli id dei nodi e li assegna ai repFactor piu vicini)

		fmt.Println("Assegnazione dei k nodeID più vicini agli NFT...")

		rows := csvAll[1:] // salta header
		nfts := make([]logica.NFT, 0, len(rows))

		for i, row := range rows {

			if len(row) < 17 {
				continue
			} // safety

			name := strings.TrimSpace(row[1])

			col := func(k int) string {
				if k >= 0 && k < len(row) {
					return strings.TrimSpace(row[k])
				}
				return ""
			}

			assigned := logica.ClosestNodesForNFTWithDir(listNFTId[i], dir, repFactor)
			var nodiSelected []string

			for i := 0; i < repFactor; i++ {
				nodiSelected = append(nodiSelected, assigned[i].Key)
			}

			nfts = append(nfts, logica.NFT{
				Index:             col(0),
				Name:              name,
				Volume:            col(2),
				Volume_USD:        col(3),
				Market_Cap:        col(4),
				Market_Cap_USD:    col(5),
				Sales:             col(6),
				Floor_Price:       col(7),
				Floor_Price_USD:   col(8),
				Average_Price:     col(9),
				Average_Price_USD: col(10),
				Owners:            col(11),
				Assets:            col(12),
				Owner_Asset_Ratio: col(13),
				Category:          col(14),
				Website:           col(15),
				Logo:              col(16),

				TokenID:            listNFTId[i],
				AssignedNodesToken: nodiSelected,
			})
		}

		fmt.Printf("NFT assegnati: %d\n", len(nfts))

		for _, h := range parts {
			if err := logica.WaitReady(h, 12*time.Second); err != nil {
				log.Fatalf("❌ Nodo %s non pronto: %v", h, err) // fermati se uno non è pronto
			}
		}

		//-------------Salvatggio degli NFT sugli appositi Nodi-------------------------------------------------------------------//

		fmt.Printf("struct size: %d\n", len(nfts))

		for j := 0; j < len(nfts); j++ {
			var nodi []string
			for i := 0; i < repFactor; i++ {
				nodi = append(nodi, nfts[j].AssignedNodesToken[i])
			}

			if err := logica.StoreNFTToNodes(nfts[j], nfts[j].TokenID, nfts[j].Name, nodi, 24*3600); err != nil {
				fmt.Println("Errore:", err)
				continue
			}

			nodi = nil

		}

		select {} // blocca per sempre

	} else {

		// ------------------- Bootstrap dei Peer, recupero le variabili d'ambiente per costruire la routing table ------------------- //

		dataDir := "/data"
		if err := os.MkdirAll(dataDir, 0o755); err != nil {
			log.Fatalf("Impossibile creare %s: %v", dataDir, err)
		}

		nodeID := strings.TrimSpace(os.Getenv("NODE_ID"))
		if nodeID == "" {
			log.Fatalf("NODE_ID non impostato")
		}
		//selfSHA := common.Sha1ID(nodeID)

		bucketSize := logica.RequireIntEnv("BUCKET_SIZE", 3)
		if bucketSize <= 0 {
			bucketSize = 5
		}

		// ------------------- Costruzione routing table con capienza bucketSize e salvataggio -------------------

		kbPath := filepath.Join(dataDir, "kbucket.json")
		logica.SetKBucketGlobals(kbPath, bucketSize)

		if err := logica.EnsureKBucketFile(kbPath, nodeID); err != nil {
			log.Fatalf("Init kbucket.json: %v", err)
		}

		// Bootstrap + espansione (ping → find_node → ping mirato)
		alpha := logica.RequireIntEnv("ALPHA", 3)
		seedSample := logica.RequireIntEnv("SEEDSAMPLE", 10)
		iters := logica.RequireIntEnv("JOIN_ITERS", 3)

		//------------------Faccio durare JoinAndExpandLite al massimo 20 sec---------------------

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		if err := logica.JoinAndExpandLite(ctx, "node1:8000", nodeID, alpha, seedSample, iters); err != nil {
			log.Printf("[bootstrap %s] JoinAndExpandLite warning: %v", nodeID, err)
		}

		log.Printf("[bootstrap %s] join completato ✅", nodeID)

		select {} // blocca per sempre

	}

}
