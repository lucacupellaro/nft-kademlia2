package main

import (
	"fmt"
	"kademlia-nft/common"
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

		repFactor := logica.RequireIntEnv("REPLICATION_FACTOR")

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

		fmt.Printf("NFT 0z %s\n", colName[0])

		listNFTId := logica.GenerateBytesOfAllNftsSHA1(colName)
		fmt.Printf("Primo NFT: %s\n", colName[0])
		fmt.Printf("Primo ID  : %x\n", listNFTId[0])

		fmt.Printf("NFT id %x:\n", listNFTId[0])

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
		if err := logica.SaveByteMappingJSON(out, dir); err != nil {
			log.Fatalf("Errore salvataggio byte_mapping.json: %v", err)
		}
		fmt.Printf("✅ File salvato in: %s\n", out)

		// per ogni nft , creo unafunzione che per ogni nft scorre tutti e gli id dei nodi e li assegna ai 2 piu vicini)

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
			//nodiSelected = append(nodiSelected, assigned[0].Key, assigned[1].Key)

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
			//nodi = append(nodi, nfts[j].AssignedNodesToken[0])
			//nodi = append(nodi, nfts[j].AssignedNodesToken[1])

			if err := logica.StoreNFTToNodes(nfts[j], nfts[j].TokenID, nfts[j].Name, nodi, 24*3600); err != nil {
				fmt.Println("Errore:", err)
				continue
			}

			nodi = nil

		}

		select {} // blocca per sempre

	} else {

		//------------------- Bootstrap K-Bucket robusto -------------------//

		dataDir := "/data"
		if err := os.MkdirAll(dataDir, 0o755); err != nil {
			log.Fatalf("Impossibile creare %s: %v", dataDir, err)
		}

		nodeID := strings.TrimSpace(os.Getenv("NODE_ID"))
		if nodeID == "" {
			log.Fatalf("NODE_ID non impostato")
		}
		tokenNodo := common.Sha1ID(nodeID)

		bucketSize := logica.RequireIntEnv("BUCKET_SIZE")
		if bucketSize <= 0 {
			bucketSize = 5
		}
		fmt.Printf("Bucket size: %d\n", bucketSize)

		// retry: aspetta che il seeder sia pronto e che la lista nodi sia “sufficiente”

		var dir *logica.ByteMapping
		retryMax := 60 // ~60 secondi
		for attempt := 1; attempt <= retryMax; attempt++ {
			nl, err := logica.GetNodeListIDs("node1:8000", nodeID)
			if err != nil {
				log.Printf("[bootstrap %s] get node list: tentativo %d/%d: %v", nodeID, attempt, retryMax, err)
				time.Sleep(1 * time.Second)
				continue
			}

			// filtra, normalizza, de-duplica
			seen := make(map[string]bool)
			clean := make([]string, 0, len(nl))
			for _, n := range nl {
				n = strings.TrimSpace(n)
				if n == "" || n == nodeID || seen[n] {
					continue
				}
				seen[n] = true
				clean = append(clean, n)
			}

			if len(clean) == 0 {
				log.Printf("[bootstrap %s] lista nodi vuota (tentativo %d/%d) — retry", nodeID, attempt, retryMax)
				time.Sleep(1 * time.Second)
				continue
			}

			dir = logica.BuildByteMappingSHA1(clean)
			if dir == nil || len(dir.List) == 0 {
				log.Printf("[bootstrap %s] mapping nullo (tentativo %d/%d) — retry", nodeID, attempt, retryMax)
				time.Sleep(1 * time.Second)
				continue
			}

			// ok, abbiamo una directory sensata
			nodes := clean
			fmt.Printf("[bootstrap %s] lista nodi ottenuta (%d nodi) dopo %d tentativi\n", nodeID, len(nodes), attempt)
			break
		}
		if dir == nil {
			log.Fatalf("[bootstrap %s] impossibile ottenere directory nodi dopo %d tentativi", nodeID, retryMax)
		}

		// calcola i più vicini (cap su bucketSize e nodi disponibili)
		if bucketSize > len(dir.List) {
			bucketSize = len(dir.List)
		}
		picks := logica.ClosestNodesForNFTWithDir(tokenNodo, dir, bucketSize)
		if len(picks) == 0 {
			log.Printf("[bootstrap %s] nessun vicino calcolato; riprovo tra 1s", nodeID)
			time.Sleep(1 * time.Second)
			// una sola riprova “soft”
			picks = logica.ClosestNodesForNFTWithDir(tokenNodo, dir, bucketSize)
		}

		bucket := make([][]byte, 0, len(picks))
		for _, p := range picks {
			if len(p.SHA) == 20 { // SHA1 length
				bucket = append(bucket, p.SHA)
			}
		}

		// rimuovi eventuale self e ordina (se la tua RemoveAndSortMe lo fa)
		bucket = logica.RemoveAndSortMe(bucket, tokenNodo)

		// salva kbucket.json
		if err := logica.SaveKBucket(nodeID, bucket, filepath.Join(dataDir, "kbucket.json")); err != nil {
			log.Fatalf("Errore salvataggio K-bucket: %v", err) // <-- usa err giusto
		}

		log.Printf("[bootstrap %s] kbucket salvato con %d vicini", nodeID, len(bucket))

		select {} // blocca per sempre

	}

}
