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

		//fissato nft quindi per ogni nft , creo unafunzione che per ogni nft scorre tutti e gli id dei nodi e li assegna ai 2 piu vicini)

		fmt.Println("Assegnazione dei k nodeID più vicini agli NFT...")

		rows := csvAll[1:] // salta header
		nfts := make([]logica.NFT, 0, len(rows))

		for i, row := range rows {

			if len(row) < 17 {
				continue
			} // safety

			name := strings.TrimSpace(row[1])
			//key := logica.NewIDFromToken(name, 20) // ID dal Name (come vuoi tu)

			col := func(k int) string {
				if k >= 0 && k < len(row) {
					return strings.TrimSpace(row[k])
				}
				return ""
			}

			assigned := logica.ClosestNodesForNFTWithDir(listNFTId[i], dir, 2)
			var nodiSelected []string
			nodiSelected = append(nodiSelected, assigned[0].Key, assigned[1].Key)

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
			nodi = append(nodi, nfts[j].AssignedNodesToken[0])
			nodi = append(nodi, nfts[j].AssignedNodesToken[1])

			if err := logica.StoreNFTToNodes(nfts[j], nfts[j].TokenID, nfts[j].Name, nodi, 24*3600); err != nil {
				fmt.Println("Errore:", err)
				continue
			}

			//fmt.Printf("Salvati NFT numero: %d\n", j)

			nodi = nil

		}

		select {} // blocca per sempre

	} else {

		var nodes []string
		var TokenNodo []byte
		var Bucket [][]byte
		var BucketSort [][]byte

		//-------------------I container si mettono in ascolto qui-------------------//

		nodeID := os.Getenv("NODE_ID")
		if nodeID == "" {
			nodeID = "default"
		}

		TokenNodo = common.Sha1ID(nodeID)

		//fmt.Printf("Sono il nodo %s, PID: %d\n", logica.DecodeID(TokenNodo), os.Getpid())

		//---------Recuperlo la lista dei nodi chiedendola al Seeder-------------------------
		nodes, err := logica.GetNodeListIDs("node1:8000", os.Getenv("NODE_ID"))

		if err != nil {
			log.Fatalf("Errore recupero nodi dal seeder: %v", err)
		}

		s := strings.Join(nodes, ",")

		var dir *logica.ByteMapping

		parts := strings.Split(s, ",")

		dir = logica.BuildByteMappingSHA1(parts)

		/*
			var nodiTokenizati [][]byte
			for i := 0; i < len(nodes); i++ {
				nodiTokenizati = append(nodiTokenizati, logica.NewIDFromToken(nodes[i], 20))
			}
		*/

		//--------------------Ogni container si trova i k bucket piu vicini e li salva nel proprio volume-------------------//

		Bucket = logica.AssignNFTToNodes(TokenNodo, dir.IDs, 7)

		BucketSort = logica.RemoveAndSortMe(Bucket, TokenNodo)

		//fmt.Printf("sto salvando il kbucket per il nodo,%s\n", logica.DecodeID(TokenNodo))

		err2 := logica.SaveKBucket(nodeID, BucketSort, "/data/kbucket.json")

		if err2 != nil {
			log.Fatalf("Errore salvataggio K-bucket: %v", err)
		}

		select {} // blocca per sempre

	}

}
