package main

import (
	"bufio"
	"context"
	"fmt"
	"kademlia-nft/common"
	"kademlia-nft/internal/ui"
	"kademlia-nft/logica"
	"log"
	"os"
	"strconv"
	"strings"
)

func main() {

	choice := ui.ShowWelcomeMenu()
	fmt.Println("Hai scelto:", choice)

	if choice == 1 {
		fmt.Printf("Hai scelto l'opzione 1\n")
		nodi, err := ui.ListActiveComposeServices("kademlia-nft")
		if err != nil {
			log.Fatal("Errore recupero nodi:", err)
		}

		fmt.Println("Container attivi:")
		for _, n := range nodi {
			fmt.Println(" -", n)
		}
	}

	if choice == 2 {
		fmt.Printf("Hai scelto l'opzione 2. PING\n")

		nodii, err := ui.ListActiveComposeServices("kademlia-nft")
		if err != nil {
			log.Fatal("Errore recupero nodi:", err)
		}

		//ora ho nome dei nodi e hash
		out, err := ui.Reverse2(nodii)
		if err != nil {
			log.Fatal("Errore Reverse2:", err)
		}

		/*


			fmt.Printf("Da quale nodo vuoi fare il PING?\n")

			nodi, err := ui.ListActiveComposeServices("kademlia-nft")
			if err != nil {
				log.Fatal("Errore recupero nodi:", err)
			}
			fmt.Println("Container attivi:")
			for _, n := range nodi {
				fmt.Println(" -", n)
			}

			fmt.Printf("Verso quale nodo vuoi fare il PING?\n")
			fmt.Println("Container attivi:")
			for _, n := range nodi {
				fmt.Println(" -", n)
			}
		*/

		ui.PingNode("node7", "node8", out)

	}

	if choice == 3 {
		var nodi []string
		nodi, err := ui.ListActiveComposeServices("kademlia-nft")
		if err != nil {
			log.Fatal("Errore recupero nodi:", err)
		}
		fmt.Println("Container attivi:")
		for _, n := range nodi {
			fmt.Println(" -", n)
		}

		fmt.Println("Da quale nodo vuoi far partire la simulazione?")
		fmt.Println("Per selezionare un nodo, usa il comando 'use <nome-nodo>'")
		nodoScelto := bufio.NewReader(os.Stdin)
		line, _ := nodoScelto.ReadString('\n')
		line = strings.TrimSpace(line)

		fmt.Println("Hai scelto il nodo:", line)

		fmt.Println("Quale Nft vuoi cercare?")
		nftScelto := bufio.NewReader(os.Stdin)
		line1, _ := nftScelto.ReadString('\n')
		line1 = strings.TrimSpace(line1)

		fmt.Println("Hai scelto il NFT:", line1)

		//fmt.Printf("%x", key)

		//------------------------Inizia la ricerca dell'NFT-------------------------------------------//
		//node := "nodo3"
		//name := "Lift-off Pass"

		nodii, err := ui.ListActiveComposeServices("kademlia-nft")
		if err != nil {
			log.Fatal("Errore recupero nodi:", err)
		}

		//ora ho nome dei nodi e hash
		out, err := ui.Reverse2(nodii)
		fmt.Println(out)
		if err != nil {
			log.Fatal("Errore Reverse2:", err)
		}
		//fmt.Printf("Nodi: %v\n", out)

		if err := ui.LookupNFTOnNodeByName(line, out, line1, 30); err != nil {
			fmt.Println("Errore:", err)
		}

	}
	if choice == 4 {

		var nodi []string

		fmt.Println("Aggiungi un NFT")
		nodoScelto := bufio.NewReader(os.Stdin)
		line, _ := nodoScelto.ReadString('\n')
		line = strings.TrimSpace(line)
		fmt.Println("Hai scelto il NFT:", line)

		nodi, err := ui.ListActiveComposeServices("kademlia-nft")
		if err != nil {
			log.Fatal("Errore recupero nodi:", err)
		}
		fmt.Println("Container attivi:")
		for _, n := range nodi {
			fmt.Println(" -", n)
		}

		logica.RemoveNode1(&nodi)

		//faccio il mapping dei nodi
		var dir *logica.ByteMapping

		dir = logica.BuildByteMappingSHA1(nodi)

		key := common.Sha1ID(line)

		assigned := logica.ClosestNodesForNFTWithDir(key, dir, 2)
		var nodiSelected []string
		nodiSelected = append(nodiSelected, assigned[0].Key, assigned[1].Key)

		nfts := make([]logica.NFT, 0, 1)
		nfts = append(nfts, logica.NFT{
			Index:             "col(0)",
			Name:              line,
			Volume:            "col(2)",
			Volume_USD:        "col(3)",
			Market_Cap:        "col(4)",
			Market_Cap_USD:    "col(5)",
			Sales:             "col(6)",
			Floor_Price:       "col(7)",
			Floor_Price_USD:   "col(8)",
			Average_Price:     "col(9)",
			Average_Price_USD: "col(10)",
			Owners:            "col(11)",
			Assets:            "col(12)",
			Owner_Asset_Ratio: "col(13)",
			Category:          "col(14)",
			Website:           "col(15)",
			Logo:              "col(16)",

			TokenID:            key,
			AssignedNodesToken: nodiSelected,
		})

		fmt.Printf("sto salvando nft %s nei nodi: %s,%s\n", nfts[0].Name, nfts[0].AssignedNodesToken[0], nfts[0].AssignedNodesToken[1])
		var nodiii []string
		nodiii = append(nodiii, nfts[0].AssignedNodesToken[0])
		nodiii = append(nodiii, nfts[0].AssignedNodesToken[1])

		if err := ui.StoreNFTToNodes2(nfts[0], nfts[0].TokenID, nfts[0].Name, nodiii, 24*3600); err != nil {
			fmt.Println("Errore:", err)

		}

	}
	if choice == 5 {

		var nodi []string
		var biggerNode string
		var n int

		fmt.Println("Aggiungo un nuovo nodo")

		nodi, err := ui.ListActiveComposeServices("kademlia-nft")
		if err != nil {
			log.Fatal("Errore recupero nodi:", err)
		}

		fmt.Println("Container attivi:")
		for _, n := range nodi {
			fmt.Println(" -", n)
		}

		biggerNode, n = ui.BiggerNodes(nodi)

		fmt.Println("Il nodo più grande è:", biggerNode, "con numero:", n)

		n2 := 8000 + n

		ctx := context.Background()

		if err := ui.AddNode(ctx, biggerNode, "node1:8000", strconv.Itoa(n2)); err != nil {
			fmt.Println("Errore:", err)
			os.Exit(1)
		}
	}
	if choice == 6 {

		fmt.Println("Rebalancing della risorse")
		nodi, err := ui.ListActiveComposeServices("kademlia-nft")
		if err != nil {
			log.Fatal("Errore recupero nodi:", err)
		}

		fmt.Println("Container attivi:")
		for _, n := range nodi {
			fmt.Println(" -", n)
		}

		fmt.Println("Rebalancing della risorse per il nodo,", "node6")

		logica.RemoveNode1(&nodi)

		//faccio il mapping dei nodi
		//var dir *logica.ByteMapping

		//dir = logica.BuildByteMappingSHA1(nodi)

		targetAddr := "localhost:8006"
		//targetID := logica.Sha1ID(targetAddr) // ID SHA1 del nodo4
		activeNodes := nodi // es: ["node1:8000", "node2:8000", ...]

		err = ui.RebalanceNode(targetAddr, "node6", activeNodes, 2)
		if err != nil {
			log.Fatal(err)
		}
	}
	if choice == 7 {

		fmt.Println("Hai scelto rimozione nodo")
		fmt.Println("Scegli il nodo da rimuovere")

		nodi, err := ui.ListActiveComposeServices("kademlia-nft")
		if err != nil {
			log.Fatal("Errore recupero nodi:", err)
		}

		fmt.Println("Container attivi:")
		for _, n := range nodi {
			fmt.Println(" -", n)
		}

		err = ui.RemoveNode("node10")
		if err != nil {
			fmt.Println("Errore:", err)
		}

	}

}
