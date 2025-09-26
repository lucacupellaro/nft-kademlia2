package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"kademlia-nft/common"
	"kademlia-nft/internal/ui"
	pb "kademlia-nft/proto/kad"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ---------- CONFIG ----------
const (
	composeProject = "kademlia-nft"
	defaultEnvPath = ".env"
	defaultOutCSV  = "results/lookup.csv"
	maxHopsDefault = 30
)

// Pair deve essere lo stesso tipo che usi già (esa=id hex, hash=node name)
type Pair = ui.Pair

func main() {
	csvPath := flag.String("csv", "", "Percorso CSV con colonna Name (obbligatorio)")
	envPath := flag.String("env", defaultEnvPath, "Percorso del file .env")
	outCSV := flag.String("out", defaultOutCSV, "Output CSV (overwrite)") // <-- descrizione aggiornata
	maxHops := flag.Int("maxhops", maxHopsDefault, "Max hop per lookup")
	flag.Parse()

	if *csvPath == "" {
		log.Fatal("Devi specificare -csv /percorso/collections.csv (con colonna 'Name').")
	}

	// carica parametri da .env (N, BUCKET_SIZE, REPLICATION_FACTOR)
	env := readEnvFile(*envPath)
	N := firstInt(env["N"], 0)
	bucket := firstInt(env["BUCKET_SIZE"], 0)
	repl := firstInt(env["REPLICATION_FACTOR"], 0)

	// lista nodi attivi e reverse map
	nodes, err := ui.ListActiveComposeServices(composeProject)
	if err != nil {
		log.Fatalf("Errore recupero nodi: %v", err)
	}
	if len(nodes) == 0 {
		log.Fatal("Nessun nodo attivo trovato.")
	}
	reverse, err := ui.Reverse2(nodes)
	if err != nil {
		log.Fatalf("Errore Reverse2: %v", err)
	}

	// === APERTURA CSV in modalità OVERWRITE (truncate) ===
	outF, err := os.Create(*outCSV) // tronca/ricrea il file ad ogni run
	if err != nil {
		log.Fatalf("Impossibile creare %s: %v", *outCSV, err)
	}
	defer outF.Close()

	w := csv.NewWriter(outF)
	defer w.Flush()

	// header (sempre una volta)
	if err := w.Write([]string{
		"nodePartenza", "NameNft", "Hop", "NumeroNodi", "repliche", "KsizeBucket", "time_ms",
	}); err != nil {
		log.Fatalf("Scrittura header fallita: %v", err)
	}

	// leggi i Name dal CSV di input
	names, err := readNames(*csvPath)
	if err != nil {
		log.Fatalf("Errore lettura CSV input: %v", err)
	}
	if len(names) == 0 {
		log.Fatalf("Nel CSV %s non ho trovato la colonna 'Name' con valori.", *csvPath)
	}

	rand.Seed(time.Now().UnixNano())

	// loop sulle lookup
	for i, name := range names {
		candidates := make([]string, 0, len(nodes))
		for _, n := range nodes {
			if !strings.EqualFold(strings.TrimSpace(n), "node1") {
				candidates = append(candidates, n)
			}
		}
		if len(candidates) == 0 {
			log.Fatal("Nessun nodo disponibile per la partenza dopo aver escluso node1.")
		}

		// dentro il loop
		start := candidates[rand.Intn(len(candidates))] // nodo casuale (senza node1)
		fmt.Printf("[%d/%d] %s  (start=%s)\n", i+1, len(names), name, start)

		t0 := time.Now()
		hops, found, err := LookupNFTOnNodeByNameStats(start, reverse, name, *maxHops)
		elapsed := time.Since(t0).Milliseconds()

		if err != nil {
			fmt.Printf("  ✖ errore lookup: %v\n", err)
			_ = w.Write([]string{
				start,
				name,
				"-1", // errore => hops -1
				strconv.Itoa(N),
				strconv.Itoa(repl),
				strconv.Itoa(bucket),
				strconv.FormatInt(elapsed, 10),
			})
			w.Flush()
			continue
		}

		if !found {
			fmt.Println("  ✖ non trovato")
			_ = w.Write([]string{
				start,
				name,
				"-1", // non trovato => hops -1
				strconv.Itoa(N),
				strconv.Itoa(repl),
				strconv.Itoa(bucket),
				strconv.FormatInt(elapsed, 10),
			})
		} else {
			fmt.Printf("  ✅ trovato in %d hop, %d ms\n", hops, elapsed)
			_ = w.Write([]string{
				start,
				name,
				strconv.Itoa(hops),
				strconv.Itoa(N),
				strconv.Itoa(repl),
				strconv.Itoa(bucket),
				strconv.FormatInt(elapsed, 10),
			})
		}
		w.Flush()
	}

	fmt.Printf("✅ Finito. Risultati in: %s\n", *outCSV)
}

// ---------- Lettura nomi dal CSV (colonna Name) ----------
func readNames(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(bufio.NewReader(f))
	r.FieldsPerRecord = -1 // variabile
	rows, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, errors.New("CSV vuoto")
	}

	// trova indice colonna "Name" (case-insensitive)
	header := rows[0]
	col := -1
	for i, h := range header {
		if strings.EqualFold(strings.TrimSpace(h), "Name") {
			col = i
			break
		}
	}
	if col < 0 {
		return nil, fmt.Errorf("colonna 'Name' non trovata. Header: %v", header)
	}

	var names []string
	for _, row := range rows[1:] {
		if col >= len(row) {
			continue
		}
		n := strings.TrimSpace(row[col])
		if n != "" {
			names = append(names, n)
		}
	}
	return names, nil
}

// ---------- .env parsing ----------
func readEnvFile(path string) map[string]string {
	out := map[string]string{}
	f, err := os.Open(path)
	if err != nil {
		return out // silenzioso: useremo zero/default
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if i := strings.IndexByte(line, '='); i > 0 {
			k := strings.TrimSpace(line[:i])
			v := strings.TrimSpace(line[i+1:])
			out[k] = v
		}
	}
	return out
}

func firstInt(s string, def int) int {
	if s == "" {
		return def
	}
	if n, err := strconv.Atoi(s); err == nil {
		return n
	}
	return def
}

// ======================================================================
// Versione della tua lookup che RITORNA hop e found (nessuna RPC aggiuntiva)
// ======================================================================
func LookupNFTOnNodeByNameStats(startNode string, str []Pair, nftName string, maxHops int) (hops int, found bool, err error) {
	if maxHops <= 0 {
		maxHops = 15
	}

	nftID20 := common.Sha1ID(nftName)

	// visitati per ID (hex, lowercase) e per nome (lowercase)
	visitedIDs := make(map[string]bool)
	visitedNames := make(map[string]bool)

	// --- inizializza lo start node come visitato (sia nome sia ID) ---
	startNameLC := strings.ToLower(strings.TrimSpace(startNode))
	startIDHex := strings.ToLower(hex.EncodeToString(common.Sha1ID(startNode)))
	visitedNames[startNameLC] = true
	visitedIDs[startIDHex] = true

	// hop 0: risolvi solo lo start node
	hostPort, err := ui.ResolveStartHostPort(startNode)
	if err != nil {
		return 0, false, fmt.Errorf("risoluzione %q fallita: %w", startNode, err)
	}
	currentLabel := startNode

	for hop := 0; hop < maxHops; hop++ {
		hops = hop + 1
		fmt.Printf("🔎 Hop %d: cerco '%s' su %s (%s)\n", hops, nftName, currentLabel, hostPort)

		conn, err := grpc.Dial(hostPort, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return hops, false, fmt.Errorf("dial fallito %s: %w", hostPort, err)
		}
		client := pb.NewKademliaClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		resp, rpcErr := client.LookupNFT(ctx, &pb.LookupNFTReq{
			FromId: "CLI",
			Key:    &pb.Key{Key: nftID20},
		})
		cancel()
		_ = conn.Close()
		if rpcErr != nil {
			return hops, false, fmt.Errorf("RPC fallita su %s: %w", currentLabel, rpcErr)
		}

		if resp.GetFound() {
			return hops, true, nil
		}

		nearest := resp.GetNearest()
		if len(nearest) == 0 {
			return hops, false, nil
		}

		// candidata da interrogare
		type cand struct {
			idHex string // id esadecimale (lowercase)
			addr  string // host:port
			name  string // "nodeX" se ricavabile (altrimenti "")
			label string // per log
		}
		usabili := make([]cand, 0, len(nearest))

		for _, n := range nearest {
			id := strings.ToLower(strings.TrimSpace(n.GetId()))
			if id == "" {
				continue
			}
			if visitedIDs[id] {
				// già visto per ID
				continue
			}

			var addr, label, name string
			host := strings.TrimSpace(n.GetHost())
			port := int(n.GetPort())
			if host != "" && port > 0 {
				addr = fmt.Sprintf("%s:%d", host, port)
				label = host
			} else {
				// fallback: mappa ID -> "nodeX"
				if nm := ui.Check(id, str); nm != "NOTFOUND" {
					name = nm
					if hp, e := ui.ResolveStartHostPort(nm); e == nil {
						addr = hp
						label = nm + " (fallback)"
					}
				}
			}

			// se ho ricavato il nome, evita duplicati anche per nome
			if name != "" && visitedNames[strings.ToLower(name)] {
				continue
			}
			if addr == "" {
				continue
			}

			usabili = append(usabili, cand{
				idHex: id,
				addr:  addr,
				name:  name,
				label: label,
			})
		}

		if len(usabili) == 0 {
			return hops, false, nil
		}

		// scegli il più vicino via XOR
		ids := make([]string, len(usabili))
		for i, c := range usabili {
			ids[i] = c.idHex
		}
		bestID, selErr := ui.SceltaNodoPiuVicino(nftID20, ids)
		if selErr != nil {
			bestID = ids[0]
		}
		bestID = strings.ToLower(strings.TrimSpace(bestID))

		// trova l'endpoint del best
		var next cand
		for _, c := range usabili {
			if c.idHex == bestID {
				next = c
				break
			}
		}
		if next.addr == "" {
			// ultimo tentativo via mapping
			if nm := ui.Check(bestID, str); nm != "NOTFOUND" {
				if hp, e := ui.ResolveStartHostPort(nm); e == nil {
					next.addr = hp
					next.name = nm
					next.label = nm + " (fallback2)"
				}
			}
		}
		if next.addr == "" {
			return hops, false, nil
		}

		// **marca visitati sia ID sia nome (se disponibile/ricavabile)**
		visitedIDs[bestID] = true
		if next.name == "" {
			// prova a ricavarlo comunque dal mapping, così eviti doppi passaggi futuri
			if nm := ui.Check(bestID, str); nm != "NOTFOUND" {
				next.name = nm
			}
		}
		if next.name != "" {
			visitedNames[strings.ToLower(next.name)] = true
		}

		// prepara il prossimo hop
		hostPort = next.addr
		if next.label != "" {
			currentLabel = next.label
		} else if next.name != "" {
			currentLabel = next.name
		} else {
			// etichetta di ripiego: id troncato
			if len(bestID) > 8 {
				currentLabel = bestID[:8]
			} else {
				currentLabel = bestID
			}
		}
	}

	return maxHops, false, nil
}
