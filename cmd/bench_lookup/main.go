package main

import (
	"bufio"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"kademlia-nft/test"
	"log"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"kademlia-nft/internal/ui"
)

// ---------- CONFIG ----------
const (
	composeProject = "kademlia-nft"
	defaultEnvPath = ".env"
	defaultOutCSV  = "results/lookup.csv"
	maxHopsDefault = 30
	defaultSummary = "results/nRuns.csv"
	waitReadyMax   = 40 * time.Second // attesa massima servizi dopo il rebuild
	waitProbeEvery = 700 * time.Millisecond
	csvNFT         = "csv/NFT_Top_Collections.csv"
	outSummary     = "results/nodiPlus.csv"
	maxHops        = 30
)

type Pair = ui.Pair

type RunStats struct {
	TotalNFTs  int
	Found      int
	NotFound   int
	AvgHops    float64
	MaxHops    int
	NNodes     int
	BucketSize int
	ReplFactor int
	Iteration  int
}

// ------------------------------------------------------------

func main() {
	a := 3
	if a == 3 {
		// carica i nomi NFT
		names, err := readNames(csvNFT)
		if err != nil {
			log.Fatalf("Errore lettura CSV NFT: %v", err)
		}
		if len(names) == 0 {
			log.Fatal("CSV NFT vuoto")
		}

		// nodo attivo + reverse
		nodes, err := ui.ListActiveComposeServices(composeProject)
		if err != nil {
			log.Fatalf("Errore lista nodi: %v", err)
		}
		if len(nodes) == 0 {
			log.Fatal("Nessun nodo attivo")
		}
		reverse, err := ui.Reverse2(nodes)
		if err != nil {
			log.Fatalf("Errore Reverse2: %v", err)
		}

		// scegli un NFT qualunque da testare
		nftName := names[0]
		fmt.Printf("NFT scelto per il test concorrente: %s\n", nftName)

		test.ConcurrentLookupTest(nodes, reverse, nftName, maxHops)
		return

	}
	if a == 1 {
		names, err := readNames(csvNFT)
		if err != nil {
			log.Fatalf("Errore lettura CSV NFT: %v", err)
		}

		_ = os.MkdirAll("results", 0o755)

		// apri file di output
		f, err := os.Create(outSummary)
		if err != nil {
			log.Fatalf("Impossibile creare %s: %v", outSummary, err)
		}
		defer f.Close()
		w := csv.NewWriter(f)
		defer w.Flush()

		// header
		_ = w.Write([]string{"iterazione", "numero_nodi", "kbucket", "nHops_medio", "stdHops", "NFT_trovati", "NFT_non_trovati"})

		w.Flush()

		iter := 0
		for N := 10; N <= 20; N++ {
			iter++
			kb := N/2 + 2

			fmt.Printf("\n=== Iterazione %d: N=%d, KB=%d ===\n", iter, N, kb)

			// aggiorna .env
			if err := setEnvVar(defaultEnvPath, "N", strconv.Itoa(N)); err != nil {
				log.Fatalf("Errore set N: %v", err)
			}
			if err := setEnvVar(defaultEnvPath, "BUCKET_SIZE", strconv.Itoa(kb)); err != nil {
				log.Fatalf("Errore set BUCKET_SIZE: %v", err)
			}

			// rigenera docker-compose.yml
			if out, err := runCmd("bash", "generate-compose.sh"); err != nil {
				log.Fatalf("generate-compose.sh fallito: %v\n%s", err, out)
			}

			// rebuild cluster
			if out, err := composeUpBuild(composeProject); err != nil {
				log.Fatalf("docker compose up fallito: %v\n%s", err, out)
			}

			// lista nodi attivi
			nodes, err := ui.ListActiveComposeServices(composeProject)
			if err != nil {
				log.Fatalf("Errore lista nodi: %v", err)
			}
			if len(nodes) == 0 {
				log.Fatal("Nessun nodo attivo dopo il rebuild")
			}
			reverse, err := ui.Reverse2(nodes)
			if err != nil {
				log.Fatalf("Errore Reverse2: %v", err)
			}

			// attesa readiness
			if err := waitClusterReady(nodes, waitReadyMax); err != nil {
				log.Printf("⚠️ Cluster forse non pronto: %v", err)
			}
			trovati := 0
			nonTrovati := 0

			// run completo
			hops := []int{}
			for i, name := range names {
				start := test.RandomNode(nodes)
				h, found, err := test.LookupNFTOnNodeByNameStats(start, reverse, name, maxHops)
				if err != nil || !found {
					nonTrovati++
					continue
				}
				trovati++
				hops = append(hops, h)
				fmt.Printf("[%d/%d] %s trovato in %d hop\n", i+1, len(names), name, h)
			}

			mean, std := test.MeanStd(hops)

			_ = w.Write([]string{
				strconv.Itoa(iter),
				strconv.Itoa(N),
				strconv.Itoa(kb),
				fmt.Sprintf("%.4f", mean),
				fmt.Sprintf("%.4f", std),
				strconv.Itoa(trovati),
				strconv.Itoa(nonTrovati),
			})
			w.Flush()

			fmt.Printf(">>> Risultati: avgHops=%.3f, stdHops=%.3f (su %d lookup)\n", mean, std, len(hops))
		}

		fmt.Printf("\n🏁 Test completato. Risultati in %s\n", outSummary)

	} else {

		csvPath := flag.String("csv", "", "Percorso CSV con colonna Name (obbligatorio)")
		envPath := flag.String("env", defaultEnvPath, "Percorso del file .env")

		// file dettagli (per-run). Viene sovrascritto ad ogni iterazione
		outCSV := flag.String("out", defaultOutCSV, "Output CSV dettagli singola iterazione (overwrite)")

		// file riassunto multi-run
		outSummary := flag.String("outSummary", defaultSummary, "Output CSV riassunto multi-run")

		maxHops := flag.Int("maxhops", maxHopsDefault, "Max hop per lookup")

		bucketFrom := flag.Int("bucketFrom", 4, "Bucket size iniziale")
		bucketTo := flag.Int("bucketTo", -1, "Bucket size finale (default: N dal .env)")

		// se vuoi disattivare il rebuild each-iteration, passa -noRebuild
		noRebuild := flag.Bool("noRebuild", false, "Se true NON ricostruisce i container ad ogni iterazione")

		flag.Parse()

		if *csvPath == "" {
			log.Fatal("Devi specificare -csv /percorso/collections.csv (con colonna 'Name').")
		}

		// carica parametri da .env (N, BUCKET_SIZE, REPLICATION_FACTOR)
		env := readEnvFile(*envPath)
		N := firstInt(env["N"], 0)
		currentBucket := firstInt(env["BUCKET_SIZE"], 0)
		repl := firstInt(env["REPLICATION_FACTOR"], 0)

		// bucketTo default = N (se non dato)
		if *bucketTo < 0 {
			*bucketTo = N
		}
		if *bucketFrom <= 0 {
			*bucketFrom = 4
		}
		if *bucketFrom > *bucketTo {
			log.Fatalf("bucketFrom (%d) > bucketTo (%d)", *bucketFrom, *bucketTo)
		}

		// prepara cartella results
		_ = os.MkdirAll("results", 0o755)

		// apri il CSV di riassunto (append se già esiste, altrimenti crea e scrive header)
		summaryExists := fileExists(*outSummary)
		sumF, err := os.OpenFile(*outSummary, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			log.Fatalf("Impossibile aprire %s: %v", *outSummary, err)
		}
		defer sumF.Close()
		sumW := csv.NewWriter(sumF)
		defer sumW.Flush()

		if !summaryExists {
			_ = sumW.Write([]string{
				"iterazione",
				"NFT",
				"NFT NON TROVATI",
				"MEDIA HOP FATTI",
				"NFT TROVATI",
				"MAX HOPS",
				"NODI",
				"BUCKETSIZE",
				"FATTORE DI REPLICAZIONI",
			})
			sumW.Flush()
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

		iteration := 0
		for b := *bucketFrom; b <= *bucketTo; b++ {
			iteration++

			// 0) aggiorna BUCKET_SIZE nel .env
			if err := setEnvVar(*envPath, "BUCKET_SIZE", strconv.Itoa(b)); err != nil {
				log.Printf("⚠️ impossibile aggiornare BUCKET_SIZE nel .env: %v", err)
			} else {
				currentBucket = b
			}

			// 1) rebuild cluster (se non disattivato)
			if !*noRebuild {
				log.Printf("🔧 Iter %d — docker compose up -d --build --force-recreate (K=%d)...", iteration, b)
				if out, err := composeUpBuild(composeProject); err != nil {
					log.Fatalf("docker compose up --build fallito: %v\n%s", err, out)
				} else if strings.TrimSpace(out) != "" {
					fmt.Print(out)
				}
			}

			// 2) ricarica lista nodi attivi e reverse map
			nodes, err := ui.ListActiveComposeServices(composeProject)
			if err != nil {
				log.Fatalf("Errore recupero nodi: %v", err)
			}
			if len(nodes) == 0 {
				log.Fatal("Nessun nodo attivo trovato dopo il rebuild.")
			}
			reverse, err := ui.Reverse2(nodes)
			if err != nil {
				log.Fatalf("Errore Reverse2: %v", err)
			}

			// 3) attesa readiness dei servizi (porta gRPC raggiungibile)
			if err := waitClusterReady(nodes, waitReadyMax); err != nil {
				log.Printf("⚠️ cluster forse non ancora pronto: %v (procedo comunque)", err)
			}

			// 4) esegui la run completa (dettaglio per NFT)
			stats, err := oneRun(names, nodes, reverse, *outCSV, *maxHops, N, currentBucket, repl)
			if err != nil {
				log.Fatalf("Errore run (bucket=%d): %v", b, err)
			}
			stats.Iteration = iteration

			// 5) scrivi riga riassuntiva
			_ = sumW.Write([]string{
				strconv.Itoa(stats.Iteration),
				strconv.Itoa(stats.TotalNFTs),
				strconv.Itoa(stats.NotFound),
				fmt.Sprintf("%.6f", stats.AvgHops),
				strconv.Itoa(stats.Found),
				strconv.Itoa(stats.MaxHops),
				strconv.Itoa(stats.NNodes),
				strconv.Itoa(stats.BucketSize),
				strconv.Itoa(stats.ReplFactor),
			})
			sumW.Flush()

			fmt.Printf("✅ Iterazione %d (BUCKET_SIZE=%d): trovati=%d, non_trovati=%d, avg_hops=%.3f, max_hops=%d\n",
				stats.Iteration, b, stats.Found, stats.NotFound, stats.AvgHops, stats.MaxHops)
		}

		fmt.Printf("🏁 Completato. Riassunto: %s\n", *outSummary)

	}

}

// ------------------------------------------------------------

func oneRun(names []string, nodes []string, reverse []Pair, outCSV string, maxHops int, N int, bucket int, repl int) (RunStats, error) {
	rs := RunStats{
		TotalNFTs:  len(names),
		Found:      0,
		NotFound:   0,
		AvgHops:    0,
		MaxHops:    0,
		NNodes:     N,
		BucketSize: bucket,
		ReplFactor: repl,
	}

	// apri CSV dettagli per questa iterazione (overwrite)
	if err := os.MkdirAll(filepath.Dir(outCSV), 0o755); err != nil {
		return rs, err
	}
	outF, err := os.Create(outCSV)
	if err != nil {
		return rs, fmt.Errorf("impossibile creare %s: %w", outCSV, err)
	}
	defer outF.Close()

	w := csv.NewWriter(outF)
	defer w.Flush()

	// header
	_ = w.Write([]string{
		"nodePartenza", "NameNft", "Hop", "NumeroNodi", "repliche", "KsizeBucket", "time_ms",
	})
	w.Flush()

	// scegli nodi partenza (escludo node1 come da tuo codice)
	candidates := make([]string, 0, len(nodes))
	for _, n := range nodes {
		if !strings.EqualFold(strings.TrimSpace(n), "node1") {
			candidates = append(candidates, n)
		}
	}
	if len(candidates) == 0 {
		return rs, errors.New("nessun nodo disponibile per la partenza dopo aver escluso node1")
	}

	var sumHops int64

	for i, name := range names {
		start := candidates[rand.Intn(len(candidates))]
		fmt.Printf("[%d/%d] %s  (start=%s)\n", i+1, len(names), name, start)

		t0 := time.Now()
		hops, found, err := test.LookupNFTOnNodeByNameStats(start, reverse, name, maxHops)
		elapsed := time.Since(t0).Milliseconds()

		if err != nil {
			fmt.Printf("  ✖ errore lookup: %v\n", err)
			_ = w.Write([]string{
				start, name, "-1",
				strconv.Itoa(N), strconv.Itoa(repl), strconv.Itoa(bucket),
				strconv.FormatInt(elapsed, 10),
			})
			w.Flush()
			rs.NotFound++
			continue
		}

		if !found {
			fmt.Println("  ✖ non trovato")
			_ = w.Write([]string{
				start, name, "-1",
				strconv.Itoa(N), strconv.Itoa(repl), strconv.Itoa(bucket),
				strconv.FormatInt(elapsed, 10),
			})
			w.Flush()
			rs.NotFound++
		} else {
			fmt.Printf("  ✅ trovato in %d hop, %d ms\n", hops, elapsed)
			_ = w.Write([]string{
				start, name, strconv.Itoa(hops),
				strconv.Itoa(N), strconv.Itoa(repl), strconv.Itoa(bucket),
				strconv.FormatInt(elapsed, 10),
			})
			w.Flush()

			rs.Found++
			sumHops += int64(hops)
			if hops > rs.MaxHops {
				rs.MaxHops = hops
			}
		}
	}

	// media hop SOLO sui trovati
	if rs.Found > 0 {
		rs.AvgHops = float64(sumHops) / float64(rs.Found)
	}
	return rs, nil
}

// ---------- Lettura nomi dal CSV (colonna Name) ----------
func readNames(path string) ([]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(bufio.NewReader(f))
	r.FieldsPerRecord = -1
	rows, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, errors.New("CSV vuoto")
	}

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

// ---------- .env parsing / update ----------
func readEnvFile(path string) map[string]string {
	out := map[string]string{}
	f, err := os.Open(path)
	if err != nil {
		return out
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

func setEnvVar(path, key, val string) error {
	// carica tutto
	lines := []string{}
	exists := false

	// se il file non esiste, lo creo
	fin, _ := os.Open(path)
	if fin != nil {
		sc := bufio.NewScanner(fin)
		for sc.Scan() {
			lines = append(lines, sc.Text())
		}
		_ = fin.Close()
	}

	for i, line := range lines {
		trim := strings.TrimSpace(line)
		if trim == "" || strings.HasPrefix(trim, "#") {
			continue
		}
		if idx := strings.IndexByte(trim, '='); idx > 0 {
			k := strings.TrimSpace(trim[:idx])
			if k == key {
				lines[i] = fmt.Sprintf("%s=%s", key, val)
				exists = true
				break
			}
		}
	}
	if !exists {
		lines = append(lines, fmt.Sprintf("%s=%s", key, val))
	}

	tmp := path + ".tmp"
	fout, err := os.Create(tmp)
	if err != nil {
		return err
	}
	for _, l := range lines {
		if _, err := fmt.Fprintln(fout, l); err != nil {
			_ = fout.Close()
			_ = os.Remove(tmp)
			return err
		}
	}
	_ = fout.Close()
	return os.Rename(tmp, path)
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

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// ---------- Docker helpers ----------

func composeUpBuild(project string) (string, error) {
	// prova "docker compose"; se non esiste, fallback a "docker-compose"
	out, err := runCmd("docker", "compose", "-p", project, "up", "-d", "--build", "--force-recreate")
	if err == nil {
		return out, nil
	}
	// fallback
	return runCmd("docker-compose", "-p", project, "up", "-d", "--build", "--force-recreate")
}

func runCmd(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	cmd.Env = os.Environ()
	b, err := cmd.CombinedOutput()
	return string(b), err
}

// attende che TUTTI i nodi espongano la porta gRPC (risolta via ui.ResolveStartHostPort)
func waitClusterReady(nodes []string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	pending := map[string]string{} // node -> hostPort

	// resolve subito
	for _, n := range nodes {
		if hp, err := ui.ResolveStartHostPort(n); err == nil && hp != "" {
			pending[n] = hp
		}
	}
	if len(pending) == 0 {
		return errors.New("nessun host:port risolto per i nodi")
	}

	for {
		for node, hp := range pending {
			conn, err := net.DialTimeout("tcp", hp, 500*time.Millisecond)
			if err == nil {
				_ = conn.Close()
				delete(pending, node)
			}
		}
		if len(pending) == 0 {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout attesa readiness: restano non pronti: %v", keys(pending))
		}
		time.Sleep(waitProbeEvery)
	}
}

func keys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
