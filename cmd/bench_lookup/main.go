package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"kademlia-nft/internal/ui"
)

// ---------- CONFIG ----------
const (
	composeProject  = "kademlia-nft"
	defaultEnvPath  = ".env"
	defaultOutCSV   = "results/lookup.csv"
	defaultSummary  = "results/nRuns.csv"
	waitReadyMax    = 40 * time.Second // attesa massima servizi dopo il rebuild
	waitProbeEvery  = 700 * time.Millisecond
	csvNFT          = "csv/NFT_Top_Collections.csv"
	outSummary      = "results/nodiPlus.csv"
	maxHopsDefault  = 30
	parallelTimeout = 2 * time.Second // timeout per singolo lookup parallelo
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
	rand.Seed(time.Now().UnixNano())

	// --- flags SOLO per file/percorsi (non influenzano la scelta del test) ---
	csvPath := flag.String("csv", csvNFT, "Percorso del file CSV (colonna 'Name')")
	envPath := flag.String("env", defaultEnvPath, "Percorso del file .env")
	outCSV := flag.String("out", defaultOutCSV, "Output CSV dettagli singola run (overwrite)")
	outSummaryPath := flag.String("outSummary", defaultSummary, "Output CSV riassunto (append)")
	flag.Parse()

	// --- 1) carica .env e parametri ---
	env := readEnvFile(*envPath)
	NEnv := firstInt(env["N"], 0)
	kbEnv := firstInt(env["BUCKET_SIZE"], 0)
	alphaEnv := firstInt(env["ALPHA"], 0)
	replEnv := firstInt(env["REPLICATION_FACTOR"], 0)
	maxHops := firstInt(env["LOOKUP_MAX_HOPS"], maxHopsDefault)
	testID := firstInt(env["TEST"], 1) // <-- L'UNICA VARIABILE CHE SCEGLIE IL TEST

	// validazioni base
	if NEnv <= 0 || kbEnv <= 0 || alphaEnv <= 0 || replEnv <= 0 {
		log.Fatalf("Parametri .env mancanti/invalidi (N=%d, K=%d, alpha=%d, repl=%d)", NEnv, kbEnv, alphaEnv, replEnv)
	}

	// --- 2) carica i nomi NFT (comune a tutti i test tranne loop ricostruzioni) ---
	names, err := readNames(*csvPath)
	if err != nil {
		log.Fatalf("Errore lettura CSV NFT: %v", err)
	}
	if len(names) == 0 {
		log.Fatal("CSV NFT vuoto (colonna 'Name').")
	}

	// --- 3) scopri nodi attivi e prepara reverse (comune ai test che NON fanno rebuild per-iterazione) ---
	nodes, err := ui.ListActiveComposeServices(composeProject)
	if err != nil {
		log.Fatalf("Errore lista nodi: %v", err)
	}
	if len(nodes) == 0 {
		log.Fatal("Nessun nodo attivo — avvia prima il cluster.")
	}
	// escludi il seeder se non partecipa come peer
	nodes = filterOut(nodes, "node1")

	// reverse per i test "statici"
	reverse, err := ui.Reverse2(nodes)
	if err != nil {
		log.Fatalf("Errore Reverse2: %v", err)
	}

	// readiness
	if err := waitClusterReady(nodes, waitReadyMax); err != nil {
		log.Printf("⚠️ cluster forse non ancora pronto: %v (procedo comunque)", err)
	}

	// --- 4) switch sui test, deciso da TEST nel .env ---
	switch testID {

	// TEST 1: sweep su N con rebuild, hop medio, ecc.
	case 1:
		runSweepOnN(names, kbEnv, alphaEnv)

	// TEST 2: lookup concorrenti sullo STESSO NFT (nuova lookup α)
	case 2:
		nftName := names[0]
		fmt.Printf("NFT scelto per il test concorrente: %s\n", nftName)
		ConcurrentLookupTestAlpha(nodes, reverse, nftName, alphaEnv, maxHops)

	// TEST 3: 5 lookup paralleli su 5 NFT diversi (nodi di partenza random)
	case 3:
		runFiveParallelLookups(nodes, reverse, names, alphaEnv, maxHops)

	// DEFAULT: “static run” — cerca tutti gli NFT, log su CSV + summary append
	default:
		runStaticAllLookups(nodes, reverse, names, *outCSV, *outSummaryPath, NEnv, kbEnv, replEnv, alphaEnv, maxHops)
	}
}

// ------------------------------------------------------------
// TEST 1: sweep su N con rebuild e reseeding, K e alpha fissi
// ------------------------------------------------------------
func runSweepOnN(names []string, kbEnv, alphaEnv int) {
	_ = os.MkdirAll("results", 0o755)
	f, err := os.Create(outSummary) // overwrite ad ogni run del test
	if err != nil {
		log.Fatalf("Impossibile creare %s: %v", outSummary, err)
	}
	defer f.Close()
	w := csv.NewWriter(f)
	defer w.Flush()
	_ = w.Write([]string{
		"iterazione", "numero_nodi", "kbucket", "alpha",
		"nHops_medio", "stdHops", "maxHops", "NFT_trovati", "NFT_non_trovati",
	})
	w.Flush()

	iter := 0
	for N := 30; N <= 110; N += 20 {
		iter++
		fmt.Printf("\n=== Iterazione %d: N=%d, KB=%d, α=%d ===\n", iter, N, kbEnv, alphaEnv)

		// Aggiorna solo N nel .env
		if err := setEnvVar(defaultEnvPath, "N", strconv.Itoa(N)); err != nil {
			log.Fatalf("Errore set N: %v", err)
		}

		// Rigenera compose + rebuild cluster
		if out, err := runCmd("bash", "generate-compose.sh"); err != nil {
			log.Fatalf("generate-compose.sh fallito: %v\n%s", err, out)
		}
		_, _ = runCmd("bash", "-lc", "shopt -s dotglob nullglob; sudo rm -rf ./data/*")
		if out, err := composeUpBuild(composeProject); err != nil {
			log.Fatalf("docker compose up fallito: %v\n%s", err, out)
		}

		// Nodi dopo il rebuild
		nodes, err := ui.ListActiveComposeServices(composeProject)
		if err != nil {
			log.Fatalf("Errore lista nodi: %v", err)
		}
		if len(nodes) == 0 {
			log.Fatal("Nessun nodo attivo dopo il rebuild")
		}
		nodes = filterOut(nodes, "node1")

		reverse, err := ui.Reverse2(nodes)
		if err != nil {
			log.Fatalf("Errore Reverse2: %v", err)
		}
		if err := waitClusterReady(nodes, waitReadyMax); err != nil {
			log.Printf("⚠️ Cluster forse non pronto: %v (procedo comunque)", err)
		}
		time.Sleep(2 * time.Second)

		// Lookup di tutte le chiavi
		trovati, nonTrovati := 0, 0
		hops := make([]int, 0, len(names))
		for i, name := range names {
			start := RandomNode(nodes)
			h, found, err := ui.LookupNFTOnNodeByNameAlpha(start, reverse, name, alphaEnv, maxHopsDefault)
			if err != nil || !found {
				nonTrovati++
				continue
			}
			trovati++
			hops = append(hops, h)
			fmt.Printf("[%d/%d] %s trovato in %d hop\n", i+1, len(names), name, h)
		}

		mean, std := MeanStd(hops)
		maxHop := maxIntSlice(hops)
		_ = w.Write([]string{
			strconv.Itoa(iter),
			strconv.Itoa(N),
			strconv.Itoa(kbEnv),
			strconv.Itoa(alphaEnv),
			fmt.Sprintf("%.4f", mean),
			fmt.Sprintf("%.4f", std),
			strconv.Itoa(maxHop),
			strconv.Itoa(trovati),
			strconv.Itoa(nonTrovati),
		})
		w.Flush()

		fmt.Printf(">>> Risultati: avgHops=%.3f, stdHops=%.3f (su %d lookup)\n", mean, std, len(hops))
	}

	fmt.Printf("\n🏁 Test completato. Risultati in %s\n", outSummary)
}

// ------------------------------------------------------------
// TEST 10: 5 lookup paralleli su 5 NFT diversi
// ------------------------------------------------------------
func runFiveParallelLookups(nodes []string, reverse []Pair, names []string, alphaEnv, maxHops int) {
	if len(names) < 5 {
		log.Fatalf("Servono almeno 5 NFT nel CSV (trovati: %d)", len(names))
	}

	// prendi i primi 5 (oppure usa un campionamento casuale se preferisci)
	selected := make([]string, 5)
	copy(selected, names[:5])

	type res struct {
		name    string
		start   string
		found   bool
		hops    int
		latency time.Duration
		err     error
	}

	results := make(chan res, 5)
	var wg sync.WaitGroup
	wg.Add(5)

	for _, nm := range selected {
		start := RandomNode(nodes)
		go func(nm, startNode string) {
			defer wg.Done()

			// timeout “guardia” per singolo lookup
			ctx, cancel := context.WithTimeout(context.Background(), parallelTimeout)
			defer cancel()

			t0 := time.Now()
			// Se hai una versione con ctx: ui.LookupNFTOnNodeByNameAlphaCtx(ctx, ...)
			_ = ctx // non usato dalla tua API attuale, ma lo teniamo come struttura
			h, found, err := ui.LookupNFTOnNodeByNameAlpha(startNode, reverse, nm, alphaEnv, maxHops)
			lat := time.Since(t0)

			results <- res{name: nm, start: startNode, found: found, hops: h, latency: lat, err: err}
		}(nm, start)
	}

	wg.Wait()
	close(results)

	foundCnt, maxHop := 0, 0
	var sumHop int
	var sumLat time.Duration

	for r := range results {
		if r.err != nil {
			fmt.Printf("✖ %q (start=%s): err=%v\n", r.name, r.start, r.err)
			continue
		}
		if !r.found {
			fmt.Printf("✖ %q (start=%s): non trovato\n", r.name, r.start)
			continue
		}
		fmt.Printf("✅ %q (start=%s): hops=%d, lat=%s\n", r.name, r.start, r.hops, r.latency)
		foundCnt++
		if r.hops > maxHop {
			maxHop = r.hops
		}
		sumHop += r.hops
		sumLat += r.latency
	}

	if foundCnt > 0 {
		avgHop := float64(sumHop) / float64(foundCnt)
		avgLat := time.Duration(int64(sumLat) / int64(foundCnt))
		fmt.Printf("RIEPILOGO: trovati=%d/5 avgHop=%.3f maxHop=%d avgLatency=%s\n", foundCnt, avgHop, maxHop, avgLat)
	} else {
		fmt.Println("RIEPILOGO: nessun NFT trovato")
	}
}

// ------------------------------------------------------------
// DEFAULT: run “statica” — cerca TUTTI gli NFT e scrive CSV + summary
// ------------------------------------------------------------
func runStaticAllLookups(
	nodes []string,
	reverse []Pair,
	names []string,
	outCSV string,
	outSummary string,
	N, bucketSize, repl, alpha int,
	maxHops int,
) {
	_ = os.MkdirAll(filepath.Dir(outCSV), 0o755)

	// CSV dettagli (overwrite)
	outF, err := os.Create(outCSV)
	if err != nil {
		log.Fatalf("Impossibile creare %s: %v", outCSV, err)
	}
	defer outF.Close()
	w := csv.NewWriter(outF)
	defer w.Flush()
	_ = w.Write([]string{"nodePartenza", "NameNft", "Hop", "NumeroNodi", "repliche", "KsizeBucket", "time_ms", "alpha"})
	w.Flush()

	// Summary (append, header se nuovo)
	sumExists := fileExists(outSummary)
	sumF, err := os.OpenFile(outSummary, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		log.Fatalf("Impossibile aprire %s: %v", outSummary, err)
	}
	defer sumF.Close()
	sumW := csv.NewWriter(sumF)
	defer sumW.Flush()
	if !sumExists {
		_ = sumW.Write([]string{
			"iterazione", "NFT", "NFT NON TROVATI", "MEDIA HOP FATTI", "NFT TROVATI", "MAX HOPS",
			"NODI", "BUCKETSIZE", "FATTORE DI REPLICAZIONI", "ALPHA",
		})
		sumW.Flush()
	}

	// Esecuzione
	trovati, nonTrovati := 0, 0
	hops := make([]int, 0, len(names))

	for i, name := range names {
		start := RandomNode(nodes)
		fmt.Printf("[%d/%d] %s  (start=%s, α=%d)\n", i+1, len(names), name, start, alpha)

		t0 := time.Now()
		h, found, err := ui.LookupNFTOnNodeByNameAlpha(start, reverse, name, alpha, maxHops)
		elapsed := time.Since(t0).Milliseconds()

		if err != nil || !found {
			if err != nil {
				fmt.Printf("  ✖ errore lookup: %v\n", err)
			} else {
				fmt.Println("  ✖ non trovato")
			}
			_ = w.Write([]string{
				start, name, "-1",
				strconv.Itoa(N), strconv.Itoa(repl), strconv.Itoa(bucketSize),
				strconv.FormatInt(elapsed, 10), strconv.Itoa(alpha),
			})
			w.Flush()
			nonTrovati++
			continue
		}

		fmt.Printf("  ✅ trovato in %d hop, %d ms\n", h, elapsed)
		_ = w.Write([]string{
			start, name, strconv.Itoa(h),
			strconv.Itoa(N), strconv.Itoa(repl), strconv.Itoa(bucketSize),
			strconv.FormatInt(elapsed, 10), strconv.Itoa(alpha),
		})
		w.Flush()
		trovati++
		hops = append(hops, h)
	}

	mean, std := MeanStd(hops)
	fmt.Printf("%.6f\n", std)
	maxHop := maxIntSlice(hops)
	iter := 1

	_ = sumW.Write([]string{
		strconv.Itoa(iter),
		strconv.Itoa(len(names)),
		strconv.Itoa(nonTrovati),
		fmt.Sprintf("%.6f", mean),
		strconv.Itoa(trovati),
		strconv.Itoa(maxHop),
		strconv.Itoa(N),
		strconv.Itoa(bucketSize),
		strconv.Itoa(repl),
		strconv.Itoa(alpha),
	})
	sumW.Flush()

	fmt.Printf("✅ Run statica completata — trovati=%d, non_trovati=%d, avg_hops=%.3f, max_hops=%d (N=%d, K=%d, repl=%d, α=%d)\n",
		trovati, nonTrovati, mean, maxHop, N, bucketSize, repl, alpha)
}

// ------------------------------------------------------------
// Test concorrente basato su nuova lookup α (stesso NFT)
// ------------------------------------------------------------
func ConcurrentLookupTestAlpha(nodes []string, reverse []Pair, nftName string, alpha int, maxRounds int) {
	if len(nodes) == 0 {
		fmt.Println("Nessun nodo attivo")
		return
	}
	workers := minInt(5, len(nodes))
	fmt.Printf("Avvio %d lookup concorrenti per '%s' (α=%d)\n", workers, nftName, alpha)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		start := RandomNode(nodes)
		wg.Add(1)
		go func(s string, idx int) {
			defer wg.Done()
			fmt.Printf("[T%02d] start=%s\n", idx, s)
			hops, found, err := ui.LookupNFTOnNodeByNameAlpha(s, reverse, nftName, alpha, maxRounds)
			if err != nil {
				fmt.Printf("[T%02d] errore: %v\n", idx, err)
				return
			}
			if !found {
				fmt.Printf("[T%02d] non trovato (hop=-1)\n", idx)
			} else {
				fmt.Printf("[T%02d] trovato in %d hop\n", idx, hops)
			}
		}(start, i+1)
	}
	wg.Wait()
}

// ------------------------------------------------------------
// Helpers “generici” (riusati da tutti i test)
// ------------------------------------------------------------

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
	if n, err := strconv.Atoi(strings.TrimSpace(s)); err == nil && n != 0 {
		return n
	}
	return def
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func composeUpBuild(project string) (string, error) {
	out, err := runCmd("docker", "compose", "-p", project, "up", "-d", "--build", "--force-recreate")
	if err == nil {
		return out, nil
	}
	return runCmd("docker-compose", "-p", project, "up", "-d", "--build", "--force-recreate")
}

func runCmd(name string, args ...string) (string, error) {
	cmd := exec.Command(name, args...)
	cmd.Env = os.Environ()
	b, err := cmd.CombinedOutput()
	return string(b), err
}

func waitClusterReady(nodes []string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	pending := map[string]string{} // node -> hostPort

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

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxIntSlice(xs []int) int {
	m := 0
	for _, v := range xs {
		if v > m {
			m = v
		}
	}
	return m
}

func filterOut(xs []string, bad string) []string {
	out := make([]string, 0, len(xs))
	for _, v := range xs {
		if !strings.EqualFold(strings.TrimSpace(v), bad) {
			out = append(out, v)
		}
	}
	return out
}

func RandomNode(nodes []string) string {
	if len(nodes) == 0 {
		return ""
	}
	return nodes[rand.Intn(len(nodes))]
}

func MeanStd(data []int) (float64, float64) {
	n := len(data)
	if n == 0 {
		return 0, 0
	}
	var sum float64
	for _, v := range data {
		sum += float64(v)
	}
	mean := sum / float64(n)

	var sq float64
	for _, v := range data {
		d := float64(v) - mean
		sq += d * d
	}
	std := math.Sqrt(sq / float64(n))
	return mean, std
}
