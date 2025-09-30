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
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"kademlia-nft/internal/ui"
)

// ---------- CONFIG ----------
const (
	composeProject = "kademlia-nft"
	defaultEnvPath = ".env"
	defaultOutCSV  = "results/lookup.csv"
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
	a := 1
	if a == 3 {
		// carica i nomi NFT
		names, err := readNames(csvNFT)
		if err != nil {
			log.Fatalf("Errore lettura CSV NFT: %v", err)
		}
		if len(names) == 0 {
			log.Fatal("CSV NFT vuoto")
		}

		// nodi attivi + reverse
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

		alpha := firstInt(os.Getenv("ALPHA"), 3)
		if alpha <= 0 {
			alpha = 3
		}
		ConcurrentLookupTestAlpha(nodes, reverse, nftName, alpha, maxHops)
		return
	}
	if a == 1 {
		//test per variare N con alpha e k fissi e vedere l'andamento degli hop medi
		// test per variare N con alpha e k fissi e vedere l'andamento degli hop medi
		{
			// 0) leggi parametri fissi dal .env (una sola volta)
			env := readEnvFile(defaultEnvPath)
			kbEnv := firstInt(env["BUCKET_SIZE"], 4) // resta invariato per tutte le iterazioni
			alphaEnv := firstInt(env["ALPHA"], 3)    // resta invariato per tutte le iterazioni
			// replEnv := firstInt(env["REPLICATION_FACTOR"], 3) // se ti serve nei log

			// 1) setup output
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

			// 2) carica gli NFT da cercare
			names, err := readNames(csvNFT)
			if err != nil {
				log.Fatalf("Errore lettura CSV NFT: %v", err)
			}
			if len(names) == 0 {
				log.Fatalf("Nel CSV %s non ho trovato la colonna 'Name' con valori.", csvNFT)
			}

			iter := 0
			for N := 30; N <= 100; N += 10 {
				iter++
				fmt.Printf("\n=== Iterazione %d: N=%d, KB=%d, α=%d ===\n", iter, N, kbEnv, alphaEnv)

				// 3) aggiorna SOLO N nel .env
				if err := setEnvVar(defaultEnvPath, "N", strconv.Itoa(N)); err != nil {
					log.Fatalf("Errore set N: %v", err)
				}
				// NON toccare BUCKET_SIZE / ALPHA qui: restano kbEnv/alphaEnv

				// 4) rigenera compose e rebuild del cluster
				if out, err := runCmd("bash", "generate-compose.sh"); err != nil {
					log.Fatalf("generate-compose.sh fallito: %v\n%s", err, out)
				}

				// --- OPZIONALE: azzera SOLO i dati (se usi bind-mount ./data -> /data)
				// _, _ = runCmd("bash", "-lc", "shopt -s dotglob nullglob; sudo rm -rf ./data/*")

				// down/up (senza cache) per partire pulito
				if out, err := composeUpBuild(composeProject); err != nil {
					log.Fatalf("docker compose up fallito: %v\n%s", err, out)
				}

				// 5) scopri i nodi e crea la reverse map
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
				// opzionale: escludi il seeder (se non partecipa come peer)
				nodes = slices.DeleteFunc(nodes, func(s string) bool { return s == "node1" })

				// 6) attesa readiness servizi gRPC
				if err := waitClusterReady(nodes, waitReadyMax); err != nil {
					log.Printf("⚠️ Cluster forse non pronto: %v (procedo comunque)", err)
				}

				// 7) esegui tutte le lookup
				trovati, nonTrovati := 0, 0
				hops := make([]int, 0, len(names))
				for i, name := range names {
					start := test.RandomNode(nodes)
					h, found, err := ui.LookupNFTOnNodeByNameAlpha(start, reverse, name, alphaEnv, maxHops)
					if err != nil || !found {
						nonTrovati++
						continue
					}
					trovati++
					hops = append(hops, h)
					fmt.Printf("[%d/%d] %s trovato in %d hop\n", i+1, len(names), name, h)
				}

				// 8) statistiche e salvataggio riga
				mean, std := test.MeanStd(hops)
				maxHop := 0
				for _, v := range hops {
					if v > maxHop {
						maxHop = v
					}
				}

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

	} else {

		//test per cercare tutti gli nft
		// test statico: cerca tutti gli NFT del dataset con configurazione presa dall'env, senza toccare .env e senza rebuild

		// flags SOLO per file/percorsi
		csvPath := flag.String("csv", "", "/percorso/collections.csv")
		envPath := flag.String("env", defaultEnvPath, "Percorso del file .env")
		outCSV := flag.String("out", defaultOutCSV, "Output CSV dettagli singola run (overwrite)")
		outSummary := flag.String("outSummary", defaultSummary, "Output CSV riassunto (append)")
		flag.Parse()

		if *csvPath == "" {
			log.Fatal("Devi specificare -csv /percorso/collections.csv (con colonna 'Name').")
		}

		// 1) carica parametri da .env
		env := readEnvFile(*envPath)

		// helper: prende un int dal map, con default
		firstInt := func(s string, def int) int {
			if v, err := strconv.Atoi(strings.TrimSpace(s)); err == nil && v != 0 {
				return v
			}
			return def
		}

		N := firstInt(env["N"], 0)
		bucketSize := firstInt(env["BUCKET_SIZE"], 0)
		repl := firstInt(env["REPLICATION_FACTOR"], 3)
		alpha := firstInt(env["ALPHA"], 3)              // <-- ora viene davvero dal .env
		maxHops := firstInt(env["LOOKUP_MAX_HOPS"], 10) // o "MAX_HOPS", come preferisci

		// validazioni minime (tutte da .env)
		if N <= 0 {
			log.Fatal("Nel .env manca N o è <= 0")
		}
		if bucketSize <= 0 {
			log.Fatal("Nel .env manca BUCKET_SIZE o è <= 0")
		}
		if repl <= 0 {
			log.Fatal("Nel .env manca REPLICATION_FACTOR o è <= 0")
		}
		if alpha <= 0 {
			log.Fatal("Nel .env manca ALPHA o è <= 0")
		}
		// maxHops può avere un default ragionevole, quindi niente fatal

		// 2) prepara cartella risultati
		_ = os.MkdirAll("results", 0o755)

		// 3) apri/crea summary (append, con header se nuovo)
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
				"iterazione", "NFT", "NFT NON TROVATI", "MEDIA HOP FATTI", "NFT TROVATI", "MAX HOPS",
				"NODI", "BUCKETSIZE", "FATTORE DI REPLICAZIONI", "ALPHA",
			})
			sumW.Flush()
		}

		// 4) leggi i Name dal CSV
		names, err := readNames(*csvPath)
		if err != nil {
			log.Fatalf("Errore lettura CSV input: %v", err)
		}
		if len(names) == 0 {
			log.Fatalf("Nel CSV %s non ho trovato la colonna 'Name' con valori.", *csvPath)
		}

		// 5) scopri nodi attivi e reverse map (puoi tenerla, ma i parametri *restano* quelli del .env)
		nodes, err := ui.ListActiveComposeServices(composeProject)
		if err != nil {
			log.Fatalf("Errore recupero nodi: %v", err)
		}
		if len(nodes) == 0 {
			log.Fatal("Nessun nodo attivo trovato. Avvia prima il cluster.")
		}
		reverse, err := ui.Reverse2(nodes)
		if err != nil {
			log.Fatalf("Errore Reverse2: %v", err)
		}
		// opzionale: escludi node1 se è solo seeder
		filtered := make([]string, 0, len(nodes))
		for _, n := range nodes {
			if n != "node1" {
				filtered = append(filtered, n)
			}
		}
		nodes = filtered

		// 6) readiness
		if err := waitClusterReady(nodes, waitReadyMax); err != nil {
			log.Printf("⚠️ cluster forse non ancora pronto: %v (procedo comunque)", err)
		}

		// 7) apri CSV dettagli (overwrite per questa run)
		if err := os.MkdirAll(filepath.Dir(*outCSV), 0o755); err != nil {
			log.Fatalf("Impossibile creare dir %s: %v", filepath.Dir(*outCSV), err)
		}
		outF, err := os.Create(*outCSV)
		if err != nil {
			log.Fatalf("Impossibile creare %s: %v", *outCSV, err)
		}
		defer outF.Close()
		w := csv.NewWriter(outF)
		defer w.Flush()
		_ = w.Write([]string{"nodePartenza", "NameNft", "Hop", "NumeroNodi", "repliche", "KsizeBucket", "time_ms", "alpha"})
		w.Flush()

		// 8) run unica
		rand.Seed(time.Now().UnixNano())

		trovati, nonTrovati := 0, 0
		hops := make([]int, 0, len(names))

		for i, name := range names {
			start := nodes[rand.Intn(len(nodes))]
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

		// 9) statistiche e summary
		mean, std := test.MeanStd(hops)
		fmt.Printf("  📊 statistiche: media_hop=%.3f, std_hop=%.3f\n", mean, std)
		maxHop := 0
		for _, v := range hops {
			if v > maxHop {
				maxHop = v
			}
		}
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
}

// ------------------------------------------------------------

func oneRun(names []string, nodes []string, reverse []Pair, outCSV string, maxHops int, N int, bucket int, repl int, alpha int) (RunStats, error) {
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
		"nodePartenza", "NameNft", "Hop", "NumeroNodi", "repliche", "KsizeBucket", "time_ms", "alpha",
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
		fmt.Printf("[%d/%d] %s  (start=%s, α=%d)\n", i+1, len(names), name, start, alpha)

		t0 := time.Now()
		hops, found, err := ui.LookupNFTOnNodeByNameAlpha(start, reverse, name, alpha, maxHops)
		elapsed := time.Since(t0).Milliseconds()

		if err != nil {
			fmt.Printf("  ✖ errore lookup: %v\n", err)
			_ = w.Write([]string{
				start, name, "-1",
				strconv.Itoa(N), strconv.Itoa(repl), strconv.Itoa(bucket),
				strconv.FormatInt(elapsed, 10), strconv.Itoa(alpha),
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
				strconv.FormatInt(elapsed, 10), strconv.Itoa(alpha),
			})
			w.Flush()
			rs.NotFound++
		} else {
			fmt.Printf("  ✅ trovato in %d hop, %d ms\n", hops, elapsed)
			_ = w.Write([]string{
				start, name, strconv.Itoa(hops),
				strconv.Itoa(N), strconv.Itoa(repl), strconv.Itoa(bucket),
				strconv.FormatInt(elapsed, 10), strconv.Itoa(alpha),
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

// ---------- Test concorrente basato su nuova lookup α ----------
func ConcurrentLookupTestAlpha(nodes []string, reverse []Pair, nftName string, alpha int, maxRounds int) {
	if len(nodes) == 0 {
		fmt.Println("Nessun nodo attivo")
		return
	}
	workers := minInt(5, len(nodes))
	fmt.Printf("Avvio %d lookup concorrenti per '%s' (α=%d)\n", workers, nftName, alpha)

	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		start := test.RandomNode(nodes)
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

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
