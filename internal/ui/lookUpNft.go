package ui

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"kademlia-nft/common"

	"sort"

	pb "kademlia-nft/proto/kad"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Pair: esa = SHA1 hex dell'ID, hash = ID/alias del nodo (es. "node7")
func Check(value string, list []Pair) string {
	val := strings.ToLower(strings.TrimSpace(value))
	for _, v := range list {
		if strings.ToLower(strings.TrimSpace(v.esaSha1)) == val {

			return strings.TrimSpace(v.name)
		}
	}
	return "NOTFOUND"
}

// --------------------------------------------------
func LookupNFTOnNodeByName(startNode string, str []Pair, nftName string, maxHops int) error {
	if maxHops <= 0 {
		maxHops = 15
	}

	nftID20 := common.Sha1ID(nftName) // []byte(20)
	visitedIDs := make(map[string]bool)

	// Hop 0: risolvi SOLO lo startNode
	hostPort, err := ResolveStartHostPort(startNode)
	if err != nil {
		return fmt.Errorf("risoluzione %q fallita: %w", startNode, err)
	}
	currentLabel := startNode // per log

	for hop := 0; hop < maxHops; hop++ {
		fmt.Printf("🔎 Hop %d: cerco '%s' su %s (%s)\n", hop+1, nftName, currentLabel, hostPort)

		conn, err := grpc.Dial(hostPort, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return fmt.Errorf("dial fallito %s: %w", hostPort, err)
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
			return fmt.Errorf("RPC fallita su %s: %w", currentLabel, rpcErr)
		}

		if resp.GetFound() {
			fmt.Printf("✅ Trovato su nodo %s\n", resp.GetHolder().GetId())
			fmt.Printf("Contenuto JSON:\n%s\n", string(resp.GetValue().GetBytes()))
			return nil
		}

		nearest := resp.GetNearest()
		if len(nearest) == 0 {
			fmt.Println("✖ NFT non trovato e nessun nodo vicino restituito — arresto.")
			return nil
		}

		// Prepara candidati usabili: (id, addr, label) con fallback via check()
		type cand struct {
			id    string
			addr  string // host:port
			label string // per log: host o "nodeX" o id short
		}
		usabili := make([]cand, 0, len(nearest))

		fmt.Println("… nodi vicini suggeriti:")
		for _, n := range nearest {
			id := strings.TrimSpace(n.GetId())
			if id == "" {
				continue // senza ID non posso fare XOR
			}
			lcID := strings.ToLower(id)

			var addr, label string
			host := strings.TrimSpace(n.GetHost())
			port := int(n.GetPort())
			if host != "" && port > 0 {
				addr = fmt.Sprintf("%s:%d", host, port)
				label = host
			} else {
				// Fallback: mappa ID -> "nodeX" usando la tua tabella 'str'
				name := Check(id, str)
				if name != "NOTFOUND" {
					if hp, e := ResolveStartHostPort(name); e == nil {
						addr = hp
						label = name
					}
				}
			}

			if addr == "" {
				fmt.Printf("   - %s (endpoint mancante)\n", id)
				continue
			}
			if visitedIDs[lcID] {
				// già visitato: non lo ripropongo
				fmt.Printf("   - %s (%s) [già visitato]\n", id, addr)
				continue
			}

			if label == "" {
				// etichetta estetica di fallback
				if host != "" {
					label = host
				} else {
					if len(id) >= 8 {
						label = id[:8]
					} else {
						label = id
					}
				}
			}

			fmt.Printf("   - %s (%s)\n", id, addr)
			usabili = append(usabili, cand{id: lcID, addr: addr, label: label})
		}

		if len(usabili) == 0 {
			fmt.Println("✖ Nessun vicino utilizzabile non visitato — arresto.")
			return nil
		}

		// Seleziona il più vicino usando la TUA funzione (input: lista di ID hex)
		ids := make([]string, len(usabili))
		for i, c := range usabili {
			ids[i] = c.id
		}

		fmt.Printf("candidati: %v,", ids)
		bestID, err := SceltaNodoPiuVicino(nftID20, ids)
		if err != nil {
			fmt.Printf("⚠️  Impossibile scegliere il nodo più vicino: %v — prendo il primo candidato.\n", err)
			bestID = ids[0]
		}
		bestID = strings.ToLower(strings.TrimSpace(bestID))

		// Recupera endpoint del bestID
		var nextAddr, nextLabel string
		for _, c := range usabili {
			if c.id == bestID {
				nextAddr = c.addr
				nextLabel = c.label
				break
			}
		}
		if nextAddr == "" {
			// come ulteriore fallback, prova mapping ID->nodeX e risolvi
			if name := Check(bestID, str); name != "NOTFOUND" {
				if hp, e := ResolveStartHostPort(name); e == nil {
					nextAddr = hp
					nextLabel = name
				}
			}
		}
		if nextAddr == "" {
			fmt.Println("✖ Best candidato senza endpoint — arresto.")
			return nil
		}

		// Marca visitato per ID
		visitedIDs[bestID] = true

		// Prepara hop successivo
		hostPort = nextAddr
		if nextLabel != "" {
			currentLabel = nextLabel
		} else {
			currentLabel = bestID[:8]
		}
		fmt.Printf("➡️  Prossimo nodo scelto: %s\n", currentLabel)
	}

	fmt.Printf("⛔ Max hop (%d) raggiunto senza trovare '%s'.\n", maxHops, nftName)
	return nil
}

// Lookup parallela stile Kademlia con grado di concorrenza alpha.
// - startNode: nome del nodo di partenza (es. "node10")
// - str: reverse map ID->nome (le tue Pair) usata come fallback per risolvere host:port
// - nftName: chiave logica dell'NFT
// - alpha: quante query in parallelo per round (tipico 3)
// - maxRounds: limite ai round (non ai singoli hop RPC)
func LookupNFTOnNodeByNameAlpha(startNode string, str []Pair, nftName string, alpha int, maxRounds int) (round int, found bool, err error) {
	if alpha <= 0 {
		alpha = 3
	}
	if maxRounds <= 0 {
		maxRounds = 30
	}

	target := common.Sha1ID(nftName) // []byte(20) target della distanza XOR

	// ── utilità locali ──────────────────────────────────────────────────────────
	xor := func(a, b []byte) []byte {
		out := make([]byte, len(a))
		for i := range a {
			out[i] = a[i] ^ b[i]
		}
		return out
	}
	type cand struct {
		idHex string
		addr  string
		label string
		dist  []byte // distanza XOR dal target
	}
	idsInShort := map[string]bool{} // per de-duplicare la shortlist
	short := make([]cand, 0, 64)    // shortlist sempre ordinata per dist asc

	// ordina shortlist per distanza (tie-break per idHex)
	sortShort := func() {
		sort.Slice(short, func(i, j int) bool {
			if c := bytes.Compare(short[i].dist, short[j].dist); c != 0 {
				return c < 0
			}
			return short[i].idHex < short[j].idHex
		})
	}

	// costruisce cands usabili da un elenco di nearest (host:port dal server o via fallback reverse-map)
	buildFromNearest := func(nearest []*pb.Node, skipID string) []cand {
		out := make([]cand, 0, len(nearest))
		for _, n := range nearest {
			id := strings.ToLower(strings.TrimSpace(n.GetId()))
			if len(id) != 40 || id == skipID {
				continue
			}
			if idsInShort[id] {
				continue
			}

			// endpoint
			var addr, label string
			host := strings.TrimSpace(n.GetHost())
			port := int(n.GetPort())
			if host != "" && port > 0 {
				addr = fmt.Sprintf("%s:%d", host, port)
				label = host
			} else {
				// fallback: mappa ID→nome nodo e risolvi host:port
				if name := Check(id, str); name != "NOTFOUND" {
					if hp, e := ResolveStartHostPort(name); e == nil && hp != "" {
						addr = hp
						label = name
					}
				}
			}
			if addr == "" {
				continue
			}
			if label == "" {
				if host != "" {
					label = host
				} else if len(id) >= 8 {
					label = id[:8]
				} else {
					label = id
				}
			}

			// distanza
			idBytes, err := hex.DecodeString(id)
			if err != nil || len(idBytes) != len(target) {
				continue
			}
			out = append(out, cand{
				idHex: id,
				addr:  addr,
				label: label,
				dist:  xor(target, idBytes),
			})
		}
		return out
	}

	// merge nella shortlist, ritorna quanti nuovi abbiamo aggiunto
	mergeIntoShort := func(add []cand) (added int) {
		for _, c := range add {
			if idsInShort[c.idHex] {
				continue
			}
			idsInShort[c.idHex] = true
			short = append(short, c)
			added++
		}
		if added > 0 {
			sortShort()
		}
		return added
	}

	// prende fino a α candidati **non interrogati** dalla shortlist (in ordine)
	takeTopUnqueried := func(queried map[string]bool, k int) []cand {
		out := make([]cand, 0, k)
		for _, c := range short {
			if len(out) >= k {
				break
			}
			if queried[c.idHex] {
				continue
			}
			out = append(out, c)
		}
		return out
	}

	// ── fase 0: query allo start node ──────────────────────────────────────────
	hostPort, e := ResolveStartHostPort(startNode)
	if e != nil || hostPort == "" {
		return 0, false, fmt.Errorf("risoluzione %q fallita: %w", startNode, e)
	}
	selfHex := strings.ToLower(hex.EncodeToString(common.Sha1ID(startNode)))

	fmt.Printf("🔎 Hop 1: cerco '%s' su %s (%s)\n", nftName, startNode, hostPort)
	{
		conn, err := grpc.Dial(hostPort, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return 0, false, fmt.Errorf("dial %s: %w", hostPort, err)
		}
		client := pb.NewKademliaClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		resp, rpcErr := client.LookupNFT(ctx, &pb.LookupNFTReq{
			FromId: "CLI",
			Key:    &pb.Key{Key: target},
		})
		cancel()
		_ = conn.Close()
		if rpcErr != nil {
			return 0, false, fmt.Errorf("RPC su %s: %w", startNode, rpcErr)
		}
		if resp.GetFound() {
			fmt.Printf("✅ Trovato su nodo %s\n", resp.GetHolder().GetId())
			fmt.Printf("Contenuto JSON:\n%s\n", string(resp.GetValue().GetBytes()))
			return 1, true, nil
		}
		add := buildFromNearest(resp.GetNearest(), selfHex) // escludi self
		_ = mergeIntoShort(add)
	}

	// già da ora: non re-interrogare mai lo start
	queried := map[string]bool{selfHex: true}

	// ── round successivi (batch α) ─────────────────────────────────────────────
	for round = 2; round <= maxRounds; round++ {
		next := takeTopUnqueried(queried, alpha)
		if len(next) == 0 {
			fmt.Printf("✖ non trovato dopo %d round (α=%d)\n", round-1, alpha)
			return round - 1, false, nil
		}

		// marca subito per evitare duplicazioni in round successivi
		names := make([]string, 0, len(next))
		for _, c := range next {
			queried[c.idHex] = true
			names = append(names, c.label)
		}
		fmt.Printf("🔎 Hop %d (α=%d): interrogo in parallelo: %s\n", round, alpha, strings.Join(names, ", "))

		type result struct {
			found   bool
			holder  *pb.Node
			value   []byte
			nearest []*pb.Node
			err     error
		}
		out := make(chan result, len(next))

		// lancia goroutine per il batch
		for _, c := range next {
			go func(c cand) {
				conn, err := grpc.Dial(c.addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
				if err != nil {
					out <- result{err: fmt.Errorf("dial %s: %w", c.addr, err)}
					return
				}
				client := pb.NewKademliaClient(conn)
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				resp, rpcErr := client.LookupNFT(ctx, &pb.LookupNFTReq{
					FromId: "CLI",
					Key:    &pb.Key{Key: target},
				})
				cancel()
				_ = conn.Close()
				if rpcErr != nil {
					out <- result{err: rpcErr}
					return
				}
				if resp.GetFound() {
					out <- result{found: true, holder: resp.GetHolder(), value: resp.GetValue().GetBytes()}
					return
				}
				out <- result{nearest: resp.GetNearest()}
			}(c)
		}

		addedAny := 0
		for i := 0; i < len(next); i++ {
			r := <-out
			if r.err != nil {
				// logga ma non fermarti: altri possono aver successo
				fmt.Printf("   ⚠️ errore batch: %v\n", r.err)
				continue
			}
			if r.found {
				fmt.Printf("✅ Trovato (via batch) su nodo %s\n", r.holder.GetId())
				fmt.Printf("Contenuto JSON:\n%s\n", string(r.value))
				return round, true, nil
			}
			add := buildFromNearest(r.nearest, selfHex) // continua ad escludere self
			addedAny += mergeIntoShort(add)
		}

		// criterio di stop **rilassato**:
		// se in questo round non abbiamo scoperto NESSUN nuovo candidato
		// e non ci sono più non-interrogati nella shortlist → stop.
		if addedAny == 0 {
			future := takeTopUnqueried(queried, alpha)
			if len(future) == 0 {
				fmt.Println("ℹ️ Nessun nuovo nodo e nessun non-interrogato — stop.")
				return round, false, nil
			}
		}
	}

	fmt.Printf("⛔ limite di round (%d) raggiunto, non trovato.\n", maxRounds)
	return maxRounds, false, nil
}

// ---- helper types & funzioni ----

type candidate struct {
	idHex   string
	addr    string // host:port
	label   string
	dist    []byte // XOR distance
	queried bool
}

func xorDistToTarget(target []byte, idHex string) []byte {
	idHex = strings.ToLower(strings.TrimSpace(idHex))
	b, err := hex.DecodeString(idHex)
	if err != nil || len(b) != len(target) {
		return nil
	}
	out := make([]byte, len(target))
	for i := range target {
		out[i] = target[i] ^ b[i]
	}
	return out
}

// converte una lista di pb.Node in candidati, risolvendo host:port (usa fallback via ui.Check)
func nodesToCands(target []byte, nodes []*pb.Node, str []Pair) []candidate {
	cands := make([]candidate, 0, len(nodes))
	for _, n := range nodes {
		id := strings.ToLower(strings.TrimSpace(n.GetId()))
		if id == "" {
			continue
		}

		var addr, label string
		host := strings.TrimSpace(n.GetHost())
		port := int(n.GetPort())
		if host != "" && port > 0 {
			addr = fmt.Sprintf("%s:%d", host, port)
			label = host
		} else {
			if name := Check(id, str); name != "NOTFOUND" {
				if hp, e := ResolveStartHostPort(name); e == nil {
					addr = hp
					label = name
				}
			}
		}
		if addr == "" {
			// non usabile per RPC
			continue
		}
		if label == "" {
			if host != "" {
				label = host
			} else if len(id) >= 8 {
				label = id[:8]
			} else {
				label = id
			}
		}
		dist := xorDistToTarget(target, id)
		if dist == nil {
			continue
		}
		cands = append(cands, candidate{idHex: id, addr: addr, label: label, dist: dist})
	}
	return cands
}

// inizializza la shortlist ordinata per distanza (dedup per idHex)
func buildShortlist(target []byte, nodes []*pb.Node, str []Pair) ([]candidate, []byte) {
	m := map[string]candidate{}
	for _, c := range nodesToCands(target, nodes, str) {
		if old, ok := m[c.idHex]; !ok || bytes.Compare(c.dist, old.dist) < 0 {
			m[c.idHex] = c
		}
	}
	short := make([]candidate, 0, len(m))
	for _, c := range m {
		short = append(short, c)
	}
	sort.Slice(short, func(i, j int) bool { return bytes.Compare(short[i].dist, short[j].dist) < 0 })
	var best []byte
	if len(short) > 0 {
		best = append([]byte(nil), short[0].dist...)
	}
	return short, best
}

// merge di nuovi nearest nella shortlist esistente, mantenendo l’ordinamento per distanza.
// Ritorna: quanti aggiunti, e la nuova best distance.
func mergeIntoShortlist(target []byte, short *[]candidate, nodes []*pb.Node, str []Pair) (int, []byte) {
	// porta *short in mappa per dedup
	m := map[string]candidate{}
	for _, c := range *short {
		m[c.idHex] = c
	}

	added := 0
	for _, c := range nodesToCands(target, nodes, str) {
		if old, ok := m[c.idHex]; !ok || bytes.Compare(c.dist, old.dist) < 0 {
			m[c.idHex] = c
			added++
		}
	}
	// ricostruisci slice ordinata
	newShort := make([]candidate, 0, len(m))
	for _, c := range m {
		newShort = append(newShort, c)
	}
	sort.Slice(newShort, func(i, j int) bool { return bytes.Compare(newShort[i].dist, newShort[j].dist) < 0 })
	*short = newShort

	var best []byte
	if len(newShort) > 0 {
		best = newShort[0].dist
	}
	return added, best
}

// prende i primi α non ancora interrogati
func takeTopUnqueried(short []candidate, queried map[string]bool, alpha int) []candidate {
	out := make([]candidate, 0, alpha)
	for _, c := range short {
		if len(out) >= alpha {
			break
		}
		if queried[c.idHex] {
			continue
		}
		out = append(out, c)
	}
	return out
}
