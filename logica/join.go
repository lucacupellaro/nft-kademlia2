// logica/join.go
package logica

import (
	"context"

	"encoding/hex"
	"math/rand"
	"sort"
	"strings"
	"sync"

	"kademlia-nft/common"
)

const (
	maxPingParallel = 12
	alphaDefault    = 2
	itersDefault    = 2
)

// util
func uniq(ss []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(ss))
	for _, s := range ss {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
func randID160() []byte { b := make([]byte, 20); _, _ = rand.Read(b); return b }

// Ping molti con worker-pool; se ok, entra nei bucket via TouchContact.
func pingMany(ctx context.Context, self string, names []string, par int) []string {
	names = uniq(names)
	sem := make(chan struct{}, par)
	var mu sync.Mutex
	live := make([]string, 0, len(names))
	var wg sync.WaitGroup
	for _, n := range names {
		wg.Add(1)
		sem <- struct{}{}
		go func(name string) {
			defer wg.Done()
			defer func() { <-sem }()
			if err := RpcPing(ctx, self, name); err == nil {
				mu.Lock()
				live = append(live, name)
				mu.Unlock()
			}
		}(n)
	}
	wg.Wait()
	return live
}

// Join + 1..N ondate di espansione.
func JoinAndExpand(ctx context.Context, seederAddr, selfName string, seedSample, alpha, iters int) error {
	if alpha <= 0 {
		alpha = alphaDefault
	}
	if iters <= 0 {
		iters = itersDefault
	}
	if seedSample <= 0 {
		seedSample = 2 * kCapacity
	} // kCapacity è già globale tua

	// 1) seed dal seeder (funzione che hai già)
	seeds, err := GetNodeListIDs(seederAddr, selfName)
	if err != nil {
		return err
	}
	clean := make([]string, 0, len(seeds))
	for _, s := range seeds {
		s = strings.TrimSpace(s)
		if s != "" && s != selfName {
			clean = append(clean, s)
		}
	}
	clean = uniq(clean)
	rand.Shuffle(len(clean), func(i, j int) { clean[i], clean[j] = clean[j], clean[i] })
	if len(clean) > seedSample {
		clean = clean[:seedSample]
	}

	// 2) ping seed → vivi
	live := pingMany(ctx, selfName, clean, maxPingParallel)
	if len(live) == 0 {
		return nil // nessuno vivo: ci riproverai più tardi
	}

	selfHex := hex.EncodeToString(common.Sha1ID(selfName))
	selfRaw := common.Sha1ID(selfName)

	for it := 0; it < iters; it++ {
		if len(live) == 0 {
			break
		}

		// carica KBucket corrente (formato semplice)
		kb, err := loadKBucket(kBucketPath)
		if err != nil {
			return err
		}

		// 3) scoperta: da ciascun vivo fai 2 query (target=self, target=random)
		type prospect struct {
			Name     string
			ScoreHex string
		}
		pros := make([]prospect, 0, 256)
		var mu sync.Mutex
		sem := make(chan struct{}, alpha)

		for _, src := range live {
			for _, tgt := range [][]byte{selfRaw, randID160()} {
				src, tgt := src, tgt
				sem <- struct{}{}
				go func() {
					defer func() { <-sem }()
					names, err := RpcFindNodeNames(ctx, selfName, src, tgt)
					if err != nil {
						return
					}
					// accumula prospects
					mu.Lock()
					for _, nm := range names {
						if nm == "" || nm == selfName {
							continue
						}
						hexID := hex.EncodeToString(common.Sha1ID(nm))
						// filtra: no self, no già presente
						already := false
						for _, h := range kb.BucketHex {
							if h == hexID {
								already = true
								break
							}
						}
						if already || hexID == selfHex {
							continue
						}
						score := hex.EncodeToString(xor(tgt, common.Sha1ID(nm)))
						pros = append(pros, prospect{Name: nm, ScoreHex: score})
					}
					mu.Unlock()
				}()
			}
		}
		// aspetta fine
		for i := 0; i < cap(sem); i++ {
			sem <- struct{}{}
		}

		if len(pros) == 0 {
			break
		}

		// 4) ordina per vicinanza al target e pingane un numero ragionevole (budget)
		// (senza per-bucket, semplice e compat)
		sort.SliceStable(pros, func(i, j int) bool { return pros[i].ScoreHex < pros[j].ScoreHex })
		toPing := make([]string, 0, 2*kCapacity)
		for _, p := range pros {
			if len(toPing) >= 2*kCapacity {
				break
			}
			toPing = append(toPing, p.Name)
		}
		toPing = uniq(toPing)

		// 5) ping mirato → entrano via TouchContact
		live = pingMany(ctx, selfName, toPing, maxPingParallel)
	}

	return nil
}
