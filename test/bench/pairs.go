//go:build bench

package bench

import (
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
)

// pair is a labeled comparison between two corpus records, identified by
// their indices in the slice returned by loadJSONL. Match is the ground
// truth (records share a CanonicalID).
type pair struct {
	A     int  `json:"a"`
	B     int  `json:"b"`
	Match bool `json:"match"`
}

// pairOptions configures samplePairs. Zero values produce an error rather
// than a silent default — pair counts shape every downstream metric so the
// caller must state intent explicitly.
type pairOptions struct {
	// Positives is the maximum number of positive (same-CanonicalID) pairs
	// to draw. If the corpus contains fewer eligible positives, every
	// available positive is returned.
	Positives int
	// Negatives is the maximum number of negative (different-CanonicalID)
	// pairs to draw. Sampling stops early if the candidate space is
	// exhausted.
	Negatives int
	// Seed seeds the deterministic PRNG. The same Seed against the same
	// corpus produces the same pair list, byte for byte.
	Seed uint64
}

// samplePairs draws labeled pairs from records under opts. Positives are
// drawn uniformly at random from the within-canonical-group pair space
// (without replacement); negatives are drawn by picking two distinct
// canonical groups and one record from each, also without replacement.
//
// The output is shuffled so positives and negatives interleave — downstream
// streaming consumers see a representative mix rather than all positives
// first.
func samplePairs(records []record, opts pairOptions) ([]pair, error) {
	if len(records) < 2 {
		return nil, errors.New("bench: need at least 2 records to sample pairs")
	}
	if opts.Positives < 0 || opts.Negatives < 0 {
		return nil, fmt.Errorf("bench: pair counts must be non-negative, got positives=%d negatives=%d",
			opts.Positives, opts.Negatives)
	}
	if opts.Positives == 0 && opts.Negatives == 0 {
		return nil, errors.New("bench: at least one of Positives or Negatives must be > 0")
	}

	groups := groupByCanonical(records)
	keys := sortedKeys(groups)

	rng := rand.New(rand.NewPCG(opts.Seed, opts.Seed^0x9E3779B97F4A7C15)) //nolint:gosec // G404: PRNG is intentional — sampling must be reproducible from Seed

	positives, err := samplePositives(groups, keys, opts.Positives, rng)
	if err != nil {
		return nil, err
	}
	negatives, err := sampleNegatives(groups, keys, opts.Negatives, rng)
	if err != nil {
		return nil, err
	}

	pairs := make([]pair, 0, len(positives)+len(negatives))
	pairs = append(pairs, positives...)
	pairs = append(pairs, negatives...)
	rng.Shuffle(len(pairs), func(i, j int) { pairs[i], pairs[j] = pairs[j], pairs[i] })
	return pairs, nil
}

// sortedKeys returns groups' keys in sorted order so samplePairs is
// deterministic across map iteration: Go randomises map order per process,
// so any sampling driven by `for k := range groups` would shift the PRNG
// stream and break reproducibility.
func sortedKeys(groups map[string][]int) []string {
	keys := make([]string, 0, len(groups))
	for k := range groups {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// samplePositives walks every canonical group with size >= 2 and emits
// (size choose 2) candidate pairs. If the candidate pool is smaller than
// want, all are returned; otherwise it draws want pairs uniformly without
// replacement via Fisher–Yates partial shuffle.
func samplePositives(groups map[string][]int, keys []string, want int, rng *rand.Rand) ([]pair, error) {
	if want == 0 {
		return nil, nil
	}
	var pool []pair
	for _, k := range keys {
		idxs := groups[k]
		if len(idxs) < 2 {
			continue
		}
		for i := 0; i < len(idxs); i++ {
			for j := i + 1; j < len(idxs); j++ {
				pool = append(pool, pair{A: idxs[i], B: idxs[j], Match: true})
			}
		}
	}
	if len(pool) == 0 {
		return nil, errors.New("bench: corpus has no canonical group with >=2 records — cannot sample positive pairs")
	}
	if want >= len(pool) {
		return pool, nil
	}
	for i := 0; i < want; i++ {
		j := i + rng.IntN(len(pool)-i)
		pool[i], pool[j] = pool[j], pool[i]
	}
	return pool[:want], nil
}

// sampleNegatives draws want pairs of records from two distinct canonical
// groups. It guards against accidental positive collisions by requiring
// different CanonicalIDs, and against duplicate pairs via a (a,b) set with
// canonicalised ordering. The candidate space is huge (≈ N²) so rejection
// sampling terminates fast in practice; we cap retries to fail loudly
// rather than spin forever on a degenerate corpus.
func sampleNegatives(groups map[string][]int, keys []string, want int, rng *rand.Rand) ([]pair, error) {
	if want == 0 {
		return nil, nil
	}
	if len(keys) < 2 {
		return nil, errors.New("bench: corpus has fewer than 2 canonical groups — cannot sample negative pairs")
	}

	seen := make(map[[2]int]struct{}, want)
	out := make([]pair, 0, want)

	maxAttempts := want*100 + 1000
	for attempt := 0; len(out) < want && attempt < maxAttempts; attempt++ {
		ai := rng.IntN(len(keys))
		bi := rng.IntN(len(keys))
		if ai == bi {
			continue
		}
		ag, bg := groups[keys[ai]], groups[keys[bi]]
		a := ag[rng.IntN(len(ag))]
		b := bg[rng.IntN(len(bg))]
		if a > b {
			a, b = b, a
		}
		key := [2]int{a, b}
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, pair{A: key[0], B: key[1], Match: false})
	}
	if len(out) < want {
		return nil, fmt.Errorf("bench: could not draw %d unique negative pairs after %d attempts (got %d)",
			want, maxAttempts, len(out))
	}
	return out, nil
}
