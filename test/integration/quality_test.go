//go:build integration

package integration_test

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

// matchedPair is one (a, b) hit produced by the matcher under test.
type matchedPair struct {
	aID string
	bID string
}

// matchQuality summarises precision and recall of a candidate pair set
// against a ground-truth pair set.
type matchQuality struct {
	tp        int
	fp        int
	fn        int
	precision float64
	recall    float64
}

// computeQuality compares got pairs against the ground-truth pair set and
// returns the resulting precision and recall metrics. A pair is counted as a
// true positive if its (aID, bID) tuple is present in truth; precision is
// tp/(tp+fp), recall is tp/(tp+fn).
func computeQuality(got []matchedPair, truth map[string]struct{}) matchQuality {
	q := matchQuality{}
	gotSet := make(map[string]struct{}, len(got))
	for _, p := range got {
		key := truePairKey(p.aID, p.bID)
		if _, dup := gotSet[key]; dup {
			continue
		}
		gotSet[key] = struct{}{}
		if _, ok := truth[key]; ok {
			q.tp++
		} else {
			q.fp++
		}
	}
	for key := range truth {
		if _, ok := gotSet[key]; !ok {
			q.fn++
		}
	}
	if q.tp+q.fp > 0 {
		q.precision = float64(q.tp) / float64(q.tp+q.fp)
	}
	if q.tp+q.fn > 0 {
		q.recall = float64(q.tp) / float64(q.tp+q.fn)
	}
	return q
}

// assertMatchQuality fails the test if precision or recall fall below the
// supplied minimums. On failure, the report includes the missing true pairs
// (recall) and a sample of false positives (precision) for fast debugging.
func assertMatchQuality(t *testing.T, got []matchedPair, truth map[string]struct{}, minPrecision, minRecall float64) {
	t.Helper()
	q := computeQuality(got, truth)

	t.Logf("match quality: tp=%d fp=%d fn=%d precision=%.3f recall=%.3f",
		q.tp, q.fp, q.fn, q.precision, q.recall)

	if q.recall < minRecall {
		missed := sortedMissingPairs(got, truth)
		assert.Failf(t, "recall below threshold",
			"got recall=%.3f want>=%.3f; missed %d pairs (showing up to 10): %v",
			q.recall, minRecall, len(missed), firstN(missed, 10))
	}
	if q.precision < minPrecision {
		extra := sortedExtraPairs(got, truth)
		assert.Failf(t, "precision below threshold",
			"got precision=%.3f want>=%.3f; %d false positives (showing up to 10): %v",
			q.precision, minPrecision, len(extra), firstN(extra, 10))
	}
}

func sortedMissingPairs(got []matchedPair, truth map[string]struct{}) []string {
	gotSet := make(map[string]struct{}, len(got))
	for _, p := range got {
		gotSet[truePairKey(p.aID, p.bID)] = struct{}{}
	}
	out := make([]string, 0)
	for key := range truth {
		if _, ok := gotSet[key]; !ok {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

func sortedExtraPairs(got []matchedPair, truth map[string]struct{}) []string {
	out := make([]string, 0)
	for _, p := range got {
		key := truePairKey(p.aID, p.bID)
		if _, ok := truth[key]; !ok {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

func firstN(s []string, n int) []string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
