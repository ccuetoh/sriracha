package benchmark

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeRecords builds a synthetic corpus with the given canonical group sizes.
// Index i in the returned slice belongs to the group named groups[i % len(groups)]…
// no, it's simpler: the input is one int per group representing that group's
// size, so makeRecords([]int{3, 2, 1}) yields records [g0, g0, g0, g1, g1, g2].
func makeRecords(sizes []int) []Record {
	var out []Record
	for gi, n := range sizes {
		for k := 0; k < n; k++ {
			out = append(out, Record{
				CanonicalID: string(rune('a' + gi)),
				EntityID:    string(rune('a'+gi)) + string(rune('0'+k)),
			})
		}
	}
	return out
}

func TestSamplePairs(t *testing.T) {
	t.Parallel()

	t.Run("DeterministicForSameSeed", func(t *testing.T) {
		t.Parallel()
		records := makeRecords([]int{3, 3, 3, 3})
		opts := PairOptions{Positives: 5, Negatives: 5, Seed: 42}
		a, err := SamplePairs(records, opts)
		require.NoError(t, err)
		b, err := SamplePairs(records, opts)
		require.NoError(t, err)
		assert.Equal(t, a, b)
	})

	t.Run("DiffersForDifferentSeeds", func(t *testing.T) {
		t.Parallel()
		records := makeRecords([]int{3, 3, 3, 3})
		a, err := SamplePairs(records, PairOptions{Positives: 5, Negatives: 5, Seed: 1})
		require.NoError(t, err)
		b, err := SamplePairs(records, PairOptions{Positives: 5, Negatives: 5, Seed: 2})
		require.NoError(t, err)
		assert.NotEqual(t, a, b)
	})

	t.Run("PositiveLabelsMatchGroundTruth", func(t *testing.T) {
		t.Parallel()
		records := makeRecords([]int{4, 4, 4})
		pairs, err := SamplePairs(records, PairOptions{Positives: 6, Negatives: 6, Seed: 1})
		require.NoError(t, err)
		var pos, neg int
		for _, p := range pairs {
			same := records[p.A].CanonicalID == records[p.B].CanonicalID
			assert.Equal(t, same, p.Match, "label must reflect ground-truth canonical_id agreement")
			if p.Match {
				pos++
			} else {
				neg++
			}
		}
		assert.Equal(t, 6, pos)
		assert.Equal(t, 6, neg)
	})

	t.Run("NegativePairsHaveDifferentCanonicalIDs", func(t *testing.T) {
		t.Parallel()
		records := makeRecords([]int{2, 2, 2, 2})
		pairs, err := SamplePairs(records, PairOptions{Positives: 0, Negatives: 10, Seed: 7})
		require.NoError(t, err)
		for _, p := range pairs {
			assert.NotEqual(t, records[p.A].CanonicalID, records[p.B].CanonicalID)
			assert.False(t, p.Match)
		}
	})

	t.Run("CapsAtAvailablePositives", func(t *testing.T) {
		t.Parallel()
		records := makeRecords([]int{2, 2})
		pairs, err := SamplePairs(records, PairOptions{Positives: 1000, Negatives: 0, Seed: 1})
		require.NoError(t, err)
		assert.Len(t, pairs, 2, "two groups of 2 yields exactly two positive pairs")
	})

	t.Run("ErrorsOnTooFewRecords", func(t *testing.T) {
		t.Parallel()
		_, err := SamplePairs([]Record{{CanonicalID: "a"}}, PairOptions{Positives: 1, Seed: 1})
		require.Error(t, err)
	})

	t.Run("ErrorsOnNegativeCounts", func(t *testing.T) {
		t.Parallel()
		records := makeRecords([]int{2, 2})
		_, err := SamplePairs(records, PairOptions{Positives: -1, Seed: 1})
		require.Error(t, err)
	})

	t.Run("ErrorsWhenAllZero", func(t *testing.T) {
		t.Parallel()
		records := makeRecords([]int{2, 2})
		_, err := SamplePairs(records, PairOptions{Seed: 1})
		require.Error(t, err)
	})

	t.Run("ErrorsWhenNoPositiveGroupsAvailable", func(t *testing.T) {
		t.Parallel()
		records := makeRecords([]int{1, 1, 1, 1})
		_, err := SamplePairs(records, PairOptions{Positives: 1, Seed: 1})
		require.Error(t, err)
	})

	t.Run("ErrorsWhenSingleCanonicalGroupForNegatives", func(t *testing.T) {
		t.Parallel()
		records := makeRecords([]int{4})
		_, err := SamplePairs(records, PairOptions{Negatives: 1, Seed: 1})
		require.Error(t, err)
	})

	t.Run("PositivesOnlyEmitsAllOnSmallCorpus", func(t *testing.T) {
		t.Parallel()
		records := makeRecords([]int{3})
		pairs, err := SamplePairs(records, PairOptions{Positives: 100, Seed: 1})
		require.NoError(t, err)
		assert.Len(t, pairs, 3)
	})
}
