//go:build bench

package bench

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// makeRecords builds a synthetic corpus where sizes[i] is the number of
// records assigned to canonical group "a"+i. So makeRecords([]int{3, 2})
// yields [g0, g0, g0, g1, g1].
func makeRecords(sizes []int) []record {
	var out []record
	for gi, n := range sizes {
		for k := 0; k < n; k++ {
			out = append(out, record{
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
		opts := pairOptions{Positives: 5, Negatives: 5, Seed: 42}
		a, err := samplePairs(records, opts)
		require.NoError(t, err)
		b, err := samplePairs(records, opts)
		require.NoError(t, err)
		assert.Equal(t, a, b)
	})

	t.Run("DiffersForDifferentSeeds", func(t *testing.T) {
		t.Parallel()
		records := makeRecords([]int{3, 3, 3, 3})
		a, err := samplePairs(records, pairOptions{Positives: 5, Negatives: 5, Seed: 1})
		require.NoError(t, err)
		b, err := samplePairs(records, pairOptions{Positives: 5, Negatives: 5, Seed: 2})
		require.NoError(t, err)
		assert.NotEqual(t, a, b)
	})

	t.Run("PositiveLabelsMatchGroundTruth", func(t *testing.T) {
		t.Parallel()
		records := makeRecords([]int{4, 4, 4})
		pairs, err := samplePairs(records, pairOptions{Positives: 6, Negatives: 6, Seed: 1})
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
		pairs, err := samplePairs(records, pairOptions{Positives: 0, Negatives: 10, Seed: 7})
		require.NoError(t, err)
		for _, p := range pairs {
			assert.NotEqual(t, records[p.A].CanonicalID, records[p.B].CanonicalID)
			assert.False(t, p.Match)
		}
	})

	t.Run("CapsAtAvailablePositives", func(t *testing.T) {
		t.Parallel()
		records := makeRecords([]int{2, 2})
		pairs, err := samplePairs(records, pairOptions{Positives: 1000, Negatives: 0, Seed: 1})
		require.NoError(t, err)
		assert.Len(t, pairs, 2, "two groups of 2 yields exactly two positive pairs")
	})

	t.Run("ErrorsOnTooFewRecords", func(t *testing.T) {
		t.Parallel()
		_, err := samplePairs([]record{{CanonicalID: "a"}}, pairOptions{Positives: 1, Seed: 1})
		require.Error(t, err)
	})

	t.Run("ErrorsOnNegativeCounts", func(t *testing.T) {
		t.Parallel()
		records := makeRecords([]int{2, 2})
		_, err := samplePairs(records, pairOptions{Positives: -1, Seed: 1})
		require.Error(t, err)
	})

	t.Run("ErrorsWhenAllZero", func(t *testing.T) {
		t.Parallel()
		records := makeRecords([]int{2, 2})
		_, err := samplePairs(records, pairOptions{Seed: 1})
		require.Error(t, err)
	})

	t.Run("ErrorsWhenNoPositiveGroupsAvailable", func(t *testing.T) {
		t.Parallel()
		records := makeRecords([]int{1, 1, 1, 1})
		_, err := samplePairs(records, pairOptions{Positives: 1, Seed: 1})
		require.Error(t, err)
	})

	t.Run("ErrorsWhenSingleCanonicalGroupForNegatives", func(t *testing.T) {
		t.Parallel()
		records := makeRecords([]int{4})
		_, err := samplePairs(records, pairOptions{Negatives: 1, Seed: 1})
		require.Error(t, err)
	})

	t.Run("PositivesOnlyEmitsAllOnSmallCorpus", func(t *testing.T) {
		t.Parallel()
		records := makeRecords([]int{3})
		pairs, err := samplePairs(records, pairOptions{Positives: 100, Seed: 1})
		require.NoError(t, err)
		assert.Len(t, pairs, 3)
	})
}
