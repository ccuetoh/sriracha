//go:build bench

package bench

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSummariseLatencies(t *testing.T) {
	t.Parallel()

	t.Run("EmptyYieldsZero", func(t *testing.T) {
		t.Parallel()
		stats := summariseLatencies(nil)
		assert.Equal(t, latencyStats{}, stats)
	})

	t.Run("SingleSample", func(t *testing.T) {
		t.Parallel()
		stats := summariseLatencies([]time.Duration{100 * time.Microsecond})
		assert.Equal(t, 1, stats.Count)
		assert.Equal(t, 100*time.Microsecond, stats.Mean)
		assert.Equal(t, 100*time.Microsecond, stats.P50)
		assert.Equal(t, 100*time.Microsecond, stats.P99)
		assert.Equal(t, 100*time.Microsecond, stats.Max)
	})

	t.Run("PercentilesSortedAscending", func(t *testing.T) {
		t.Parallel()
		durs := make([]time.Duration, 100)
		for i := range durs {
			durs[i] = time.Duration(i+1) * time.Millisecond
		}
		stats := summariseLatencies(durs)
		assert.Equal(t, 100, stats.Count)
		assert.Equal(t, 50*time.Millisecond, stats.P50)
		assert.Equal(t, 95*time.Millisecond, stats.P95)
		assert.Equal(t, 99*time.Millisecond, stats.P99)
		assert.Equal(t, 100*time.Millisecond, stats.Max)
	})
}

func TestSweep(t *testing.T) {
	t.Parallel()

	t.Run("PerfectSeparationGivesPerfectF1", func(t *testing.T) {
		t.Parallel()
		scores := []float64{0.9, 0.8, 0.1, 0.2}
		labels := []bool{true, true, false, false}
		points, err := sweep(scores, labels)
		require.NoError(t, err)
		require.Len(t, points, 101)

		best, err := pickBest(points, func(p operatingPoint) float64 { return p.F1 })
		require.NoError(t, err)
		assert.InDelta(t, 1.0, best.F1, 1e-9)
		assert.InDelta(t, 1.0, best.Precision, 1e-9)
		assert.InDelta(t, 1.0, best.Recall, 1e-9)
		assert.InDelta(t, 1.0, best.Accuracy, 1e-9)
	})

	t.Run("AccuracyAndMCCComputed", func(t *testing.T) {
		t.Parallel()
		scores := []float64{0.9, 0.6, 0.4, 0.1}
		labels := []bool{true, false, true, false}
		points, err := sweep(scores, labels)
		require.NoError(t, err)
		assert.NotZero(t, points[50].Accuracy)
	})

	t.Run("LengthMismatchErrors", func(t *testing.T) {
		t.Parallel()
		_, err := sweep([]float64{0.5}, []bool{true, false})
		require.Error(t, err)
	})

	t.Run("EmptyErrors", func(t *testing.T) {
		t.Parallel()
		_, err := sweep(nil, nil)
		require.Error(t, err)
	})
}

func TestAUROC(t *testing.T) {
	t.Parallel()

	t.Run("PerfectSeparation", func(t *testing.T) {
		t.Parallel()
		v := auroc([]float64{0.9, 0.8, 0.2, 0.1}, []bool{true, true, false, false})
		assert.InDelta(t, 1.0, v, 1e-9)
	})

	t.Run("RandomScoresApproachHalf", func(t *testing.T) {
		t.Parallel()
		v := auroc([]float64{0.5, 0.5, 0.5, 0.5}, []bool{true, false, true, false})
		assert.InDelta(t, 0.5, v, 1e-9)
	})

	t.Run("ReversedScoresGiveZero", func(t *testing.T) {
		t.Parallel()
		v := auroc([]float64{0.1, 0.2, 0.8, 0.9}, []bool{true, true, false, false})
		assert.InDelta(t, 0.0, v, 1e-9)
	})

	t.Run("AllPositivesYieldsZero", func(t *testing.T) {
		t.Parallel()
		assert.InDelta(t, 0.0, auroc([]float64{0.5, 0.6}, []bool{true, true}), 1e-9)
	})

	t.Run("EmptyYieldsZero", func(t *testing.T) {
		t.Parallel()
		assert.InDelta(t, 0.0, auroc(nil, nil), 1e-9)
	})

	t.Run("LengthMismatchYieldsZero", func(t *testing.T) {
		t.Parallel()
		assert.InDelta(t, 0.0, auroc([]float64{0.5}, []bool{true, false}), 1e-9)
	})
}

func TestAUPRC(t *testing.T) {
	t.Parallel()

	t.Run("PerfectSeparation", func(t *testing.T) {
		t.Parallel()
		v := auprc([]float64{0.9, 0.8, 0.2, 0.1}, []bool{true, true, false, false})
		assert.InDelta(t, 1.0, v, 1e-9)
	})

	t.Run("AllPositivesYieldsAvgPrecision1", func(t *testing.T) {
		t.Parallel()
		v := auprc([]float64{0.5, 0.6}, []bool{true, true})
		assert.InDelta(t, 1.0, v, 1e-9)
	})

	t.Run("AllNegativesYieldsZero", func(t *testing.T) {
		t.Parallel()
		assert.InDelta(t, 0.0, auprc([]float64{0.5, 0.6}, []bool{false, false}), 1e-9)
	})

	t.Run("EmptyYieldsZero", func(t *testing.T) {
		t.Parallel()
		assert.InDelta(t, 0.0, auprc(nil, nil), 1e-9)
	})

	t.Run("LengthMismatchYieldsZero", func(t *testing.T) {
		t.Parallel()
		assert.InDelta(t, 0.0, auprc([]float64{0.5}, []bool{true, false}), 1e-9)
	})
}

func TestPickers(t *testing.T) {
	t.Parallel()

	points := []operatingPoint{
		{Threshold: 0.1, Precision: 0.5, Recall: 1.0, F1: 0.667, Accuracy: 0.5},
		{Threshold: 0.5, Precision: 0.9, Recall: 0.95, F1: 0.92, Accuracy: 0.9},
		{Threshold: 0.9, Precision: 1.0, Recall: 0.5, F1: 0.667, Accuracy: 0.7},
	}

	t.Run("PickBestF1", func(t *testing.T) {
		t.Parallel()
		best, err := pickBest(points, func(p operatingPoint) float64 { return p.F1 })
		require.NoError(t, err)
		assert.InDelta(t, 0.5, best.Threshold, 1e-9)
	})

	t.Run("PickBestEmptyErrors", func(t *testing.T) {
		t.Parallel()
		_, err := pickBest(nil, func(p operatingPoint) float64 { return p.F1 })
		require.Error(t, err)
	})

	t.Run("PickAtMinRecallFinds", func(t *testing.T) {
		t.Parallel()
		pt, ok := pickAtMinRecall(points, 0.95)
		require.True(t, ok)
		assert.InDelta(t, 0.95, pt.Recall, 1e-9)
		assert.InDelta(t, 0.9, pt.Precision, 1e-9)
	})

	t.Run("PickAtMinRecallNotFound", func(t *testing.T) {
		t.Parallel()
		_, ok := pickAtMinRecall(points, 1.5)
		assert.False(t, ok)
	})

	t.Run("PickAtMinPrecisionFinds", func(t *testing.T) {
		t.Parallel()
		pt, ok := pickAtMinPrecision(points, 0.99)
		require.True(t, ok)
		assert.InDelta(t, 1.0, pt.Precision, 1e-9)
	})

	t.Run("PickAtMinPrecisionNotFound", func(t *testing.T) {
		t.Parallel()
		_, ok := pickAtMinPrecision(points, 1.5)
		assert.False(t, ok)
	})
}

func TestPercentile(t *testing.T) {
	t.Parallel()
	t.Run("EmptyYieldsZero", func(t *testing.T) {
		t.Parallel()
		assert.Equal(t, time.Duration(0), percentile(nil, 0.5))
	})
	t.Run("PercentileBelowZeroClamps", func(t *testing.T) {
		t.Parallel()
		durs := []time.Duration{1, 2, 3}
		assert.Equal(t, time.Duration(1), percentile(durs, -1))
	})
	t.Run("PercentileAboveOneClamps", func(t *testing.T) {
		t.Parallel()
		durs := []time.Duration{1, 2, 3}
		assert.Equal(t, time.Duration(3), percentile(durs, 2.0))
	})
}

func TestThroughput(t *testing.T) {
	t.Parallel()
	assert.Equal(t, 0.0, throughput(100, 0))
	assert.Equal(t, 0.0, throughput(100, -time.Second))
	assert.InDelta(t, 100.0, throughput(100, time.Second), 1e-9)
}

func TestMCC(t *testing.T) {
	t.Parallel()
	t.Run("PerfectClassifier", func(t *testing.T) {
		t.Parallel()
		assert.InDelta(t, 1.0, mcc(10, 0, 10, 0), 1e-9)
	})
	t.Run("WorstClassifier", func(t *testing.T) {
		t.Parallel()
		assert.InDelta(t, -1.0, mcc(0, 10, 0, 10), 1e-9)
	})
	t.Run("ZeroDenom", func(t *testing.T) {
		t.Parallel()
		assert.InDelta(t, 0.0, mcc(0, 0, 0, 0), 1e-9)
	})
}
