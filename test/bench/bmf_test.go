//go:build bench

package bench

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestResultMetrics(t *testing.T) {
	t.Parallel()

	r := result{
		AUROC:        0.92,
		AUPRC:        0.94,
		BestF1:       operatingPoint{F1: 0.90, Precision: 0.93, Recall: 0.87},
		BestAccuracy: operatingPoint{Accuracy: 0.91},
		BestMCC:      operatingPoint{MCC: 0.81},
		Tokenize: performance{
			Latency:    latencyStats{P50: 80 * time.Microsecond, P99: 200 * time.Microsecond},
			Throughput: 10000.0,
		},
		Match: performance{
			Latency:    latencyStats{P99: 13 * time.Microsecond},
			Throughput: 150000.0,
		},
	}

	metrics := resultMetrics(r)
	assert.InDelta(t, 0.92, metrics["auroc"].Value, 1e-9)
	assert.InDelta(t, 0.94, metrics["auprc"].Value, 1e-9)
	assert.InDelta(t, 0.90, metrics["f1"].Value, 1e-9)
	assert.InDelta(t, 0.93, metrics["precision"].Value, 1e-9)
	assert.InDelta(t, 0.87, metrics["recall"].Value, 1e-9)
	assert.InDelta(t, 0.91, metrics["accuracy"].Value, 1e-9)
	assert.InDelta(t, 0.81, metrics["mcc"].Value, 1e-9)
	assert.InDelta(t, float64(80*time.Microsecond), metrics["tokenize_latency_p50_ns"].Value, 1e-9)
	assert.InDelta(t, float64(200*time.Microsecond), metrics["tokenize_latency_p99_ns"].Value, 1e-9)
	assert.InDelta(t, 10000.0, metrics["tokenize_throughput_per_sec"].Value, 1e-9)
	assert.InDelta(t, float64(13*time.Microsecond), metrics["match_latency_p99_ns"].Value, 1e-9)
	assert.InDelta(t, 150000.0, metrics["match_throughput_per_sec"].Value, 1e-9)
}

func TestWriteBMF(t *testing.T) {
	t.Parallel()

	t.Run("EmitsBencherCompatibleShape", func(t *testing.T) {
		t.Parallel()
		reports := bmfReport{
			"untuned": map[string]bmfMetric{
				"auroc": {Value: 0.92},
			},
			"calibrated": map[string]bmfMetric{
				"auroc": {Value: 0.94},
			},
		}
		var buf bytes.Buffer
		require.NoError(t, writeBMF(&buf, reports))

		// Round-trip via json to assert shape.
		var parsed map[string]map[string]map[string]float64
		require.NoError(t, json.Unmarshal(buf.Bytes(), &parsed))
		assert.InDelta(t, 0.92, parsed["untuned"]["auroc"]["value"], 1e-9)
		assert.InDelta(t, 0.94, parsed["calibrated"]["auroc"]["value"], 1e-9)
	})

	t.Run("EmptyReportsEmitsEmptyObject", func(t *testing.T) {
		t.Parallel()
		var buf bytes.Buffer
		require.NoError(t, writeBMF(&buf, bmfReport{}))
		assert.Equal(t, "{}\n", buf.String())
	})

	t.Run("PropagatesWriterError", func(t *testing.T) {
		t.Parallel()
		err := writeBMF(failingWriter{}, bmfReport{"x": {"y": {Value: 1}}})
		require.Error(t, err)
	})
}

// failingWriter always errors; lets us cover the encoder error path
// without involving the OS or a closed pipe.
type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, assert.AnError }
