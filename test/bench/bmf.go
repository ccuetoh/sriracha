//go:build bench

package bench

import (
	"encoding/json"
	"fmt"
	"io"
)

// bmfMetric is one numeric value in Bencher Metric Format. lower_value /
// upper_value (confidence bounds) are not emitted by this harness — our
// per-run metrics are point estimates, and Bencher's threshold engine
// derives statistical bounds from history.
type bmfMetric struct {
	Value float64 `json:"value"`
}

// bmfReport is the top-level Bencher Metric Format object: a map keyed by
// benchmark name (e.g. "untuned", "calibrated"), each value a map keyed by
// metric slug (e.g. "auroc", "tokenize_latency_p99_ns"). `bencher run
// --adapter json` consumes this shape directly.
type bmfReport map[string]map[string]bmfMetric

// resultMetrics flattens a result into the metric slugs Bencher tracks.
// Latency values are in nanoseconds (Bencher records numbers; the unit is
// captured by the metric kind in Bencher's project settings, not by this
// emitter).
func resultMetrics(r result) map[string]bmfMetric {
	return map[string]bmfMetric{
		"auroc":                       {Value: r.AUROC},
		"auprc":                       {Value: r.AUPRC},
		"f1":                          {Value: r.BestF1.F1},
		"precision":                   {Value: r.BestF1.Precision},
		"recall":                      {Value: r.BestF1.Recall},
		"accuracy":                    {Value: r.BestAccuracy.Accuracy},
		"mcc":                         {Value: r.BestMCC.MCC},
		"tokenize_latency_p50_ns":     {Value: float64(r.Tokenize.Latency.P50)},
		"tokenize_latency_p99_ns":     {Value: float64(r.Tokenize.Latency.P99)},
		"tokenize_throughput_per_sec": {Value: r.Tokenize.Throughput},
		"match_latency_p99_ns":        {Value: float64(r.Match.Latency.P99)},
		"match_throughput_per_sec":    {Value: r.Match.Throughput},
	}
}

// writeBMF marshals reports as indented JSON to w. Indentation is purely
// for human readability of the artifact uploaded to the GitHub run page;
// `bencher run --adapter json` parses either form.
func writeBMF(w io.Writer, reports bmfReport) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(reports); err != nil {
		return fmt.Errorf("bench: encode BMF: %w", err)
	}
	return nil
}
