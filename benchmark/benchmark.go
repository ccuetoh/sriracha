package benchmark

import (
	"errors"
	"fmt"
	"time"

	"go.sriracha.dev/session"
	"go.sriracha.dev/sriracha"
)

// Options drives Run end-to-end. Pairs is forwarded verbatim to
// SamplePairs. Threshold, when > 0, asks Run to evaluate the confusion
// matrix at exactly that operating point in addition to the sweep —
// useful for benchmarking against a previously calibrated production
// threshold.
type Options struct {
	Pairs     PairOptions
	Threshold float64
}

// Counts captures the raw size signals of a benchmark run: how many
// records went in, how many fields were dropped during sanitisation, and
// how many of each pair class were sampled. Reporting these alongside
// quality metrics is what keeps "F1=0.95" honest — you can see whether
// it was measured over 50 pairs or 50,000.
type Counts struct {
	Records       int            `json:"records"`
	Positives     int            `json:"positives"`
	Negatives     int            `json:"negatives"`
	DroppedFields map[string]int `json:"dropped_fields,omitempty"`
}

// Performance bundles a latency distribution with throughput and the wall
// time spent. Throughput is operations per second over the same wall
// window the latency measurements were drawn from.
type Performance struct {
	Latency       LatencyStats  `json:"latency"`
	TotalDuration time.Duration `json:"total_duration"`
	Throughput    float64       `json:"throughput_per_sec"`
}

// Result is the full output of Run: schema fingerprint, sample sizes,
// quality (AUROC, AUPRC, operating points, ROC), and performance
// (tokenize and match latency + throughput). It is shaped for direct
// json.Marshal without further massaging.
//
// Optional operating points use pointer fields so a JSON consumer can
// distinguish "no point met the constraint" (null) from "the point was
// (0, 0)".
type Result struct {
	FieldSetVersion     string `json:"field_set_version"`
	FieldSetFingerprint string `json:"field_set_fingerprint"`

	Counts Counts `json:"counts"`

	AUROC float64 `json:"auroc"`
	AUPRC float64 `json:"auprc"`

	BestF1       OperatingPoint `json:"best_f1"`
	BestAccuracy OperatingPoint `json:"best_accuracy"`
	BestMCC      OperatingPoint `json:"best_mcc"`

	PrecisionAtRecall95 *OperatingPoint `json:"precision_at_recall_95,omitempty"`
	RecallAtPrecision99 *OperatingPoint `json:"recall_at_precision_99,omitempty"`
	AtThreshold         *OperatingPoint `json:"at_threshold,omitempty"`

	ROC []OperatingPoint `json:"roc"`

	Tokenize Performance `json:"tokenize"`
	Match    Performance `json:"match"`
}

// Run loads records → tokenizes → samples pairs → matches → reports.
// The session is borrowed, never destroyed by Run; the caller owns its
// lifecycle (and the locked secret buffer behind it).
//
// Returns a Result on success. Any tokenize or match failure short-
// circuits with an error tagged by record index / pair index so a user
// can find the offending row in the corpus.
func Run(sess session.Session, records []Record, opts Options) (Result, error) {
	if sess == nil {
		return Result{}, errors.New("benchmark: session must not be nil")
	}
	if len(records) < 2 {
		return Result{}, errors.New("benchmark: need at least 2 records to run a benchmark")
	}
	if opts.Threshold < 0 || opts.Threshold > 1 {
		return Result{}, fmt.Errorf("benchmark: threshold must be in [0,1], got %v", opts.Threshold)
	}

	pairs, err := SamplePairs(records, opts.Pairs)
	if err != nil {
		return Result{}, err
	}

	tokens, tokenizePerf, drops, err := tokenizeAll(sess, records)
	if err != nil {
		return Result{}, err
	}

	scores, labels, matchPerf, err := matchAll(sess, pairs, tokens)
	if err != nil {
		return Result{}, err
	}

	sweep, err := Sweep(scores, labels)
	if err != nil {
		return Result{}, err
	}

	bestF1, err := PickBest(sweep, func(p OperatingPoint) float64 { return p.F1 })
	if err != nil {
		return Result{}, err
	}
	bestAcc, err := PickBest(sweep, func(p OperatingPoint) float64 { return p.Accuracy })
	if err != nil {
		return Result{}, err
	}
	bestMCC, err := PickBest(sweep, func(p OperatingPoint) float64 { return p.MCC })
	if err != nil {
		return Result{}, err
	}

	fs := sess.FieldSet()
	res := Result{
		FieldSetVersion:     fs.Version,
		FieldSetFingerprint: fs.Fingerprint(),
		Counts: Counts{
			Records:       len(records),
			Positives:     countLabels(pairs, true),
			Negatives:     countLabels(pairs, false),
			DroppedFields: stringifyDrops(drops),
		},
		AUROC:        AUROC(scores, labels),
		AUPRC:        AUPRC(scores, labels),
		BestF1:       bestF1,
		BestAccuracy: bestAcc,
		BestMCC:      bestMCC,
		ROC:          sweep,
		Tokenize:     tokenizePerf,
		Match:        matchPerf,
	}

	if pt, ok := PickAtMinRecall(sweep, 0.95); ok {
		res.PrecisionAtRecall95 = &pt
	}
	if pt, ok := PickAtMinPrecision(sweep, 0.99); ok {
		res.RecallAtPrecision99 = &pt
	}
	if opts.Threshold > 0 {
		pt := confusion(scores, labels, opts.Threshold)
		res.AtThreshold = &pt
	}

	return res, nil
}

// tokenizeAll converts every record to a BloomToken under sess and times
// each call. Sanitisation drops fields the normalizer rejects (common in
// real-world corpora) so a single bad country code does not abort the
// run; the per-path drop counts are returned alongside.
func tokenizeAll(sess session.Session, records []Record) ([]sriracha.BloomToken, Performance, map[sriracha.FieldPath]int, error) {
	tokens := make([]sriracha.BloomToken, len(records))
	durs := make([]time.Duration, len(records))
	drops := make(map[sriracha.FieldPath]int)

	wallStart := time.Now()
	for i, r := range records {
		clean, dropped := Sanitize(r.Fields)
		for path := range dropped {
			drops[path]++
		}
		callStart := time.Now()
		tok, err := sess.TokenizeBloom(clean)
		if err != nil {
			return nil, Performance{}, nil, fmt.Errorf("benchmark: tokenize record %d (entity=%q dataset=%q): %w",
				i, r.EntityID, r.Dataset, err)
		}
		durs[i] = time.Since(callStart)
		tokens[i] = tok
	}
	wall := time.Since(wallStart)

	return tokens, Performance{
		Latency:       SummariseLatencies(durs),
		TotalDuration: wall,
		Throughput:    throughput(len(records), wall),
	}, drops, nil
}

// matchAll runs Match over every sampled pair. We call Match with
// threshold=0 because we only care about the aggregate Score here — the
// thresholded IsMatch decision is recomputed by Sweep across the full
// 101-point grid.
func matchAll(sess session.Session, pairs []Pair, tokens []sriracha.BloomToken) ([]float64, []bool, Performance, error) {
	scores := make([]float64, len(pairs))
	labels := make([]bool, len(pairs))
	durs := make([]time.Duration, len(pairs))

	wallStart := time.Now()
	for i, p := range pairs {
		if p.A < 0 || p.A >= len(tokens) || p.B < 0 || p.B >= len(tokens) {
			return nil, nil, Performance{}, fmt.Errorf("benchmark: pair %d references out-of-range token indices (a=%d b=%d, len=%d)",
				i, p.A, p.B, len(tokens))
		}
		callStart := time.Now()
		res, err := sess.Match(tokens[p.A], tokens[p.B], 0)
		if err != nil {
			return nil, nil, Performance{}, fmt.Errorf("benchmark: match pair %d (a=%d b=%d): %w", i, p.A, p.B, err)
		}
		durs[i] = time.Since(callStart)
		scores[i] = res.Score
		labels[i] = p.Match
	}
	wall := time.Since(wallStart)

	return scores, labels, Performance{
		Latency:       SummariseLatencies(durs),
		TotalDuration: wall,
		Throughput:    throughput(len(pairs), wall),
	}, nil
}

// throughput returns ops/sec, defending against a zero-duration
// denominator (which can happen on extremely fast runs against the
// monotonic clock's resolution) by returning 0.
func throughput(n int, d time.Duration) float64 {
	if d <= 0 {
		return 0
	}
	return float64(n) / d.Seconds()
}

// countLabels reports how many pairs match the requested label. Used to
// fill out Counts without re-scanning the pair list at multiple call
// sites.
func countLabels(pairs []Pair, want bool) int {
	n := 0
	for _, p := range pairs {
		if p.Match == want {
			n++
		}
	}
	return n
}

// stringifyDrops converts the per-FieldPath drop counter to a JSON-ready
// map keyed by canonical path string. Returns nil when nothing was
// dropped so omitempty can elide the field rather than emit `{}`.
func stringifyDrops(drops map[sriracha.FieldPath]int) map[string]int {
	if len(drops) == 0 {
		return nil
	}
	out := make(map[string]int, len(drops))
	for path, n := range drops {
		out[path.String()] = n
	}
	return out
}
