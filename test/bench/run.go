//go:build bench

package bench

import (
	"errors"
	"fmt"
	"time"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/session"
	"github.com/ccuetoh/sriracha/token"
)

// options drives run end-to-end. Pairs is forwarded verbatim to
// samplePairs. Threshold, when > 0, asks run to evaluate the confusion
// matrix at exactly that operating point in addition to the sweep —
// useful for reporting metrics at a previously calibrated threshold.
type options struct {
	Pairs     pairOptions
	Threshold float64
}

// counts captures the raw size signals of a benchmark run.
type counts struct {
	Records       int            `json:"records"`
	Positives     int            `json:"positives"`
	Negatives     int            `json:"negatives"`
	DroppedFields map[string]int `json:"dropped_fields,omitempty"`
}

// performance bundles a latency distribution with throughput and the wall
// time spent. Throughput is operations per second over the same wall
// window the latency measurements were drawn from.
type performance struct {
	Latency       latencyStats  `json:"latency"`
	TotalDuration time.Duration `json:"total_duration"`
	Throughput    float64       `json:"throughput_per_sec"`
}

// result is the full output of run: schema fingerprint, sample sizes,
// quality (auroc, auprc, operating points, ROC), and performance
// (tokenize and match latency + throughput).
type result struct {
	FieldSetVersion     string `json:"field_set_version"`
	FieldSetFingerprint string `json:"field_set_fingerprint"`

	Counts counts `json:"counts"`

	AUROC float64 `json:"auroc"`
	AUPRC float64 `json:"auprc"`

	BestF1       operatingPoint `json:"best_f1"`
	BestAccuracy operatingPoint `json:"best_accuracy"`
	BestMCC      operatingPoint `json:"best_mcc"`

	PrecisionAtRecall95 *operatingPoint `json:"precision_at_recall_95,omitempty"`
	RecallAtPrecision99 *operatingPoint `json:"recall_at_precision_99,omitempty"`
	AtThreshold         *operatingPoint `json:"at_threshold,omitempty"`

	ROC []operatingPoint `json:"roc"`

	Tokenize performance `json:"tokenize"`
	Match    performance `json:"match"`
}

// run loads records → tokenizes → samples pairs → matches → reports.
// The session is borrowed, never destroyed by run; the caller owns its
// lifecycle (and the locked secret buffer behind it).
func run(sess session.Session, records []record, opts options) (result, error) {
	if sess == nil {
		return result{}, errors.New("bench: session must not be nil")
	}
	if len(records) < 2 {
		return result{}, errors.New("bench: need at least 2 records to run a benchmark")
	}
	if opts.Threshold < 0 || opts.Threshold > 1 {
		return result{}, fmt.Errorf("bench: threshold must be in [0,1], got %v", opts.Threshold)
	}

	pairs, err := samplePairs(records, opts.Pairs)
	if err != nil {
		return result{}, err
	}

	tokens, tokenizePerf, drops, err := tokenizeAll(sess, records)
	if err != nil {
		return result{}, err
	}

	scores, labels, matchPerf, err := matchAll(sess, pairs, tokens)
	if err != nil {
		return result{}, err
	}

	points, err := sweep(scores, labels)
	if err != nil {
		return result{}, err
	}

	bestF1, err := pickBest(points, func(p operatingPoint) float64 { return p.F1 })
	if err != nil {
		return result{}, err
	}
	bestAcc, err := pickBest(points, func(p operatingPoint) float64 { return p.Accuracy })
	if err != nil {
		return result{}, err
	}
	bestMCC, err := pickBest(points, func(p operatingPoint) float64 { return p.MCC })
	if err != nil {
		return result{}, err
	}

	fs := sess.FieldSet()
	res := result{
		FieldSetVersion:     fs.Version,
		FieldSetFingerprint: fs.Fingerprint(),
		Counts: counts{
			Records:       len(records),
			Positives:     countLabels(pairs, true),
			Negatives:     countLabels(pairs, false),
			DroppedFields: stringifyDrops(drops),
		},
		AUROC:        auroc(scores, labels),
		AUPRC:        auprc(scores, labels),
		BestF1:       bestF1,
		BestAccuracy: bestAcc,
		BestMCC:      bestMCC,
		ROC:          points,
		Tokenize:     tokenizePerf,
		Match:        matchPerf,
	}

	if pt, ok := pickAtMinRecall(points, 0.95); ok {
		res.PrecisionAtRecall95 = &pt
	}
	if pt, ok := pickAtMinPrecision(points, 0.99); ok {
		res.RecallAtPrecision99 = &pt
	}
	if opts.Threshold > 0 {
		pt := confusion(scores, labels, opts.Threshold)
		res.AtThreshold = &pt
	}

	return res, nil
}

// calibrate threshold-tunes the session's FieldSet against records. It
// tokenises every record once, samples calibration pairs under opts, then
// hands the labeled pairs to token.Calibrate. Used by TestQualityCalibrated
// to derive an F1-optimal threshold before the evaluation pass.
func calibrate(sess session.Session, records []record, opts pairOptions) (token.Calibration, error) {
	if sess == nil {
		return token.Calibration{}, errors.New("bench: session must not be nil")
	}
	pairs, err := samplePairs(records, opts)
	if err != nil {
		return token.Calibration{}, err
	}
	tokens, _, _, err := tokenizeAll(sess, records)
	if err != nil {
		return token.Calibration{}, err
	}
	labeled := make([]token.LabeledPair, len(pairs))
	for i, p := range pairs {
		labeled[i] = token.LabeledPair{A: tokens[p.A], B: tokens[p.B], Match: p.Match}
	}
	return token.Calibrate(labeled, sess.FieldSet())
}

// tokenizeAll converts every record to a BloomToken under sess and times
// each call. Sanitisation drops fields the normalizer rejects (common in
// real-world corpora) so a single bad country code does not abort the
// run; the per-path drop counts are returned alongside.
func tokenizeAll(sess session.Session, records []record) ([]sriracha.BloomToken, performance, map[sriracha.FieldPath]int, error) {
	tokens := make([]sriracha.BloomToken, len(records))
	durs := make([]time.Duration, len(records))
	drops := make(map[sriracha.FieldPath]int)

	wallStart := time.Now()
	for i, r := range records {
		clean, dropped := sanitize(r.Fields)
		for path := range dropped {
			drops[path]++
		}
		callStart := time.Now()
		tok, err := sess.TokenizeBloom(clean)
		if err != nil {
			return nil, performance{}, nil, fmt.Errorf("bench: tokenize record %d (entity=%q dataset=%q): %w",
				i, r.EntityID, r.Dataset, err)
		}
		durs[i] = time.Since(callStart)
		tokens[i] = tok
	}
	wall := time.Since(wallStart)

	return tokens, performance{
		Latency:       summariseLatencies(durs),
		TotalDuration: wall,
		Throughput:    throughput(len(records), wall),
	}, drops, nil
}

// matchAll runs Match over every sampled pair. We call Match with
// threshold=0 because we only care about the aggregate Score here — the
// thresholded IsMatch decision is recomputed by sweep across the full
// 101-point grid.
func matchAll(sess session.Session, pairs []pair, tokens []sriracha.BloomToken) ([]float64, []bool, performance, error) {
	scores := make([]float64, len(pairs))
	labels := make([]bool, len(pairs))
	durs := make([]time.Duration, len(pairs))

	wallStart := time.Now()
	for i, p := range pairs {
		if p.A < 0 || p.A >= len(tokens) || p.B < 0 || p.B >= len(tokens) {
			return nil, nil, performance{}, fmt.Errorf("bench: pair %d references out-of-range token indices (a=%d b=%d, len=%d)",
				i, p.A, p.B, len(tokens))
		}
		callStart := time.Now()
		res, err := sess.Match(tokens[p.A], tokens[p.B], 0)
		if err != nil {
			return nil, nil, performance{}, fmt.Errorf("bench: match pair %d (a=%d b=%d): %w", i, p.A, p.B, err)
		}
		durs[i] = time.Since(callStart)
		scores[i] = res.Score
		labels[i] = p.Match
	}
	wall := time.Since(wallStart)

	return scores, labels, performance{
		Latency:       summariseLatencies(durs),
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

// countLabels reports how many pairs match the requested label.
func countLabels(pairs []pair, want bool) int {
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
// dropped so omitempty can elide the field.
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
