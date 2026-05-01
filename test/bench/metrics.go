//go:build bench

package bench

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"
)

// latencyStats summarises a population of timed operations. Durations are
// stored at native time.Duration precision so callers can format them as
// they like (e.g. millisecond floats for JSON, time.Duration.String for
// logs). Count is included so a report can distinguish "p99=10µs over 5
// samples" from "p99=10µs over 5,000,000 samples".
type latencyStats struct {
	Count int           `json:"count"`
	Mean  time.Duration `json:"mean"`
	P50   time.Duration `json:"p50"`
	P95   time.Duration `json:"p95"`
	P99   time.Duration `json:"p99"`
	Max   time.Duration `json:"max"`
}

// summariseLatencies turns a slice of durations into the canonical
// latencyStats. The input is sorted in place — the harness never reuses
// the durations slice after this call, so the in-place sort is the cheap
// option.
//
// An empty input yields a zero-valued latencyStats with Count=0; callers
// can distinguish "no measurements" from "every measurement was zero" via
// Count.
func summariseLatencies(durs []time.Duration) latencyStats {
	if len(durs) == 0 {
		return latencyStats{}
	}
	sort.Slice(durs, func(i, j int) bool { return durs[i] < durs[j] })
	var sum time.Duration
	for _, d := range durs {
		sum += d
	}
	return latencyStats{
		Count: len(durs),
		Mean:  sum / time.Duration(len(durs)),
		P50:   percentile(durs, 0.50),
		P95:   percentile(durs, 0.95),
		P99:   percentile(durs, 0.99),
		Max:   durs[len(durs)-1],
	}
}

// percentile assumes durs is sorted ascending; it returns the
// nearest-rank percentile (no interpolation), which is the conventional
// choice for latency percentiles in observability tools.
func percentile(durs []time.Duration, p float64) time.Duration {
	if len(durs) == 0 {
		return 0
	}
	idx := int(math.Ceil(p*float64(len(durs)))) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(durs) {
		idx = len(durs) - 1
	}
	return durs[idx]
}

// operatingPoint is the full confusion matrix and derived metrics at a
// single threshold. F1 and MCC are emitted alongside the simpler ratios so
// downstream dashboards don't have to recompute them from TP/FP/TN/FN.
type operatingPoint struct {
	Threshold float64 `json:"threshold"`
	TP        int     `json:"tp"`
	FP        int     `json:"fp"`
	TN        int     `json:"tn"`
	FN        int     `json:"fn"`
	Precision float64 `json:"precision"`
	Recall    float64 `json:"recall"`
	F1        float64 `json:"f1"`
	Accuracy  float64 `json:"accuracy"`
	FPR       float64 `json:"fpr"`
	MCC       float64 `json:"mcc"`
}

// sweep evaluates per-pair scores against labels at thresholds 0.00, 0.01,
// …, 1.00 (101 points). Returns one operatingPoint per threshold in
// ascending threshold order.
func sweep(scores []float64, labels []bool) ([]operatingPoint, error) {
	if len(scores) != len(labels) {
		return nil, fmt.Errorf("bench: scores/labels length mismatch (%d vs %d)", len(scores), len(labels))
	}
	if len(scores) == 0 {
		return nil, errors.New("bench: cannot sweep empty score set")
	}
	const steps = 101
	out := make([]operatingPoint, steps)
	for s := 0; s < steps; s++ {
		t := float64(s) / 100
		out[s] = confusion(scores, labels, t)
	}
	return out, nil
}

// confusion builds one operatingPoint at threshold t. Pulling it out makes
// sweep readable and gives run a way to evaluate ad-hoc thresholds (e.g.
// the threshold returned by token.Calibrate) without re-running the whole
// 101-step sweep.
func confusion(scores []float64, labels []bool, t float64) operatingPoint {
	var tp, fp, tn, fn int
	for i, s := range scores {
		predicted := s >= t
		switch {
		case predicted && labels[i]:
			tp++
		case predicted && !labels[i]:
			fp++
		case !predicted && labels[i]:
			fn++
		default:
			tn++
		}
	}
	precision := safeRatio(tp, tp+fp)
	recall := safeRatio(tp, tp+fn)
	fpr := safeRatio(fp, fp+tn)
	accuracy := safeRatio(tp+tn, tp+fp+tn+fn)
	f1 := 0.0
	if precision+recall > 0 {
		f1 = 2 * precision * recall / (precision + recall)
	}
	return operatingPoint{
		Threshold: t,
		TP:        tp, FP: fp, TN: tn, FN: fn,
		Precision: precision,
		Recall:    recall,
		F1:        f1,
		Accuracy:  accuracy,
		FPR:       fpr,
		MCC:       mcc(tp, fp, tn, fn),
	}
}

// safeRatio returns num/den or 0 when den is zero. Avoids NaN propagating
// into the JSON report.
func safeRatio(num, den int) float64 {
	if den == 0 {
		return 0
	}
	return float64(num) / float64(den)
}

// mcc is the Matthews correlation coefficient — a single-number summary
// that stays meaningful under heavy class imbalance, which the
// OpenSanctions-style corpora always have. Returns 0 when the denominator
// degenerates.
func mcc(tp, fp, tn, fn int) float64 {
	num := float64(tp)*float64(tn) - float64(fp)*float64(fn)
	den := math.Sqrt(float64(tp+fp) * float64(tp+fn) * float64(tn+fp) * float64(tn+fn))
	if den == 0 {
		return 0
	}
	return num / den
}

// scoreLabel pairs one Match score with its ground-truth label. Used as the
// internal record type for auroc and auprc, which both need to sort by
// score while preserving labels.
type scoreLabel struct {
	score float64
	label bool
}

// sortByScoreDesc sorts arr by score descending and breaks ties by label
// (positives before negatives at the same score). Stable tie-breaking
// keeps auroc / auprc reproducible across Go versions, which would
// otherwise be at the mercy of sort.Slice's unspecified tie behaviour.
func sortByScoreDesc(arr []scoreLabel) {
	sort.SliceStable(arr, func(i, j int) bool {
		if arr[i].score != arr[j].score {
			return arr[i].score > arr[j].score
		}
		return arr[i].label && !arr[j].label
	})
}

// auroc is the area under the receiver operating characteristic curve,
// computed by trapezoidal integration after sorting by descending score.
// Tied scores are processed as a block (both arms of the trapezoid step
// at once) which is the textbook handling — anything else lets ordering
// inside a tie shift the result.
//
// Returns 0 when either class has zero members; auroc is undefined
// without both positives and negatives, and 0 is an honest signal that
// the score had nothing to discriminate against.
func auroc(scores []float64, labels []bool) float64 {
	if len(scores) != len(labels) || len(scores) == 0 {
		return 0
	}
	arr := make([]scoreLabel, len(scores))
	var pos, neg int
	for i := range scores {
		arr[i] = scoreLabel{scores[i], labels[i]}
		if labels[i] {
			pos++
		} else {
			neg++
		}
	}
	if pos == 0 || neg == 0 {
		return 0
	}
	sortByScoreDesc(arr)

	var auc, prevTPR, prevFPR float64
	tp, fp := 0, 0
	for i := 0; i < len(arr); {
		j := i
		for j < len(arr) && arr[j].score == arr[i].score {
			if arr[j].label {
				tp++
			} else {
				fp++
			}
			j++
		}
		tpr := float64(tp) / float64(pos)
		fpr := float64(fp) / float64(neg)
		auc += (fpr - prevFPR) * (tpr + prevTPR) / 2
		prevTPR, prevFPR = tpr, fpr
		i = j
	}
	return auc
}

// auprc is the area under the precision-recall curve via step-wise
// integration: at each unique score (descending) we recompute precision
// and recall and accumulate (Δrecall × precision). This matches
// scikit-learn's average_precision_score and is the strict choice when
// classes are heavily imbalanced, where auroc over-credits a model that
// just sorts the negative tail.
//
// Returns 0 when there are no positives — average precision is
// undefined and 0 is the conservative report.
func auprc(scores []float64, labels []bool) float64 {
	if len(scores) != len(labels) || len(scores) == 0 {
		return 0
	}
	arr := make([]scoreLabel, len(scores))
	var pos int
	for i := range scores {
		arr[i] = scoreLabel{scores[i], labels[i]}
		if labels[i] {
			pos++
		}
	}
	if pos == 0 {
		return 0
	}
	sortByScoreDesc(arr)

	var auc, prevRecall float64
	tp, fp := 0, 0
	for i := 0; i < len(arr); {
		j := i
		for j < len(arr) && arr[j].score == arr[i].score {
			if arr[j].label {
				tp++
			} else {
				fp++
			}
			j++
		}
		precision := safeRatio(tp, tp+fp)
		recall := float64(tp) / float64(pos)
		auc += (recall - prevRecall) * precision
		prevRecall = recall
		i = j
	}
	return auc
}

// pickBest returns the operatingPoint that maximises score(p). Ties are
// broken by the lower threshold. Used by run to surface F1-optimal and
// accuracy-optimal points from the 101-step sweep.
func pickBest(points []operatingPoint, score func(operatingPoint) float64) (operatingPoint, error) {
	if len(points) == 0 {
		return operatingPoint{}, errors.New("bench: pickBest needs at least one point")
	}
	best := points[0]
	bestScore := score(best)
	for _, p := range points[1:] {
		s := score(p)
		if s > bestScore {
			best, bestScore = p, s
		}
	}
	return best, nil
}

// pickAtMinRecall returns the operatingPoint that maximises precision
// among points with recall >= floor. Returns (zero, false) when no point
// meets the recall floor.
func pickAtMinRecall(points []operatingPoint, floor float64) (operatingPoint, bool) {
	var best operatingPoint
	found := false
	for _, p := range points {
		if p.Recall < floor {
			continue
		}
		if !found || p.Precision > best.Precision {
			best, found = p, true
		}
	}
	return best, found
}

// pickAtMinPrecision is the dual of pickAtMinRecall: maximise recall
// among points with precision >= floor.
func pickAtMinPrecision(points []operatingPoint, floor float64) (operatingPoint, bool) {
	var best operatingPoint
	found := false
	for _, p := range points {
		if p.Precision < floor {
			continue
		}
		if !found || p.Recall > best.Recall {
			best, found = p, true
		}
	}
	return best, found
}
