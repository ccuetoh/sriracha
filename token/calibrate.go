package token

import (
	"errors"
	"fmt"

	"github.com/ccuetoh/sriracha"
)

// Sentinel errors returned by Calibrate.
var (
	// ErrNoLabeledPairs reports a Calibrate call with no pairs at all.
	ErrNoLabeledPairs = errors.New("token: Calibrate requires at least one labeled pair")

	// ErrAllPairsExcluded reports that every pair fell below the policy's
	// evidence floor, so the sweep had nothing to fit on.
	ErrAllPairsExcluded = errors.New("token: every labeled pair fell below the evidence floor")

	// ErrNoPositiveF1 reports that no threshold produced a positive F1,
	// which happens when the pairs carry no positive labels. There is
	// nothing to calibrate against.
	ErrNoPositiveF1 = errors.New("token: no threshold produced a positive F1")
)

// LabeledPair is one row of ground-truth: two ProbabilisticTokens believed to be
// either the same person (Match=true) or different people (Match=false).
type LabeledPair struct {
	A     sriracha.ProbabilisticToken `json:"a"`
	B     sriracha.ProbabilisticToken `json:"b"`
	Match bool                        `json:"match"`
}

// PRPoint is one threshold and the precision/recall/F1 it produces over the
// supplied labeled pairs. The curve these points trace is a precision-recall
// curve, not an ROC curve: neither axis is the false positive rate.
type PRPoint struct {
	Threshold float64 `json:"threshold"`
	Precision float64 `json:"precision"`
	Recall    float64 `json:"recall"`
	F1        float64 `json:"f1"`
}

// Calibration is the output of Calibrate: the threshold that maximizes F1
// over the labeled pairs, plus the full precision-recall curve at 0.01 step
// granularity and the number of pairs the evidence floor excluded.
//
// F1, Precision and Recall are in-sample training metrics. They describe the
// pairs Calibrate was handed, at the threshold fitted on those same pairs, and
// they are optimistic by construction. Measure the chosen threshold on a
// held-out set before quoting any of them as expected production performance.
type Calibration struct {
	OptimalThreshold float64   `json:"optimal_threshold"`
	F1               float64   `json:"f1"`
	Precision        float64   `json:"precision"`
	Recall           float64   `json:"recall"`
	PR               []PRPoint `json:"pr"`
	ExcludedPairs    int       `json:"excluded_pairs"`
}

// Calibrate sweeps thresholds in 0.01 steps from 0.00 to 1.00 (101 points)
// and reports the threshold that maximizes F1 over pairs. Use this to pick
// the threshold for production Match calls instead of guessing.
//
// policy.Threshold is ignored, since the sweep supplies its own thresholds.
// The evidence floors are honored: a pair that would fall below
// policy.MinComparableFields or policy.MinComparableWeight is excluded from
// the sweep and counted in Calibration.ExcludedPairs. Production will decide
// those pairs on the floor rather than on the threshold, so fitting the
// threshold on them would tune it against a population it never sees. A zero
// MatchPolicy excludes nothing.
//
// The returned threshold is the midpoint of the longest run of thresholds that
// all reach the maximum F1, rounded up on an even-length run, with the lowest
// starting threshold winning a tie in run length. Picking the bottom edge of
// that plateau would put the operating point right against the non-match
// distribution, where the next unseen pair is most likely to cross it.
//
// Cost is O(N×101 + N×fields_per_token) Dice operations. For N labeled pairs
// it computes Match exactly N times and reuses the resulting Score across
// all 101 thresholds.
//
// Returns an error if pairs is empty, if the policy floors are invalid, if any
// pair fails the underlying Match call (mismatched FieldSetVersion, KeyID,
// fingerprint, params, and so on), if the floor excluded every pair, or if no
// threshold produced a positive F1.
func Calibrate(pairs []LabeledPair, fs sriracha.FieldSet, policy MatchPolicy) (Calibration, error) {
	if len(pairs) == 0 {
		return Calibration{}, ErrNoLabeledPairs
	}
	if err := policy.validateFloors(); err != nil {
		return Calibration{}, err
	}

	scores := make([]float64, 0, len(pairs))
	labels := make([]bool, 0, len(pairs))
	excluded := 0
	for i, p := range pairs {
		// The sweep supplies its own thresholds, so score every pair under
		// a bare policy and apply the floor here.
		res, err := Match(p.A, p.B, fs, MatchPolicy{})
		if err != nil {
			return Calibration{}, fmt.Errorf("token: Calibrate pair %d: %w", i, err)
		}
		if res.ComparableFields < policy.MinComparableFields ||
			res.ComparableWeight < policy.MinComparableWeight {
			excluded++
			continue
		}
		scores = append(scores, res.Score)
		labels = append(labels, p.Match)
	}
	if len(scores) == 0 {
		return Calibration{}, ErrAllPairsExcluded
	}

	const steps = 101
	pr := make([]PRPoint, 0, steps)
	for s := range steps {
		threshold := float64(s) / 100
		var tp, fp, fn int
		for i, score := range scores {
			predicted := score >= threshold
			switch {
			case predicted && labels[i]:
				tp++
			case predicted && !labels[i]:
				fp++
			case !predicted && labels[i]:
				fn++
			}
		}
		precision := safeRatio(tp, tp+fp)
		recall := safeRatio(tp, tp+fn)
		f1 := 0.0
		if precision+recall > 0 {
			f1 = 2 * precision * recall / (precision + recall)
		}
		pr = append(pr, PRPoint{Threshold: threshold, Precision: precision, Recall: recall, F1: f1})
	}

	best, err := plateauMidpoint(pr)
	if err != nil {
		return Calibration{}, err
	}

	return Calibration{
		OptimalThreshold: best.Threshold,
		F1:               best.F1,
		Precision:        best.Precision,
		Recall:           best.Recall,
		PR:               pr,
		ExcludedPairs:    excluded,
	}, nil
}

// plateauMidpoint returns the midpoint of the longest contiguous run of points
// holding the maximum F1. On equal-length runs the lowest starting index wins;
// on an even-length run the midpoint rounds up, away from the non-match
// distribution. Returns ErrNoPositiveF1 if the maximum F1 is 0.
func plateauMidpoint(pr []PRPoint) (PRPoint, error) {
	maxF1 := 0.0
	for _, p := range pr {
		if p.F1 > maxF1 {
			maxF1 = p.F1
		}
	}
	if maxF1 == 0 {
		return PRPoint{}, ErrNoPositiveF1
	}

	bestStart, bestLen := 0, 0
	runStart := -1
	for i, p := range pr {
		if p.F1 == maxF1 {
			if runStart < 0 {
				runStart = i
			}
			if n := i - runStart + 1; n > bestLen {
				bestStart, bestLen = runStart, n
			}
			continue
		}
		runStart = -1
	}
	return pr[bestStart+bestLen/2], nil
}

// safeRatio returns num/den, or 0 if den is zero. Used by Calibrate to avoid
// NaN when no positive predictions exist at a given threshold.
func safeRatio(num, den int) float64 {
	if den == 0 {
		return 0
	}
	return float64(num) / float64(den)
}
