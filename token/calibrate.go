package token

import (
	"errors"
	"fmt"

	"github.com/ccuetoh/sriracha"
)

// LabeledPair is one row of ground-truth: two BloomTokens believed to be
// either the same person (Match=true) or different people (Match=false).
type LabeledPair struct {
	A     sriracha.BloomToken `json:"a"`
	B     sriracha.BloomToken `json:"b"`
	Match bool                `json:"match"`
}

// ROCPoint is one threshold and the precision/recall/F1 it produces over the
// supplied labeled pairs.
type ROCPoint struct {
	Threshold float64 `json:"threshold"`
	Precision float64 `json:"precision"`
	Recall    float64 `json:"recall"`
	F1        float64 `json:"f1"`
}

// Calibration is the output of Calibrate: the threshold that maximizes F1
// over the labeled pairs, plus the full ROC curve at 0.01 step granularity.
type Calibration struct {
	OptimalThreshold float64    `json:"optimal_threshold"`
	F1               float64    `json:"f1"`
	Precision        float64    `json:"precision"`
	Recall           float64    `json:"recall"`
	ROC              []ROCPoint `json:"roc"`
}

// Calibrate sweeps thresholds in 0.01 steps from 0.00 to 1.00 (101 points)
// and reports the threshold that maximizes F1 over pairs. Use this to pick
// the threshold for production Match calls instead of guessing.
//
// Cost is O(N×101 + N×fields_per_token) Dice operations. For N labeled pairs
// it computes Match exactly N times and reuses the resulting Score across
// all 101 thresholds.
//
// Returns an error if pairs is empty, or if any pair fails the underlying
// Match call (mismatched FieldSetVersion, KeyID, fingerprint, params, etc.).
func Calibrate(pairs []LabeledPair, fs sriracha.FieldSet) (Calibration, error) {
	if len(pairs) == 0 {
		return Calibration{}, errors.New("token: Calibrate requires at least one labeled pair")
	}

	scores := make([]float64, len(pairs))
	labels := make([]bool, len(pairs))
	for i, p := range pairs {
		// Threshold is irrelevant here; we only need the aggregate Score, so
		// pass 0 to bypass IsMatch (which we ignore).
		res, err := Match(p.A, p.B, fs, 0)
		if err != nil {
			return Calibration{}, fmt.Errorf("token: Calibrate pair %d: %w", i, err)
		}
		scores[i] = res.Score
		labels[i] = p.Match
	}

	const steps = 101
	roc := make([]ROCPoint, 0, steps)
	bestF1 := -1.0
	var best ROCPoint
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
		point := ROCPoint{Threshold: threshold, Precision: precision, Recall: recall, F1: f1}
		roc = append(roc, point)
		if f1 > bestF1 {
			bestF1 = f1
			best = point
		}
	}

	return Calibration{
		OptimalThreshold: best.Threshold,
		F1:               best.F1,
		Precision:        best.Precision,
		Recall:           best.Recall,
		ROC:              roc,
	}, nil
}

// safeRatio returns num/den, or 0 if den is zero. Used by Calibrate to avoid
// NaN when no positive predictions exist at a given threshold.
func safeRatio(num, den int) float64 {
	if den == 0 {
		return 0
	}
	return float64(num) / float64(den)
}
