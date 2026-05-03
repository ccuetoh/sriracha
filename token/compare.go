package token

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"math/bits"
	"slices"

	"github.com/ccuetoh/sriracha"
)

// MatchResult holds the output of Match: per-field Dice scores, the weighted
// aggregate Score in [0, 1], the threshold decision, the FieldSet paths in the
// same order as PerField, and a count of fields that contributed to the
// weighted average (excludes both-absent fields and fields with non-positive
// weight).
type MatchResult struct {
	Score            float64              `json:"score"`
	PerField         []float64            `json:"per_field"`
	Paths            []sriracha.FieldPath `json:"paths"`
	IsMatch          bool                 `json:"is_match"`
	ComparableFields int                  `json:"comparable_fields"`
}

// ScoreFor returns the per-field Dice score for path along with true if the
// path appears in the result. Paths with zero or negative weight that were
// dropped from the weighted average still appear here with their raw Dice
// score.
func (r MatchResult) ScoreFor(path sriracha.FieldPath) (float64, bool) {
	for i, p := range r.Paths {
		if p == path {
			return r.PerField[i], true
		}
	}
	return 0, false
}

// ByPath returns a fresh map keyed by FieldPath with each path's Dice score.
// Useful for downstream code that wants to look up scores without scanning.
func (r MatchResult) ByPath() map[sriracha.FieldPath]float64 {
	out := make(map[sriracha.FieldPath]float64, len(r.Paths))
	for i, p := range r.Paths {
		out[p] = r.PerField[i]
	}
	return out
}

// Equal reports whether a and b are bit-identical across every field.
// It returns false if FieldSetVersion, KeyID, FieldSetFingerprint (when both
// sides set it), or field count differ. A field that is nil on one side and
// non-nil (or differently-sized) on the other compares unequal. Per-field
// byte comparison is constant-time.
func Equal(a, b sriracha.DeterministicToken) bool {
	if a.FieldSetVersion != b.FieldSetVersion {
		return false
	}
	if a.KeyID != b.KeyID {
		return false
	}
	if a.FieldSetFingerprint != "" && b.FieldSetFingerprint != "" &&
		a.FieldSetFingerprint != b.FieldSetFingerprint {
		return false
	}
	if len(a.Fields) != len(b.Fields) {
		return false
	}
	for i := range a.Fields {
		ai, bi := a.Fields[i], b.Fields[i]
		if len(ai) != len(bi) {
			return false
		}
		if len(ai) == 0 {
			continue
		}
		if subtle.ConstantTimeCompare(ai, bi) != 1 {
			return false
		}
	}
	return true
}

// DicePerField returns the Sørensen–Dice coefficient between corresponding
// fields of a and b. The result is one score in [0, 1] per field, in FieldSet
// order. A field with an all-zero filter on either side scores 0.
//
// Returns an error if FieldSetVersion, KeyID, FieldSetFingerprint (when both
// sides set it), ProbabilisticParams, or field count differ — scores would not be
// comparable.
//
// Most callers want Match — it wraps DicePerField + Score and returns the
// thresholded decision.
func DicePerField(a, b sriracha.ProbabilisticToken) ([]float64, error) {
	if a.FieldSetVersion != b.FieldSetVersion {
		return nil, fmt.Errorf("token: FieldSetVersion mismatch: %q vs %q", a.FieldSetVersion, b.FieldSetVersion)
	}
	if a.KeyID != b.KeyID {
		return nil, fmt.Errorf("token: KeyID mismatch: %q vs %q", a.KeyID, b.KeyID)
	}
	if a.FieldSetFingerprint != "" && b.FieldSetFingerprint != "" &&
		a.FieldSetFingerprint != b.FieldSetFingerprint {
		return nil, fmt.Errorf("token: FieldSetFingerprint mismatch: %q vs %q", a.FieldSetFingerprint, b.FieldSetFingerprint)
	}
	if !bloomParamsEqual(a.ProbabilisticParams, b.ProbabilisticParams) {
		return nil, fmt.Errorf("token: ProbabilisticParams mismatch")
	}
	if len(a.Fields) != len(b.Fields) {
		return nil, fmt.Errorf("token: field count mismatch: %d vs %d", len(a.Fields), len(b.Fields))
	}
	scores := make([]float64, len(a.Fields))
	for i := range a.Fields {
		ai, bi := a.Fields[i], b.Fields[i]
		if len(ai) != len(bi) {
			return nil, fmt.Errorf("token: field %d byte length mismatch: %d vs %d", i, len(ai), len(bi))
		}
		scores[i] = dice(ai, bi)
	}
	return scores, nil
}

// bloomParamsEqual reports whether two ProbabilisticConfig values are field-for-field
// identical. ProbabilisticConfig contains a []int (NgramSizes) and so is not comparable
// with ==.
func bloomParamsEqual(a, b sriracha.ProbabilisticConfig) bool {
	return a.SizeBits == b.SizeBits && a.HashCount == b.HashCount && slices.Equal(a.NgramSizes, b.NgramSizes)
}

// Score returns the weight-normalised aggregate of perField against
// fs.Fields[i].Weight. Fields with non-positive weight are excluded from both
// numerator and denominator, so callers can mask out absent fields by
// zeroing their weight. Returns an error if the lengths do not match or no
// field has positive weight.
func Score(perField []float64, fs sriracha.FieldSet) (float64, error) {
	if len(perField) != len(fs.Fields) {
		return 0, fmt.Errorf("token: perField length %d does not match FieldSet length %d", len(perField), len(fs.Fields))
	}
	var sum, totalW float64
	for i, s := range perField {
		w := fs.Fields[i].Weight
		if w <= 0 {
			continue
		}
		sum += w * s
		totalW += w
	}
	if totalW == 0 {
		return 0, errors.New("token: no comparable fields")
	}
	return sum / totalW, nil
}

// Match is the canonical entry point for probabilistic comparison: it wraps
// DicePerField + Score and returns the threshold decision in a single call.
//
// Match compares a and b under fs and returns per-field Dice scores, the
// weighted aggregate, and a threshold decision. Fields with all-zero filters
// on both sides are treated as absent and drop from the weighted average;
// asymmetric absence (zero on one side, populated on the other) keeps its
// score of 0 and counts as a real mismatch signal.
//
// If every field is both-absent (or zero-weighted), the returned MatchResult
// has Score=0, IsMatch=false, ComparableFields=0 — never an error. The error
// return is reserved for genuine mismatches: threshold out of range, version /
// key / fingerprint / params drift, or field-count disagreement between the
// tokens and fs.
func Match(a, b sriracha.ProbabilisticToken, fs sriracha.FieldSet, threshold float64) (MatchResult, error) {
	if threshold < 0 || threshold > 1 {
		return MatchResult{}, fmt.Errorf("token: threshold must be in [0,1], got %v", threshold)
	}
	perField, err := DicePerField(a, b)
	if err != nil {
		return MatchResult{}, err
	}
	if len(perField) != len(fs.Fields) {
		return MatchResult{}, fmt.Errorf("token: field count %d does not match FieldSet length %d", len(perField), len(fs.Fields))
	}

	paths := make([]sriracha.FieldPath, len(fs.Fields))
	var sum, totalW float64
	comparable := 0
	for i, spec := range fs.Fields {
		paths[i] = spec.Path
		w := spec.Weight
		if w <= 0 || (allZero(a.Fields[i]) && allZero(b.Fields[i])) {
			continue
		}
		sum += w * perField[i]
		totalW += w
		comparable++
	}

	if comparable == 0 {
		return MatchResult{PerField: perField, Paths: paths}, nil
	}
	score := sum / totalW
	return MatchResult{
		Score:            score,
		PerField:         perField,
		Paths:            paths,
		IsMatch:          score >= threshold,
		ComparableFields: comparable,
	}, nil
}

// allZero reports whether b is empty or all-zero bytes.
func allZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

// dice computes the Sørensen–Dice coefficient over two equal-length bit-packed
// byte slices. Endianness of the underlying word layout is irrelevant: set
// bits remain set regardless of word order.
func dice(a, b []byte) float64 {
	var inter, ca, cb int
	for i := range a {
		inter += bits.OnesCount8(a[i] & b[i])
		ca += bits.OnesCount8(a[i])
		cb += bits.OnesCount8(b[i])
	}
	total := ca + cb
	if total == 0 {
		return 0
	}
	return 2.0 * float64(inter) / float64(total)
}
