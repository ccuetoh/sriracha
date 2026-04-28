package token

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"math/bits"
	"slices"

	"go.sriracha.dev/sriracha"
)

// MatchResult holds the output of Match: per-field Dice scores, the weighted
// aggregate Score in [0, 1], and the threshold decision.
type MatchResult struct {
	Score    float64
	PerField []float64
	IsMatch  bool
}

// Equal reports whether a and b are bit-identical across every field.
// It returns false if FieldSetVersion, KeyID, or field count differ. A field
// that is nil on one side and non-nil (or differently-sized) on the other
// compares unequal. Per-field byte comparison is constant-time.
func Equal(a, b sriracha.DeterministicToken) bool {
	if a.FieldSetVersion != b.FieldSetVersion {
		return false
	}
	if a.KeyID != b.KeyID {
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
// Returns an error if FieldSetVersion, KeyID, BloomParams, or field count
// differ — scores would not be comparable.
func DicePerField(a, b sriracha.BloomToken) ([]float64, error) {
	if a.FieldSetVersion != b.FieldSetVersion {
		return nil, fmt.Errorf("token: FieldSetVersion mismatch: %q vs %q", a.FieldSetVersion, b.FieldSetVersion)
	}
	if a.KeyID != b.KeyID {
		return nil, fmt.Errorf("token: KeyID mismatch: %q vs %q", a.KeyID, b.KeyID)
	}
	if !bloomParamsEqual(a.BloomParams, b.BloomParams) {
		return nil, fmt.Errorf("token: BloomParams mismatch")
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

// bloomParamsEqual reports whether two BloomConfig values are field-for-field
// identical. BloomConfig contains a []int (NgramSizes) and so is not comparable
// with ==.
func bloomParamsEqual(a, b sriracha.BloomConfig) bool {
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

// Match compares a and b under fs and returns per-field Dice scores, the
// weighted aggregate, and a threshold decision. Fields with all-zero filters
// on both sides are treated as absent and drop from the weighted average;
// asymmetric absence (zero on one side, populated on the other) keeps its
// score of 0 and counts as a real mismatch signal.
func Match(a, b sriracha.BloomToken, fs sriracha.FieldSet, threshold float64) (MatchResult, error) {
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

	stripped := sriracha.FieldSet{
		Version: fs.Version,
		Fields:  make([]sriracha.FieldSpec, len(fs.Fields)),
	}
	for i, spec := range fs.Fields {
		if allZero(a.Fields[i]) && allZero(b.Fields[i]) {
			spec.Weight = 0
		}
		stripped.Fields[i] = spec
	}

	score, err := Score(perField, stripped)
	if err != nil {
		return MatchResult{}, err
	}
	return MatchResult{Score: score, PerField: perField, IsMatch: score >= threshold}, nil
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
