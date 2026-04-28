package token

import (
	"crypto/subtle"
	"fmt"
	"math/bits"
	"slices"

	"go.sriracha.dev/sriracha"
)

// Equal reports whether a and b are bit-identical across every field.
// It returns false if FieldSetVersion or field count differ. A field that is
// nil on one side and non-nil (or differently-sized) on the other compares
// unequal. Per-field byte comparison is constant-time.
func Equal(a, b sriracha.DeterministicToken) bool {
	if a.FieldSetVersion != b.FieldSetVersion {
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
// Returns an error if FieldSetVersion, BloomParams, or field count differ —
// scores would not be comparable.
func DicePerField(a, b sriracha.BloomToken) ([]float64, error) {
	if a.FieldSetVersion != b.FieldSetVersion {
		return nil, fmt.Errorf("token: FieldSetVersion mismatch: %q vs %q", a.FieldSetVersion, b.FieldSetVersion)
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
