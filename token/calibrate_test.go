package token

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ccuetoh/sriracha"
)

func TestCalibrate(t *testing.T) {
	t.Parallel()

	tok := newTok(t, "secret")
	fs := bloomFS(
		sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: false, Weight: 2.0},
		sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: false, Weight: 1.0},
	)

	tokenize := func(t *testing.T, given, family string) sriracha.BloomToken {
		t.Helper()
		tr, err := tok.TokenizeRecordBloom(sriracha.RawRecord{
			sriracha.FieldNameGiven:  given,
			sriracha.FieldNameFamily: family,
		}, fs)
		require.NoError(t, err)
		return tr
	}

	a := tokenize(t, "Christopher", "Smith")
	aTypo := tokenize(t, "Cristopher", "Smith")
	b := tokenize(t, "Maria", "Lopez")
	c := tokenize(t, "John", "Doe")

	t.Run("FindsThresholdSeparatingMatchesFromNon", func(t *testing.T) {
		t.Parallel()
		cal, err := Calibrate([]LabeledPair{
			{A: a, B: a, Match: true},
			{A: a, B: aTypo, Match: true},
			{A: a, B: b, Match: false},
			{A: a, B: c, Match: false},
			{A: b, B: c, Match: false},
		}, fs)
		require.NoError(t, err)
		assert.Greater(t, cal.OptimalThreshold, 0.0)
		assert.Less(t, cal.OptimalThreshold, 1.0)
		assert.InDelta(t, 1.0, cal.F1, 1e-9, "this fixture is fully separable; expect perfect F1")
		assert.Len(t, cal.ROC, 101, "expected 0.00..1.00 in 0.01 steps")
		assert.InDelta(t, 0.0, cal.ROC[0].Threshold, 1e-9)
		assert.InDelta(t, 1.0, cal.ROC[100].Threshold, 1e-9)
	})

	t.Run("EmptyPairsErrors", func(t *testing.T) {
		t.Parallel()
		_, err := Calibrate(nil, fs)
		assert.Error(t, err)
	})

	t.Run("MismatchedTokensErrors", func(t *testing.T) {
		t.Parallel()
		bad := a
		bad.FieldSetVersion = "different"
		_, err := Calibrate([]LabeledPair{{A: a, B: bad, Match: true}}, fs)
		assert.Error(t, err)
	})

	t.Run("SafeRatioZeroDen", func(t *testing.T) {
		t.Parallel()
		// All-negative labels — at threshold 1.01 there are no positive
		// predictions, exercising the safeRatio den==0 branch.
		cal, err := Calibrate([]LabeledPair{
			{A: a, B: b, Match: false},
		}, fs)
		require.NoError(t, err)
		assert.GreaterOrEqual(t, cal.F1, 0.0)
	})
}
