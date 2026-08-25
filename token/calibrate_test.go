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

	tokenize := func(t *testing.T, given, family string) sriracha.ProbabilisticToken {
		t.Helper()
		rec := sriracha.RawRecord{}
		if given != "" {
			rec[sriracha.FieldNameGiven] = given
		}
		if family != "" {
			rec[sriracha.FieldNameFamily] = family
		}
		tr, err := tok.TokenizeProbabilistic(rec, fs)
		require.NoError(t, err)
		return tr
	}

	a := tokenize(t, "Christopher", "Smith")
	aTypo := tokenize(t, "Cristopher", "Smith")
	b := tokenize(t, "Maria", "Lopez")
	c := tokenize(t, "John", "Doe")
	givenOnly := tokenize(t, "Christopher", "")

	separable := []LabeledPair{
		{A: a, B: a, Match: true},
		{A: a, B: aTypo, Match: true},
		{A: a, B: b, Match: false},
		{A: a, B: c, Match: false},
		{A: b, B: c, Match: false},
	}

	t.Run("FindsThresholdSeparatingMatchesFromNon", func(t *testing.T) {
		t.Parallel()
		cal, err := Calibrate(separable, fs, MatchPolicy{})
		require.NoError(t, err)
		assert.Greater(t, cal.OptimalThreshold, 0.0)
		assert.Less(t, cal.OptimalThreshold, 1.0)
		assert.InDelta(t, 1.0, cal.F1, 1e-9, "this fixture is fully separable; expect perfect F1")
		assert.Len(t, cal.PR, 101, "expected 0.00..1.00 in 0.01 steps")
		assert.InDelta(t, 0.0, cal.PR[0].Threshold, 1e-9)
		assert.InDelta(t, 1.0, cal.PR[100].Threshold, 1e-9)
		assert.Equal(t, 0, cal.ExcludedPairs)
	})

	t.Run("ThresholdIsPlateauMidpointNotBottomEdge", func(t *testing.T) {
		t.Parallel()
		cal, err := Calibrate(separable, fs, MatchPolicy{})
		require.NoError(t, err)

		var lo, hi float64 = -1, -1
		for _, p := range cal.PR {
			if p.F1 < cal.F1 {
				continue
			}
			if lo < 0 {
				lo = p.Threshold
			}
			hi = p.Threshold
		}
		require.Less(t, lo, hi, "fixture must produce a plateau wider than one point")
		assert.Greater(t, cal.OptimalThreshold, lo, "must not sit on the bottom edge of the plateau")
		assert.LessOrEqual(t, cal.OptimalThreshold, hi)
	})

	t.Run("PolicyThresholdIsIgnored", func(t *testing.T) {
		t.Parallel()
		withThreshold, err := Calibrate(separable, fs, MatchPolicy{Threshold: 0.99})
		require.NoError(t, err)
		bare, err := Calibrate(separable, fs, MatchPolicy{})
		require.NoError(t, err)
		assert.Equal(t, bare.OptimalThreshold, withThreshold.OptimalThreshold)
	})

	t.Run("EvidenceFloorExcludesPairs", func(t *testing.T) {
		t.Parallel()
		pairs := append([]LabeledPair{{A: givenOnly, B: givenOnly, Match: true}}, separable...)

		included, err := Calibrate(pairs, fs, MatchPolicy{})
		require.NoError(t, err)
		assert.Equal(t, 0, included.ExcludedPairs, "the zero policy excludes nothing")

		floored, err := Calibrate(pairs, fs, MatchPolicy{MinComparableFields: 2})
		require.NoError(t, err)
		assert.Equal(t, 1, floored.ExcludedPairs, "the single-field pair is decided by the floor, not the threshold")
	})

	t.Run("WeightFloorExcludesPairs", func(t *testing.T) {
		t.Parallel()
		pairs := append([]LabeledPair{{A: givenOnly, B: givenOnly, Match: true}}, separable...)
		cal, err := Calibrate(pairs, fs, MatchPolicy{MinComparableWeight: 2.5})
		require.NoError(t, err)
		assert.Equal(t, 1, cal.ExcludedPairs)
	})

	t.Run("AllPairsExcludedErrors", func(t *testing.T) {
		t.Parallel()
		_, err := Calibrate([]LabeledPair{{A: givenOnly, B: givenOnly, Match: true}}, fs,
			MatchPolicy{MinComparableFields: 2})
		require.ErrorIs(t, err, ErrAllPairsExcluded)
	})

	t.Run("EmptyPairsErrors", func(t *testing.T) {
		t.Parallel()
		_, err := Calibrate(nil, fs, MatchPolicy{})
		require.ErrorIs(t, err, ErrNoLabeledPairs)
	})

	t.Run("InvalidFloorErrors", func(t *testing.T) {
		t.Parallel()
		_, err := Calibrate(separable, fs, MatchPolicy{MinComparableFields: -1})
		require.ErrorIs(t, err, sriracha.ErrInvalidConfig)
	})

	t.Run("MismatchedTokensErrors", func(t *testing.T) {
		t.Parallel()
		bad := a
		bad.FieldSetVersion = "different"
		_, err := Calibrate([]LabeledPair{{A: a, B: bad, Match: true}}, fs, MatchPolicy{})
		require.ErrorIs(t, err, ErrFieldSetVersionMismatch)
	})

	t.Run("NoPositiveLabelsErrors", func(t *testing.T) {
		t.Parallel()
		// Every F1 is 0 without a positive label, so there is nothing to
		// calibrate against.
		_, err := Calibrate([]LabeledPair{{A: a, B: b, Match: false}}, fs, MatchPolicy{})
		require.ErrorIs(t, err, ErrNoPositiveF1)
	})

	t.Run("SafeRatioZeroDen", func(t *testing.T) {
		t.Parallel()
		// No pair scores 1.0, so at threshold 1.00 nothing is predicted
		// positive and precision divides by zero.
		cal, err := Calibrate([]LabeledPair{
			{A: a, B: aTypo, Match: true},
			{A: a, B: b, Match: false},
		}, fs, MatchPolicy{})
		require.NoError(t, err)
		assert.Equal(t, 0.0, cal.PR[100].Precision)
		assert.Greater(t, cal.F1, 0.0)
	})
}

func TestPlateauMidpoint(t *testing.T) {
	t.Parallel()

	curve := func(f1s ...float64) []PRPoint {
		pr := make([]PRPoint, len(f1s))
		for i, f1 := range f1s {
			pr[i] = PRPoint{Threshold: float64(i) / 100, F1: f1}
		}
		return pr
	}

	cases := []struct {
		name    string
		f1s     []float64
		want    int
		wantErr error
	}{
		{name: "SinglePoint", f1s: []float64{0.5}, want: 0},
		{name: "OddRunTakesCentre", f1s: []float64{0.1, 0.9, 0.9, 0.9, 0.2}, want: 2},
		{name: "EvenRunRoundsUp", f1s: []float64{0.1, 0.9, 0.9, 0.2}, want: 2},
		{name: "LongestRunWins", f1s: []float64{0.9, 0.1, 0.9, 0.9, 0.9, 0.1}, want: 3},
		{name: "LowestStartWinsTie", f1s: []float64{0.9, 0.9, 0.1, 0.9, 0.9}, want: 1},
		{name: "RunAtTail", f1s: []float64{0.1, 0.2, 0.9, 0.9}, want: 3},
		{name: "AllZeroF1", f1s: []float64{0, 0, 0}, wantErr: ErrNoPositiveF1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pr := curve(tc.f1s...)
			got, err := plateauMidpoint(pr)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, pr[tc.want], got)
		})
	}
}

// TestF1ScoreEqualRatiosCompareEqual pins the arithmetic plateauMidpoint
// depends on: count triples that are mathematically the same F1 must produce
// the same float, or one plateau splits into several.
func TestF1ScoreEqualRatiosCompareEqual(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name          string
		aTP, aFP, aFN int
		bTP, bFP, bFN int
	}{
		{"OneThird", 2, 8, 0, 1, 3, 1},
		{"OneHalf", 3, 6, 0, 1, 2, 0},
		{"TwoThirds", 2, 2, 0, 4, 4, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, f1Score(tc.aTP, tc.aFP, tc.aFN), f1Score(tc.bTP, tc.bFP, tc.bFN))
		})
	}
}

// TestF1ScoreAvoidsHarmonicDrift records why f1Score divides the counts
// directly. The harmonic mean of precision and recall puts these two triples
// one unit in the last place apart despite both being exactly 1/3.
func TestF1ScoreAvoidsHarmonicDrift(t *testing.T) {
	t.Parallel()

	harmonic := func(tp, fp, fn int) float64 {
		precision, recall := safeRatio(tp, tp+fp), safeRatio(tp, tp+fn)
		if precision+recall == 0 {
			return 0
		}
		return 2 * precision * recall / (precision + recall)
	}

	assert.NotEqual(t, harmonic(2, 8, 0), harmonic(1, 3, 1), "the harmonic route is what f1Score exists to avoid")
	assert.Equal(t, f1Score(2, 8, 0), f1Score(1, 3, 1))
}

// TestF1ScoreZeroCounts covers the empty case.
func TestF1ScoreZeroCounts(t *testing.T) {
	t.Parallel()

	assert.Equal(t, 0.0, f1Score(0, 0, 0))
}

// TestCalibrateExcludesZeroEvidencePairs pins that pairs with nothing
// comparable stay out of the sweep even under a zero policy. Match reports
// them as non-matches at every threshold, so counting them as predicted
// matches at threshold 0 would fit the threshold against a decision Match
// never makes.
func TestCalibrateExcludesZeroEvidencePairs(t *testing.T) {
	t.Parallel()

	tok := newTok(t, "secret")
	fs := bloomFS(
		sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: false, Weight: 2.0},
		sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: false, Weight: 1.0},
	)
	tokenize := func(t *testing.T, rec sriracha.RawRecord) sriracha.ProbabilisticToken {
		t.Helper()
		tr, err := tok.TokenizeProbabilistic(rec, fs)
		require.NoError(t, err)
		return tr
	}

	present := tokenize(t, sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Christopher",
		sriracha.FieldNameFamily: "Smith",
	})
	other := tokenize(t, sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Maria",
		sriracha.FieldNameFamily: "Lopez",
	})
	empty := tokenize(t, sriracha.RawRecord{})

	blank := LabeledPair{A: empty, B: empty, Match: true}

	t.Run("ExcludedUnderZeroPolicy", func(t *testing.T) {
		t.Parallel()

		res, err := Match(empty, empty, fs, MatchPolicy{})
		require.NoError(t, err)
		require.Equal(t, 0, res.ComparableFields, "fixture must have nothing comparable")
		require.False(t, res.IsMatch, "Match never calls a zero-evidence pair a match")

		cal, err := Calibrate([]LabeledPair{
			{A: present, B: present, Match: true},
			{A: present, B: other, Match: false},
			blank,
		}, fs, MatchPolicy{})
		require.NoError(t, err)
		assert.Equal(t, 1, cal.ExcludedPairs)
	})

	t.Run("AllBlankPairsLeaveNothingToFit", func(t *testing.T) {
		t.Parallel()

		_, err := Calibrate([]LabeledPair{blank, blank}, fs, MatchPolicy{})
		require.ErrorIs(t, err, ErrAllPairsExcluded)
	})
}
