package token

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ccuetoh/sriracha"
)

func detTok(version string, fields ...[]byte) sriracha.DeterministicToken {
	return sriracha.DeterministicToken{FieldSetVersion: version, Fields: fields}
}

func bloomTokWith(params sriracha.ProbabilisticConfig, fields ...[]byte) sriracha.ProbabilisticToken {
	return sriracha.ProbabilisticToken{FieldSetVersion: "v1", ProbabilisticParams: params, Fields: fields}
}

func TestEqual_IdenticalTokens(t *testing.T) {
	t.Parallel()
	tok := newTok(t, "secret")
	rec := sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Alice",
		sriracha.FieldNameFamily: "Smith",
	}
	fs := deterministicFS(
		sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0},
		sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: true, Weight: 1.0},
	)

	a, err := tok.TokenizeDeterministic(rec, fs)
	require.NoError(t, err)
	b, err := tok.TokenizeDeterministic(rec, fs)
	require.NoError(t, err)

	assert.True(t, Equal(a, b), "identical inputs should produce equal tokens")
}

func TestEqual_DifferentInputs(t *testing.T) {
	t.Parallel()
	tok := newTok(t, "secret")
	fs := deterministicFS(sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0})

	a, err := tok.TokenizeDeterministic(sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"}, fs)
	require.NoError(t, err)
	b, err := tok.TokenizeDeterministic(sriracha.RawRecord{sriracha.FieldNameGiven: "Bob"}, fs)
	require.NoError(t, err)

	assert.False(t, Equal(a, b), "different inputs should produce unequal tokens")
}

func TestEqual_Cases(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		a, b sriracha.DeterministicToken
		want bool
	}{
		{
			name: "FormatMismatch",
			a:    sriracha.DeterministicToken{Format: sriracha.TokenFormatDeterministic, FieldSetVersion: "v1", Fields: [][]byte{{0x01}}},
			b:    sriracha.DeterministicToken{Format: "sriracha/det/1", FieldSetVersion: "v1", Fields: [][]byte{{0x01}}},
			want: false,
		},
		{
			name: "VersionMismatch",
			a:    detTok("v1", []byte{0x01}),
			b:    detTok("v2", []byte{0x01}),
			want: false,
		},
		{
			name: "FieldCountMismatch",
			a:    detTok("v1", []byte{0x01}),
			b:    detTok("v1", []byte{0x01}, []byte{0x02}),
			want: false,
		},
		{
			name: "FieldLengthMismatch",
			a:    detTok("v1", []byte{0x01}),
			b:    detTok("v1", []byte{0x01, 0x02}),
			want: false,
		},
		{
			name: "BothNilField",
			a:    detTok("v1", nil),
			b:    detTok("v1", nil),
			want: true,
		},
		{
			name: "OneSideNil",
			a:    detTok("v1", nil),
			b:    detTok("v1", []byte{0x01}),
			want: false,
		},
		{
			name: "KeyIDMismatch",
			a:    sriracha.DeterministicToken{FieldSetVersion: "v1", KeyID: "k1", Fields: [][]byte{{0x01}}},
			b:    sriracha.DeterministicToken{FieldSetVersion: "v1", KeyID: "k2", Fields: [][]byte{{0x01}}},
			want: false,
		},
		{
			name: "FingerprintMismatchBothSet",
			a:    sriracha.DeterministicToken{FieldSetVersion: "v1", FieldSetFingerprint: "aa", Fields: [][]byte{{0x01}}},
			b:    sriracha.DeterministicToken{FieldSetVersion: "v1", FieldSetFingerprint: "bb", Fields: [][]byte{{0x01}}},
			want: false,
		},
		{
			name: "FingerprintOneSetSkipsCheck",
			a:    sriracha.DeterministicToken{FieldSetVersion: "v1", FieldSetFingerprint: "aa", Fields: [][]byte{{0x01}}},
			b:    sriracha.DeterministicToken{FieldSetVersion: "v1", Fields: [][]byte{{0x01}}},
			want: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, Equal(tc.a, tc.b))
		})
	}
}

func TestDicePerField_IdenticalRecords(t *testing.T) {
	t.Parallel()
	tok := newTok(t, "secret")
	rec := sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Christopher",
		sriracha.FieldNameFamily: "Smith",
	}
	fs := bloomFS(
		sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0},
		sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: true, Weight: 1.0},
	)

	a, err := tok.TokenizeProbabilistic(rec, fs)
	require.NoError(t, err)
	b, err := tok.TokenizeProbabilistic(rec, fs)
	require.NoError(t, err)

	scores, err := DicePerField(a, b)
	require.NoError(t, err)
	require.Len(t, scores, 2)
	for i, s := range scores {
		assert.InDelta(t, 1.0, s, 1e-9, "field %d: identical inputs should score 1.0", i)
	}
}

func TestDicePerField_PerturbedField(t *testing.T) {
	t.Parallel()
	tok := newTok(t, "secret")
	fs := bloomFS(
		sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0},
		sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: true, Weight: 1.0},
	)

	a, err := tok.TokenizeProbabilistic(sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Christopher",
		sriracha.FieldNameFamily: "Smith",
	}, fs)
	require.NoError(t, err)
	b, err := tok.TokenizeProbabilistic(sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Cristopher", // typo: missing 'h'
		sriracha.FieldNameFamily: "Smith",
	}, fs)
	require.NoError(t, err)

	scores, err := DicePerField(a, b)
	require.NoError(t, err)
	require.Len(t, scores, 2)
	assert.Greater(t, scores[0], 0.0, "perturbed name should still score above 0")
	assert.Less(t, scores[0], 1.0, "perturbed name should score below 1.0")
	assert.InDelta(t, 1.0, scores[1], 1e-9, "unchanged family field should score 1.0")
}

func TestDicePerField_AbsentFieldSemantics(t *testing.T) {
	t.Parallel()
	tok := newTok(t, "secret")
	fs := bloomFS(
		sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0},
		sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: false, Weight: 0.5},
	)

	withFamily, err := tok.TokenizeProbabilistic(sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Alice",
		sriracha.FieldNameFamily: "Smith",
	}, fs)
	require.NoError(t, err)
	withoutFamily, err := tok.TokenizeProbabilistic(sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"}, fs)
	require.NoError(t, err)
	require.Nil(t, withoutFamily.Fields[1], "absent optional field must be nil")

	t.Run("BothNilScoresZero", func(t *testing.T) {
		t.Parallel()
		scores, err := DicePerField(withoutFamily, withoutFamily)
		require.NoError(t, err)
		require.Len(t, scores, 2)
		assert.InDelta(t, 1.0, scores[0], 1e-9, "present matching field should score 1.0")
		assert.Equal(t, 0.0, scores[1], "nil-on-both field should score 0.0")
	})

	t.Run("AsymmetricAbsenceScoresZero", func(t *testing.T) {
		t.Parallel()
		scores, err := DicePerField(withFamily, withoutFamily)
		require.NoError(t, err)
		require.Len(t, scores, 2)
		assert.Equal(t, 0.0, scores[1], "nil-vs-populated field should score 0.0")
	})

	t.Run("LengthCheckSkippedWhenOneSideNil", func(t *testing.T) {
		t.Parallel()
		// A nil field on one side never trips the byte-length check, even
		// against a populated filter of any length.
		a := withoutFamily
		b := withFamily
		require.NotEqual(t, len(a.Fields[1]), len(b.Fields[1]))
		_, err := DicePerField(a, b)
		assert.NoError(t, err)
	})
}

func TestDicePerField_Errors(t *testing.T) {
	t.Parallel()
	cfg := sriracha.ProbabilisticConfig{SizeBits: 8, NgramSizes: []int{2}, HashCount: 1}
	cases := []struct {
		name string
		a, b sriracha.ProbabilisticToken
	}{
		{
			name: "FormatMismatch",
			a:    sriracha.ProbabilisticToken{Format: sriracha.TokenFormatProbabilistic, FieldSetVersion: "v1"},
			b:    sriracha.ProbabilisticToken{Format: "sriracha/bloom/1", FieldSetVersion: "v1"},
		},
		{
			name: "VersionMismatch",
			a:    sriracha.ProbabilisticToken{FieldSetVersion: "v1"},
			b:    sriracha.ProbabilisticToken{FieldSetVersion: "v2"},
		},
		{
			name: "ProbabilisticParamsMismatch",
			a:    bloomTokWith(sriracha.ProbabilisticConfig{SizeBits: 1024, NgramSizes: []int{2}, HashCount: 2}),
			b:    bloomTokWith(sriracha.ProbabilisticConfig{SizeBits: 2048, NgramSizes: []int{2}, HashCount: 2}),
		},
		{
			name: "BalancedMismatch",
			a:    bloomTokWith(sriracha.ProbabilisticConfig{SizeBits: 1024, NgramSizes: []int{2}, HashCount: 2}),
			b:    bloomTokWith(sriracha.ProbabilisticConfig{SizeBits: 1024, NgramSizes: []int{2}, HashCount: 2, Balanced: true}),
		},
		{
			name: "FieldCountMismatch",
			a:    bloomTokWith(cfg, []byte{0x00}),
			b:    bloomTokWith(cfg, []byte{0x00}, []byte{0x00}),
		},
		{
			name: "FieldByteLengthMismatch",
			a:    bloomTokWith(cfg, []byte{0x00}),
			b:    bloomTokWith(cfg, []byte{0x00, 0x00}),
		},
		{
			name: "KeyIDMismatch",
			a:    sriracha.ProbabilisticToken{FieldSetVersion: "v1", KeyID: "k1", ProbabilisticParams: cfg, Fields: [][]byte{{0x00}}},
			b:    sriracha.ProbabilisticToken{FieldSetVersion: "v1", KeyID: "k2", ProbabilisticParams: cfg, Fields: [][]byte{{0x00}}},
		},
		{
			name: "FingerprintMismatchBothSet",
			a:    sriracha.ProbabilisticToken{FieldSetVersion: "v1", FieldSetFingerprint: "aa", ProbabilisticParams: cfg, Fields: [][]byte{{0x00}}},
			b:    sriracha.ProbabilisticToken{FieldSetVersion: "v1", FieldSetFingerprint: "bb", ProbabilisticParams: cfg, Fields: [][]byte{{0x00}}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := DicePerField(tc.a, tc.b)
			assert.Error(t, err)
		})
	}
}

func TestDicePerField_FingerprintOneSideSkipsCheck(t *testing.T) {
	t.Parallel()
	tok := newTok(t, "secret")
	fs := bloomFS(sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0})

	a, err := tok.TokenizeProbabilistic(sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"}, fs)
	require.NoError(t, err)
	// token.TokenizeProbabilistic leaves FieldSetFingerprint empty; the
	// caller is responsible for setting it. Set it on a so this test
	// exercises the asymmetric (one side set, one side empty) path.
	a.FieldSetFingerprint = fs.Fingerprint()
	b := a
	b.FieldSetFingerprint = ""

	_, err = DicePerField(a, b)
	assert.NoError(t, err, "missing fingerprint on one side must skip the check, not error")
}

func TestScore(t *testing.T) {
	t.Parallel()

	twoFieldFS := func(w0, w1 float64) sriracha.FieldSet {
		return sriracha.FieldSet{
			Version: "v1",
			Fields: []sriracha.FieldSpec{
				{Path: sriracha.FieldNameGiven, Weight: w0},
				{Path: sriracha.FieldNameFamily, Weight: w1},
			},
		}
	}

	t.Run("WeightedAverage", func(t *testing.T) {
		t.Parallel()
		got, err := Score([]float64{1.0, 0.0}, twoFieldFS(2.0, 1.0))
		require.NoError(t, err)
		assert.InDelta(t, 2.0/3.0, got, 1e-9)
	})

	t.Run("LengthMismatch", func(t *testing.T) {
		t.Parallel()
		_, err := Score([]float64{1.0}, twoFieldFS(1.0, 1.0))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "perField length")
	})

	t.Run("ZeroWeightExcluded", func(t *testing.T) {
		t.Parallel()
		got, err := Score([]float64{0.5, 1.0}, twoFieldFS(0.0, 1.0))
		require.NoError(t, err)
		assert.InDelta(t, 1.0, got, 1e-9, "zero-weight field should not contribute")
	})

	t.Run("AllNonPositiveWeights", func(t *testing.T) {
		t.Parallel()
		_, err := Score([]float64{0.5, 0.5}, twoFieldFS(0.0, 0.0))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no comparable fields")
	})

	t.Run("NegativeWeightExcluded", func(t *testing.T) {
		t.Parallel()
		got, err := Score([]float64{0.0, 1.0}, twoFieldFS(-1.0, 2.0))
		require.NoError(t, err)
		assert.InDelta(t, 1.0, got, 1e-9)
	})
}

func TestMatch(t *testing.T) {
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

	identical := tokenize(t, sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Christopher",
		sriracha.FieldNameFamily: "Smith",
	})

	t.Run("AboveThreshold", func(t *testing.T) {
		t.Parallel()
		res, err := Match(identical, identical, fs, 0.9)
		require.NoError(t, err)
		assert.True(t, res.IsMatch)
		assert.InDelta(t, 1.0, res.Score, 1e-9)
		assert.Len(t, res.PerField, 2)
	})

	t.Run("BelowThreshold", func(t *testing.T) {
		t.Parallel()
		other := tokenize(t, sriracha.RawRecord{
			sriracha.FieldNameGiven:  "Maria",
			sriracha.FieldNameFamily: "Lopez",
		})
		res, err := Match(identical, other, fs, 0.9)
		require.NoError(t, err)
		assert.False(t, res.IsMatch)
		assert.Less(t, res.Score, 0.9)
	})

	t.Run("BothAbsentDropsField", func(t *testing.T) {
		t.Parallel()
		// One present field with score 1, one absent on both sides → drops out.
		a := tokenize(t, sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"})
		b := tokenize(t, sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"})
		res, err := Match(a, b, fs, 0.9)
		require.NoError(t, err)
		assert.True(t, res.IsMatch)
		assert.InDelta(t, 1.0, res.Score, 1e-9, "absent-on-both fields should not pull score below 1")
	})

	t.Run("AsymmetricAbsenceCountsAsMismatch", func(t *testing.T) {
		t.Parallel()
		a := tokenize(t, sriracha.RawRecord{
			sriracha.FieldNameGiven:  "Alice",
			sriracha.FieldNameFamily: "Smith",
		})
		b := tokenize(t, sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"})
		res, err := Match(a, b, fs, 0.9)
		require.NoError(t, err)
		assert.Less(t, res.Score, 1.0, "asymmetric absence should pull score below 1")
	})

	t.Run("AllFieldsAbsentBothSides", func(t *testing.T) {
		t.Parallel()
		empty := tokenize(t, sriracha.RawRecord{})
		res, err := Match(empty, empty, fs, 0.5)
		require.NoError(t, err, "all-absent must not error; check ComparableFields instead")
		assert.False(t, res.IsMatch)
		assert.Equal(t, 0.0, res.Score)
		assert.Equal(t, 0, res.ComparableFields)
		assert.Len(t, res.Paths, len(fs.Fields))
		assert.Len(t, res.PerField, len(fs.Fields))
	})

	t.Run("PathsAndComparableFieldsPopulated", func(t *testing.T) {
		t.Parallel()
		res, err := Match(identical, identical, fs, 0.9)
		require.NoError(t, err)
		require.Len(t, res.Paths, 2)
		assert.Equal(t, sriracha.FieldNameGiven, res.Paths[0])
		assert.Equal(t, sriracha.FieldNameFamily, res.Paths[1])
		assert.Equal(t, 2, res.ComparableFields)
	})

	t.Run("ScoreForAndByPath", func(t *testing.T) {
		t.Parallel()
		res, err := Match(identical, identical, fs, 0.9)
		require.NoError(t, err)

		got, ok := res.ScoreFor(sriracha.FieldNameGiven)
		assert.True(t, ok)
		assert.InDelta(t, 1.0, got, 1e-9)

		_, ok = res.ScoreFor(sriracha.FieldDateBirth)
		assert.False(t, ok, "ScoreFor must report missing paths as not found")

		byPath := res.ByPath()
		require.Len(t, byPath, 2)
		assert.InDelta(t, 1.0, byPath[sriracha.FieldNameGiven], 1e-9)
		assert.InDelta(t, 1.0, byPath[sriracha.FieldNameFamily], 1e-9)
	})

	t.Run("ThresholdOutOfRange", func(t *testing.T) {
		t.Parallel()
		_, err := Match(identical, identical, fs, 1.5)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "threshold")
		_, err = Match(identical, identical, fs, -0.1)
		require.Error(t, err)
	})

	t.Run("ThresholdNaN", func(t *testing.T) {
		t.Parallel()
		_, err := Match(identical, identical, fs, math.NaN())
		require.Error(t, err)
		assert.Contains(t, err.Error(), "threshold")
	})

	t.Run("DicePerFieldErrorPropagated", func(t *testing.T) {
		t.Parallel()
		other := identical
		other.FieldSetVersion = "different"
		_, err := Match(identical, other, fs, 0.5)
		require.Error(t, err)
	})

	t.Run("FormatMismatchErrors", func(t *testing.T) {
		t.Parallel()
		other := identical
		other.Format = "sriracha/bloom/1"
		_, err := Match(identical, other, fs, 0.5)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Format mismatch")
	})

	t.Run("FieldCountMismatchWithFieldSet", func(t *testing.T) {
		t.Parallel()
		shorter := sriracha.FieldSet{
			Version:             fs.Version,
			ProbabilisticParams: fs.ProbabilisticParams,
			Fields:              fs.Fields[:1],
		}
		_, err := Match(identical, identical, shorter, 0.5)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "field count")
	})
}

func clkTok(mutate func(*sriracha.CLKToken)) sriracha.CLKToken {
	tok := sriracha.CLKToken{
		Format:              sriracha.TokenFormatCLK,
		FieldSetVersion:     "v1",
		KeyID:               "k1",
		ProbabilisticParams: sriracha.ProbabilisticConfig{SizeBits: 16, NgramSizes: []int{2}, HashCount: 2, Balanced: true},
		Filter:              []byte{0xf0, 0x0f},
	}
	if mutate != nil {
		mutate(&tok)
	}
	return tok
}

func TestMatchCLK(t *testing.T) {
	t.Parallel()

	t.Run("IdenticalFiltersScoreOne", func(t *testing.T) {
		t.Parallel()
		res, err := MatchCLK(clkTok(nil), clkTok(nil), 0.9)
		require.NoError(t, err)
		assert.InDelta(t, 1.0, res.Score, 1e-9)
		assert.True(t, res.IsMatch)
	})

	t.Run("BelowThresholdIsNotMatch", func(t *testing.T) {
		t.Parallel()
		other := clkTok(func(tok *sriracha.CLKToken) { tok.Filter = []byte{0x0f, 0xf0} })
		res, err := MatchCLK(clkTok(nil), other, 0.9)
		require.NoError(t, err)
		assert.Equal(t, 0.0, res.Score, "disjoint filters score 0")
		assert.False(t, res.IsMatch)
	})

	t.Run("FingerprintOneSideSetSkipsCheck", func(t *testing.T) {
		t.Parallel()
		a := clkTok(func(tok *sriracha.CLKToken) { tok.FieldSetFingerprint = "aa" })
		_, err := MatchCLK(a, clkTok(nil), 0.5)
		assert.NoError(t, err, "missing fingerprint on one side must skip the check, not error")
	})

	t.Run("Errors", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			name        string
			a, b        sriracha.CLKToken
			threshold   float64
			errContains string
		}{
			{
				name:        "ThresholdNaN",
				a:           clkTok(nil),
				b:           clkTok(nil),
				threshold:   math.NaN(),
				errContains: "threshold",
			},
			{
				name:        "ThresholdAboveOne",
				a:           clkTok(nil),
				b:           clkTok(nil),
				threshold:   1.5,
				errContains: "threshold",
			},
			{
				name:        "ThresholdNegative",
				a:           clkTok(nil),
				b:           clkTok(nil),
				threshold:   -0.1,
				errContains: "threshold",
			},
			{
				name:        "FormatMismatch",
				a:           clkTok(nil),
				b:           clkTok(func(tok *sriracha.CLKToken) { tok.Format = "sriracha/clk/1" }),
				threshold:   0.5,
				errContains: "Format mismatch",
			},
			{
				name:        "VersionMismatch",
				a:           clkTok(nil),
				b:           clkTok(func(tok *sriracha.CLKToken) { tok.FieldSetVersion = "v2" }),
				threshold:   0.5,
				errContains: "FieldSetVersion",
			},
			{
				name:        "KeyIDMismatch",
				a:           clkTok(nil),
				b:           clkTok(func(tok *sriracha.CLKToken) { tok.KeyID = "k2" }),
				threshold:   0.5,
				errContains: "KeyID",
			},
			{
				name:        "FingerprintMismatchBothSet",
				a:           clkTok(func(tok *sriracha.CLKToken) { tok.FieldSetFingerprint = "aa" }),
				b:           clkTok(func(tok *sriracha.CLKToken) { tok.FieldSetFingerprint = "bb" }),
				threshold:   0.5,
				errContains: "FieldSetFingerprint",
			},
			{
				name:        "ParamsMismatch",
				a:           clkTok(nil),
				b:           clkTok(func(tok *sriracha.CLKToken) { tok.ProbabilisticParams.HashCount = 3 }),
				threshold:   0.5,
				errContains: "ProbabilisticParams",
			},
			{
				name:        "FilterLengthMismatch",
				a:           clkTok(nil),
				b:           clkTok(func(tok *sriracha.CLKToken) { tok.Filter = []byte{0xf0} }),
				threshold:   0.5,
				errContains: "filter byte length",
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				_, err := MatchCLK(tc.a, tc.b, tc.threshold)
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errContains)
			})
		}
	})
}

func TestDice_DirectCases(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		a, b []byte
		want float64
	}{
		{"AllZero", []byte{0x00}, []byte{0x00}, 0.0},
		{"Identical", []byte{0xff}, []byte{0xff}, 1.0},
		{"Disjoint", []byte{0xf0}, []byte{0x0f}, 0.0},
		{"HalfOverlap", []byte{0xff}, []byte{0xf0}, 2.0 * 4.0 / 12.0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.InDelta(t, tc.want, dice(tc.a, tc.b), 1e-9)
		})
	}
}
