package token

import (
	"encoding/json"
	"errors"
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

	got, err := Equal(a, b)
	require.NoError(t, err)
	assert.True(t, got, "identical inputs should produce equal tokens")
}

func TestEqual_DifferentInputs(t *testing.T) {
	t.Parallel()
	tok := newTok(t, "secret")
	fs := deterministicFS(sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0})

	a, err := tok.TokenizeDeterministic(sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"}, fs)
	require.NoError(t, err)
	b, err := tok.TokenizeDeterministic(sriracha.RawRecord{sriracha.FieldNameGiven: "Bob"}, fs)
	require.NoError(t, err)

	got, err := Equal(a, b)
	require.NoError(t, err)
	assert.False(t, got, "different inputs should produce unequal tokens")
}

func TestEqual_Cases(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		a, b    sriracha.DeterministicToken
		want    bool
		wantErr error
	}{
		{
			name:    "FormatMismatch",
			a:       sriracha.DeterministicToken{Format: sriracha.TokenFormatDeterministic, FieldSetVersion: "v1", Fields: [][]byte{{0x01}}},
			b:       sriracha.DeterministicToken{Format: "sriracha/det/1", FieldSetVersion: "v1", Fields: [][]byte{{0x01}}},
			wantErr: ErrFormatMismatch,
		},
		{
			name:    "VersionMismatch",
			a:       detTok("v1", []byte{0x01}),
			b:       detTok("v2", []byte{0x01}),
			wantErr: ErrFieldSetVersionMismatch,
		},
		{
			name:    "KeyIDMismatch",
			a:       sriracha.DeterministicToken{FieldSetVersion: "v1", KeyID: "k1", Fields: [][]byte{{0x01}}},
			b:       sriracha.DeterministicToken{FieldSetVersion: "v1", KeyID: "k2", Fields: [][]byte{{0x01}}},
			wantErr: ErrKeyIDMismatch,
		},
		{
			name:    "KeyIDSetOnOneSideOnly",
			a:       sriracha.DeterministicToken{FieldSetVersion: "v1", KeyID: "k1", Fields: [][]byte{{0x01}}},
			b:       sriracha.DeterministicToken{FieldSetVersion: "v1", Fields: [][]byte{{0x01}}},
			wantErr: ErrKeyIDMismatch,
		},
		{
			name:    "FingerprintMismatchBothSet",
			a:       sriracha.DeterministicToken{FieldSetVersion: "v1", FieldSetFingerprint: "aa", Fields: [][]byte{{0x01}}},
			b:       sriracha.DeterministicToken{FieldSetVersion: "v1", FieldSetFingerprint: "bb", Fields: [][]byte{{0x01}}},
			wantErr: ErrFingerprintMismatch,
		},
		{
			name: "FingerprintOneSetSkipsCheck",
			a:    sriracha.DeterministicToken{FieldSetVersion: "v1", FieldSetFingerprint: "aa", Fields: [][]byte{{0x01}}},
			b:    sriracha.DeterministicToken{FieldSetVersion: "v1", Fields: [][]byte{{0x01}}},
			want: true,
		},
		{
			name:    "FieldCountMismatch",
			a:       detTok("v1", []byte{0x01}),
			b:       detTok("v1", []byte{0x01}, []byte{0x02}),
			wantErr: ErrFieldCountMismatch,
		},
		{
			name: "FieldLengthMismatch",
			a:    detTok("v1", []byte{0x01}),
			b:    detTok("v1", []byte{0x01, 0x02}),
			want: false,
		},
		{
			name: "DifferentBytes",
			a:    detTok("v1", []byte{0x01}),
			b:    detTok("v1", []byte{0x02}),
			want: false,
		},
		{
			name: "OneSideAbsent",
			a:    detTok("v1", nil),
			b:    detTok("v1", []byte{0x01}),
			want: false,
		},
		{
			name: "PresentAndAbsentMix",
			a:    detTok("v1", []byte{0x01}, nil),
			b:    detTok("v1", []byte{0x01}, nil),
			want: true,
		},
		{
			name:    "AllFieldsNilBothSides",
			a:       detTok("v1", nil),
			b:       detTok("v1", nil),
			wantErr: ErrNoComparableFields,
		},
		{
			name:    "AllFieldsEmptyNonNilBothSides",
			a:       detTok("v1", []byte{}),
			b:       detTok("v1", []byte{}),
			wantErr: ErrNoComparableFields,
		},
		{
			name:    "NilAndEmptyNonNilAreBothAbsent",
			a:       detTok("v1", nil, []byte{}),
			b:       detTok("v1", []byte{}, nil),
			wantErr: ErrNoComparableFields,
		},
		{
			name: "EmptyNonNilAgainstPresent",
			a:    detTok("v1", []byte{}),
			b:    detTok("v1", []byte{0x01}),
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := Equal(tc.a, tc.b)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				assert.False(t, got, "a not-comparable pair must never report true")
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
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

	t.Run("BothAbsentScoresZero", func(t *testing.T) {
		t.Parallel()
		scores, err := DicePerField(withoutFamily, withoutFamily)
		require.NoError(t, err)
		require.Len(t, scores, 2)
		assert.InDelta(t, 1.0, scores[0], 1e-9, "present matching field should score 1.0")
		assert.Equal(t, 0.0, scores[1], "absent-on-both field should score 0.0")
	})

	t.Run("AsymmetricAbsenceScoresZero", func(t *testing.T) {
		t.Parallel()
		scores, err := DicePerField(withFamily, withoutFamily)
		require.NoError(t, err)
		require.Len(t, scores, 2)
		assert.Equal(t, 0.0, scores[1], "absent-vs-populated field should score 0.0")
	})

	t.Run("LengthCheckSkippedWhenOneSideAbsent", func(t *testing.T) {
		t.Parallel()
		// An absent field on one side never trips the byte-length check,
		// even against a populated filter of any length.
		a := withoutFamily
		b := withFamily
		require.NotEqual(t, len(a.Fields[1]), len(b.Fields[1]))
		_, err := DicePerField(a, b)
		assert.NoError(t, err)
	})

	t.Run("EmptyNonNilCountsAsAbsent", func(t *testing.T) {
		t.Parallel()
		// A peer that JSON-encodes an absent field as "" instead of null
		// must get the same answer as the nil form: absent, not a length
		// mismatch against the populated side.
		a := withoutFamily
		a.Fields = [][]byte{withoutFamily.Fields[0], {}}
		scores, err := DicePerField(a, withFamily)
		require.NoError(t, err)
		assert.Equal(t, 0.0, scores[1])
	})
}

func TestDicePerField_Errors(t *testing.T) {
	t.Parallel()
	cfg := sriracha.ProbabilisticConfig{SizeBits: 8, NgramSizes: []int{2}, HashCount: 1}
	cases := []struct {
		name    string
		a, b    sriracha.ProbabilisticToken
		wantErr error
	}{
		{
			name:    "FormatMismatch",
			a:       sriracha.ProbabilisticToken{Format: sriracha.TokenFormatProbabilistic, FieldSetVersion: "v1"},
			b:       sriracha.ProbabilisticToken{Format: "sriracha/bloom/1", FieldSetVersion: "v1"},
			wantErr: ErrFormatMismatch,
		},
		{
			name:    "VersionMismatch",
			a:       sriracha.ProbabilisticToken{FieldSetVersion: "v1"},
			b:       sriracha.ProbabilisticToken{FieldSetVersion: "v2"},
			wantErr: ErrFieldSetVersionMismatch,
		},
		{
			name:    "ProbabilisticParamsMismatch",
			a:       bloomTokWith(sriracha.ProbabilisticConfig{SizeBits: 1024, NgramSizes: []int{2}, HashCount: 2}),
			b:       bloomTokWith(sriracha.ProbabilisticConfig{SizeBits: 2048, NgramSizes: []int{2}, HashCount: 2}),
			wantErr: ErrParamsMismatch,
		},
		{
			name:    "BalancedMismatch",
			a:       bloomTokWith(sriracha.ProbabilisticConfig{SizeBits: 1024, NgramSizes: []int{2}, HashCount: 2}),
			b:       bloomTokWith(sriracha.ProbabilisticConfig{SizeBits: 1024, NgramSizes: []int{2}, HashCount: 2, Balanced: true}),
			wantErr: ErrParamsMismatch,
		},
		{
			name:    "FieldCountMismatch",
			a:       bloomTokWith(cfg, []byte{0x00}),
			b:       bloomTokWith(cfg, []byte{0x00}, []byte{0x00}),
			wantErr: ErrFieldCountMismatch,
		},
		{
			name:    "FieldByteLengthMismatch",
			a:       bloomTokWith(cfg, []byte{0x01}),
			b:       bloomTokWith(cfg, []byte{0x01, 0x01}),
			wantErr: ErrFilterLengthMismatch,
		},
		{
			name:    "KeyIDMismatch",
			a:       sriracha.ProbabilisticToken{FieldSetVersion: "v1", KeyID: "k1", ProbabilisticParams: cfg, Fields: [][]byte{{0x00}}},
			b:       sriracha.ProbabilisticToken{FieldSetVersion: "v1", KeyID: "k2", ProbabilisticParams: cfg, Fields: [][]byte{{0x00}}},
			wantErr: ErrKeyIDMismatch,
		},
		{
			name:    "FingerprintMismatchBothSet",
			a:       sriracha.ProbabilisticToken{FieldSetVersion: "v1", FieldSetFingerprint: "aa", ProbabilisticParams: cfg, Fields: [][]byte{{0x00}}},
			b:       sriracha.ProbabilisticToken{FieldSetVersion: "v1", FieldSetFingerprint: "bb", ProbabilisticParams: cfg, Fields: [][]byte{{0x00}}},
			wantErr: ErrFingerprintMismatch,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := DicePerField(tc.a, tc.b)
			require.ErrorIs(t, err, tc.wantErr)
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

func TestDefaultMatchPolicy(t *testing.T) {
	t.Parallel()
	p := DefaultMatchPolicy(0.85)
	assert.InDelta(t, 0.85, p.Threshold, 1e-9)
	assert.Equal(t, 2, p.MinComparableFields, "one field is not evidence of identity")
	assert.Equal(t, 0.0, p.MinComparableWeight)
}

func TestMatchPolicy_Validate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		policy  MatchPolicy
		wantErr error
	}{
		{name: "ZeroValue"},
		{name: "Default", policy: DefaultMatchPolicy(0.5)},
		{name: "ThresholdOne", policy: MatchPolicy{Threshold: 1}},
		{name: "ThresholdAboveOne", policy: MatchPolicy{Threshold: 1.5}, wantErr: ErrInvalidThreshold},
		{name: "ThresholdNegative", policy: MatchPolicy{Threshold: -0.1}, wantErr: ErrInvalidThreshold},
		{name: "ThresholdNaN", policy: MatchPolicy{Threshold: math.NaN()}, wantErr: ErrInvalidThreshold},
		{
			name:    "NegativeFieldFloor",
			policy:  MatchPolicy{MinComparableFields: -1},
			wantErr: sriracha.ErrInvalidConfig,
		},
		{
			name:    "NegativeWeightFloor",
			policy:  MatchPolicy{MinComparableWeight: -0.5},
			wantErr: sriracha.ErrInvalidConfig,
		},
		{
			name:    "NaNWeightFloor",
			policy:  MatchPolicy{MinComparableWeight: math.NaN()},
			wantErr: sriracha.ErrInvalidConfig,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.policy.validate()
			if tc.wantErr == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tc.wantErr)
		})
	}
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
		res, err := Match(identical, identical, fs, DefaultMatchPolicy(0.9))
		require.NoError(t, err)
		assert.True(t, res.IsMatch)
		assert.InDelta(t, 1.0, res.Score, 1e-9)
		assert.Len(t, res.PerField, 2)
		assert.Equal(t, 2, res.ComparableFields)
		assert.InDelta(t, 3.0, res.ComparableWeight, 1e-9, "ComparableWeight is the sum of contributing weights")
	})

	t.Run("BelowThreshold", func(t *testing.T) {
		t.Parallel()
		other := tokenize(t, sriracha.RawRecord{
			sriracha.FieldNameGiven:  "Maria",
			sriracha.FieldNameFamily: "Lopez",
		})
		res, err := Match(identical, other, fs, DefaultMatchPolicy(0.9))
		require.NoError(t, err)
		assert.False(t, res.IsMatch)
		assert.Less(t, res.Score, 0.9)
	})

	t.Run("BothAbsentDropsField", func(t *testing.T) {
		t.Parallel()
		// One present field with score 1, one absent on both sides, which
		// drops out of the average without pulling it down.
		a := tokenize(t, sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"})
		res, err := Match(a, a, fs, MatchPolicy{Threshold: 0.9})
		require.NoError(t, err)
		assert.True(t, res.IsMatch)
		assert.InDelta(t, 1.0, res.Score, 1e-9, "absent-on-both fields should not pull score below 1")
		assert.Equal(t, 1, res.ComparableFields)
	})

	t.Run("EvidenceFloorGatesIsMatchNotScore", func(t *testing.T) {
		t.Parallel()
		// The same single-field agreement, now under the default policy.
		// One field of evidence is not identity, so the pair is not a
		// match, but the reported Score stays the honest 1.000.
		a := tokenize(t, sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"})
		res, err := Match(a, a, fs, DefaultMatchPolicy(0.9))
		require.NoError(t, err, "falling below the floor is a result, never an error")
		assert.False(t, res.IsMatch)
		assert.InDelta(t, 1.0, res.Score, 1e-9, "the floor must not alter Score")
		assert.Equal(t, 1, res.ComparableFields)
		assert.InDelta(t, 2.0, res.ComparableWeight, 1e-9)
	})

	t.Run("WeightFloorGatesIsMatch", func(t *testing.T) {
		t.Parallel()
		a := tokenize(t, sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"})
		res, err := Match(a, a, fs, MatchPolicy{Threshold: 0.9, MinComparableWeight: 2.5})
		require.NoError(t, err)
		assert.False(t, res.IsMatch, "2.0 of comparable weight is below the 2.5 floor")
		assert.InDelta(t, 1.0, res.Score, 1e-9)

		res, err = Match(a, a, fs, MatchPolicy{Threshold: 0.9, MinComparableWeight: 2.0})
		require.NoError(t, err)
		assert.True(t, res.IsMatch, "the floor is inclusive")
	})

	t.Run("AsymmetricAbsenceCountsAsMismatch", func(t *testing.T) {
		t.Parallel()
		a := tokenize(t, sriracha.RawRecord{
			sriracha.FieldNameGiven:  "Alice",
			sriracha.FieldNameFamily: "Smith",
		})
		b := tokenize(t, sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"})
		res, err := Match(a, b, fs, MatchPolicy{Threshold: 0.9})
		require.NoError(t, err)
		assert.Less(t, res.Score, 1.0, "asymmetric absence should pull score below 1")
		assert.Equal(t, 2, res.ComparableFields, "ComparableFields counts the union of present fields")
	})

	t.Run("AllFieldsAbsentBothSides", func(t *testing.T) {
		t.Parallel()
		empty := tokenize(t, sriracha.RawRecord{})
		res, err := Match(empty, empty, fs, MatchPolicy{Threshold: 0.5})
		require.NoError(t, err, "all-absent must not error; check ComparableFields instead")
		assert.False(t, res.IsMatch)
		assert.Equal(t, 0.0, res.Score)
		assert.Equal(t, 0, res.ComparableFields)
		assert.Equal(t, 0.0, res.ComparableWeight)
		assert.Len(t, res.Paths, len(fs.Fields))
		assert.Len(t, res.PerField, len(fs.Fields))
	})

	t.Run("ZeroPolicyAppliesNoFloor", func(t *testing.T) {
		t.Parallel()
		a := tokenize(t, sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"})
		res, err := Match(a, a, fs, MatchPolicy{})
		require.NoError(t, err)
		assert.True(t, res.IsMatch, "the zero policy is threshold 0 with no floor")
	})

	t.Run("ZeroWeightFieldDoesNotCount", func(t *testing.T) {
		t.Parallel()
		masked := bloomFS(
			sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Weight: 2.0},
			sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Weight: 0},
		)
		res, err := Match(identical, identical, masked, MatchPolicy{})
		require.NoError(t, err)
		assert.Equal(t, 1, res.ComparableFields)
		assert.InDelta(t, 2.0, res.ComparableWeight, 1e-9)
	})

	t.Run("PathsPopulated", func(t *testing.T) {
		t.Parallel()
		res, err := Match(identical, identical, fs, MatchPolicy{Threshold: 0.9})
		require.NoError(t, err)
		require.Len(t, res.Paths, 2)
		assert.Equal(t, sriracha.FieldNameGiven, res.Paths[0])
		assert.Equal(t, sriracha.FieldNameFamily, res.Paths[1])
	})

	t.Run("ScoreForAndByPath", func(t *testing.T) {
		t.Parallel()
		res, err := Match(identical, identical, fs, MatchPolicy{Threshold: 0.9})
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

	t.Run("InvalidPolicy", func(t *testing.T) {
		t.Parallel()
		_, err := Match(identical, identical, fs, MatchPolicy{Threshold: 1.5})
		require.ErrorIs(t, err, ErrInvalidThreshold)
		_, err = Match(identical, identical, fs, MatchPolicy{MinComparableFields: -1})
		require.ErrorIs(t, err, sriracha.ErrInvalidConfig)
	})

	t.Run("DicePerFieldErrorPropagated", func(t *testing.T) {
		t.Parallel()
		other := identical
		other.FieldSetVersion = "different"
		_, err := Match(identical, other, fs, MatchPolicy{Threshold: 0.5})
		require.ErrorIs(t, err, ErrFieldSetVersionMismatch)
	})

	t.Run("FormatMismatchErrors", func(t *testing.T) {
		t.Parallel()
		other := identical
		other.Format = "sriracha/bloom/1"
		_, err := Match(identical, other, fs, MatchPolicy{Threshold: 0.5})
		require.ErrorIs(t, err, ErrFormatMismatch)
	})

	t.Run("FieldCountMismatchWithFieldSet", func(t *testing.T) {
		t.Parallel()
		shorter := sriracha.FieldSet{
			Version:             fs.Version,
			ProbabilisticParams: fs.ProbabilisticParams,
			Fields:              fs.Fields[:1],
		}
		_, err := Match(identical, identical, shorter, MatchPolicy{Threshold: 0.5})
		require.ErrorIs(t, err, ErrFieldCountMismatch)
	})
}

func TestMatchResult_JSON(t *testing.T) {
	t.Parallel()
	raw, err := json.Marshal(MatchResult{Score: 1, ComparableFields: 2, ComparableWeight: 3})
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(raw, &got))
	assert.Equal(t, 2.0, got["comparable_fields"])
	assert.Equal(t, 3.0, got["comparable_weight"])
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
			name      string
			a, b      sriracha.CLKToken
			threshold float64
			wantErr   error
		}{
			{
				name:      "ThresholdNaN",
				a:         clkTok(nil),
				b:         clkTok(nil),
				threshold: math.NaN(),
				wantErr:   ErrInvalidThreshold,
			},
			{
				name:      "ThresholdAboveOne",
				a:         clkTok(nil),
				b:         clkTok(nil),
				threshold: 1.5,
				wantErr:   ErrInvalidThreshold,
			},
			{
				name:      "ThresholdNegative",
				a:         clkTok(nil),
				b:         clkTok(nil),
				threshold: -0.1,
				wantErr:   ErrInvalidThreshold,
			},
			{
				name:      "FormatMismatch",
				a:         clkTok(nil),
				b:         clkTok(func(tok *sriracha.CLKToken) { tok.Format = "sriracha/clk/1" }),
				threshold: 0.5,
				wantErr:   ErrFormatMismatch,
			},
			{
				name:      "VersionMismatch",
				a:         clkTok(nil),
				b:         clkTok(func(tok *sriracha.CLKToken) { tok.FieldSetVersion = "v2" }),
				threshold: 0.5,
				wantErr:   ErrFieldSetVersionMismatch,
			},
			{
				name:      "KeyIDMismatch",
				a:         clkTok(nil),
				b:         clkTok(func(tok *sriracha.CLKToken) { tok.KeyID = "k2" }),
				threshold: 0.5,
				wantErr:   ErrKeyIDMismatch,
			},
			{
				name:      "FingerprintMismatchBothSet",
				a:         clkTok(func(tok *sriracha.CLKToken) { tok.FieldSetFingerprint = "aa" }),
				b:         clkTok(func(tok *sriracha.CLKToken) { tok.FieldSetFingerprint = "bb" }),
				threshold: 0.5,
				wantErr:   ErrFingerprintMismatch,
			},
			{
				name:      "ParamsMismatch",
				a:         clkTok(nil),
				b:         clkTok(func(tok *sriracha.CLKToken) { tok.ProbabilisticParams.HashCount = 3 }),
				threshold: 0.5,
				wantErr:   ErrParamsMismatch,
			},
			{
				name:      "FilterLengthMismatch",
				a:         clkTok(nil),
				b:         clkTok(func(tok *sriracha.CLKToken) { tok.Filter = []byte{0xf0} }),
				threshold: 0.5,
				wantErr:   ErrFilterLengthMismatch,
			},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				_, err := MatchCLK(tc.a, tc.b, tc.threshold)
				require.ErrorIs(t, err, tc.wantErr)
			})
		}
	})
}

func TestComparisonSentinelsAreDistinct(t *testing.T) {
	t.Parallel()
	sentinels := []error{
		ErrFormatMismatch,
		ErrFieldSetVersionMismatch,
		ErrKeyIDMismatch,
		ErrFingerprintMismatch,
		ErrParamsMismatch,
		ErrFieldCountMismatch,
		ErrFilterLengthMismatch,
		ErrInvalidThreshold,
		ErrNoComparableFields,
	}
	for i, a := range sentinels {
		for j, b := range sentinels {
			if i == j {
				continue
			}
			assert.False(t, errors.Is(a, b), "sentinel %d must not match sentinel %d", i, j)
		}
	}
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
