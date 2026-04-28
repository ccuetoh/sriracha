package token

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.sriracha.dev/sriracha"
)

func detTok(version string, fields ...[]byte) sriracha.DeterministicToken {
	return sriracha.DeterministicToken{FieldSetVersion: version, Fields: fields}
}

func bloomTokWith(params sriracha.BloomConfig, fields ...[]byte) sriracha.BloomToken {
	return sriracha.BloomToken{FieldSetVersion: "v1", BloomParams: params, Fields: fields}
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

	a, err := tok.TokenizeRecord(rec, fs)
	require.NoError(t, err)
	b, err := tok.TokenizeRecord(rec, fs)
	require.NoError(t, err)

	assert.True(t, Equal(a, b), "identical inputs should produce equal tokens")
}

func TestEqual_DifferentInputs(t *testing.T) {
	t.Parallel()
	tok := newTok(t, "secret")
	fs := deterministicFS(sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0})

	a, err := tok.TokenizeRecord(sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"}, fs)
	require.NoError(t, err)
	b, err := tok.TokenizeRecord(sriracha.RawRecord{sriracha.FieldNameGiven: "Bob"}, fs)
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

	a, err := tok.TokenizeRecordBloom(rec, fs)
	require.NoError(t, err)
	b, err := tok.TokenizeRecordBloom(rec, fs)
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

	a, err := tok.TokenizeRecordBloom(sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Christopher",
		sriracha.FieldNameFamily: "Smith",
	}, fs)
	require.NoError(t, err)
	b, err := tok.TokenizeRecordBloom(sriracha.RawRecord{
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

func TestDicePerField_MissingFieldZero(t *testing.T) {
	t.Parallel()
	tok := newTok(t, "secret")
	fs := bloomFS(
		sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0},
		sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: false, Weight: 0.5},
	)

	a, err := tok.TokenizeRecordBloom(sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"}, fs)
	require.NoError(t, err)
	b, err := tok.TokenizeRecordBloom(sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"}, fs)
	require.NoError(t, err)

	scores, err := DicePerField(a, b)
	require.NoError(t, err)
	require.Len(t, scores, 2)
	assert.InDelta(t, 1.0, scores[0], 1e-9, "present matching field should score 1.0")
	assert.Equal(t, 0.0, scores[1], "absent (zero-filter) field should score 0.0")
}

func TestDicePerField_Errors(t *testing.T) {
	t.Parallel()
	cfg := sriracha.BloomConfig{SizeBits: 8, NgramSizes: []int{2}, HashCount: 1}
	cases := []struct {
		name string
		a, b sriracha.BloomToken
	}{
		{
			name: "VersionMismatch",
			a:    sriracha.BloomToken{FieldSetVersion: "v1"},
			b:    sriracha.BloomToken{FieldSetVersion: "v2"},
		},
		{
			name: "BloomParamsMismatch",
			a:    bloomTokWith(sriracha.BloomConfig{SizeBits: 1024, NgramSizes: []int{2}, HashCount: 2}),
			b:    bloomTokWith(sriracha.BloomConfig{SizeBits: 2048, NgramSizes: []int{2}, HashCount: 2}),
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
			a:    sriracha.BloomToken{FieldSetVersion: "v1", KeyID: "k1", BloomParams: cfg, Fields: [][]byte{{0x00}}},
			b:    sriracha.BloomToken{FieldSetVersion: "v1", KeyID: "k2", BloomParams: cfg, Fields: [][]byte{{0x00}}},
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

	tokenize := func(t *testing.T, rec sriracha.RawRecord) sriracha.BloomToken {
		t.Helper()
		tr, err := tok.TokenizeRecordBloom(rec, fs)
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
		_, err := Match(empty, empty, fs, 0.5)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no comparable fields")
	})

	t.Run("ThresholdOutOfRange", func(t *testing.T) {
		t.Parallel()
		_, err := Match(identical, identical, fs, 1.5)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "threshold")
		_, err = Match(identical, identical, fs, -0.1)
		require.Error(t, err)
	})

	t.Run("DicePerFieldErrorPropagated", func(t *testing.T) {
		t.Parallel()
		other := identical
		other.FieldSetVersion = "different"
		_, err := Match(identical, other, fs, 0.5)
		require.Error(t, err)
	})

	t.Run("FieldCountMismatchWithFieldSet", func(t *testing.T) {
		t.Parallel()
		shorter := sriracha.FieldSet{
			Version:     fs.Version,
			BloomParams: fs.BloomParams,
			Fields:      fs.Fields[:1],
		}
		_, err := Match(identical, identical, shorter, 0.5)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "field count")
	})
}

func TestAllZero(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		in   []byte
		want bool
	}{
		{"Nil", nil, true},
		{"Empty", []byte{}, true},
		{"AllZero", []byte{0, 0, 0}, true},
		{"AnyNonZero", []byte{0, 1, 0}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, allZero(tc.in))
		})
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
