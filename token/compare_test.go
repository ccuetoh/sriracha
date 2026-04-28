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

func bloomTokWith(version string, params sriracha.BloomConfig, fields ...[]byte) sriracha.BloomToken {
	return sriracha.BloomToken{FieldSetVersion: version, BloomParams: params, Fields: fields}
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
			a:    bloomTokWith("v1", sriracha.BloomConfig{SizeBits: 1024, NgramSizes: []int{2}, HashCount: 2}),
			b:    bloomTokWith("v1", sriracha.BloomConfig{SizeBits: 2048, NgramSizes: []int{2}, HashCount: 2}),
		},
		{
			name: "FieldCountMismatch",
			a:    bloomTokWith("v1", cfg, []byte{0x00}),
			b:    bloomTokWith("v1", cfg, []byte{0x00}, []byte{0x00}),
		},
		{
			name: "FieldByteLengthMismatch",
			a:    bloomTokWith("v1", cfg, []byte{0x00}),
			b:    bloomTokWith("v1", cfg, []byte{0x00, 0x00}),
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
