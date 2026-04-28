package token

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.sriracha.dev/sriracha"
)

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

func TestEqual_VersionMismatch(t *testing.T) {
	t.Parallel()
	a := sriracha.DeterministicToken{FieldSetVersion: "v1", Fields: [][]byte{{0x01}}}
	b := sriracha.DeterministicToken{FieldSetVersion: "v2", Fields: [][]byte{{0x01}}}
	assert.False(t, Equal(a, b), "different FieldSetVersion should compare unequal")
}

func TestEqual_FieldCountMismatch(t *testing.T) {
	t.Parallel()
	a := sriracha.DeterministicToken{FieldSetVersion: "v1", Fields: [][]byte{{0x01}}}
	b := sriracha.DeterministicToken{FieldSetVersion: "v1", Fields: [][]byte{{0x01}, {0x02}}}
	assert.False(t, Equal(a, b), "different field counts should compare unequal")
}

func TestEqual_FieldLengthMismatch(t *testing.T) {
	t.Parallel()
	a := sriracha.DeterministicToken{FieldSetVersion: "v1", Fields: [][]byte{{0x01}}}
	b := sriracha.DeterministicToken{FieldSetVersion: "v1", Fields: [][]byte{{0x01, 0x02}}}
	assert.False(t, Equal(a, b), "different per-field byte lengths should compare unequal")
}

func TestEqual_BothNilField(t *testing.T) {
	t.Parallel()
	a := sriracha.DeterministicToken{FieldSetVersion: "v1", Fields: [][]byte{nil}}
	b := sriracha.DeterministicToken{FieldSetVersion: "v1", Fields: [][]byte{nil}}
	assert.True(t, Equal(a, b), "tokens with matching nil-on-both-sides fields should compare equal")
}

func TestEqual_OneSideNil(t *testing.T) {
	t.Parallel()
	a := sriracha.DeterministicToken{FieldSetVersion: "v1", Fields: [][]byte{nil}}
	b := sriracha.DeterministicToken{FieldSetVersion: "v1", Fields: [][]byte{{0x01}}}
	assert.False(t, Equal(a, b), "nil vs non-nil field should compare unequal")
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

	// Both records omit the optional family field, producing zero-filters there.
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

func TestDicePerField_VersionMismatch(t *testing.T) {
	t.Parallel()
	a := sriracha.BloomToken{FieldSetVersion: "v1"}
	b := sriracha.BloomToken{FieldSetVersion: "v2"}
	_, err := DicePerField(a, b)
	assert.Error(t, err, "version mismatch should error")
}

func TestDicePerField_BloomParamsMismatch(t *testing.T) {
	t.Parallel()
	a := sriracha.BloomToken{
		FieldSetVersion: "v1",
		BloomParams:     sriracha.BloomConfig{SizeBits: 1024, NgramSizes: []int{2}, HashCount: 2},
	}
	b := sriracha.BloomToken{
		FieldSetVersion: "v1",
		BloomParams:     sriracha.BloomConfig{SizeBits: 2048, NgramSizes: []int{2}, HashCount: 2},
	}
	_, err := DicePerField(a, b)
	assert.Error(t, err, "differing BloomParams should error")
}

func TestDicePerField_FieldCountMismatch(t *testing.T) {
	t.Parallel()
	cfg := sriracha.BloomConfig{SizeBits: 8, NgramSizes: []int{2}, HashCount: 1}
	a := sriracha.BloomToken{FieldSetVersion: "v1", BloomParams: cfg, Fields: [][]byte{{0x00}}}
	b := sriracha.BloomToken{FieldSetVersion: "v1", BloomParams: cfg, Fields: [][]byte{{0x00}, {0x00}}}
	_, err := DicePerField(a, b)
	assert.Error(t, err, "differing field counts should error")
}

func TestDicePerField_FieldByteLengthMismatch(t *testing.T) {
	t.Parallel()
	cfg := sriracha.BloomConfig{SizeBits: 8, NgramSizes: []int{2}, HashCount: 1}
	a := sriracha.BloomToken{FieldSetVersion: "v1", BloomParams: cfg, Fields: [][]byte{{0x00}}}
	b := sriracha.BloomToken{FieldSetVersion: "v1", BloomParams: cfg, Fields: [][]byte{{0x00, 0x00}}}
	_, err := DicePerField(a, b)
	assert.Error(t, err, "differing per-field byte lengths should error")
}

func TestDice_DirectCases(t *testing.T) {
	t.Parallel()
	// Both empty (no bits set on either side): cardinality 0 → score 0.
	assert.Equal(t, 0.0, dice([]byte{0x00}, []byte{0x00}), "all-zero on both sides scores 0")
	// Identical pattern.
	assert.InDelta(t, 1.0, dice([]byte{0xff}, []byte{0xff}), 1e-9, "identical bit pattern scores 1.0")
	// Disjoint patterns: 0xf0 vs 0x0f share no bits → score 0.
	assert.Equal(t, 0.0, dice([]byte{0xf0}, []byte{0x0f}), "disjoint bit patterns score 0")
	// Half-overlap: 0xff (8 bits) vs 0xf0 (4 bits), intersection 4 → 2*4/(8+4) = 0.6666...
	assert.InDelta(t, 2.0*4.0/12.0, dice([]byte{0xff}, []byte{0xf0}), 1e-9, "half-overlap score")
}
