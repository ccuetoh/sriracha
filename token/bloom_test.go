package token

import (
	"bytes"
	"math/bits"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.sriracha.dev/sriracha"
)

func bloomFSWithCfg(cfg sriracha.BloomConfig, fields ...sriracha.FieldSpec) sriracha.FieldSet {
	return sriracha.FieldSet{
		Version:     "1.0.0-test",
		Fields:      fields,
		BloomParams: cfg,
	}
}

func popcount(b []byte) int {
	var n int
	for _, x := range b {
		n += bits.OnesCount8(x)
	}
	return n
}

func TestTokenizeRecordBloom_BLIP(t *testing.T) {
	t.Parallel()
	givenSpec := sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0}
	rec := sriracha.RawRecord{sriracha.FieldNameGiven: "Christopher"}

	t.Run("Determinism", func(t *testing.T) {
		t.Parallel()
		cfg := sriracha.BloomConfig{
			SizeBits:        1024,
			NgramSizes:      []int{2, 3},
			HashCount:       2,
			FlipProbability: 0.05,
		}
		fs := bloomFSWithCfg(cfg, givenSpec)

		tokA := newTok(t, "secret")
		tokB := newTok(t, "secret")
		trA, err := tokA.TokenizeRecordBloom(rec, fs)
		require.NoError(t, err)
		trB, err := tokB.TokenizeRecordBloom(rec, fs)
		require.NoError(t, err)
		require.Len(t, trA.Fields, 1)
		require.Len(t, trB.Fields, 1)
		assert.True(t, bytes.Equal(trA.Fields[0], trB.Fields[0]),
			"identical (secret, value) must yield identical filters under BLIP")
	})

	t.Run("ChangesBits", func(t *testing.T) {
		t.Parallel()
		baseCfg := sriracha.BloomConfig{SizeBits: 1024, NgramSizes: []int{2, 3}, HashCount: 2}
		blipCfg := baseCfg
		blipCfg.FlipProbability = 0.05

		tok := newTok(t, "secret")
		base, err := tok.TokenizeRecordBloom(rec, bloomFSWithCfg(baseCfg, givenSpec))
		require.NoError(t, err)
		blip, err := tok.TokenizeRecordBloom(rec, bloomFSWithCfg(blipCfg, givenSpec))
		require.NoError(t, err)

		require.Equal(t, len(base.Fields[0]), len(blip.Fields[0]))
		assert.False(t, bytes.Equal(base.Fields[0], blip.Fields[0]),
			"BLIP at p=0.05 should change at least one bit on a 1024-bit filter")
	})

	t.Run("ZeroProbabilityIsNoOp", func(t *testing.T) {
		t.Parallel()
		baseCfg := sriracha.BloomConfig{SizeBits: 1024, NgramSizes: []int{2, 3}, HashCount: 2}
		zeroCfg := baseCfg
		zeroCfg.FlipProbability = 0

		tok := newTok(t, "secret")
		base, err := tok.TokenizeRecordBloom(rec, bloomFSWithCfg(baseCfg, givenSpec))
		require.NoError(t, err)
		zero, err := tok.TokenizeRecordBloom(rec, bloomFSWithCfg(zeroCfg, givenSpec))
		require.NoError(t, err)
		assert.True(t, bytes.Equal(base.Fields[0], zero.Fields[0]),
			"FlipProbability=0 must be a byte-identical no-op")
	})

	t.Run("DifferentValuesProduceDifferentFlipPatterns", func(t *testing.T) {
		t.Parallel()
		cfg := sriracha.BloomConfig{
			SizeBits:        1024,
			NgramSizes:      []int{2, 3},
			HashCount:       2,
			FlipProbability: 0.05,
		}
		fs := bloomFSWithCfg(cfg, givenSpec)
		tok := newTok(t, "secret")

		trA, err := tok.TokenizeRecordBloom(sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"}, fs)
		require.NoError(t, err)
		trB, err := tok.TokenizeRecordBloom(sriracha.RawRecord{sriracha.FieldNameGiven: "Bob"}, fs)
		require.NoError(t, err)
		assert.False(t, bytes.Equal(trA.Fields[0], trB.Fields[0]),
			"different values must produce different filters even under BLIP")
	})
}

func TestTokenizeRecordBloom_Balanced(t *testing.T) {
	t.Parallel()
	givenSpec := sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0}

	t.Run("Determinism", func(t *testing.T) {
		t.Parallel()
		cfg := sriracha.BloomConfig{
			SizeBits:       1024,
			NgramSizes:     []int{2, 3},
			HashCount:      2,
			TargetPopcount: 300,
		}
		fs := bloomFSWithCfg(cfg, givenSpec)
		rec := sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"}

		trA, err := newTok(t, "secret").TokenizeRecordBloom(rec, fs)
		require.NoError(t, err)
		trB, err := newTok(t, "secret").TokenizeRecordBloom(rec, fs)
		require.NoError(t, err)
		assert.True(t, bytes.Equal(trA.Fields[0], trB.Fields[0]),
			"balanced filter must be deterministic across tokenizers")
	})

	t.Run("ReachesTargetWhenBelow", func(t *testing.T) {
		t.Parallel()
		cfg := sriracha.BloomConfig{
			SizeBits:       1024,
			NgramSizes:     []int{2, 3},
			HashCount:      2,
			TargetPopcount: 300,
		}
		fs := bloomFSWithCfg(cfg, givenSpec)
		tok := newTok(t, "secret")

		// "Alice" produces few ngrams under 2/3; pre-balance popcount is small.
		basePop := popcount(mustField(t, tok, sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"}, bloomFSWithCfg(
			sriracha.BloomConfig{SizeBits: 1024, NgramSizes: []int{2, 3}, HashCount: 2}, givenSpec)))
		require.Less(t, basePop, 300, "test invariant: pre-balance popcount must be below target")

		filter := mustField(t, tok, sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"}, fs)
		assert.Equal(t, 300, popcount(filter), "balanced filter popcount must equal target")
	})

	t.Run("NoOpWhenAtOrAboveTarget", func(t *testing.T) {
		t.Parallel()
		baseCfg := sriracha.BloomConfig{SizeBits: 1024, NgramSizes: []int{2, 3}, HashCount: 2}
		tok := newTok(t, "secret")
		rec := sriracha.RawRecord{sriracha.FieldNameGiven: "Christopher"}

		baseFilter := mustField(t, tok, rec, bloomFSWithCfg(baseCfg, givenSpec))
		basePop := popcount(baseFilter)
		require.Greater(t, basePop, 0)

		// Set target equal to the natural popcount: must be a byte-identical no-op.
		eqCfg := baseCfg
		eqCfg.TargetPopcount = uint32(basePop) //nolint:gosec // G115: popcount bounded by 1024-bit filter size
		eqFilter := mustField(t, tok, rec, bloomFSWithCfg(eqCfg, givenSpec))
		assert.True(t, bytes.Equal(baseFilter, eqFilter),
			"TargetPopcount equal to natural popcount must not change the filter")

		// And below natural: also no-op.
		belowCfg := baseCfg
		belowCfg.TargetPopcount = uint32(basePop - 1) //nolint:gosec // G115: popcount bounded by 1024-bit filter size
		belowFilter := mustField(t, tok, rec, bloomFSWithCfg(belowCfg, givenSpec))
		assert.True(t, bytes.Equal(baseFilter, belowFilter),
			"TargetPopcount below natural popcount must not change the filter")
	})

	t.Run("ComposesWithBLIP", func(t *testing.T) {
		t.Parallel()
		cfg := sriracha.BloomConfig{
			SizeBits:        2048,
			NgramSizes:      []int{2, 3},
			HashCount:       3,
			FlipProbability: 0.02,
			TargetPopcount:  400,
		}
		fs := bloomFSWithCfg(cfg, givenSpec)
		rec := sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"}

		trA, err := newTok(t, "secret").TokenizeRecordBloom(rec, fs)
		require.NoError(t, err)
		trB, err := newTok(t, "secret").TokenizeRecordBloom(rec, fs)
		require.NoError(t, err)
		assert.True(t, bytes.Equal(trA.Fields[0], trB.Fields[0]),
			"BLIP+balanced must remain deterministic")
		assert.Equal(t, 400, popcount(trA.Fields[0]),
			"composed BLIP+balance must still hit TargetPopcount exactly")
	})
}

func TestTokenizeRecordBloom_HardenedMatch(t *testing.T) {
	t.Parallel()

	cfg := sriracha.HardenedBloomConfig()
	fs := bloomFSWithCfg(cfg,
		sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: false, Weight: 2.0},
		sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: false, Weight: 2.5},
	)
	tok := newTok(t, "secret")

	trA, err := tok.TokenizeRecordBloom(sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Christopher",
		sriracha.FieldNameFamily: "Smith",
	}, fs)
	require.NoError(t, err)
	trB, err := tok.TokenizeRecordBloom(sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Cristopher",
		sriracha.FieldNameFamily: "Smyth",
	}, fs)
	require.NoError(t, err)

	// Threshold is intentionally lenient: HardenedBloomConfig adds substantial
	// noise (BLIP at p=0.02, padded to popcount 400 of 2048 bits), so Dice
	// scores compress relative to the unhardened baseline. This is a smoke
	// test that Match still produces a usable signal — not a precision claim.
	res, err := Match(trA, trB, fs, 0.20)
	require.NoError(t, err)
	assert.True(t, res.IsMatch, "similar records under HardenedBloomConfig should match at threshold 0.20, got %v (score %.3f)", res.IsMatch, res.Score)
}

// mustField tokenizes rec under fs and returns the first field's bytes.
// Fails the test on any error or unexpected layout.
func mustField(t *testing.T, tok Tokenizer, rec sriracha.RawRecord, fs sriracha.FieldSet) []byte {
	t.Helper()
	tr, err := tok.TokenizeRecordBloom(rec, fs)
	require.NoError(t, err)
	require.NotEmpty(t, tr.Fields)
	return tr.Fields[0]
}

// FuzzBloomBLIP verifies that BLIP-enabled tokenization never panics, that
// the filter retains the expected byte length, and that determinism holds
// across two calls with the same inputs.
func FuzzBloomBLIP(f *testing.F) {
	f.Add("Alice")
	f.Add("Christopher")
	f.Add("")
	f.Add("\x00\xff")

	cfg := sriracha.BloomConfig{
		SizeBits:        512,
		NgramSizes:      []int{2, 3},
		HashCount:       2,
		FlipProbability: 0.10,
	}
	fs := bloomFSWithCfg(cfg, sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: false, Weight: 1.0})
	tok, _ := New([]byte("fuzz-secret"))
	fieldBytes := int((cfg.SizeBits + 63) / 64 * 8)

	f.Fuzz(func(t *testing.T, given string) {
		rec := sriracha.RawRecord{sriracha.FieldNameGiven: given}
		tr1, err := tok.TokenizeRecordBloom(rec, fs)
		if err != nil {
			return
		}
		if len(tr1.Fields) != 1 {
			t.Fatalf("Fields length %d, want 1", len(tr1.Fields))
		}
		if got := len(tr1.Fields[0]); got != fieldBytes {
			t.Fatalf("field byte length %d, want %d", got, fieldBytes)
		}
		tr2, err := tok.TokenizeRecordBloom(rec, fs)
		if err != nil {
			t.Fatalf("second BLIP tokenization failed: %v", err)
		}
		if !bytes.Equal(tr1.Fields[0], tr2.Fields[0]) {
			t.Fatalf("BLIP non-deterministic: %x vs %x", tr1.Fields[0], tr2.Fields[0])
		}
	})
}

// FuzzBloomBalanced verifies that balanced tokenization never panics and that
// the post-balance popcount equals TargetPopcount whenever the natural
// popcount is below the target.
func FuzzBloomBalanced(f *testing.F) {
	f.Add("Alice")
	f.Add("Christopher")
	f.Add("")
	f.Add("a")

	const target uint32 = 200
	balCfg := sriracha.BloomConfig{
		SizeBits:       1024,
		NgramSizes:     []int{2, 3},
		HashCount:      2,
		TargetPopcount: target,
	}
	rawCfg := balCfg
	rawCfg.TargetPopcount = 0

	fs := bloomFSWithCfg(balCfg, sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: false, Weight: 1.0})
	rawFs := bloomFSWithCfg(rawCfg, sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: false, Weight: 1.0})

	tok, _ := New([]byte("fuzz-secret"))

	f.Fuzz(func(t *testing.T, given string) {
		rec := sriracha.RawRecord{sriracha.FieldNameGiven: given}
		tr, err := tok.TokenizeRecordBloom(rec, fs)
		if err != nil {
			return
		}
		raw, err := tok.TokenizeRecordBloom(rec, rawFs)
		if err != nil {
			t.Fatalf("raw tokenization failed: %v", err)
		}
		rawPop := popcount(raw.Fields[0])
		if rawPop >= int(target) {
			return
		}
		if got := popcount(tr.Fields[0]); got != int(target) {
			t.Fatalf("popcount %d, want %d", got, target)
		}
	})
}
