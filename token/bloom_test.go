package token

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"math"
	"math/bits"
	"slices"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/normalize"
)

func bloomFSWithCfg(cfg sriracha.ProbabilisticConfig, fields ...sriracha.FieldSpec) sriracha.FieldSet {
	return sriracha.FieldSet{
		Version:             "1.0.0-test",
		Fields:              fields,
		ProbabilisticParams: cfg,
	}
}

func balancedCfg() sriracha.ProbabilisticConfig {
	return sriracha.ProbabilisticConfig{
		SizeBits:   1024,
		NgramSizes: []int{2, 3},
		HashCount:  3,
		Balanced:   true,
	}
}

func popcount(b []byte) int {
	var n int
	for _, x := range b {
		n += bits.OnesCount8(x)
	}
	return n
}

func TestValidateBloomConfig(t *testing.T) {
	t.Parallel()
	valid := sriracha.ProbabilisticConfig{
		SizeBits:   1024,
		NgramSizes: []int{2, 3},
		HashCount:  2,
	}
	cases := []struct {
		name    string
		mutate  func(cfg *sriracha.ProbabilisticConfig)
		wantErr bool
	}{
		{"Valid", func(cfg *sriracha.ProbabilisticConfig) {}, false},
		{"ValidBalanced", func(cfg *sriracha.ProbabilisticConfig) { cfg.Balanced = true }, false},
		{"ZeroSizeBits", func(cfg *sriracha.ProbabilisticConfig) { cfg.SizeBits = 0 }, true},
		{"OddSizeBitsBalanced", func(cfg *sriracha.ProbabilisticConfig) { cfg.SizeBits = 1023; cfg.Balanced = true }, true},
		{"OddSizeBitsUnbalanced", func(cfg *sriracha.ProbabilisticConfig) { cfg.SizeBits = 1023 }, false},
		{"ZeroHashCount", func(cfg *sriracha.ProbabilisticConfig) { cfg.HashCount = 0 }, true},
		{"NegativeHashCount", func(cfg *sriracha.ProbabilisticConfig) { cfg.HashCount = -1 }, true},
		{"EmptyNgramSizes", func(cfg *sriracha.ProbabilisticConfig) { cfg.NgramSizes = nil }, true},
		{"NegativeNgramSize", func(cfg *sriracha.ProbabilisticConfig) { cfg.NgramSizes = []int{2, -1} }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := valid
			cfg.NgramSizes = append([]int(nil), valid.NgramSizes...)
			tc.mutate(&cfg)
			tok := newTok(t, "secret")
			fs := bloomFSWithCfg(cfg, sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0})
			rec := sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"}
			_, err := tok.TokenizeProbabilistic(rec, fs)
			_, clkErr := tok.TokenizeCLK(rec, fs)
			if tc.wantErr {
				require.ErrorIs(t, err, sriracha.ErrInvalidConfig)
				require.ErrorIs(t, clkErr, sriracha.ErrInvalidConfig, "TokenizeCLK must reject the same configs")
				assert.True(t, strings.HasPrefix(err.Error(), "token: "), "got %q", err.Error())
			} else {
				assert.NoError(t, err)
				// CLK is always balanced, so it additionally requires an
				// even SizeBits.
				if cfg.SizeBits%2 != 0 {
					require.ErrorIs(t, clkErr, sriracha.ErrInvalidConfig)
				} else {
					assert.NoError(t, clkErr)
				}
			}
		})
	}
}

func TestTokenizeProbabilistic_AbsentAndEmptyValues(t *testing.T) {
	t.Parallel()
	cfg := balancedCfg()
	cases := []struct {
		name    string
		record  sriracha.RawRecord
		path    sriracha.FieldPath
		wantErr error
	}{
		{"MissingKey", sriracha.RawRecord{}, sriracha.FieldNameGiven, sriracha.ErrRequiredFieldMissing},
		{"EmptyName", sriracha.RawRecord{sriracha.FieldNameGiven: ""}, sriracha.FieldNameGiven, sriracha.ErrEmptyValue},
		{"IdentifierNormalizesToEmpty", sriracha.RawRecord{sriracha.FieldIdentifierPassport: "---"}, sriracha.FieldIdentifierPassport, sriracha.ErrEmptyValue},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tok := newTok(t, "secret")

			optional := bloomFSWithCfg(cfg, sriracha.FieldSpec{Path: tc.path, Required: false, Weight: 1.0})
			tr, err := tok.TokenizeProbabilistic(tc.record, optional)
			require.NoError(t, err)
			require.Len(t, tr.Fields, 1)
			assert.Nil(t, tr.Fields[0], "absent optional field must be nil, not an all-zero filter")

			required := bloomFSWithCfg(cfg, sriracha.FieldSpec{Path: tc.path, Required: true, Weight: 1.0})
			_, err = tok.TokenizeProbabilistic(tc.record, required)
			require.ErrorIs(t, err, tc.wantErr)

			var fieldErr sriracha.FieldError
			require.ErrorAs(t, err, &fieldErr)
			assert.Equal(t, tc.path, fieldErr.Path)
		})
	}
}

// TestTokenizeProbabilistic_NormalizationErrorSentinel pins that a
// normalization failure reaches the caller with the normalize sentinel and
// the offending path intact.
func TestTokenizeProbabilistic_NormalizationErrorSentinel(t *testing.T) {
	t.Parallel()
	fs := bloomFSWithCfg(balancedCfg(), sriracha.FieldSpec{Path: sriracha.FieldDateBirth, Required: true, Weight: 1.0})
	_, err := newTok(t, "secret").TokenizeProbabilistic(sriracha.RawRecord{sriracha.FieldDateBirth: "not-a-date"}, fs)
	require.ErrorIs(t, err, normalize.ErrInvalidValue)
	assert.True(t, strings.HasPrefix(err.Error(), "token: "), "got %q", err.Error())

	var fieldErr sriracha.FieldError
	require.ErrorAs(t, err, &fieldErr)
	assert.Equal(t, sriracha.FieldDateBirth, fieldErr.Path)
}

func TestGramDoubleHash_KnownDigest(t *testing.T) {
	t.Parallel()
	key := []byte("known-test-key")
	gram := []byte("ab")
	pathBytes := []byte(sriracha.FieldNameGiven.String())

	// Recompute the digest independently to pin the preimage layout:
	// len(gram) || gram || len(path) || path with 4-byte big-endian lengths
	// and no counter.
	ref := hmac.New(sha256.New, key)
	var lp [4]byte
	binary.BigEndian.PutUint32(lp[:], uint32(len(gram))) //nolint:gosec // G115: tiny test fixture
	ref.Write(lp[:])
	ref.Write(gram)
	binary.BigEndian.PutUint32(lp[:], uint32(len(pathBytes))) //nolint:gosec // G115: tiny test fixture
	ref.Write(lp[:])
	ref.Write(pathBytes)
	sum := ref.Sum(nil)
	wantH1 := binary.BigEndian.Uint64(sum[:8])
	wantH2 := binary.BigEndian.Uint64(sum[8:16]) | 1

	h1, h2 := gramDoubleHash(hmac.New(sha256.New, key), gram, pathBytes)
	assert.Equal(t, wantH1, h1, "h1 must be the big-endian uint64 of digest bytes 0..8")
	assert.Equal(t, wantH2, h2, "h2 must be the big-endian uint64 of digest bytes 8..16 with the low bit set")
	assert.Equal(t, uint64(1), h2&1, "h2 must be odd")
}

func TestSetGramBits_DoubleHashingRule(t *testing.T) {
	t.Parallel()
	// sizes [1] and a one-rune value produce exactly one gram with no
	// padding, so the filter holds only that gram's positions.
	const baseBits = 1024
	const hashCount = 5
	cfg := sriracha.ProbabilisticConfig{SizeBits: baseBits, NgramSizes: []int{1}, HashCount: hashCount}
	key := []byte("position-rule-key")
	path := sriracha.FieldNameGiven

	h1, h2 := gramDoubleHash(hmac.New(sha256.New, key), []byte("a"), []byte(path.String()))
	want := make([]uint, 0, hashCount)
	for i := range hashCount {
		pos := (h1 + uint64(i)*h2) % uint64(baseBits) //nolint:gosec // G115: i bounded by hashCount
		if !slices.Contains(want, uint(pos)) {
			want = append(want, uint(pos))
		}
	}
	slices.Sort(want)

	bs := acquireBitset(baseBits)
	defer releaseBitset(baseBits, bs)
	setGramBits(hmac.New(sha256.New, key), bs, "a", path, cfg, baseBits)
	got := make([]uint, 0, hashCount)
	for i := uint(0); i < baseBits; i++ {
		if bs.Test(i) {
			got = append(got, i)
		}
	}
	assert.Equal(t, want, got, "set positions must follow pos_i = (h1 + i*h2) mod baseBits")
}

func TestSetGramBits_DifferentGramsLandDifferently(t *testing.T) {
	t.Parallel()
	const baseBits = 1024
	cfg := sriracha.ProbabilisticConfig{SizeBits: baseBits, NgramSizes: []int{1}, HashCount: 3}
	key := []byte("different-grams-key")
	path := sriracha.FieldNameGiven

	collect := func(value string) []uint {
		bs := acquireBitset(baseBits)
		defer releaseBitset(baseBits, bs)
		setGramBits(hmac.New(sha256.New, key), bs, value, path, cfg, baseBits)
		var out []uint
		for i := uint(0); i < baseBits; i++ {
			if bs.Test(i) {
				out = append(out, i)
			}
		}
		return out
	}

	a := collect("a")
	b := collect("b")
	assert.NotEqual(t, a, b, "different grams must produce different position sets")
}

func TestEachNgram_Padding(t *testing.T) {
	t.Parallel()

	t.Run("OneRuneValueProducesGrams", func(t *testing.T) {
		t.Parallel()
		got := ngrams("a", []int{2, 3})
		assert.Equal(t, []string{"_a", "a_", "__a", "_a_", "a__"}, got,
			"a one-rune value must produce grams for every size")
	})

	t.Run("EdgeCharactersAppearInAsManyGramsAsInteriorOnes", func(t *testing.T) {
		t.Parallel()
		grams := ngrams("abc", []int{2})
		assert.Equal(t, []string{"_a", "ab", "bc", "c_"}, grams)
		count := func(r string) int {
			n := 0
			for _, g := range grams {
				n += strings.Count(g, r)
			}
			return n
		}
		assert.Equal(t, count("b"), count("a"), "boundary characters must appear in as many bigrams as interior ones")
		assert.Equal(t, count("b"), count("c"), "boundary characters must appear in as many bigrams as interior ones")
	})

	t.Run("BalancedShortValueIsPresent", func(t *testing.T) {
		t.Parallel()
		tok := newTok(t, "secret")
		fs := bloomFSWithCfg(balancedCfg(), sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0})
		tr, err := tok.TokenizeProbabilistic(sriracha.RawRecord{sriracha.FieldNameGiven: "a"}, fs)
		require.NoError(t, err)
		require.Len(t, tr.Fields, 1)
		require.NotNil(t, tr.Fields[0], "a one-rune value must produce a populated filter")
		assert.Equal(t, 512, popcount(tr.Fields[0]))
	})
}

func TestTokenizeProbabilistic_BalancedPopcountExact(t *testing.T) {
	t.Parallel()
	cfg := balancedCfg()
	fs := bloomFSWithCfg(cfg,
		sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: false, Weight: 2.0},
		sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: false, Weight: 2.5},
		sriracha.FieldSpec{Path: sriracha.FieldDateBirth, Required: false, Weight: 2.0},
		sriracha.FieldSpec{Path: sriracha.FieldContactEmail, Required: false, Weight: 2.0},
	)
	records := []struct {
		name string
		rec  sriracha.RawRecord
	}{
		{"FullRecord", sriracha.RawRecord{
			sriracha.FieldNameGiven:    "alice",
			sriracha.FieldNameFamily:   "smith",
			sriracha.FieldDateBirth:    "1990-05-15",
			sriracha.FieldContactEmail: "alice@example.com",
		}},
		{"ShortValues", sriracha.RawRecord{
			sriracha.FieldNameGiven:  "a",
			sriracha.FieldNameFamily: "b",
		}},
		{"LongValue", sriracha.RawRecord{
			sriracha.FieldNameGiven: strings.Repeat("abcdefghij", 20),
		}},
	}
	for _, tc := range records {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tok := newTok(t, "secret")
			tr, err := tok.TokenizeProbabilistic(tc.rec, fs)
			require.NoError(t, err)
			for i, f := range tr.Fields {
				if f == nil {
					continue
				}
				assert.Equalf(t, int(cfg.SizeBits/2), popcount(f),
					"field %d popcount must be exactly SizeBits/2", i)
			}
		})
	}
}

func TestTokenizeProbabilistic_BalancedDeterminism(t *testing.T) {
	t.Parallel()
	fs := bloomFSWithCfg(balancedCfg(), sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0})
	rec := sriracha.RawRecord{sriracha.FieldNameGiven: "alice"}

	same1, err := newTok(t, "secret").TokenizeProbabilistic(rec, fs)
	require.NoError(t, err)
	same2, err := newTok(t, "secret").TokenizeProbabilistic(rec, fs)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(same1.Fields[0], same2.Fields[0]),
		"identical (secret, value) must yield identical balanced filters")

	other, err := newTok(t, "other-secret").TokenizeProbabilistic(rec, fs)
	require.NoError(t, err)
	assert.False(t, bytes.Equal(same1.Fields[0], other.Fields[0]),
		"different secrets must yield different balanced filters")
}

func TestTokenizeProbabilistic_FormatStamped(t *testing.T) {
	t.Parallel()
	tok := newTok(t, "secret")
	fs := bloomFSWithCfg(balancedCfg(), sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0})
	tr, err := tok.TokenizeProbabilistic(sriracha.RawRecord{sriracha.FieldNameGiven: "alice"}, fs)
	require.NoError(t, err)
	assert.Equal(t, sriracha.TokenFormatProbabilistic, tr.Format)
}

func TestPermutation(t *testing.T) {
	t.Parallel()

	t.Run("BijectionAndStability", func(t *testing.T) {
		t.Parallel()
		tok := newTok(t, "secret")
		const size = 1024
		perm := tok.permutation(size)
		require.Len(t, perm, size)

		seen := make([]bool, size)
		for _, p := range perm {
			require.Less(t, int(p), size)
			require.False(t, seen[p], "permutation must not repeat position %d", p)
			seen[p] = true
		}

		again := tok.permutation(size)
		assert.Equal(t, perm, again, "permutation must be stable across calls")
	})

	t.Run("DiffersBetweenSecrets", func(t *testing.T) {
		t.Parallel()
		a := newTok(t, "secret-a").permutation(1024)
		b := newTok(t, "secret-b").permutation(1024)
		assert.NotEqual(t, a, b, "different secrets must derive different permutations")
	})

	t.Run("SameSecretSamePermutation", func(t *testing.T) {
		t.Parallel()
		a := newTok(t, "secret").permutation(512)
		b := newTok(t, "secret").permutation(512)
		assert.Equal(t, a, b, "the permutation must depend only on (secret, SizeBits)")
	})
}

func TestUniformIndex(t *testing.T) {
	t.Parallel()

	t.Run("RejectsBiasedTail", func(t *testing.T) {
		t.Parallel()
		// 2^64 mod 3 == 1, so the single sample math.MaxUint64 falls in the
		// biased tail and must be rejected in favour of the next sample.
		samples := []uint64{math.MaxUint64, 5}
		i := 0
		next := func() uint64 {
			v := samples[i]
			i++
			return v
		}
		got := uniformIndex(next, 3)
		assert.Equal(t, uint64(2), got, "5 mod 3 after rejecting the biased sample")
		assert.Equal(t, 2, i, "the biased sample must be consumed and discarded")
	})

	t.Run("PowerOfTwoAcceptsEverything", func(t *testing.T) {
		t.Parallel()
		next := func() uint64 { return math.MaxUint64 }
		assert.Equal(t, uint64(3), uniformIndex(next, 4))
	})
}

func TestDiceOrderingPreservedUnderBalancing(t *testing.T) {
	t.Parallel()
	fs := bloomFSWithCfg(sriracha.DefaultProbabilisticConfig(),
		sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: true, Weight: 1.0})
	tok := newTok(t, "secret")

	tokenize := func(family string) sriracha.ProbabilisticToken {
		tr, err := tok.TokenizeProbabilistic(sriracha.RawRecord{sriracha.FieldNameFamily: family}, fs)
		require.NoError(t, err)
		return tr
	}
	smith := tokenize("smith")
	smyth := tokenize("smyth")
	jones := tokenize("jones")

	typo, err := DicePerField(smith, smyth)
	require.NoError(t, err)
	disjoint, err := DicePerField(smith, jones)
	require.NoError(t, err)
	self, err := DicePerField(smith, smith)
	require.NoError(t, err)

	assert.InDelta(t, 1.0, self[0], 1e-9, "identical values must score 1.0 under balancing")
	assert.Greater(t, typo[0], disjoint[0],
		"smith vs smyth must score strictly higher than smith vs jones under the balanced default")
	assert.Less(t, typo[0], 1.0)
}

func TestTokenizeCLK(t *testing.T) {
	t.Parallel()
	cfg := sriracha.DefaultProbabilisticConfig()
	fs := bloomFSWithCfg(cfg,
		sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: false, Weight: 2.0},
		sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: false, Weight: 2.5},
	)
	full := sriracha.RawRecord{
		sriracha.FieldNameGiven:  "alice",
		sriracha.FieldNameFamily: "smith",
	}

	t.Run("EndToEndOrdering", func(t *testing.T) {
		t.Parallel()
		tok := newTok(t, "secret")
		same1, err := tok.TokenizeCLK(full, fs)
		require.NoError(t, err)
		same2, err := tok.TokenizeCLK(full, fs)
		require.NoError(t, err)
		typo, err := tok.TokenizeCLK(sriracha.RawRecord{
			sriracha.FieldNameGiven:  "alice",
			sriracha.FieldNameFamily: "smyth",
		}, fs)
		require.NoError(t, err)
		disjoint, err := tok.TokenizeCLK(sriracha.RawRecord{
			sriracha.FieldNameGiven:  "ursula",
			sriracha.FieldNameFamily: "kroeber",
		}, fs)
		require.NoError(t, err)

		identical, err := MatchCLK(same1, same2, 0.9)
		require.NoError(t, err)
		assert.InDelta(t, 1.0, identical.Score, 1e-9, "the same record must score 1.0")
		assert.True(t, identical.IsMatch)

		typoRes, err := MatchCLK(same1, typo, 0.9)
		require.NoError(t, err)
		disjointRes, err := MatchCLK(same1, disjoint, 0.9)
		require.NoError(t, err)
		// Balanced filters concentrate unrelated Dice scores near 0.5, so
		// only the relative ordering is asserted.
		assert.Less(t, typoRes.Score, 1.0)
		assert.Greater(t, typoRes.Score, disjointRes.Score,
			"a typo record must score strictly between identical and disjoint records")
	})

	t.Run("PopcountExactlyHalf", func(t *testing.T) {
		t.Parallel()
		tok := newTok(t, "secret")
		clk, err := tok.TokenizeCLK(full, fs)
		require.NoError(t, err)
		assert.Equal(t, int(cfg.SizeBits/2), popcount(clk.Filter))
	})

	t.Run("MetadataStamped", func(t *testing.T) {
		t.Parallel()
		tok := newTok(t, "secret", WithKeyID("k1"))
		clk, err := tok.TokenizeCLK(full, fs)
		require.NoError(t, err)
		assert.Equal(t, sriracha.TokenFormatCLK, clk.Format)
		assert.Equal(t, fs.Version, clk.FieldSetVersion)
		assert.Equal(t, "k1", clk.KeyID)
		assert.Equal(t, cfg, clk.ProbabilisticParams)
		assert.Empty(t, clk.FieldSetFingerprint, "token.TokenizeCLK must not populate FieldSetFingerprint")
	})

	t.Run("ZeroContributingFieldsErrors", func(t *testing.T) {
		t.Parallel()
		tok := newTok(t, "secret")
		cases := []struct {
			name string
			rec  sriracha.RawRecord
		}{
			{"EmptyRecord", sriracha.RawRecord{}},
			{"OnlyEmptyValues", sriracha.RawRecord{sriracha.FieldNameGiven: ""}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				_, err := tok.TokenizeCLK(tc.rec, fs)
				require.ErrorIs(t, err, ErrNoContributingFields)
			})
		}
	})

	t.Run("RequiredMissingErrors", func(t *testing.T) {
		t.Parallel()
		tok := newTok(t, "secret")
		required := bloomFSWithCfg(cfg, sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0})
		_, err := tok.TokenizeCLK(sriracha.RawRecord{}, required)
		require.ErrorIs(t, err, sriracha.ErrRequiredFieldMissing)
	})

	t.Run("RequiredEmptyErrors", func(t *testing.T) {
		t.Parallel()
		tok := newTok(t, "secret")
		required := bloomFSWithCfg(cfg, sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0})
		_, err := tok.TokenizeCLK(sriracha.RawRecord{sriracha.FieldNameGiven: ""}, required)
		require.ErrorIs(t, err, sriracha.ErrEmptyValue)
	})

	t.Run("NormalizationError", func(t *testing.T) {
		t.Parallel()
		tok := newTok(t, "secret")
		dateFS := bloomFSWithCfg(cfg, sriracha.FieldSpec{Path: sriracha.FieldDateBirth, Required: true, Weight: 1.0})
		_, err := tok.TokenizeCLK(sriracha.RawRecord{sriracha.FieldDateBirth: "not-a-date"}, dateFS)
		require.ErrorIs(t, err, normalize.ErrInvalidValue)

		var fieldErr sriracha.FieldError
		require.ErrorAs(t, err, &fieldErr)
		assert.Equal(t, sriracha.FieldDateBirth, fieldErr.Path)
	})

	t.Run("OddSizeBitsRejected", func(t *testing.T) {
		t.Parallel()
		// CLK is always balanced, so an odd SizeBits is rejected even when
		// cfg.Balanced is false and ProbabilisticConfig.Validate accepts it.
		odd := cfg
		odd.NgramSizes = append([]int(nil), cfg.NgramSizes...)
		odd.Balanced = false
		odd.SizeBits = 1023
		oddFS := bloomFSWithCfg(odd, sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: false, Weight: 1.0})
		require.NoError(t, odd.Validate(), "the config itself must be valid")
		_, err := newTok(t, "secret").TokenizeCLK(full, oddFS)
		require.ErrorIs(t, err, sriracha.ErrInvalidConfig)
		assert.Contains(t, err.Error(), "even for CLK")
	})

	t.Run("UnbalancedConfig", func(t *testing.T) {
		t.Parallel()
		plain := cfg
		plain.NgramSizes = append([]int(nil), cfg.NgramSizes...)
		plain.Balanced = false
		plainFS := bloomFSWithCfg(plain,
			sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: false, Weight: 2.0},
			sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: false, Weight: 2.5},
		)
		tok := newTok(t, "secret")
		clk1, err := tok.TokenizeCLK(full, plainFS)
		require.NoError(t, err)
		clk2, err := tok.TokenizeCLK(full, plainFS)
		require.NoError(t, err)
		assert.True(t, bytes.Equal(clk1.Filter, clk2.Filter))
		assert.Equal(t, int(plain.SizeBits/2), popcount(clk1.Filter),
			"CLK filters are balanced regardless of cfg.Balanced")
	})

	t.Run("FieldPathSeparatesGrams", func(t *testing.T) {
		t.Parallel()
		// The same value under two different paths must produce different
		// CLK filters, because the per-gram preimage includes the path.
		tok := newTok(t, "secret")
		givenOnly := bloomFSWithCfg(cfg, sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0})
		familyOnly := bloomFSWithCfg(cfg, sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: true, Weight: 1.0})
		a, err := tok.TokenizeCLK(sriracha.RawRecord{sriracha.FieldNameGiven: "smith"}, givenOnly)
		require.NoError(t, err)
		b, err := tok.TokenizeCLK(sriracha.RawRecord{sriracha.FieldNameFamily: "smith"}, familyOnly)
		require.NoError(t, err)
		assert.False(t, bytes.Equal(a.Filter, b.Filter))
	})
}

// FuzzBloomBalanced verifies the balanced construction invariants for
// arbitrary input: tokenization never panics, every present field's filter
// has the expected byte length and a popcount of exactly SizeBits/2, and
// two calls with the same inputs produce identical bytes.
func FuzzBloomBalanced(f *testing.F) {
	f.Add("Alice")
	f.Add("Christopher")
	f.Add("")
	f.Add("a")
	f.Add("\x00\xff")

	cfg := sriracha.ProbabilisticConfig{
		SizeBits:   512,
		NgramSizes: []int{2, 3},
		HashCount:  2,
		Balanced:   true,
	}
	fs := bloomFSWithCfg(cfg, sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: false, Weight: 1.0})
	tok, _ := New(testSecret("fuzz-secret"))
	fieldBytes := int((cfg.SizeBits + 63) / 64 * 8)

	f.Fuzz(func(t *testing.T, given string) {
		rec := sriracha.RawRecord{sriracha.FieldNameGiven: given}
		tr1, err := tok.TokenizeProbabilistic(rec, fs)
		if err != nil {
			return
		}
		if len(tr1.Fields) != 1 {
			t.Fatalf("Fields length %d, want 1", len(tr1.Fields))
		}
		norm, err := normalize.Normalize(given, sriracha.FieldNameGiven)
		if err != nil {
			return
		}
		if norm == "" {
			if tr1.Fields[0] != nil {
				t.Fatalf("empty value: field must be nil, got %d bytes", len(tr1.Fields[0]))
			}
			return
		}
		if got := len(tr1.Fields[0]); got != fieldBytes {
			t.Fatalf("field byte length %d, want %d", got, fieldBytes)
		}
		if got := popcount(tr1.Fields[0]); got != int(cfg.SizeBits/2) {
			t.Fatalf("popcount %d, want exactly %d", got, cfg.SizeBits/2)
		}
		tr2, err := tok.TokenizeProbabilistic(rec, fs)
		if err != nil {
			t.Fatalf("second balanced tokenization failed: %v", err)
		}
		if !bytes.Equal(tr1.Fields[0], tr2.Fields[0]) {
			t.Fatalf("balanced tokenization non-deterministic: %x vs %x", tr1.Fields[0], tr2.Fields[0])
		}
	})
}
