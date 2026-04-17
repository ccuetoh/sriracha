package token

import (
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.sriracha.dev/internal/bitset"
	"go.sriracha.dev/sriracha"
)

// dice computes the Sorensen-Dice coefficient of two same-size bitsets.
func dice(t *testing.T, a, b *bitset.Bitset) float64 {
	t.Helper()
	inter, err := bitset.And(a, b)
	require.NoError(t, err)
	intersection := bitset.Popcount(inter)
	total := bitset.Popcount(a) + bitset.Popcount(b)
	if total == 0 {
		return 0
	}
	return 2.0 * float64(intersection) / float64(total)
}

func newTok(t *testing.T, secret string) *Tokenizer {
	t.Helper()
	tok, err := New([]byte(secret))
	require.NoErrorf(t, err, "New(%q)", secret)
	return tok
}

func deterministicFS(fields ...sriracha.FieldSpec) sriracha.FieldSet {
	return sriracha.FieldSet{
		Version: "test-v1",
		Fields:  fields,
	}
}

func bloomFS(fields ...sriracha.FieldSpec) sriracha.FieldSet {
	return sriracha.FieldSet{
		Version: "test-v1",
		Fields:  fields,
		BloomParams: sriracha.BloomConfig{
			SizeBits:   1024,
			NgramSizes: []int{2, 3},
			HashCount:  2,
		},
	}
}

func TestTokenizeRecord_Idempotent(t *testing.T) {
	t.Parallel()
	tok := newTok(t, "secret")
	rec := sriracha.RawRecord{sriracha.FieldNameGiven: "John"}
	fs := deterministicFS(sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0})

	tr1, err := tok.TokenizeRecord(rec, fs)
	require.NoError(t, err)
	tr2, err := tok.TokenizeRecord(rec, fs)
	require.NoError(t, err)
	assert.Equal(t, tr1.Payload, tr2.Payload, "idempotency: payloads differ for identical input")
}

func TestTokenizeRecord_CrossFieldIsolation(t *testing.T) {
	t.Parallel()
	tok := newTok(t, "secret")
	rec := sriracha.RawRecord{
		sriracha.FieldNameGiven:  "John",
		sriracha.FieldNameFamily: "John",
	}
	fs := deterministicFS(
		sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0},
		sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: true, Weight: 1.0},
	)

	tr, err := tok.TokenizeRecord(rec, fs)
	require.NoError(t, err)
	require.Len(t, tr.Payload, 64, "expected 64 bytes")
	assert.NotEqual(t, tr.Payload[:32], tr.Payload[32:], "cross-field isolation: same value with different paths produced identical tokens")
}

func TestTokenizeRecord_DifferentSecret(t *testing.T) {
	t.Parallel()
	rec := sriracha.RawRecord{sriracha.FieldNameGiven: "John"}
	fs := deterministicFS(sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0})

	tr1, err := newTok(t, "secret-a").TokenizeRecord(rec, fs)
	require.NoError(t, err)
	tr2, err := newTok(t, "secret-b").TokenizeRecord(rec, fs)
	require.NoError(t, err)
	assert.NotEqual(t, tr1.Payload, tr2.Payload, "different secrets produced identical payloads")
}

func TestValidateTokenRecord_TamperedPayload(t *testing.T) {
	t.Parallel()
	tok := newTok(t, "secret")
	rec := sriracha.RawRecord{sriracha.FieldNameGiven: "John"}
	fs := deterministicFS(sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0})

	tr, err := tok.TokenizeRecord(rec, fs)
	require.NoError(t, err)
	tr.Payload[0] ^= 0xff
	assert.Error(t, ValidateTokenRecord(tr), "expected checksum mismatch error for tampered payload")
}

func TestValidateTokenRecord_Valid(t *testing.T) {
	t.Parallel()
	tok := newTok(t, "secret")
	rec := sriracha.RawRecord{sriracha.FieldNameGiven: "John"}
	fs := deterministicFS(sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0})

	tr, err := tok.TokenizeRecord(rec, fs)
	require.NoError(t, err)
	assert.NoError(t, ValidateTokenRecord(tr), "expected nil error for valid token")
}

func TestTokenizeRecord_MissingRequired(t *testing.T) {
	t.Parallel()
	tok := newTok(t, "secret")
	fs := deterministicFS(sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0})

	_, err := tok.TokenizeRecord(sriracha.RawRecord{}, fs)
	assert.Error(t, err, "expected error for missing required field")
}

func TestTokenizeRecord_MissingOptionalSkipped(t *testing.T) {
	t.Parallel()
	tok := newTok(t, "secret")
	rec := sriracha.RawRecord{sriracha.FieldNameGiven: "John"}
	fs := deterministicFS(
		sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0},
		sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: false, Weight: 0.5},
	)

	tr, err := tok.TokenizeRecord(rec, fs)
	require.NoError(t, err)
	assert.Len(t, tr.Payload, 32, "expected 32 bytes (optional field skipped)")
}

func TestTokenizeRecord_EmptyAllOptional(t *testing.T) {
	t.Parallel()
	tok := newTok(t, "secret")
	fs := deterministicFS(
		sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: false, Weight: 1.0},
		sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: false, Weight: 0.5},
	)

	tr, err := tok.TokenizeRecord(sriracha.RawRecord{}, fs)
	require.NoError(t, err)
	assert.Empty(t, tr.Payload, "expected empty payload")
	assert.Equal(t, sha256.Sum256(nil), tr.Checksum, "checksum mismatch for empty payload")
	assert.NoError(t, ValidateTokenRecord(tr), "ValidateTokenRecord failed for empty payload")
}

func TestTokenizeRecord_NormalizationError(t *testing.T) {
	t.Parallel()
	tok := newTok(t, "secret")
	// Date fields reject non-ISO-8601 values, triggering a normalization error.
	rec := sriracha.RawRecord{sriracha.FieldDateBirth: "not-a-date"}
	fs := deterministicFS(sriracha.FieldSpec{Path: sriracha.FieldDateBirth, Required: true, Weight: 1.0})
	_, err := tok.TokenizeRecord(rec, fs)
	assert.Error(t, err, "expected normalization error for invalid date")
}

func TestTokenizeRecordBloom_NormalizationError(t *testing.T) {
	t.Parallel()
	tok := newTok(t, "secret")
	rec := sriracha.RawRecord{sriracha.FieldDateBirth: "not-a-date"}
	fs := bloomFS(sriracha.FieldSpec{Path: sriracha.FieldDateBirth, Required: true, Weight: 1.0})
	_, err := tok.TokenizeRecordBloom(rec, fs)
	assert.Error(t, err, "expected normalization error for invalid date")
}

// Note: the spec suggests "John" vs "Jon" but both are short (3-4 chars),
// yielding very few bigrams/trigrams and unreliable Dice scores.
// "Christopher" vs "Cristopher" tests the same property with more ngrams.
func TestTokenizeRecordBloom_SimilarNames(t *testing.T) {
	t.Parallel()
	tok := newTok(t, "secret")
	fs := bloomFS(sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0})

	tr1, err := tok.TokenizeRecordBloom(sriracha.RawRecord{sriracha.FieldNameGiven: "Christopher"}, fs)
	require.NoError(t, err)
	tr2, err := tok.TokenizeRecordBloom(sriracha.RawRecord{sriracha.FieldNameGiven: "Cristopher"}, fs)
	require.NoError(t, err)

	bs1, err := bitset.FromBytes(tr1.Payload)
	require.NoError(t, err)
	bs2, err := bitset.FromBytes(tr2.Payload)
	require.NoError(t, err)

	d := dice(t, bs1, bs2)
	t.Logf("Dice(Christopher, Cristopher) = %.4f", d)
	assert.Greater(t, d, 0.80, "Dice(Christopher, Cristopher) = %.4f, expected > 0.80", d)
}

func TestTokenizeRecordBloom_DissimilarNames(t *testing.T) {
	t.Parallel()
	tok := newTok(t, "secret")
	fs := bloomFS(sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0})

	tr1, err := tok.TokenizeRecordBloom(sriracha.RawRecord{sriracha.FieldNameGiven: "John"}, fs)
	require.NoError(t, err)
	tr2, err := tok.TokenizeRecordBloom(sriracha.RawRecord{sriracha.FieldNameGiven: "Maria"}, fs)
	require.NoError(t, err)

	bs1, err := bitset.FromBytes(tr1.Payload)
	require.NoError(t, err)
	bs2, err := bitset.FromBytes(tr2.Payload)
	require.NoError(t, err)

	d := dice(t, bs1, bs2)
	assert.Less(t, d, 0.30, "Dice(John, Maria) = %.4f, expected < 0.30", d)
}

func TestTokenizeRecordBloom_MissingOptionalZeroFilter(t *testing.T) {
	t.Parallel()
	tok := newTok(t, "secret")
	fs := bloomFS(
		sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0},
		sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: false, Weight: 0.5},
	)

	tr, err := tok.TokenizeRecordBloom(sriracha.RawRecord{sriracha.FieldNameGiven: "John"}, fs)
	require.NoError(t, err)
	require.Len(t, tr.Payload, 2*128, "expected %d bytes", 2*128)
	for _, b := range tr.Payload[128:] {
		if b != 0 {
			assert.Fail(t, "missing optional field should produce all-zero filter")
			break
		}
	}
}

func TestTokenizeRecordBloom_PayloadLength(t *testing.T) {
	t.Parallel()
	tok := newTok(t, "secret")
	fs := bloomFS(
		sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0},
		sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: true, Weight: 1.0},
	)
	rec := sriracha.RawRecord{
		sriracha.FieldNameGiven:  "John",
		sriracha.FieldNameFamily: "Doe",
	}

	tr, err := tok.TokenizeRecordBloom(rec, fs)
	require.NoError(t, err)
	assert.Len(t, tr.Payload, 256, "expected 256 bytes for 2-field bloom")
}

func TestNew_ErrorOnEmptySecret(t *testing.T) {
	t.Parallel()
	_, err := New(nil)
	assert.Error(t, err, "expected error for nil secret")
	_, err = New([]byte{})
	assert.Error(t, err, "expected error for empty secret")
}

func TestNgrams(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		input string
		sizes []int
		want  []string
	}{
		{
			name:  "Single",
			input: "ab",
			sizes: []int{2},
			want:  []string{"ab"},
		},
		{
			name:  "ShorterThanMinSize",
			input: "a",
			sizes: []int{2},
			want:  []string{},
		},
		{
			name:  "Unicode",
			input: "\u03b1\u03b2\u03b3",
			sizes: []int{2},
			want:  []string{"\u03b1\u03b2", "\u03b2\u03b3"},
		},
		{
			name:  "Empty",
			input: "",
			sizes: []int{2},
			want:  []string{},
		},
		{
			name:  "EmptySizes",
			input: "abc",
			sizes: []int{},
			want:  []string{},
		},
		{
			name: "DescendingOrder",
			// sizes[0]=3 > sizes[1]=2 — exercises the minSize update branch.
			input: "ab",
			sizes: []int{3, 2},
			want:  []string{"ab"},
		},
		{
			name:  "MultipleSizes",
			input: "abc",
			sizes: []int{2, 3},
			want:  []string{"ab", "bc", "abc"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ngrams(tc.input, tc.sizes)
			if len(tc.want) == 0 {
				assert.Empty(t, got)
			} else {
				assert.Equal(t, tc.want, got)
			}
		})
	}
}
