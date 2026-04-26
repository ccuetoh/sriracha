package token

import (
	"crypto/sha256"
	"encoding/binary"
	"testing"

	"github.com/bits-and-blooms/bitset"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.sriracha.dev/sriracha"
)

// filterFromBytes deserialises a Bloom filter payload into a BitSet.
func filterFromBytes(data []byte) *bitset.BitSet {
	nwords := len(data) / 8
	words := make([]uint64, nwords)
	for i := range nwords {
		words[i] = binary.LittleEndian.Uint64(data[i*8:])
	}
	return bitset.From(words)
}

// dice computes the Sorensen-Dice coefficient of two BitSets.
func dice(t *testing.T, a, b *bitset.BitSet) float64 {
	t.Helper()
	popInter := int(a.IntersectionCardinality(b)) //nolint:gosec // G115: bitset cardinality never exceeds math.MaxInt
	total := int(a.Count()) + int(b.Count())      //nolint:gosec // G115: bitset cardinality never exceeds math.MaxInt
	if total == 0 {
		return 0
	}
	return 2.0 * float64(popInter) / float64(total)
}

func newTok(t *testing.T, secret string) *Tokenizer {
	t.Helper()
	tok, err := New([]byte(secret))
	require.NoErrorf(t, err, "New(%q)", secret)
	t.Cleanup(tok.Destroy)
	return tok
}

func deterministicFS(fields ...sriracha.FieldSpec) sriracha.FieldSet {
	return sriracha.FieldSet{
		Version: "1.0.0-test",
		Fields:  fields,
	}
}

func bloomFS(fields ...sriracha.FieldSpec) sriracha.FieldSet {
	return sriracha.FieldSet{
		Version: "1.0.0-test",
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

func TestValidateTokenRecord(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		tamper  bool
		wantErr bool
	}{
		{"valid token", false, false},
		{"tampered payload", true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tok := newTok(t, "secret")
			rec := sriracha.RawRecord{sriracha.FieldNameGiven: "John"}
			fs := deterministicFS(sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0})
			tr, err := tok.TokenizeRecord(rec, fs)
			require.NoError(t, err)
			if tc.tamper {
				tr.Payload[0] ^= 0xff
			}
			if tc.wantErr {
				assert.Error(t, ValidateTokenRecord(tr), "expected checksum mismatch error for tampered payload")
			} else {
				assert.NoError(t, ValidateTokenRecord(tr), "expected nil error for valid token")
			}
		})
	}
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

func TestTokenizeRecordBloom_MissingRequired(t *testing.T) {
	t.Parallel()
	tok := newTok(t, "secret")
	fs := bloomFS(sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0})
	_, err := tok.TokenizeRecordBloom(sriracha.RawRecord{}, fs)
	assert.Error(t, err, "expected error for missing required field")
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
// "Christopher" vs "Cristopher" tests the similar-names property with more ngrams.
func TestTokenizeRecordBloom_NameSimilarity(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		nameA     string
		nameB     string
		wantAbove bool
		threshold float64
	}{
		{"similar names (typo)", "Christopher", "Cristopher", true, 0.80},
		{"dissimilar names", "John", "Maria", false, 0.30},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tok := newTok(t, "secret")
			fs := bloomFS(sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0})

			tr1, err := tok.TokenizeRecordBloom(sriracha.RawRecord{sriracha.FieldNameGiven: tc.nameA}, fs)
			require.NoError(t, err)
			tr2, err := tok.TokenizeRecordBloom(sriracha.RawRecord{sriracha.FieldNameGiven: tc.nameB}, fs)
			require.NoError(t, err)

			bs1 := filterFromBytes(tr1.Payload)
			bs2 := filterFromBytes(tr2.Payload)

			d := dice(t, bs1, bs2)
			if tc.wantAbove {
				assert.Greater(t, d, tc.threshold, "Dice(%s, %s) = %.4f, expected > %.2f", tc.nameA, tc.nameB, d, tc.threshold)
			} else {
				assert.Less(t, d, tc.threshold, "Dice(%s, %s) = %.4f, expected < %.2f", tc.nameA, tc.nameB, d, tc.threshold)
			}
		})
	}
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

func TestTokenizer_Destroy(t *testing.T) {
	t.Parallel()
	tok := newTok(t, "secret")
	tok.Destroy()
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

func BenchmarkTokenizeRecord(b *testing.B) {
	tok, _ := New([]byte("bench-secret-32-bytes-long!!!!!"))
	rec := sriracha.RawRecord{
		sriracha.FieldNameGiven:    "Alice",
		sriracha.FieldNameFamily:   "Smith",
		sriracha.FieldDateBirth:    "1990-05-15",
		sriracha.FieldContactEmail: "alice@example.com",
	}
	fs := deterministicFS(
		sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: false, Weight: 2.0},
		sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: false, Weight: 2.5},
		sriracha.FieldSpec{Path: sriracha.FieldDateBirth, Required: false, Weight: 2.0},
		sriracha.FieldSpec{Path: sriracha.FieldContactEmail, Required: false, Weight: 2.0},
	)
	b.ResetTimer()
	for range b.N {
		_, _ = tok.TokenizeRecord(rec, fs)
	}
}

func BenchmarkTokenizeRecordBloom(b *testing.B) {
	tok, _ := New([]byte("bench-secret-32-bytes-long!!!!!"))
	rec := sriracha.RawRecord{
		sriracha.FieldNameGiven:    "Alice",
		sriracha.FieldNameFamily:   "Smith",
		sriracha.FieldDateBirth:    "1990-05-15",
		sriracha.FieldContactEmail: "alice@example.com",
	}
	fs := bloomFS(
		sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: false, Weight: 2.0},
		sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: false, Weight: 2.5},
		sriracha.FieldSpec{Path: sriracha.FieldDateBirth, Required: false, Weight: 2.0},
		sriracha.FieldSpec{Path: sriracha.FieldContactEmail, Required: false, Weight: 2.0},
	)
	b.ResetTimer()
	for range b.N {
		_, _ = tok.TokenizeRecordBloom(rec, fs)
	}
}

func BenchmarkNgrams(b *testing.B) {
	sizes := []int{2, 3}
	input := "Christopher"
	b.ResetTimer()
	for range b.N {
		_ = ngrams(input, sizes)
	}
}

func BenchmarkValidateTokenRecord(b *testing.B) {
	tok, _ := New([]byte("bench-secret"))
	rec := sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"}
	fs := deterministicFS(sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0})
	tr, _ := tok.TokenizeRecord(rec, fs)
	b.ResetTimer()
	for range b.N {
		_ = ValidateTokenRecord(tr)
	}
}

// FuzzNgrams verifies that ngrams never panics and that every returned gram
// has the correct rune length.
func FuzzNgrams(f *testing.F) {
	f.Add("hello", 2)
	f.Add("", 3)
	f.Add("\u03b1\u03b2\u03b3", 2)
	f.Add("a", 1)

	f.Fuzz(func(t *testing.T, s string, size int) {
		if size <= 0 || size > 20 {
			return
		}
		grams := ngrams(s, []int{size})
		runes := []rune(s)
		for _, g := range grams {
			gr := []rune(g)
			if len(gr) != size {
				t.Fatalf("ngrams(%q, [%d]): got gram %q with len %d, want %d", s, size, g, len(gr), size)
			}
		}
		// Count must match expected sliding-window count.
		n := len(runes)
		want := 0
		if n >= size {
			want = n - size + 1
		}
		if len(grams) != want {
			t.Fatalf("ngrams(%q, [%d]): got %d grams, want %d", s, size, len(grams), want)
		}
	})
}

// FuzzTokenizeRecord verifies that TokenizeRecord never panics for arbitrary
// field values and that ValidateTokenRecord always accepts its own output.
func FuzzTokenizeRecord(f *testing.F) {
	f.Add("Alice", "Smith")
	f.Add("", "")
	f.Add("\x00", "\xff")

	fs := deterministicFS(
		sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: false, Weight: 1.0},
		sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: false, Weight: 1.0},
	)
	tok, _ := New([]byte("fuzz-secret"))

	f.Fuzz(func(t *testing.T, given, family string) {
		rec := sriracha.RawRecord{
			sriracha.FieldNameGiven:  given,
			sriracha.FieldNameFamily: family,
		}
		tr, err := tok.TokenizeRecord(rec, fs)
		if err != nil {
			return
		}
		if err := ValidateTokenRecord(tr); err != nil {
			t.Fatalf("ValidateTokenRecord rejected its own output: %v", err)
		}
	})
}

// FuzzTokenizeRecordBloom verifies that TokenizeRecordBloom never panics for
// arbitrary field values and that its checksum is always valid.
func FuzzTokenizeRecordBloom(f *testing.F) {
	f.Add("Alice", "Smith")
	f.Add("", "")
	f.Add("Christopher", "Jones")

	fs := bloomFS(
		sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: false, Weight: 1.0},
		sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: false, Weight: 1.0},
	)
	tok, _ := New([]byte("fuzz-secret"))

	f.Fuzz(func(t *testing.T, given, family string) {
		rec := sriracha.RawRecord{
			sriracha.FieldNameGiven:  given,
			sriracha.FieldNameFamily: family,
		}
		tr, err := tok.TokenizeRecordBloom(rec, fs)
		if err != nil {
			return
		}
		if err := ValidateTokenRecord(tr); err != nil {
			t.Fatalf("ValidateTokenRecord rejected TokenizeRecordBloom output: %v", err)
		}
		// Payload must be exactly 2 × fieldFilterBytes long.
		expectedBytes := 2 * int((fs.BloomParams.SizeBits+63)/64*8)
		if len(tr.Payload) != expectedBytes {
			t.Fatalf("payload length %d, want %d", len(tr.Payload), expectedBytes)
		}
	})
}
