package token

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.sriracha.dev/sriracha"
)

func newTok(t *testing.T, secret string) Tokenizer {
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
	assert.True(t, Equal(tr1, tr2), "idempotency: tokens differ for identical input")
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
	require.Len(t, tr.Fields, 2, "expected 2 field entries")
	require.Len(t, tr.Fields[0], 32, "expected 32-byte HMAC for given name")
	require.Len(t, tr.Fields[1], 32, "expected 32-byte HMAC for family name")
	assert.NotEqual(t, tr.Fields[0], tr.Fields[1], "cross-field isolation: same value with different paths produced identical tokens")
}

func TestTokenizeRecord_DifferentSecret(t *testing.T) {
	t.Parallel()
	rec := sriracha.RawRecord{sriracha.FieldNameGiven: "John"}
	fs := deterministicFS(sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0})

	tr1, err := newTok(t, "secret-a").TokenizeRecord(rec, fs)
	require.NoError(t, err)
	tr2, err := newTok(t, "secret-b").TokenizeRecord(rec, fs)
	require.NoError(t, err)
	assert.False(t, Equal(tr1, tr2), "different secrets produced identical tokens")
}

func TestTokenizeRecord_MissingRequired(t *testing.T) {
	t.Parallel()
	tok := newTok(t, "secret")
	fs := deterministicFS(sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0})

	_, err := tok.TokenizeRecord(sriracha.RawRecord{}, fs)
	assert.Error(t, err, "expected error for missing required field")
}

func TestTokenizeRecord_MissingOptionalNilEntry(t *testing.T) {
	t.Parallel()
	tok := newTok(t, "secret")
	rec := sriracha.RawRecord{sriracha.FieldNameGiven: "John"}
	fs := deterministicFS(
		sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0},
		sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: false, Weight: 0.5},
	)

	tr, err := tok.TokenizeRecord(rec, fs)
	require.NoError(t, err)
	require.Len(t, tr.Fields, 2, "expected positional alignment with FieldSet")
	assert.Len(t, tr.Fields[0], 32, "present field should have 32-byte HMAC")
	assert.Nil(t, tr.Fields[1], "absent optional field should be nil")
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
	require.Len(t, tr.Fields, 2)
	assert.Nil(t, tr.Fields[0], "absent optional field should be nil")
	assert.Nil(t, tr.Fields[1], "absent optional field should be nil")
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

func TestTokenizeRecord_VersionPropagated(t *testing.T) {
	t.Parallel()
	tok := newTok(t, "secret")
	fs := deterministicFS(sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0})
	tr, err := tok.TokenizeRecord(sriracha.RawRecord{sriracha.FieldNameGiven: "John"}, fs)
	require.NoError(t, err)
	assert.Equal(t, fs.Version, tr.FieldSetVersion, "FieldSetVersion should be carried on the token")
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

// "John" vs "Jon" yields very few bigrams/trigrams and unreliable Dice scores,
// so this case uses "Christopher" vs "Cristopher" to exercise typo similarity
// with a meaningful number of ngrams.
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

			scores, err := DicePerField(tr1, tr2)
			require.NoError(t, err)
			require.Len(t, scores, 1)
			d := scores[0]
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
	require.Len(t, tr.Fields, 2)
	require.Len(t, tr.Fields[1], 128, "absent optional field should have full-length zero filter")
	for _, b := range tr.Fields[1] {
		if b != 0 {
			assert.Fail(t, "missing optional field should produce all-zero filter")
			break
		}
	}
}

func TestTokenizeRecordBloom_FieldLayout(t *testing.T) {
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
	require.Len(t, tr.Fields, 2, "expected one filter per FieldSet entry")
	assert.Len(t, tr.Fields[0], 128, "expected 128 bytes per 1024-bit filter")
	assert.Len(t, tr.Fields[1], 128, "expected 128 bytes per 1024-bit filter")
	assert.Equal(t, fs.BloomParams, tr.BloomParams, "BloomParams should be carried on the token")
	assert.Equal(t, fs.Version, tr.FieldSetVersion, "FieldSetVersion should be carried on the token")
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
			input: "αβγ",
			sizes: []int{2},
			want:  []string{"αβ", "βγ"},
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

// FuzzNgrams verifies that ngrams never panics and that every returned gram
// has the correct rune length.
func FuzzNgrams(f *testing.F) {
	f.Add("hello", 2)
	f.Add("", 3)
	f.Add("αβγ", 2)
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
// field values and that its output is self-consistent under Equal.
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
		tr1, err := tok.TokenizeRecord(rec, fs)
		if err != nil {
			return
		}
		tr2, err := tok.TokenizeRecord(rec, fs)
		if err != nil {
			t.Fatalf("second TokenizeRecord call failed: %v", err)
		}
		if !Equal(tr1, tr2) {
			t.Fatalf("Equal returned false for identical inputs")
		}
	})
}

// FuzzTokenizeRecordBloom verifies that TokenizeRecordBloom never panics for
// arbitrary field values, that its layout is positional (one filter per field
// of the FieldSet), and that DicePerField scores a token against itself at 1.0.
func FuzzTokenizeRecordBloom(f *testing.F) {
	f.Add("Alice", "Smith")
	f.Add("", "")
	f.Add("Christopher", "Jones")

	fs := bloomFS(
		sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: false, Weight: 1.0},
		sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: false, Weight: 1.0},
	)
	tok, _ := New([]byte("fuzz-secret"))
	fieldFilterBytes := int((fs.BloomParams.SizeBits + 63) / 64 * 8)

	f.Fuzz(func(t *testing.T, given, family string) {
		rec := sriracha.RawRecord{
			sriracha.FieldNameGiven:  given,
			sriracha.FieldNameFamily: family,
		}
		tr, err := tok.TokenizeRecordBloom(rec, fs)
		if err != nil {
			return
		}
		if len(tr.Fields) != len(fs.Fields) {
			t.Fatalf("Fields length %d, want %d", len(tr.Fields), len(fs.Fields))
		}
		for i, f := range tr.Fields {
			if len(f) != fieldFilterBytes {
				t.Fatalf("field %d byte length %d, want %d", i, len(f), fieldFilterBytes)
			}
		}
		scores, err := DicePerField(tr, tr)
		if err != nil {
			t.Fatalf("DicePerField against self: %v", err)
		}
		for i, s := range scores {
			// A token compared against itself should score either 1 (any bits set)
			// or 0 (all-zero filter). Anything else indicates a bug.
			if s != 0 && s != 1 {
				t.Fatalf("DicePerField self-comparison field %d = %v, want 0 or 1", i, s)
			}
		}
	})
}
