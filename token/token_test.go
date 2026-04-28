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

func TestNew(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		secret  []byte
		wantErr bool
	}{
		{"NilSecret", nil, true},
		{"EmptySecret", []byte{}, true},
		{"ValidSecret", []byte("secret"), false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tok, err := New(tc.secret)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			tok.Destroy()
		})
	}
}

func TestTokenizeRecord(t *testing.T) {
	t.Parallel()
	givenSpec := sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0}
	familySpec := sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: true, Weight: 1.0}
	givenOptional := sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: false, Weight: 1.0}
	familyOptional := sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: false, Weight: 0.5}

	cases := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "Idempotent",
			run: func(t *testing.T) {
				tok := newTok(t, "secret")
				rec := sriracha.RawRecord{sriracha.FieldNameGiven: "John"}
				fs := deterministicFS(givenSpec)

				tr1, err := tok.TokenizeRecord(rec, fs)
				require.NoError(t, err)
				tr2, err := tok.TokenizeRecord(rec, fs)
				require.NoError(t, err)
				assert.True(t, Equal(tr1, tr2), "identical inputs should produce equal tokens")
			},
		},
		{
			name: "CrossFieldIsolation",
			run: func(t *testing.T) {
				tok := newTok(t, "secret")
				rec := sriracha.RawRecord{
					sriracha.FieldNameGiven:  "John",
					sriracha.FieldNameFamily: "John",
				}
				tr, err := tok.TokenizeRecord(rec, deterministicFS(givenSpec, familySpec))
				require.NoError(t, err)
				require.Len(t, tr.Fields, 2)
				assert.Len(t, tr.Fields[0], 32, "expected 32-byte HMAC for given name")
				assert.Len(t, tr.Fields[1], 32, "expected 32-byte HMAC for family name")
				assert.NotEqual(t, tr.Fields[0], tr.Fields[1], "same value with different paths should differ")
			},
		},
		{
			name: "DifferentSecret",
			run: func(t *testing.T) {
				rec := sriracha.RawRecord{sriracha.FieldNameGiven: "John"}
				fs := deterministicFS(givenSpec)

				tr1, err := newTok(t, "secret-a").TokenizeRecord(rec, fs)
				require.NoError(t, err)
				tr2, err := newTok(t, "secret-b").TokenizeRecord(rec, fs)
				require.NoError(t, err)
				assert.False(t, Equal(tr1, tr2), "different secrets should produce different tokens")
			},
		},
		{
			name: "MissingRequired",
			run: func(t *testing.T) {
				tok := newTok(t, "secret")
				_, err := tok.TokenizeRecord(sriracha.RawRecord{}, deterministicFS(givenSpec))
				assert.Error(t, err)
			},
		},
		{
			name: "MissingOptionalNilEntry",
			run: func(t *testing.T) {
				tok := newTok(t, "secret")
				rec := sriracha.RawRecord{sriracha.FieldNameGiven: "John"}
				tr, err := tok.TokenizeRecord(rec, deterministicFS(givenSpec, familyOptional))
				require.NoError(t, err)
				require.Len(t, tr.Fields, 2)
				assert.Len(t, tr.Fields[0], 32, "present field should have 32-byte HMAC")
				assert.Nil(t, tr.Fields[1], "absent optional field should be nil")
			},
		},
		{
			name: "EmptyAllOptional",
			run: func(t *testing.T) {
				tok := newTok(t, "secret")
				tr, err := tok.TokenizeRecord(sriracha.RawRecord{}, deterministicFS(givenOptional, familyOptional))
				require.NoError(t, err)
				require.Len(t, tr.Fields, 2)
				assert.Nil(t, tr.Fields[0])
				assert.Nil(t, tr.Fields[1])
			},
		},
		{
			name: "NormalizationError",
			run: func(t *testing.T) {
				tok := newTok(t, "secret")
				rec := sriracha.RawRecord{sriracha.FieldDateBirth: "not-a-date"}
				fs := deterministicFS(sriracha.FieldSpec{Path: sriracha.FieldDateBirth, Required: true, Weight: 1.0})
				_, err := tok.TokenizeRecord(rec, fs)
				assert.Error(t, err)
			},
		},
		{
			name: "VersionPropagated",
			run: func(t *testing.T) {
				tok := newTok(t, "secret")
				fs := deterministicFS(givenSpec)
				tr, err := tok.TokenizeRecord(sriracha.RawRecord{sriracha.FieldNameGiven: "John"}, fs)
				require.NoError(t, err)
				assert.Equal(t, fs.Version, tr.FieldSetVersion)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func TestTokenizeRecordBloom(t *testing.T) {
	t.Parallel()
	givenSpec := sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0}
	familySpec := sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: true, Weight: 1.0}
	familyOptional := sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: false, Weight: 0.5}

	cases := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "MissingRequired",
			run: func(t *testing.T) {
				tok := newTok(t, "secret")
				_, err := tok.TokenizeRecordBloom(sriracha.RawRecord{}, bloomFS(givenSpec))
				assert.Error(t, err)
			},
		},
		{
			name: "NormalizationError",
			run: func(t *testing.T) {
				tok := newTok(t, "secret")
				rec := sriracha.RawRecord{sriracha.FieldDateBirth: "not-a-date"}
				fs := bloomFS(sriracha.FieldSpec{Path: sriracha.FieldDateBirth, Required: true, Weight: 1.0})
				_, err := tok.TokenizeRecordBloom(rec, fs)
				assert.Error(t, err)
			},
		},
		{
			name: "MissingOptionalZeroFilter",
			run: func(t *testing.T) {
				tok := newTok(t, "secret")
				fs := bloomFS(givenSpec, familyOptional)
				tr, err := tok.TokenizeRecordBloom(sriracha.RawRecord{sriracha.FieldNameGiven: "John"}, fs)
				require.NoError(t, err)
				require.Len(t, tr.Fields, 2)
				assert.Equal(t, make([]byte, 128), tr.Fields[1], "absent optional field should be all-zero filter")
			},
		},
		{
			name: "FieldLayoutAndMetadata",
			run: func(t *testing.T) {
				tok := newTok(t, "secret")
				fs := bloomFS(givenSpec, familySpec)
				rec := sriracha.RawRecord{
					sriracha.FieldNameGiven:  "John",
					sriracha.FieldNameFamily: "Doe",
				}
				tr, err := tok.TokenizeRecordBloom(rec, fs)
				require.NoError(t, err)
				require.Len(t, tr.Fields, 2, "expected one filter per FieldSet entry")
				assert.Len(t, tr.Fields[0], 128, "expected 128 bytes per 1024-bit filter")
				assert.Len(t, tr.Fields[1], 128, "expected 128 bytes per 1024-bit filter")
				assert.Equal(t, fs.BloomParams, tr.BloomParams)
				assert.Equal(t, fs.Version, tr.FieldSetVersion)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
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
		// Skip out-of-domain sizes; only positive, bounded sizes are valid input.
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
		// Skip inputs that legitimately fail tokenization (e.g. invalid normalization).
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
		// Skip inputs that legitimately fail tokenization (e.g. invalid normalization).
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
			// A token compared against itself scores 1 (any bits set) or 0
			// (all-zero filter). Anything else indicates a bug.
			if s != 0 && s != 1 {
				t.Fatalf("DicePerField self-comparison field %d = %v, want 0 or 1", i, s)
			}
		}
	})
}
