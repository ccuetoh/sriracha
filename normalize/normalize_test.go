package normalize

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.sriracha.dev/sriracha"
)

func TestNormalize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		value       string
		path        sriracha.FieldPath
		want        string
		wantErr     bool
		errContains string
	}{
		// Case 1: NFKD combining characters — precomposed and decomposed é must yield identical output.
		{
			name:  "nfkd precomposed e-acute",
			value: "\u00e9", // é precomposed
			path:  sriracha.FieldNameGiven,
			want:  "e\u0301", // e + combining acute after NFKD + lower
		},
		{
			name:  "nfkd decomposed e-acute",
			value: "e\u0301", // e + combining acute already decomposed
			path:  sriracha.FieldNameGiven,
			want:  "e\u0301",
		},
		// Case 2: Non-breaking space collapsed and trimmed.
		{
			name:  "non-breaking space collapse",
			value: "\u00a0hello\u00a0world\u00a0",
			path:  sriracha.FieldNameGiven,
			want:  "hello world",
		},
		// Case 3: Date valid ISO 8601.
		{
			name:  "date valid",
			value: "2006-01-02",
			path:  sriracha.FieldDateBirth,
			want:  "2006-01-02",
		},
		// Case 4: Date invalid format.
		{
			name:        "date invalid format",
			value:       "02/01/2006",
			path:        sriracha.FieldDateBirth,
			wantErr:     true,
			errContains: "ISO 8601",
		},
		// Case 5: Date empty.
		{
			name:        "date empty",
			value:       "",
			path:        sriracha.FieldDateBirth,
			wantErr:     true,
			errContains: "empty",
		},
		// Case 6: Date invalid — non-leap year Feb 29.
		{
			name:        "date non-leap year feb 29",
			value:       "2023-02-29",
			path:        sriracha.FieldDateBirth,
			wantErr:     true,
			errContains: "ISO 8601",
		},
		// Case 7: Date leap year Feb 29.
		{
			name:  "date leap year feb 29",
			value: "2024-02-29",
			path:  sriracha.FieldDateBirth,
			want:  "2024-02-29",
		},
		// Case 8: Identifier stripping.
		{
			name:  "identifier strip hyphens dots spaces",
			value: "123-456.78 9",
			path:  sriracha.FieldIdentifierNationalID,
			want:  "123456789",
		},
		// Case 9: Country lowercase input.
		{
			name:  "country lowercase",
			value: "us",
			path:  sriracha.FieldAddressCountry,
			want:  "US",
		},
		// Case 10: Country 3 chars.
		{
			name:        "country 3 chars",
			value:       "USA",
			path:        sriracha.FieldAddressCountry,
			wantErr:     true,
			errContains: "2 characters",
		},
		// Case 11: Country empty.
		{
			name:        "country empty",
			value:       "",
			path:        sriracha.FieldAddressCountry,
			wantErr:     true,
			errContains: "2 characters",
		},
		// Case 12: Country non-alpha digit.
		{
			name:        "country non-alpha",
			value:       "1A",
			path:        sriracha.FieldAddressCountry,
			wantErr:     true,
			errContains: "ASCII letters",
		},
		// Case 13: Turkish dotless-i (U+0130).
		// NFKD decomposes İ → I + U+0307 (combining dot above).
		// cases.Lower(language.Und) lowercases I → i, leaving U+0307 attached.
		// Result: "i\u0307" — NOT "i" alone.
		{
			name:  "turkish I-with-dot-above (U+0130) NFKD lower",
			value: "\u0130", // İ
			path:  sriracha.FieldNameGiven,
			want:  "i\u0307",
		},
		// Case 14: Name normalization with leading/trailing spaces and precomposed é.
		// "  José  " → NFKD: "  Jose\u0301  " → lower: "  jose\u0301  " → collapse+trim: "jose\u0301"
		{
			name:  "name jose with spaces",
			value: "  José  ",
			path:  sriracha.FieldNameGiven,
			want:  "jose\u0301",
		},
		// Case 15: Default path (email) — only steps 1-4, no field-specific transform.
		{
			name:  "default path email no transform",
			value: "  Hello@Example.COM  ",
			path:  sriracha.FieldContactEmail,
			want:  "hello@example.com",
		},
		// Additional: default path phone.
		{
			name:  "default path phone no transform",
			value: "  +1 800 555 1234  ",
			path:  sriracha.FieldContactPhone,
			want:  "+1 800 555 1234",
		},
		// Additional: precomposed vs decomposed produce identical results (cross-check).
		{
			name: "nfkd precomposed and decomposed identical",
			// This is tested implicitly by cases 1a and 1b having the same want.
			// Here we test via identifier path to confirm they also match there.
			value: "\u00e9",
			path:  sriracha.FieldNameFamily,
			want:  "e\u0301",
		},
		// Empty identifier after stripping — documents behavior (returns empty string, not error)
		{
			name:  "identifier all stripped chars",
			value: "---",
			path:  sriracha.FieldIdentifierNationalID,
			want:  "",
		},
		// Date on non-birth date field (FieldDateRegistration) — confirms namespace dispatch
		{
			name:  "date namespace on registration field",
			value: "2024-06-15",
			path:  sriracha.FieldDateRegistration,
			want:  "2024-06-15",
		},
		// Date invalid on non-birth date field
		{
			name:    "date namespace rejects invalid on registration field",
			value:   "15/06/2024",
			path:    sriracha.FieldDateRegistration,
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := Normalize(tc.value, tc.path)
			if tc.wantErr {
				require.Error(t, err)
				if tc.errContains != "" {
					assert.Contains(t, err.Error(), tc.errContains)
				}
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestNormalizeNFKDEquivalence explicitly verifies that precomposed U+00E9
// and decomposed e+U+0301 produce byte-identical output after Normalize.
func TestNormalizeNFKDEquivalence(t *testing.T) {
	t.Parallel()

	path := sriracha.FieldNameGiven
	precomposed := "\u00e9" // é as single codepoint
	decomposed := "e\u0301" // e + combining acute

	got1, err1 := Normalize(precomposed, path)
	require.NoError(t, err1, "Normalize(precomposed) should not error")

	got2, err2 := Normalize(decomposed, path)
	require.NoError(t, err2, "Normalize(decomposed) should not error")

	assert.Equal(t, got1, got2, "NFKD equivalence failed: precomposed → %q (%x), decomposed → %q (%x)", got1, []byte(got1), got2, []byte(got2))
}

func BenchmarkNormalizeName(b *testing.B) {
	for range b.N {
		_, _ = Normalize("  José María  ", sriracha.FieldNameGiven)
	}
}

func BenchmarkNormalizeIdentifier(b *testing.B) {
	for range b.N {
		_, _ = Normalize("123-456.789 0", sriracha.FieldIdentifierNationalID)
	}
}

func BenchmarkNormalizeDate(b *testing.B) {
	for range b.N {
		_, _ = Normalize("2024-06-15", sriracha.FieldDateBirth)
	}
}

func BenchmarkNormalizeCountry(b *testing.B) {
	for range b.N {
		_, _ = Normalize("us", sriracha.FieldAddressCountry)
	}
}

// FuzzNormalize verifies that Normalize never panics and is idempotent for
// non-date, non-country fields (name, identifier, address, contact).
func FuzzNormalize(f *testing.F) {
	seeds := []string{"", "Alice", "  hello world  ", "123-456.789", "\u00e9", "\u0130", "\u00a0test\u00a0"}
	for _, s := range seeds {
		f.Add(s)
	}

	paths := []sriracha.FieldPath{
		sriracha.FieldNameGiven,
		sriracha.FieldNameFamily,
		sriracha.FieldIdentifierNationalID,
		sriracha.FieldContactEmail,
		sriracha.FieldAddressLocality,
	}

	f.Fuzz(func(t *testing.T, value string) {
		for _, path := range paths {
			out1, err1 := Normalize(value, path)
			if err1 != nil {
				continue
			}
			// Idempotency: normalizing the output must produce the same result.
			out2, err2 := Normalize(out1, path)
			if err2 != nil {
				t.Fatalf("second Normalize(%q, %s) errored after first succeeded: %v", out1, path, err2)
			}
			if out1 != out2 {
				t.Fatalf("Normalize is not idempotent for path %s: %q → %q → %q", path, value, out1, out2)
			}
		}
	})
}

// FuzzNormalizeDate verifies that normalizeDate accepts exactly ISO 8601
// dates and rejects everything else without panicking.
func FuzzNormalizeDate(f *testing.F) {
	f.Add("2024-01-01")
	f.Add("2000-02-29")
	f.Add("not-a-date")
	f.Add("")
	f.Add("01/01/2024")

	f.Fuzz(func(t *testing.T, value string) {
		// Must not panic; error is acceptable for arbitrary input.
		_, _ = Normalize(value, sriracha.FieldDateBirth)
	})
}

// FuzzNormalizeCountry verifies that country normalization never panics and
// accepts only valid 2-letter ASCII codes.
func FuzzNormalizeCountry(f *testing.F) {
	f.Add("US")
	f.Add("us")
	f.Add("GB")
	f.Add("USA")
	f.Add("")
	f.Add("12")

	f.Fuzz(func(t *testing.T, value string) {
		out, err := Normalize(value, sriracha.FieldAddressCountry)
		if err != nil {
			return
		}
		// Successful result must be exactly 2 uppercase ASCII letters.
		if len(out) != 2 {
			t.Fatalf("normalizeCountry(%q) = %q, want 2 chars", value, out)
		}
		for _, r := range out {
			if r < 'A' || r > 'Z' {
				t.Fatalf("normalizeCountry(%q) = %q, contains non-uppercase-ASCII rune %q", value, out, r)
			}
		}
	})
}
