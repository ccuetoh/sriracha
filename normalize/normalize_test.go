package normalize

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.sriracha.dev/sriracha"
)

type normalizeCase struct {
	name        string
	value       string
	path        sriracha.FieldPath
	want        string
	wantErr     bool
	errContains string
}

func runNormalizeCases(t *testing.T, cases []normalizeCase) {
	t.Helper()
	for _, tc := range cases {
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

// TestNormalize_Unicode covers NFKD decomposition, lowercasing, and whitespace
// handling — the steps that run for every field path.
func TestNormalize_Unicode(t *testing.T) {
	t.Parallel()
	runNormalizeCases(t, []normalizeCase{
		{
			name:  "NFKDPrecomposedEAcute",
			value: "é", // é precomposed
			path:  sriracha.FieldNameGiven,
			want:  "é", // e + combining acute after NFKD + lower
		},
		{
			name:  "NFKDDecomposedEAcute",
			value: "é", // already decomposed
			path:  sriracha.FieldNameGiven,
			want:  "é",
		},
		{
			name:  "NonBreakingSpaceCollapse",
			value: " hello world ",
			path:  sriracha.FieldNameGiven,
			want:  "hello world",
		},
		{
			// NFKD decomposes U+0130 (İ) → I + U+0307 (combining dot above);
			// lowercasing then yields "i" + U+0307, not bare "i". This documents
			// the language-independent fold (no Turkish-aware special-casing).
			name:  "TurkishIWithDotAbove",
			value: "İ",
			path:  sriracha.FieldNameGiven,
			want:  "i̇",
		},
		{
			name:  "NameWithSpacesAndAccent",
			value: "  José  ",
			path:  sriracha.FieldNameGiven,
			want:  "josé",
		},
	})
}

// TestNormalize_Date covers the YYYY-MM-DD validator that runs for any date
// namespace path.
func TestNormalize_Date(t *testing.T) {
	t.Parallel()
	runNormalizeCases(t, []normalizeCase{
		{
			name:  "ValidISO",
			value: "2006-01-02",
			path:  sriracha.FieldDateBirth,
			want:  "2006-01-02",
		},
		{
			name:        "InvalidFormat",
			value:       "02/01/2006",
			path:        sriracha.FieldDateBirth,
			wantErr:     true,
			errContains: "ISO 8601",
		},
		{
			name:        "Empty",
			value:       "",
			path:        sriracha.FieldDateBirth,
			wantErr:     true,
			errContains: "empty",
		},
		{
			name:        "NonLeapFeb29",
			value:       "2023-02-29",
			path:        sriracha.FieldDateBirth,
			wantErr:     true,
			errContains: "ISO 8601",
		},
		{
			name:  "LeapFeb29",
			value: "2024-02-29",
			path:  sriracha.FieldDateBirth,
			want:  "2024-02-29",
		},
		{
			name:  "RegistrationNamespaceValid",
			value: "2024-06-15",
			path:  sriracha.FieldDateRegistration,
			want:  "2024-06-15",
		},
		{
			name:    "RegistrationNamespaceInvalid",
			value:   "15/06/2024",
			path:    sriracha.FieldDateRegistration,
			wantErr: true,
		},
	})
}

// TestNormalize_Identifier covers separator stripping for identifier paths.
func TestNormalize_Identifier(t *testing.T) {
	t.Parallel()
	runNormalizeCases(t, []normalizeCase{
		{
			name:  "StripHyphensDotsSpaces",
			value: "123-456.78 9",
			path:  sriracha.FieldIdentifierNationalID,
			want:  "123456789",
		},
		{
			// Documents that an all-separator value reduces to "" (not an error).
			name:  "AllStrippedChars",
			value: "---",
			path:  sriracha.FieldIdentifierNationalID,
			want:  "",
		},
	})
}

// TestNormalize_Country covers the ISO 3166-1 alpha-2 validator on the country
// field.
func TestNormalize_Country(t *testing.T) {
	t.Parallel()
	runNormalizeCases(t, []normalizeCase{
		{
			name:  "LowercaseUpcased",
			value: "us",
			path:  sriracha.FieldAddressCountry,
			want:  "US",
		},
		{
			name:        "ThreeCharsRejected",
			value:       "USA",
			path:        sriracha.FieldAddressCountry,
			wantErr:     true,
			errContains: "2 characters",
		},
		{
			name:        "EmptyRejected",
			value:       "",
			path:        sriracha.FieldAddressCountry,
			wantErr:     true,
			errContains: "2 characters",
		},
		{
			name:        "NonAlphaRejected",
			value:       "1A",
			path:        sriracha.FieldAddressCountry,
			wantErr:     true,
			errContains: "ASCII letters",
		},
	})
}

// TestNormalize_Default covers paths with no field-specific transform —
// only steps 1–4 of the pipeline run.
func TestNormalize_Default(t *testing.T) {
	t.Parallel()
	runNormalizeCases(t, []normalizeCase{
		{
			name:  "EmailLowercaseAndTrim",
			value: "  Hello@Example.COM  ",
			path:  sriracha.FieldContactEmail,
			want:  "hello@example.com",
		},
		{
			name:  "PhoneTrimAndCollapse",
			value: "  +1 800 555 1234  ",
			path:  sriracha.FieldContactPhone,
			want:  "+1 800 555 1234",
		},
	})
}

// TestNormalizeNFKDEquivalence verifies that precomposed U+00E9 and decomposed
// e+U+0301 yield byte-identical output — the property that lets matching work
// across input encodings.
func TestNormalizeNFKDEquivalence(t *testing.T) {
	t.Parallel()

	path := sriracha.FieldNameGiven
	got1, err1 := Normalize("é", path)
	require.NoError(t, err1)
	got2, err2 := Normalize("é", path)
	require.NoError(t, err2)

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
	seeds := []string{"", "Alice", "  hello world  ", "123-456.789", "é", "İ", " test "}
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
			// Skip inputs that legitimately fail normalization for this path.
			if err1 != nil {
				continue
			}
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

// FuzzNormalizeDate verifies that normalizeDate never panics on arbitrary
// input — errors are acceptable, panics are not.
func FuzzNormalizeDate(f *testing.F) {
	f.Add("2024-01-01")
	f.Add("2000-02-29")
	f.Add("not-a-date")
	f.Add("")
	f.Add("01/01/2024")

	f.Fuzz(func(t *testing.T, value string) {
		_, _ = Normalize(value, sriracha.FieldDateBirth)
	})
}

// FuzzNormalizeCountry verifies that country normalization never panics and
// that any successful result is exactly two uppercase ASCII letters.
func FuzzNormalizeCountry(f *testing.F) {
	f.Add("US")
	f.Add("us")
	f.Add("GB")
	f.Add("USA")
	f.Add("")
	f.Add("12")

	f.Fuzz(func(t *testing.T, value string) {
		out, err := Normalize(value, sriracha.FieldAddressCountry)
		// Skip inputs that legitimately fail country validation.
		if err != nil {
			return
		}
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
