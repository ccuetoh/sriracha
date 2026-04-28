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

// TestNormalize_Unicode covers NFKD decomposition, lowercasing, whitespace
// handling, and the diacritic-stripping applied to name-namespace paths.
func TestNormalize_Unicode(t *testing.T) {
	t.Parallel()
	runNormalizeCases(t, []normalizeCase{
		{
			name:  "NFKDPrecomposedEAcuteName",
			value: "é", // é precomposed; name path strips combining marks
			path:  sriracha.FieldNameGiven,
			want:  "e",
		},
		{
			name:  "NFKDDecomposedEAcuteName",
			value: "é", // already decomposed; name path strips combining marks
			path:  sriracha.FieldNameGiven,
			want:  "e",
		},
		{
			name:  "NonBreakingSpaceCollapse",
			value: " hello world ",
			path:  sriracha.FieldAddressLocality,
			want:  "hello world",
		},
		{
			// NFKD decomposes U+0130 (İ) → I + U+0307 (combining dot above);
			// lowercasing yields "i" + U+0307. The name namespace then strips
			// the combining mark, producing bare "i". Non-name paths preserve it.
			name:  "TurkishIWithDotAboveName",
			value: "İ",
			path:  sriracha.FieldNameGiven,
			want:  "i",
		},
		{
			name:  "TurkishIWithDotAboveNonName",
			value: "İ",
			path:  sriracha.FieldAddressLocality,
			want:  "i̇",
		},
		{
			name:  "NameWithSpacesAndAccent",
			value: "  José  ",
			path:  sriracha.FieldNameGiven,
			want:  "jose",
		},
		{
			name:  "NameMullerStrippedNotFolded",
			value: "Müller",
			path:  sriracha.FieldNameFamily,
			want:  "muller",
		},
		{
			name:  "NameInvalidUTF8Replaced",
			value: "ab\xffcd",
			path:  sriracha.FieldNameGiven,
			want:  "ab�cd",
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

// TestNormalize_Email covers the contact-email validator.
func TestNormalize_Email(t *testing.T) {
	t.Parallel()
	runNormalizeCases(t, []normalizeCase{
		{
			name:  "LowercaseAndTrim",
			value: "  Hello@Example.COM  ",
			path:  sriracha.FieldContactEmail,
			want:  "hello@example.com",
		},
		{
			name:        "MultiAtRejected",
			value:       "a@b@c.com",
			path:        sriracha.FieldContactEmail,
			wantErr:     true,
			errContains: "exactly one '@'",
		},
		{
			name:        "NoAtRejected",
			value:       "missing-at-sign",
			path:        sriracha.FieldContactEmail,
			wantErr:     true,
			errContains: "exactly one '@'",
		},
	})
}

// TestNormalize_Phone covers best-effort phone normalization (digits + leading +).
func TestNormalize_Phone(t *testing.T) {
	t.Parallel()
	runNormalizeCases(t, []normalizeCase{
		{
			name:  "StripFormattingKeepLeadingPlus",
			value: "  +1 (800) 555-1234  ",
			path:  sriracha.FieldContactPhone,
			want:  "+18005551234",
		},
		{
			name:  "PlusOnlyAtStart",
			value: "1+800+5551234",
			path:  sriracha.FieldContactPhone,
			want:  "18005551234",
		},
		{
			name:  "NoPlus",
			value: "8005551234",
			path:  sriracha.FieldContactPhone,
			want:  "8005551234",
		},
		{
			name:        "TooShortRejected",
			value:       "12345",
			path:        sriracha.FieldContactPhone,
			wantErr:     true,
			errContains: "at least 7 digits",
		},
	})
}

// TestNormalize_Default covers paths with no field-specific transform —
// only steps 1–4 of the pipeline run.
func TestNormalize_Default(t *testing.T) {
	t.Parallel()
	runNormalizeCases(t, []normalizeCase{
		{
			name:  "AddressLocalityLowercaseAndTrim",
			value: "  Buenos Aires  ",
			path:  sriracha.FieldAddressLocality,
			want:  "buenos aires",
		},
		{
			name:  "AddressPostalCodeUntouched",
			value: "  K1A 0B1  ",
			path:  sriracha.FieldAddressPostalCode,
			want:  "k1a 0b1",
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
	seeds := []string{"", "Alice", "  hello world  ", "123-456.789", "é", "İ", " test ", "user@example.com", "+1 800 555 1234"}
	for _, s := range seeds {
		f.Add(s)
	}

	paths := []sriracha.FieldPath{
		sriracha.FieldNameGiven,
		sriracha.FieldNameFamily,
		sriracha.FieldIdentifierNationalID,
		sriracha.FieldContactEmail,
		sriracha.FieldContactPhone,
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
