package normalize

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ccuetoh/sriracha"
)

// TestGoldenNormalize pins the normalization outputs that the token golden
// vectors depend on. A failure means normalized bytes drifted, which breaks
// matching against previously issued tokens.
func TestGoldenNormalize(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		val  string
		path sriracha.FieldPath
		want string
	}{
		{"LatinDiacritics", "José", sriracha.FieldNameGiven, "jose"},
		{"VietnameseLatinBase", "Đặng", sriracha.FieldNameFamily, "đang"},
		{"ThaiMarksKept", "สุดา", sriracha.FieldNameGiven, "สุดา"},
		{"ZeroWidthSpaceStripped", "Jo\u200bse", sriracha.FieldNameGiven, "jose"},
		{"SoftHyphenStripped", "Mu\u00adller", sriracha.FieldNameFamily, "muller"},
		{"IdentifierSeparators", "A-12.34 5", sriracha.FieldIdentifierNationalID, "a12345"},
		{"DatePassthrough", "1990-01-15", sriracha.FieldDateBirth, "1990-01-15"},
		{"EmailCanonical", "Foo@Bar.COM.", sriracha.FieldContactEmail, "foo@bar.com"},
		{"PhoneDigits", "+1 (555) 123-4567", sriracha.FieldContactPhone, "+15551234567"},
		{"CountryUppercased", "us", sriracha.FieldAddressCountry, "US"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := Normalize(tc.val, tc.path)
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
