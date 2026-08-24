package token

import (
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ccuetoh/sriracha"
)

// The golden vectors below pin the v2 token derivation. A failure here
// means emitted token bytes drifted, which breaks matching against stored
// tokens. That requires a new format version and updated vectors in the
// same commit, never a silent update.

func goldenSecret() []byte {
	secret := make([]byte, 32)
	for i := range secret {
		secret[i] = byte(i)
	}
	return secret
}

func goldenFields() []sriracha.FieldSpec {
	return []sriracha.FieldSpec{
		{Path: sriracha.FieldNameGiven, Weight: 2},
		{Path: sriracha.FieldNameFamily, Weight: 2.5},
		{Path: sriracha.FieldDateBirth, Weight: 2},
		{Path: sriracha.FieldAddressCountry, Weight: 0.5},
	}
}

func goldenRecord() sriracha.RawRecord {
	return sriracha.RawRecord{
		sriracha.FieldNameGiven:      "Alice",
		sriracha.FieldNameFamily:     "Smith",
		sriracha.FieldDateBirth:      "1990-01-15",
		sriracha.FieldAddressCountry: "US",
	}
}

func TestGoldenDeterministic(t *testing.T) {
	t.Parallel()

	fs := sriracha.FieldSet{Version: "golden-1", Fields: goldenFields(), ProbabilisticParams: sriracha.DefaultProbabilisticConfig()}
	tok, err := New(goldenSecret())
	require.NoError(t, err)
	defer tok.Destroy()

	det, err := tok.TokenizeDeterministic(goldenRecord(), fs)
	require.NoError(t, err)
	require.Len(t, det.Fields, 4)

	want := []string{
		"89ff70aed9b9f38bd063dd6d66a108302d6fb2e38e44817cf56ddab3afe6da22",
		"01316cdd6c78cf516e80bdcd8678893fbc8bf77ebdfe47153e895319aa0c9bd5",
		"ebded1f52cdc2d418fc88db3a0284b2e7620d04a457997915130e259793ceb25",
		"7faf77a6c8ae7cbf738674b9a867edad294f2c316e4b988269e7da4bcc62eda6",
	}
	for i, w := range want {
		assert.Equal(t, w, hex.EncodeToString(det.Fields[i]), "field %d", i)
	}
	assert.Equal(t, "7664135757e29986aee04fe8833573b39d9a473b247a8ed990711bab6885b95e", fs.Fingerprint())
}

func TestGoldenProbabilistic(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		cfg       sriracha.ProbabilisticConfig
		filterLen int
		bloomSums []string
		clkSum    string
	}{
		{
			name:      "Fast",
			cfg:       sriracha.FastProbabilisticConfig(),
			filterLen: 64,
			bloomSums: []string{
				"05c96d1241e5e3013135386accc3b2967b01ddc371c254bfbf47d990aca14df9",
				"76913f953aa83ba70fe62160064cc03fbec74d2d728b8ff105709d8bef1e5a9b",
				"5336b7b843b2a0593b6e602351fe0bd1aea9819e2640c6da3fe8645fc9e3ca70",
				"3badf4b4e75f9c5adc73ce2b284b605cc71bf7d2a582173dac3ddebefeb65b93",
			},
			clkSum: "277be3d8bf78a6227ceac26affcde0eb321523b87b8209db391cab2c9fee90d4",
		},
		{
			name:      "Default",
			cfg:       sriracha.DefaultProbabilisticConfig(),
			filterLen: 128,
			bloomSums: []string{
				"aac77b437a664dcb462693996607c266d9b27f9a53046fcf480c6d06c243abd8",
				"2c7af9f83cea9fd128d1adb45d27890af337802eb9ea44bc46ab85d9967e0d36",
				"820060aeda72bbaaf3f6e187be8510199500347095c19006005ba42c6d08ad68",
				"4b12aa3399c4dee798615d7f7213147b2c9f85a9046ed18063effd32b3d28dc1",
			},
			clkSum: "9ac8824bcaaf8b666d5916fc77f8db6546a78c1b0d5fa0b4340c9e6bdfae5a9c",
		},
		{
			name:      "HighPrecision",
			cfg:       sriracha.HighPrecisionProbabilisticConfig(),
			filterLen: 256,
			bloomSums: []string{
				"729bc9f62c06f93de5bd85c4e6182450538be732017ec12e20f8c3d54dcbb0cb",
				"d4f397c344bdfef2b4d93438785835b1fb01dac764b0eac9fcb676980d53fb34",
				"14259d04d062e122b1dfa3f4d140bf6f84eb92e2e711104a07807af4dccce8b5",
				"d4ea1da3c9ef9d500782ecdeaf66a7882b261a4ac1dc574a630b5eea33ce333e",
			},
			clkSum: "fd05a92da0b102c564849d0a531c38acfd9fe699ec5847c50f4f72b186621cb6",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fs := sriracha.FieldSet{Version: "golden-1", Fields: goldenFields(), ProbabilisticParams: tc.cfg}
			tok, err := New(goldenSecret())
			require.NoError(t, err)
			defer tok.Destroy()

			prob, err := tok.TokenizeProbabilistic(goldenRecord(), fs)
			require.NoError(t, err)
			require.Len(t, prob.Fields, 4)
			for i, wantSum := range tc.bloomSums {
				require.Len(t, prob.Fields[i], tc.filterLen, "field %d", i)
				sum := sha256.Sum256(prob.Fields[i])
				assert.Equal(t, wantSum, hex.EncodeToString(sum[:]), "field %d", i)
			}

			clk, err := tok.TokenizeCLK(goldenRecord(), fs)
			require.NoError(t, err)
			require.Len(t, clk.Filter, tc.filterLen)
			sum := sha256.Sum256(clk.Filter)
			assert.Equal(t, tc.clkSum, hex.EncodeToString(sum[:]))
		})
	}
}

// TestGoldenFilterBytes pins one full filter so a byte-level reference
// exists beyond the digests above.
func TestGoldenFilterBytes(t *testing.T) {
	t.Parallel()

	fs := sriracha.FieldSet{Version: "golden-1", Fields: goldenFields(), ProbabilisticParams: sriracha.FastProbabilisticConfig()}
	tok, err := New(goldenSecret())
	require.NoError(t, err)
	defer tok.Destroy()

	prob, err := tok.TokenizeProbabilistic(goldenRecord(), fs)
	require.NoError(t, err)
	const wantCountry = "0000001000000000000000000020010000000000000000000000000000000000" +
		"0000000000000000010000000000088000000000000000000000000000000000"
	assert.Equal(t, wantCountry, hex.EncodeToString(prob.Fields[3]))
}
