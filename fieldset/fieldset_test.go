package fieldset

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.sriracha.dev/sriracha"
)

func TestValidate_DefaultFieldSet(t *testing.T) {
	t.Parallel()
	require.NoError(t, Validate(DefaultFieldSet()), "DefaultFieldSet() should be valid")
}

func TestValidate(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name        string
		fs          sriracha.FieldSet
		wantErr     bool
		errContains string
	}{
		{
			name: "EmptyVersion",
			fs: sriracha.FieldSet{
				Version: "",
				Fields:  []sriracha.FieldSpec{{Path: sriracha.FieldNameGiven, Weight: 1.0}},
			},
			wantErr:     true,
			errContains: "version",
		},
		{
			name: "DuplicatePath",
			fs: sriracha.FieldSet{
				Version: "0.1",
				Fields: []sriracha.FieldSpec{
					{Path: sriracha.FieldNameGiven, Weight: 1.0},
					{Path: sriracha.FieldNameGiven, Weight: 2.0},
				},
			},
			wantErr:     true,
			errContains: "duplicate",
		},
		{
			name: "NegativeWeight",
			fs: sriracha.FieldSet{
				Version: "0.1",
				Fields: []sriracha.FieldSpec{
					{Path: sriracha.FieldNameGiven, Weight: -1.0},
				},
			},
			wantErr:     true,
			errContains: "negative",
		},
		{
			name: "ZeroWeightIsValid",
			fs: sriracha.FieldSet{
				Version: "0.1",
				Fields: []sriracha.FieldSpec{
					{Path: sriracha.FieldNameGiven, Weight: 0.0},
				},
			},
			wantErr: false,
		},
		{
			name:    "EmptyFields",
			fs:      sriracha.FieldSet{Version: "0.1", Fields: nil},
			wantErr: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := Validate(tc.fs)
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.errContains)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestCompatible(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		a, b sriracha.FieldSet
		want bool
	}{
		{
			name: "SameVersion",
			a:    sriracha.FieldSet{Version: "0.1"},
			b:    sriracha.FieldSet{Version: "0.1"},
			want: true,
		},
		{
			name: "DifferentVersion",
			a:    sriracha.FieldSet{Version: "0.1"},
			b:    sriracha.FieldSet{Version: "0.2"},
			want: false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.want, Compatible(tc.a, tc.b))
		})
	}
}

func TestNegotiateVersion(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		supported []string
		requested string
		want      string
		wantErr   bool
	}{
		{
			name:      "ExactMatch",
			supported: []string{"0.1", "0.2"},
			requested: "0.1",
			want:      "0.1",
		},
		{
			name:      "ExactMatchHigher",
			supported: []string{"0.1", "0.2"},
			requested: "0.2",
			want:      "0.2",
		},
		{
			name:      "NoOverlap",
			supported: []string{"0.2"},
			requested: "0.1",
			wantErr:   true,
		},
		{
			name:      "EmptySupported",
			supported: []string{},
			requested: "0.1",
			wantErr:   true,
		},
		{
			name:      "EmptyRequested",
			supported: []string{"0.1"},
			requested: "",
			wantErr:   true,
		},
		{
			// "1.10" > "1.9" numerically, but "1.9" > "1.10" lexicographically.
			name:      "SemverVsString",
			supported: []string{"1.9", "1.10"},
			requested: "1.10",
			want:      "1.10",
		},
		{
			// Duplicates in supported should be handled gracefully.
			name:      "DuplicateInSupported",
			supported: []string{"0.1", "0.1"},
			requested: "0.1",
			want:      "0.1",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := NegotiateVersion(tc.supported, tc.requested)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tc.want, got)
			}
		})
	}
}

func TestCompareSemver(t *testing.T) {
	t.Parallel()
	cases := []struct {
		a, b string
		want int
	}{
		{"1.0.0", "1.0.0", 0},
		{"1.1.0", "1.0.0", 1},
		{"1.0.0", "1.1.0", -1},
		{"1.10.0", "1.9.0", 1},
		{"2.0", "1.9", 1},
		{"0.1", "0.2", -1},
		{"1.0.1", "1.0.0", 1},
	}
	sign := func(n int) int {
		if n < 0 {
			return -1
		}
		if n > 0 {
			return 1
		}
		return 0
	}
	for _, tc := range cases {
		t.Run(tc.a+"_vs_"+tc.b, func(t *testing.T) {
			t.Parallel()
			got := compareSemver(tc.a, tc.b)
			assert.Equal(t, tc.want, sign(got), "compareSemver(%q, %q) = %d, want sign %d", tc.a, tc.b, got, tc.want)
		})
	}
}

func TestDefaultFieldSetContents(t *testing.T) {
	t.Parallel()
	fs := DefaultFieldSet()
	require.Len(t, fs.Fields, 16, "DefaultFieldSet() should have 16 fields")

	checks := []struct {
		path   sriracha.FieldPath
		weight float64
	}{
		{sriracha.FieldIdentifierNationalID, 3.0},
		{sriracha.FieldNameFamily, 2.5},
		{sriracha.FieldDateBirth, 2.0},
		{sriracha.FieldContactEmail, 2.0},
		{sriracha.FieldAddressCountry, 0.5},
	}
	weights := make(map[string]float64, len(fs.Fields))
	for _, f := range fs.Fields {
		weights[f.Path.String()] = f.Weight
	}
	for _, c := range checks {
		got, ok := weights[c.path.String()]
		if assert.True(t, ok, "DefaultFieldSet() missing field %s", c.path) {
			assert.Equal(t, c.weight, got, "field %s weight", c.path)
		}
	}
	def := sriracha.DefaultBloomConfig()
	assert.Equal(t, def.SizeBits, fs.BloomParams.SizeBits, "BloomParams.SizeBits")
}

func TestDefaultFieldSet_IsCopy(t *testing.T) {
	t.Parallel()
	fs1 := DefaultFieldSet()
	fs2 := DefaultFieldSet()
	fs1.Fields[0].Weight = 999.0
	assert.NotEqual(t, 999.0, fs2.Fields[0].Weight, "DefaultFieldSet() should return independent copies")
}

func TestParseIntSafe(t *testing.T) {
	t.Parallel()
	cases := []struct {
		s    string
		want int
	}{
		{"0", 0},
		{"10", 10},
		{"123", 123},
		{"1abc", 1},
		{"", 0},
	}
	for _, tc := range cases {
		t.Run(tc.s, func(t *testing.T) {
			t.Parallel()
			got := parseIntSafe(tc.s)
			assert.Equal(t, tc.want, got)
		})
	}
}
