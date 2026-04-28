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
	validBloom := sriracha.DefaultBloomConfig()
	cases := []struct {
		name        string
		fs          sriracha.FieldSet
		wantErr     bool
		errContains string
	}{
		{
			name: "EmptyVersion",
			fs: sriracha.FieldSet{
				Version:     "",
				Fields:      []sriracha.FieldSpec{{Path: sriracha.FieldNameGiven, Weight: 1.0}},
				BloomParams: validBloom,
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
				BloomParams: validBloom,
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
				BloomParams: validBloom,
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
				BloomParams: validBloom,
			},
			wantErr: false,
		},
		{
			name:    "EmptyFields",
			fs:      sriracha.FieldSet{Version: "0.1", Fields: nil, BloomParams: validBloom},
			wantErr: false,
		},
		{
			name: "BloomZeroSizeBits",
			fs: sriracha.FieldSet{
				Version:     "0.1",
				BloomParams: sriracha.BloomConfig{SizeBits: 0, HashCount: 2, NgramSizes: []int{2}},
			},
			wantErr:     true,
			errContains: "SizeBits",
		},
		{
			name: "BloomZeroHashCount",
			fs: sriracha.FieldSet{
				Version:     "0.1",
				BloomParams: sriracha.BloomConfig{SizeBits: 1024, HashCount: 0, NgramSizes: []int{2}},
			},
			wantErr:     true,
			errContains: "HashCount",
		},
		{
			name: "BloomEmptyNgramSizes",
			fs: sriracha.FieldSet{
				Version:     "0.1",
				BloomParams: sriracha.BloomConfig{SizeBits: 1024, HashCount: 2, NgramSizes: nil},
			},
			wantErr:     true,
			errContains: "NgramSizes",
		},
		{
			name: "BloomNonPositiveNgramSize",
			fs: sriracha.FieldSet{
				Version:     "0.1",
				BloomParams: sriracha.BloomConfig{SizeBits: 1024, HashCount: 2, NgramSizes: []int{0, 2}},
			},
			wantErr:     true,
			errContains: "NgramSizes[0]",
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

func TestDefaultFieldSet_NgramSizesIndependent(t *testing.T) {
	t.Parallel()
	fs1 := DefaultFieldSet()
	fs2 := DefaultFieldSet()
	require.NotEmpty(t, fs1.BloomParams.NgramSizes)
	fs1.BloomParams.NgramSizes[0] = 99
	assert.NotEqual(t, 99, fs2.BloomParams.NgramSizes[0],
		"DefaultFieldSet must deep-copy BloomParams.NgramSizes")
}
