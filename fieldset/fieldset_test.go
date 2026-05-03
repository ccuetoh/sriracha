package fieldset

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ccuetoh/sriracha"
)

func TestValidate_DefaultFieldSet(t *testing.T) {
	t.Parallel()
	require.NoError(t, Validate(DefaultFieldSet()), "DefaultFieldSet() should be valid")
}

func TestValidate(t *testing.T) {
	t.Parallel()
	validBloom := sriracha.DefaultProbabilisticConfig()
	cases := []struct {
		name        string
		fs          sriracha.FieldSet
		wantErr     bool
		errContains string
	}{
		{
			name: "EmptyVersion",
			fs: sriracha.FieldSet{
				Version:             "",
				Fields:              []sriracha.FieldSpec{{Path: sriracha.FieldNameGiven, Weight: 1.0}},
				ProbabilisticParams: validBloom,
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
				ProbabilisticParams: validBloom,
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
				ProbabilisticParams: validBloom,
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
				ProbabilisticParams: validBloom,
			},
			wantErr: false,
		},
		{
			name:    "EmptyFields",
			fs:      sriracha.FieldSet{Version: "0.1", Fields: nil, ProbabilisticParams: validBloom},
			wantErr: false,
		},
		{
			name: "BloomZeroSizeBits",
			fs: sriracha.FieldSet{
				Version:             "0.1",
				ProbabilisticParams: sriracha.ProbabilisticConfig{SizeBits: 0, HashCount: 2, NgramSizes: []int{2}},
			},
			wantErr:     true,
			errContains: "SizeBits",
		},
		{
			name: "BloomZeroHashCount",
			fs: sriracha.FieldSet{
				Version:             "0.1",
				ProbabilisticParams: sriracha.ProbabilisticConfig{SizeBits: 1024, HashCount: 0, NgramSizes: []int{2}},
			},
			wantErr:     true,
			errContains: "HashCount",
		},
		{
			name: "BloomEmptyNgramSizes",
			fs: sriracha.FieldSet{
				Version:             "0.1",
				ProbabilisticParams: sriracha.ProbabilisticConfig{SizeBits: 1024, HashCount: 2, NgramSizes: nil},
			},
			wantErr:     true,
			errContains: "NgramSizes",
		},
		{
			name: "BloomNonPositiveNgramSize",
			fs: sriracha.FieldSet{
				Version:             "0.1",
				ProbabilisticParams: sriracha.ProbabilisticConfig{SizeBits: 1024, HashCount: 2, NgramSizes: []int{0, 2}},
			},
			wantErr:     true,
			errContains: "NgramSizes[0]",
		},
		{
			name: "BloomFlipProbabilityNegative",
			fs: sriracha.FieldSet{
				Version: "0.1",
				ProbabilisticParams: sriracha.ProbabilisticConfig{
					SizeBits:        1024,
					HashCount:       2,
					NgramSizes:      []int{2, 3},
					FlipProbability: -0.01,
				},
			},
			wantErr:     true,
			errContains: "FlipProbability",
		},
		{
			name: "BloomFlipProbabilityOne",
			fs: sriracha.FieldSet{
				Version: "0.1",
				ProbabilisticParams: sriracha.ProbabilisticConfig{
					SizeBits:        1024,
					HashCount:       2,
					NgramSizes:      []int{2, 3},
					FlipProbability: 1.0,
				},
			},
			wantErr:     true,
			errContains: "FlipProbability",
		},
		{
			name: "BloomFlipProbabilityAboveOne",
			fs: sriracha.FieldSet{
				Version: "0.1",
				ProbabilisticParams: sriracha.ProbabilisticConfig{
					SizeBits:        1024,
					HashCount:       2,
					NgramSizes:      []int{2, 3},
					FlipProbability: 1.5,
				},
			},
			wantErr:     true,
			errContains: "FlipProbability",
		},
		{
			name: "BloomTargetPopcountEqualsSize",
			fs: sriracha.FieldSet{
				Version: "0.1",
				ProbabilisticParams: sriracha.ProbabilisticConfig{
					SizeBits:       1024,
					HashCount:      2,
					NgramSizes:     []int{2, 3},
					TargetPopcount: 1024,
				},
			},
			wantErr:     true,
			errContains: "TargetPopcount",
		},
		{
			name: "BloomTargetPopcountAboveSize",
			fs: sriracha.FieldSet{
				Version: "0.1",
				ProbabilisticParams: sriracha.ProbabilisticConfig{
					SizeBits:       1024,
					HashCount:      2,
					NgramSizes:     []int{2, 3},
					TargetPopcount: 4096,
				},
			},
			wantErr:     true,
			errContains: "TargetPopcount",
		},
		{
			name: "BloomHardenedConfigValid",
			fs: sriracha.FieldSet{
				Version:             "0.1",
				ProbabilisticParams: sriracha.HardenedProbabilisticConfig(),
			},
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

const expectedDefaultFieldCount = 16

func TestDefaultFieldSetContents(t *testing.T) {
	t.Parallel()
	fs := DefaultFieldSet()
	require.Len(t, fs.Fields, expectedDefaultFieldCount)

	weights := make(map[string]float64, len(fs.Fields))
	for _, f := range fs.Fields {
		weights[f.Path.String()] = f.Weight
	}
	wantWeights := map[sriracha.FieldPath]float64{
		sriracha.FieldIdentifierNationalID: 3.0,
		sriracha.FieldNameFamily:           2.5,
		sriracha.FieldDateBirth:            2.0,
		sriracha.FieldContactEmail:         2.0,
		sriracha.FieldAddressCountry:       0.5,
	}
	for path, want := range wantWeights {
		got, ok := weights[path.String()]
		if assert.Truef(t, ok, "DefaultFieldSet() missing field %s", path) {
			assert.Equalf(t, want, got, "field %s weight", path)
		}
	}
	assert.Equal(t, uint32(2048), fs.ProbabilisticParams.SizeBits)
}

func TestDefaultFieldSet_IsCopy(t *testing.T) {
	t.Parallel()
	fs1 := DefaultFieldSet()
	fs2 := DefaultFieldSet()
	fs1.Fields[0].Weight = 999.0
	assert.NotEqual(t, 999.0, fs2.Fields[0].Weight, "DefaultFieldSet() should return independent copies")
}

func TestValidateRecord(t *testing.T) {
	t.Parallel()

	fs := sriracha.FieldSet{
		Version: "v1",
		Fields: []sriracha.FieldSpec{
			{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0},
			{Path: sriracha.FieldNameFamily, Required: false, Weight: 1.0},
			{Path: sriracha.FieldDateBirth, Required: false, Weight: 1.0},
		},
		ProbabilisticParams: sriracha.DefaultProbabilisticConfig(),
	}

	t.Run("Valid", func(t *testing.T) {
		t.Parallel()
		errs := ValidateRecord(sriracha.RawRecord{
			sriracha.FieldNameGiven:  "Alice",
			sriracha.FieldNameFamily: "Smith",
			sriracha.FieldDateBirth:  "1990-01-01",
		}, fs)
		assert.Empty(t, errs)
	})

	t.Run("ReturnsAllErrors", func(t *testing.T) {
		t.Parallel()
		// Required missing + bad date + unknown path: must surface all three.
		errs := ValidateRecord(sriracha.RawRecord{
			sriracha.FieldNameFamily:   "Smith",
			sriracha.FieldDateBirth:    "not-a-date",
			sriracha.FieldContactEmail: "alice@example.com",
		}, fs)
		require.Len(t, errs, 3)
		joined := errs[0].Error() + "|" + errs[1].Error() + "|" + errs[2].Error()
		assert.Contains(t, joined, "required")
		assert.Contains(t, joined, "ISO 8601")
		assert.Contains(t, joined, "unknown field")
	})

	t.Run("OptionalAbsent", func(t *testing.T) {
		t.Parallel()
		errs := ValidateRecord(sriracha.RawRecord{
			sriracha.FieldNameGiven: "Alice",
		}, fs)
		assert.Empty(t, errs, "absent optional fields must not be flagged")
	})
}

func TestDefaultFieldSet_NgramSizesIndependent(t *testing.T) {
	t.Parallel()
	fs1 := DefaultFieldSet()
	fs2 := DefaultFieldSet()
	require.NotEmpty(t, fs1.ProbabilisticParams.NgramSizes)
	fs1.ProbabilisticParams.NgramSizes[0] = 99
	assert.NotEqual(t, 99, fs2.ProbabilisticParams.NgramSizes[0],
		"DefaultFieldSet must deep-copy ProbabilisticParams.NgramSizes")
}
