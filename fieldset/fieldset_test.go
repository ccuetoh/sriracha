package fieldset

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/normalize"
)

func TestDefaultFieldSet_Valid(t *testing.T) {
	t.Parallel()
	require.NoError(t, DefaultFieldSet().Validate(), "DefaultFieldSet() should be valid")
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
	assert.Equal(t, "0.2", fs.Version)
	assert.Equal(t, sriracha.DefaultProbabilisticConfig(), fs.ProbabilisticParams)
	assert.False(t, fs.ProbabilisticParams.Balanced, "the default schema uses unbalanced per-field filters; CLK balances on its own")
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

	cases := []struct {
		name string
		// record is validated against fs.
		record sriracha.RawRecord
		// wantLeaves is the number of joined leaves expected, 0 for a valid
		// record.
		wantLeaves int
		// wantIs lists the sentinels every leaf set must contain.
		wantIs []error
		// wantPaths lists the paths recoverable from the leaves, in order.
		wantPaths []sriracha.FieldPath
	}{
		{
			name: "Valid",
			record: sriracha.RawRecord{
				sriracha.FieldNameGiven:  "Alice",
				sriracha.FieldNameFamily: "Smith",
				sriracha.FieldDateBirth:  "1990-01-01",
			},
		},
		{
			name: "OptionalAbsent",
			record: sriracha.RawRecord{
				sriracha.FieldNameGiven: "Alice",
			},
		},
		{
			name: "OptionalNormalizesToEmpty",
			record: sriracha.RawRecord{
				sriracha.FieldNameGiven:  "Alice",
				sriracha.FieldNameFamily: "",
			},
		},
		{
			name: "ReportsEveryProblemInOnePass",
			record: sriracha.RawRecord{
				sriracha.FieldNameFamily:   "Smith",
				sriracha.FieldDateBirth:    "not-a-date",
				sriracha.FieldContactEmail: "alice@example.com",
			},
			wantLeaves: 3,
			wantIs: []error{
				sriracha.ErrRequiredFieldMissing,
				normalize.ErrInvalidValue,
				sriracha.ErrUnknownField,
			},
			wantPaths: []sriracha.FieldPath{
				sriracha.FieldNameGiven,
				sriracha.FieldDateBirth,
				sriracha.FieldContactEmail,
			},
		},
		{
			name: "RequiredMissing",
			record: sriracha.RawRecord{
				sriracha.FieldNameFamily: "Smith",
			},
			wantLeaves: 1,
			wantIs:     []error{sriracha.ErrRequiredFieldMissing},
			wantPaths:  []sriracha.FieldPath{sriracha.FieldNameGiven},
		},
		{
			name: "RequiredNormalizesToEmpty",
			record: sriracha.RawRecord{
				sriracha.FieldNameGiven: "",
			},
			wantLeaves: 1,
			wantIs:     []error{sriracha.ErrEmptyValue},
			wantPaths:  []sriracha.FieldPath{sriracha.FieldNameGiven},
		},
		{
			name: "UnknownPathsSorted",
			record: sriracha.RawRecord{
				sriracha.FieldNameGiven:    "Alice",
				sriracha.FieldContactPhone: "+1 800 555 1234",
				sriracha.FieldContactEmail: "alice@example.com",
			},
			wantLeaves: 2,
			wantIs:     []error{sriracha.ErrUnknownField},
			wantPaths: []sriracha.FieldPath{
				sriracha.FieldContactEmail,
				sriracha.FieldContactPhone,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := ValidateRecord(tc.record, fs)
			if tc.wantLeaves == 0 {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)

			joined, ok := err.(interface{ Unwrap() []error })
			require.True(t, ok, "ValidateRecord must return a joined error")
			leaves := joined.Unwrap()
			require.Len(t, leaves, tc.wantLeaves)

			for _, sentinel := range tc.wantIs {
				assert.ErrorIsf(t, err, sentinel, "sentinel %v must stay reachable through the join", sentinel)
			}

			paths := make([]sriracha.FieldPath, 0, len(leaves))
			for _, leaf := range leaves {
				var fieldErr sriracha.FieldError
				require.ErrorAs(t, leaf, &fieldErr, "every leaf must be a FieldError")
				paths = append(paths, fieldErr.Path)
			}
			assert.Equal(t, tc.wantPaths, paths)
		})
	}
}

func TestValidateRecord_NormalizationErrorPropagates(t *testing.T) {
	t.Parallel()

	fs := sriracha.FieldSet{
		Version:             "v1",
		Fields:              []sriracha.FieldSpec{{Path: sriracha.FieldDateBirth, Weight: 1.0}},
		ProbabilisticParams: sriracha.DefaultProbabilisticConfig(),
	}

	err := ValidateRecord(sriracha.RawRecord{sriracha.FieldDateBirth: "15/06/2024"}, fs)
	require.Error(t, err)
	assert.ErrorIs(t, err, normalize.ErrInvalidValue)
	assert.Contains(t, err.Error(), "ISO 8601")

	var fieldErr sriracha.FieldError
	require.ErrorAs(t, err, &fieldErr)
	assert.Equal(t, sriracha.FieldDateBirth, fieldErr.Path)
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
