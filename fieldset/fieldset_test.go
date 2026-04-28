package fieldset

import (
	"fmt"
	"sync"
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
	assert.Equal(t, sriracha.DefaultBloomConfig().SizeBits, fs.BloomParams.SizeBits)
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

func TestRegistry(t *testing.T) {
	t.Parallel()

	mkFS := func(version string) sriracha.FieldSet {
		return sriracha.FieldSet{
			Version: version,
			Fields: []sriracha.FieldSpec{
				{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0},
			},
			BloomParams: sriracha.DefaultBloomConfig(),
		}
	}

	t.Run("DefaultRegistered", func(t *testing.T) {
		t.Parallel()
		got, ok := Lookup("0.1")
		require.True(t, ok)
		assert.Equal(t, "0.1", got.Version)
	})

	t.Run("Versions", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, Register(mkFS("versions-test-1")))
		require.NoError(t, Register(mkFS("versions-test-2")))
		v := Versions()
		assert.Contains(t, v, "0.1")
		assert.Contains(t, v, "versions-test-1")
		assert.Contains(t, v, "versions-test-2")
		// Sorted: alphabetical, so "0.1" < "versions-test-1" < "versions-test-2".
		idx1 := indexOf(v, "0.1")
		idx2 := indexOf(v, "versions-test-1")
		assert.Less(t, idx1, idx2, "versions should be sorted")
	})

	t.Run("DuplicateRejected", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, Register(mkFS("dup-test")))
		err := Register(mkFS("dup-test"))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already registered")
	})

	t.Run("InvalidRejected", func(t *testing.T) {
		t.Parallel()
		err := Register(sriracha.FieldSet{Version: "", BloomParams: sriracha.DefaultBloomConfig()})
		require.Error(t, err)
	})

	t.Run("LookupUnknown", func(t *testing.T) {
		t.Parallel()
		_, ok := Lookup("does-not-exist")
		assert.False(t, ok)
	})

	t.Run("LookupDeepCopy", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, Register(mkFS("deep-copy-test")))
		a, _ := Lookup("deep-copy-test")
		a.Fields[0].Weight = 999.0
		a.BloomParams.NgramSizes[0] = 99
		b, _ := Lookup("deep-copy-test")
		assert.NotEqual(t, 999.0, b.Fields[0].Weight, "Lookup must return independent copy")
		assert.NotEqual(t, 99, b.BloomParams.NgramSizes[0], "Lookup must deep-copy NgramSizes")
	})

	t.Run("ConcurrentRegisterAndLookup", func(t *testing.T) {
		t.Parallel()
		const n = 10
		var wg sync.WaitGroup
		for i := range n {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				v := fmt.Sprintf("concurrent-%d", i)
				assert.NoError(t, Register(mkFS(v)))
				got, ok := Lookup(v)
				assert.True(t, ok)
				assert.Equal(t, v, got.Version)
			}(i)
		}
		wg.Wait()
	})
}

func indexOf(slice []string, s string) int {
	for i, v := range slice {
		if v == s {
			return i
		}
	}
	return -1
}
