package sriracha

import (
	"encoding/json"
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestProbabilisticConfigs(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		cfg        ProbabilisticConfig
		sizeBits   uint32
		ngramSizes []int
		hashCount  int
	}{
		{"Fast", FastProbabilisticConfig(), 512, []int{2}, 2},
		{"Default", DefaultProbabilisticConfig(), 1024, []int{2, 3}, 3},
		{"HighPrecision", HighPrecisionProbabilisticConfig(), 2048, []int{2, 3}, 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tc.sizeBits, tc.cfg.SizeBits, "SizeBits")
			assert.Equal(t, tc.ngramSizes, tc.cfg.NgramSizes, "NgramSizes")
			assert.Equal(t, tc.hashCount, tc.cfg.HashCount, "HashCount")
			assert.False(t, tc.cfg.Balanced, "presets default to unbalanced per-field filters")
			assert.Zero(t, tc.cfg.SizeBits%2, "presets must use an even SizeBits so CLK can balance them")
		})
	}
}

func TestDeterministicToken_JSON(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "RoundTrip",
			run: func(t *testing.T) {
				orig := DeterministicToken{
					Format:              TokenFormatDeterministic,
					FieldSetVersion:     "0.2",
					KeyID:               "k1",
					FieldSetFingerprint: "deadbeef",
					Fields:              [][]byte{{0x01, 0x02}, nil, {}},
				}
				data, err := json.Marshal(orig)
				require.NoError(t, err)
				var got DeterministicToken
				require.NoError(t, json.Unmarshal(data, &got))
				assert.Equal(t, orig.Format, got.Format)
				assert.Equal(t, orig.FieldSetVersion, got.FieldSetVersion)
				assert.Equal(t, orig.KeyID, got.KeyID)
				assert.Equal(t, orig.FieldSetFingerprint, got.FieldSetFingerprint)
				require.Len(t, got.Fields, 3)
				assert.Equal(t, []byte{0x01, 0x02}, got.Fields[0])
				assert.Nil(t, got.Fields[1])
				assert.Equal(t, []byte{}, got.Fields[2])
			},
		},
		{
			name: "EmptyKeyOmitted",
			run: func(t *testing.T) {
				orig := DeterministicToken{FieldSetVersion: "0.2", Fields: [][]byte{{0x01}}}
				data, err := json.Marshal(orig)
				require.NoError(t, err)
				assert.NotContains(t, string(data), "key_id")
				assert.NotContains(t, string(data), "field_set_fingerprint")
				assert.Contains(t, string(data), `"format"`, "format is not omitempty")
			},
		},
		{
			name: "BadBase64Rejected",
			run: func(t *testing.T) {
				var got DeterministicToken
				err := json.Unmarshal([]byte(`{"field_set_version":"0.1","fields":["not!valid!base64!"]}`), &got)
				require.Error(t, err)
			},
		},
		{
			name: "MalformedJSONRejected",
			run: func(t *testing.T) {
				var got DeterministicToken
				err := json.Unmarshal([]byte(`{not json}`), &got)
				require.Error(t, err)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func TestProbabilisticToken_JSON(t *testing.T) {
	t.Parallel()

	cfg := ProbabilisticConfig{SizeBits: 1024, NgramSizes: []int{2, 3}, HashCount: 2, Balanced: true}
	cases := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "RoundTrip",
			run: func(t *testing.T) {
				orig := ProbabilisticToken{
					Format:              TokenFormatProbabilistic,
					FieldSetVersion:     "0.2",
					KeyID:               "k1",
					FieldSetFingerprint: "cafebabe",
					ProbabilisticParams: cfg,
					Fields:              [][]byte{{0xff, 0x00}, nil},
				}
				data, err := json.Marshal(orig)
				require.NoError(t, err)
				var got ProbabilisticToken
				require.NoError(t, json.Unmarshal(data, &got))
				assert.Equal(t, orig.Format, got.Format)
				assert.Equal(t, orig.FieldSetVersion, got.FieldSetVersion)
				assert.Equal(t, orig.KeyID, got.KeyID)
				assert.Equal(t, orig.FieldSetFingerprint, got.FieldSetFingerprint)
				assert.Equal(t, orig.ProbabilisticParams, got.ProbabilisticParams)
				assert.True(t, got.ProbabilisticParams.Balanced, "Balanced must survive the round trip")
				require.Len(t, got.Fields, 2)
				assert.Equal(t, []byte{0xff, 0x00}, got.Fields[0])
				assert.Nil(t, got.Fields[1], "absent fields must stay nil through JSON")
			},
		},
		{
			name: "UnbalancedOmitsFlag",
			run: func(t *testing.T) {
				plain := cfg
				plain.Balanced = false
				data, err := json.Marshal(ProbabilisticToken{ProbabilisticParams: plain})
				require.NoError(t, err)
				assert.NotContains(t, string(data), "balanced", "Balanced=false must be omitted")
			},
		},
		{
			name: "BadBase64Rejected",
			run: func(t *testing.T) {
				var got ProbabilisticToken
				err := json.Unmarshal([]byte(`{"field_set_version":"0.2","probabilistic_params":{},"fields":["not!valid!base64!"]}`), &got)
				require.Error(t, err)
			},
		},
		{
			name: "MalformedJSONRejected",
			run: func(t *testing.T) {
				var got ProbabilisticToken
				err := json.Unmarshal([]byte(`not json`), &got)
				require.Error(t, err)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func TestCLKToken_JSON(t *testing.T) {
	t.Parallel()

	cfg := ProbabilisticConfig{SizeBits: 1024, NgramSizes: []int{2, 3}, HashCount: 3, Balanced: true}
	cases := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "RoundTrip",
			run: func(t *testing.T) {
				orig := CLKToken{
					Format:              TokenFormatCLK,
					FieldSetVersion:     "0.2",
					KeyID:               "k1",
					FieldSetFingerprint: "cafebabe",
					ProbabilisticParams: cfg,
					Filter:              []byte{0xff, 0x00, 0x2a},
				}
				data, err := json.Marshal(orig)
				require.NoError(t, err)
				var got CLKToken
				require.NoError(t, json.Unmarshal(data, &got))
				assert.Equal(t, orig, got)
			},
		},
		{
			name: "EmptyKeyOmitted",
			run: func(t *testing.T) {
				data, err := json.Marshal(CLKToken{Format: TokenFormatCLK, FieldSetVersion: "0.2"})
				require.NoError(t, err)
				assert.NotContains(t, string(data), "key_id")
				assert.NotContains(t, string(data), "field_set_fingerprint")
				assert.Contains(t, string(data), `"format"`)
			},
		},
		{
			name: "BadBase64Rejected",
			run: func(t *testing.T) {
				var got CLKToken
				err := json.Unmarshal([]byte(`{"format":"sriracha/clk/2","filter":"not!valid!base64!"}`), &got)
				require.Error(t, err)
			},
		},
		{
			name: "MalformedJSONRejected",
			run: func(t *testing.T) {
				var got CLKToken
				err := json.Unmarshal([]byte(`{broken`), &got)
				require.Error(t, err)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func TestAnnotate(t *testing.T) {
	t.Parallel()

	fs := FieldSet{
		Version: "v1",
		Fields: []FieldSpec{
			{Path: FieldNameGiven, Required: true, Weight: 1.0},
			{Path: FieldNameFamily, Required: false, Weight: 1.0},
		},
	}

	t.Run("DeterministicHidesBytes", func(t *testing.T) {
		t.Parallel()
		tr := DeterministicToken{
			FieldSetVersion:     "v1",
			KeyID:               "k1",
			FieldSetFingerprint: "abcd",
			Fields:              [][]byte{{0xde, 0xad}, nil},
		}
		got := tr.Annotate(fs)
		assert.Equal(t, "v1", got.FieldSetVersion)
		assert.Equal(t, "k1", got.KeyID)
		assert.Equal(t, "abcd", got.FieldSetFingerprint)
		require.Len(t, got.Fields, 2)
		assert.Equal(t, FieldNameGiven, got.Fields[0].Path)
		assert.True(t, got.Fields[0].Present)
		assert.Equal(t, 2, got.Fields[0].ByteCount)
		assert.False(t, got.Fields[1].Present)
		assert.Equal(t, 0, got.Fields[1].ByteCount)
	})

	t.Run("BloomNilFilterIsAbsent", func(t *testing.T) {
		t.Parallel()
		tr := ProbabilisticToken{
			FieldSetVersion: "v1",
			Fields:          [][]byte{{0x01}, nil},
		}
		got := tr.Annotate(fs)
		require.Len(t, got.Fields, 2)
		assert.True(t, got.Fields[0].Present, "populated filter is present")
		assert.False(t, got.Fields[1].Present, "nil filter must be reported absent")
	})

	t.Run("MismatchedLengths", func(t *testing.T) {
		t.Parallel()
		tr := DeterministicToken{Fields: [][]byte{{0x01}, {0x02}, {0x03}}}
		got := tr.Annotate(fs)
		require.Len(t, got.Fields, 3, "result length is max(len(fields), len(fs.Fields))")
		assert.Equal(t, FieldPath{}, got.Fields[2].Path, "extra fields beyond fs report empty path")
	})
}

func TestToken_String(t *testing.T) {
	t.Parallel()

	t.Run("DeterministicPopulated", func(t *testing.T) {
		t.Parallel()
		tr := DeterministicToken{
			FieldSetVersion:     "0.1",
			KeyID:               "k1",
			FieldSetFingerprint: "deadbeef0123456789abcdef",
			Fields:              [][]byte{make([]byte, 32), nil, make([]byte, 32)},
		}
		s := tr.String()
		assert.Contains(t, s, "v=0.1")
		assert.Contains(t, s, "key=k1")
		assert.Contains(t, s, "fp=deadbeef")
		assert.NotContains(t, s, "0123456789", "fp must be truncated to 8 hex chars")
		assert.Contains(t, s, "fields=2/3")
		assert.Contains(t, s, "bytes=64")
	})

	t.Run("DeterministicEmptyKey", func(t *testing.T) {
		t.Parallel()
		tr := DeterministicToken{FieldSetVersion: "0.1"}
		s := tr.String()
		assert.Contains(t, s, "key=")
		assert.Contains(t, s, "fp=")
		assert.Contains(t, s, "fields=0/0")
	})

	t.Run("DeterministicShortFingerprint", func(t *testing.T) {
		t.Parallel()
		tr := DeterministicToken{FieldSetVersion: "0.1", FieldSetFingerprint: "abc"}
		assert.Contains(t, tr.String(), "fp=abc")
	})

	t.Run("EmptyFieldCountsAsAbsent", func(t *testing.T) {
		t.Parallel()
		// A JSON round trip can turn an absent field into an empty non-nil
		// slice. Presence is len(field) > 0 everywhere, so both forms are
		// absent and the counts match Annotate.
		tr := ProbabilisticToken{
			FieldSetVersion: "0.2",
			Fields:          [][]byte{make([]byte, 8), {}, nil},
		}
		assert.Contains(t, tr.String(), "fields=1/3")
	})

	t.Run("BloomPopulated", func(t *testing.T) {
		t.Parallel()
		tr := ProbabilisticToken{
			FieldSetVersion:     "0.1",
			KeyID:               "k1",
			ProbabilisticParams: ProbabilisticConfig{SizeBits: 1024},
			Fields:              [][]byte{make([]byte, 128)},
		}
		s := tr.String()
		assert.Contains(t, s, "size=1024b")
		assert.Contains(t, s, "fields=1/1")
		assert.Contains(t, s, "bytes=128")
	})
}

func TestAnnotatedToken_JSON(t *testing.T) {
	t.Parallel()

	at := AnnotatedToken{
		FieldSetVersion: "0.2",
		KeyID:           "k1",
		Fields:          []AnnotatedField{{Path: FieldNameGiven, Present: true, ByteCount: 32}},
	}
	data, err := json.Marshal(at)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"field_set_version":"0.2"`,
		"the version tag must match the one on the token types")
	assert.NotContains(t, string(data), `"version"`)

	var got AnnotatedToken
	require.NoError(t, json.Unmarshal(data, &got))
	assert.Equal(t, at, got)
}

func TestProbabilisticConfigValidate(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name        string
		cfg         ProbabilisticConfig
		wantErr     bool
		errContains string
	}{
		{
			name: "Default",
			cfg:  DefaultProbabilisticConfig(),
		},
		{
			name: "Fast",
			cfg:  FastProbabilisticConfig(),
		},
		{
			name: "HighPrecision",
			cfg:  HighPrecisionProbabilisticConfig(),
		},
		{
			name:        "ZeroSizeBits",
			cfg:         ProbabilisticConfig{SizeBits: 0, HashCount: 2, NgramSizes: []int{2}},
			wantErr:     true,
			errContains: "SizeBits must be > 0",
		},
		{
			name:        "BalancedOddSizeBits",
			cfg:         ProbabilisticConfig{SizeBits: 1023, HashCount: 2, NgramSizes: []int{2}, Balanced: true},
			wantErr:     true,
			errContains: "even when Balanced",
		},
		{
			name: "UnbalancedOddSizeBits",
			cfg:  ProbabilisticConfig{SizeBits: 1023, HashCount: 2, NgramSizes: []int{2}},
		},
		{
			name:        "ZeroHashCount",
			cfg:         ProbabilisticConfig{SizeBits: 1024, HashCount: 0, NgramSizes: []int{2}},
			wantErr:     true,
			errContains: "HashCount",
		},
		{
			name:        "NegativeHashCount",
			cfg:         ProbabilisticConfig{SizeBits: 1024, HashCount: -1, NgramSizes: []int{2}},
			wantErr:     true,
			errContains: "HashCount",
		},
		{
			name:        "EmptyNgramSizes",
			cfg:         ProbabilisticConfig{SizeBits: 1024, HashCount: 2},
			wantErr:     true,
			errContains: "NgramSizes must not be empty",
		},
		{
			name:        "NonPositiveNgramSize",
			cfg:         ProbabilisticConfig{SizeBits: 1024, HashCount: 2, NgramSizes: []int{0, 2}},
			wantErr:     true,
			errContains: "NgramSizes[0]",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.cfg.Validate()
			if !tc.wantErr {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.ErrorIs(t, err, ErrInvalidConfig)
			assert.Contains(t, err.Error(), tc.errContains)
		})
	}
}

func TestFieldSetValidate(t *testing.T) {
	t.Parallel()

	validParams := DefaultProbabilisticConfig()
	cases := []struct {
		name     string
		fs       FieldSet
		wantErr  error
		wantPath FieldPath
	}{
		{
			name: "Valid",
			fs: FieldSet{
				Version:             "v1",
				Fields:              []FieldSpec{{Path: FieldNameGiven, Weight: 1.0}, {Path: FieldNameFamily, Weight: 0}},
				ProbabilisticParams: validParams,
			},
		},
		{
			name: "NoFields",
			fs:   FieldSet{Version: "v1", ProbabilisticParams: validParams},
		},
		{
			name:    "EmptyVersion",
			fs:      FieldSet{Fields: []FieldSpec{{Path: FieldNameGiven, Weight: 1.0}}, ProbabilisticParams: validParams},
			wantErr: ErrMissingVersion,
		},
		{
			name: "EmptyPath",
			fs: FieldSet{
				Version:             "v1",
				Fields:              []FieldSpec{{Path: FieldPath{}, Weight: 1.0}},
				ProbabilisticParams: validParams,
			},
			wantErr: ErrInvalidFieldPath,
		},
		{
			name: "DuplicatePath",
			fs: FieldSet{
				Version:             "v1",
				Fields:              []FieldSpec{{Path: FieldNameGiven, Weight: 1.0}, {Path: FieldNameGiven, Weight: 2.0}},
				ProbabilisticParams: validParams,
			},
			wantErr:  ErrDuplicateField,
			wantPath: FieldNameGiven,
		},
		{
			name: "NegativeWeight",
			fs: FieldSet{
				Version:             "v1",
				Fields:              []FieldSpec{{Path: FieldNameGiven, Weight: -1.0}},
				ProbabilisticParams: validParams,
			},
			wantErr:  ErrInvalidWeight,
			wantPath: FieldNameGiven,
		},
		{
			name: "NaNWeight",
			fs: FieldSet{
				Version:             "v1",
				Fields:              []FieldSpec{{Path: FieldNameGiven, Weight: math.NaN()}},
				ProbabilisticParams: validParams,
			},
			wantErr:  ErrInvalidWeight,
			wantPath: FieldNameGiven,
		},
		{
			name: "PositiveInfWeight",
			fs: FieldSet{
				Version:             "v1",
				Fields:              []FieldSpec{{Path: FieldNameGiven, Weight: math.Inf(1)}},
				ProbabilisticParams: validParams,
			},
			wantErr:  ErrInvalidWeight,
			wantPath: FieldNameGiven,
		},
		{
			name: "NegativeInfWeight",
			fs: FieldSet{
				Version:             "v1",
				Fields:              []FieldSpec{{Path: FieldNameGiven, Weight: math.Inf(-1)}},
				ProbabilisticParams: validParams,
			},
			wantErr:  ErrInvalidWeight,
			wantPath: FieldNameGiven,
		},
		{
			name: "BadProbabilisticParams",
			fs: FieldSet{
				Version:             "v1",
				Fields:              []FieldSpec{{Path: FieldNameGiven, Weight: 1.0}},
				ProbabilisticParams: ProbabilisticConfig{SizeBits: 0, HashCount: 2, NgramSizes: []int{2}},
			},
			wantErr: ErrInvalidConfig,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.fs.Validate()
			if tc.wantErr == nil {
				assert.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.ErrorIs(t, err, tc.wantErr)
			if tc.wantPath.String() == "" {
				return
			}
			var fieldErr FieldError
			require.ErrorAs(t, err, &fieldErr)
			assert.Equal(t, tc.wantPath, fieldErr.Path)
		})
	}
}
