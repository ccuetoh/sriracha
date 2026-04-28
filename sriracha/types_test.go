package sriracha

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDefaultBloomConfig(t *testing.T) {
	t.Parallel()

	cfg := DefaultBloomConfig()
	assert.Equal(t, uint32(1024), cfg.SizeBits, "SizeBits")
	assert.Equal(t, []int{2, 3}, cfg.NgramSizes, "NgramSizes")
	assert.Equal(t, 2, cfg.HashCount, "HashCount")
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
					FieldSetVersion:     "0.1",
					KeyID:               "k1",
					FieldSetFingerprint: "deadbeef",
					Fields:              [][]byte{{0x01, 0x02}, nil, {}},
				}
				data, err := json.Marshal(orig)
				require.NoError(t, err)
				var got DeterministicToken
				require.NoError(t, json.Unmarshal(data, &got))
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
				orig := DeterministicToken{FieldSetVersion: "0.1", Fields: [][]byte{{0x01}}}
				data, err := json.Marshal(orig)
				require.NoError(t, err)
				assert.NotContains(t, string(data), "key_id")
				assert.NotContains(t, string(data), "field_set_fingerprint")
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

func TestBloomToken_JSON(t *testing.T) {
	t.Parallel()

	cfg := BloomConfig{SizeBits: 1024, NgramSizes: []int{2, 3}, HashCount: 2}
	cases := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "RoundTrip",
			run: func(t *testing.T) {
				orig := BloomToken{
					FieldSetVersion:     "0.1",
					KeyID:               "k1",
					FieldSetFingerprint: "cafebabe",
					BloomParams:         cfg,
					Fields:              [][]byte{{0xff, 0x00}, nil},
				}
				data, err := json.Marshal(orig)
				require.NoError(t, err)
				var got BloomToken
				require.NoError(t, json.Unmarshal(data, &got))
				assert.Equal(t, orig.FieldSetVersion, got.FieldSetVersion)
				assert.Equal(t, orig.KeyID, got.KeyID)
				assert.Equal(t, orig.FieldSetFingerprint, got.FieldSetFingerprint)
				assert.Equal(t, orig.BloomParams, got.BloomParams)
				require.Len(t, got.Fields, 2)
				assert.Equal(t, []byte{0xff, 0x00}, got.Fields[0])
				assert.Nil(t, got.Fields[1])
			},
		},
		{
			name: "BadBase64Rejected",
			run: func(t *testing.T) {
				var got BloomToken
				err := json.Unmarshal([]byte(`{"field_set_version":"0.1","bloom_params":{},"fields":["not!valid!base64!"]}`), &got)
				require.Error(t, err)
			},
		},
		{
			name: "MalformedJSONRejected",
			run: func(t *testing.T) {
				var got BloomToken
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
		assert.Equal(t, "v1", got.Version)
		assert.Equal(t, "k1", got.KeyID)
		assert.Equal(t, "abcd", got.FieldSetFingerprint)
		require.Len(t, got.Fields, 2)
		assert.Equal(t, FieldNameGiven, got.Fields[0].Path)
		assert.True(t, got.Fields[0].Present)
		assert.Equal(t, 2, got.Fields[0].ByteCount)
		assert.False(t, got.Fields[1].Present)
		assert.Equal(t, 0, got.Fields[1].ByteCount)
	})

	t.Run("BloomZeroFilterIsAbsent", func(t *testing.T) {
		t.Parallel()
		tr := BloomToken{
			FieldSetVersion: "v1",
			Fields:          [][]byte{{0x01}, {0x00, 0x00}},
		}
		got := tr.Annotate(fs)
		require.Len(t, got.Fields, 2)
		assert.True(t, got.Fields[0].Present, "non-zero filter is present")
		assert.False(t, got.Fields[1].Present, "all-zero filter must be reported absent")
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

	t.Run("BloomPopulated", func(t *testing.T) {
		t.Parallel()
		tr := BloomToken{
			FieldSetVersion: "0.1",
			KeyID:           "k1",
			BloomParams:     BloomConfig{SizeBits: 1024},
			Fields:          [][]byte{make([]byte, 128)},
		}
		s := tr.String()
		assert.Contains(t, s, "size=1024b")
		assert.Contains(t, s, "fields=1/1")
		assert.Contains(t, s, "bytes=128")
	})
}
