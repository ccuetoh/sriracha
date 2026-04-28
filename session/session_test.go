package session

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.sriracha.dev/sriracha"
	"go.sriracha.dev/token"
)

func newSess(t *testing.T) *Session {
	t.Helper()
	fs := sriracha.FieldSet{
		Version: "v1-test",
		Fields: []sriracha.FieldSpec{
			{Path: sriracha.FieldNameGiven, Required: true, Weight: 2.0},
			{Path: sriracha.FieldNameFamily, Required: false, Weight: 1.0},
		},
		BloomParams: sriracha.DefaultBloomConfig(),
	}
	s, err := New([]byte("secret"), fs, token.WithKeyID("k1"))
	require.NoError(t, err)
	t.Cleanup(s.Destroy)
	return s
}

func TestNew(t *testing.T) {
	t.Parallel()
	t.Run("InvalidFieldSetRejected", func(t *testing.T) {
		t.Parallel()
		_, err := New([]byte("s"), sriracha.FieldSet{})
		require.Error(t, err, "empty Version must fail validation before allocating locked memory")
	})

	t.Run("EmptySecretRejected", func(t *testing.T) {
		t.Parallel()
		fs := sriracha.FieldSet{
			Version:     "v1",
			Fields:      []sriracha.FieldSpec{{Path: sriracha.FieldNameGiven, Weight: 1.0}},
			BloomParams: sriracha.DefaultBloomConfig(),
		}
		_, err := New(nil, fs)
		require.Error(t, err)
	})
}

func TestSession_TokenizeAndMatch(t *testing.T) {
	t.Parallel()
	s := newSess(t)

	a, err := s.TokenizeBloom(sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Christopher",
		sriracha.FieldNameFamily: "Smith",
	})
	require.NoError(t, err)
	b, err := s.TokenizeBloom(sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Cristopher",
		sriracha.FieldNameFamily: "Smith",
	})
	require.NoError(t, err)

	res, err := s.Match(a, b, 0.5)
	require.NoError(t, err)
	assert.True(t, res.IsMatch)
	assert.Equal(t, 2, res.ComparableFields)
}

func TestSession_DeterministicEqual(t *testing.T) {
	t.Parallel()
	s := newSess(t)
	rec := sriracha.RawRecord{
		sriracha.FieldNameGiven: "Alice",
	}
	a, err := s.Tokenize(rec)
	require.NoError(t, err)
	b, err := s.Tokenize(rec)
	require.NoError(t, err)
	assert.True(t, s.Equal(a, b))
}

func TestSession_TokenizeField(t *testing.T) {
	t.Parallel()
	s := newSess(t)
	got, err := s.TokenizeField("Alice", sriracha.FieldNameGiven)
	require.NoError(t, err)
	assert.Len(t, got, 32)
}

func TestSession_ValidateRecord(t *testing.T) {
	t.Parallel()
	s := newSess(t)
	errs := s.ValidateRecord(sriracha.RawRecord{
		sriracha.FieldNameGiven: "Alice",
	})
	assert.Empty(t, errs)

	errs = s.ValidateRecord(sriracha.RawRecord{
		sriracha.FieldNameFamily: "Smith",
	})
	require.Len(t, errs, 1, "missing required field must surface")
}

func TestSession_FieldSetIsCopy(t *testing.T) {
	t.Parallel()
	s := newSess(t)
	fs1 := s.FieldSet()
	fs1.Fields[0].Weight = 999
	fs2 := s.FieldSet()
	assert.NotEqual(t, 999.0, fs2.Fields[0].Weight, "FieldSet() must return an independent copy")
	fs1.BloomParams.NgramSizes[0] = 99
	fs2 = s.FieldSet()
	assert.NotEqual(t, 99, fs2.BloomParams.NgramSizes[0], "FieldSet() NgramSizes must be deep-copied")
}
