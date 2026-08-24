package session

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/token"
)

func newSess(t *testing.T) Session {
	t.Helper()
	fs := sriracha.FieldSet{
		Version: "v1-test",
		Fields: []sriracha.FieldSpec{
			{Path: sriracha.FieldNameGiven, Required: true, Weight: 2.0},
			{Path: sriracha.FieldNameFamily, Required: false, Weight: 1.0},
		},
		ProbabilisticParams: sriracha.DefaultProbabilisticConfig(),
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
			Version:             "v1",
			Fields:              []sriracha.FieldSpec{{Path: sriracha.FieldNameGiven, Weight: 1.0}},
			ProbabilisticParams: sriracha.DefaultProbabilisticConfig(),
		}
		_, err := New(nil, fs)
		require.Error(t, err)
	})
}

func TestSession_TokenizeAndMatch(t *testing.T) {
	t.Parallel()
	s := newSess(t)

	a, err := s.TokenizeProbabilistic(sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Christopher",
		sriracha.FieldNameFamily: "Smith",
	})
	require.NoError(t, err)
	b, err := s.TokenizeProbabilistic(sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Cristopher",
		sriracha.FieldNameFamily: "Smith",
	})
	require.NoError(t, err)

	res, err := s.Match(a, b, 0.5)
	require.NoError(t, err)
	assert.True(t, res.IsMatch)
	assert.Equal(t, 2, res.ComparableFields)
}

func TestSession_TokenizeCLKAndMatchCLK(t *testing.T) {
	t.Parallel()
	s := newSess(t)

	a, err := s.TokenizeCLK(sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Christopher",
		sriracha.FieldNameFamily: "Smith",
	})
	require.NoError(t, err)
	b, err := s.TokenizeCLK(sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Christopher",
		sriracha.FieldNameFamily: "Smith",
	})
	require.NoError(t, err)

	res, err := s.MatchCLK(a, b, 0.9)
	require.NoError(t, err)
	assert.True(t, res.IsMatch)
	assert.InDelta(t, 1.0, res.Score, 1e-9)

	want := s.FieldSet().Fingerprint()
	assert.Equal(t, want, a.FieldSetFingerprint,
		"session.TokenizeCLK must stamp the cached fingerprint")
}

func TestSession_TokenizeCLKErrorReturnsEmptyToken(t *testing.T) {
	t.Parallel()
	s := newSess(t)

	// Missing required field means the underlying tokenizer errors; the
	// session must not stamp the fingerprint onto an error result.
	clk, err := s.TokenizeCLK(sriracha.RawRecord{})
	require.Error(t, err)
	assert.Empty(t, clk.FieldSetFingerprint, "error path must not stamp fingerprint")
}

func TestSession_DeterministicEqual(t *testing.T) {
	t.Parallel()
	s := newSess(t)
	rec := sriracha.RawRecord{
		sriracha.FieldNameGiven: "Alice",
	}
	a, err := s.TokenizeDeterministic(rec)
	require.NoError(t, err)
	b, err := s.TokenizeDeterministic(rec)
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
	fs1.ProbabilisticParams.NgramSizes[0] = 99
	fs2 = s.FieldSet()
	assert.NotEqual(t, 99, fs2.ProbabilisticParams.NgramSizes[0], "FieldSet() NgramSizes must be deep-copied")
}

func TestSession_TokenizesStampFingerprint(t *testing.T) {
	t.Parallel()
	s := newSess(t)
	want := s.FieldSet().Fingerprint()

	detTok, err := s.TokenizeDeterministic(sriracha.RawRecord{
		sriracha.FieldNameGiven: "Alice",
	})
	require.NoError(t, err)
	assert.Equal(t, want, detTok.FieldSetFingerprint,
		"session.TokenizeDeterministic must stamp the cached fingerprint")

	probTok, err := s.TokenizeProbabilistic(sriracha.RawRecord{
		sriracha.FieldNameGiven: "Alice",
	})
	require.NoError(t, err)
	assert.Equal(t, want, probTok.FieldSetFingerprint,
		"session.TokenizeProbabilistic must stamp the cached fingerprint")
}

func TestSession_TokenizeErrorReturnsEmptyToken(t *testing.T) {
	t.Parallel()
	s := newSess(t)

	// Missing required field — token.* returns an error without producing
	// useful token bytes. Session must not stamp the fingerprint onto an
	// error result.
	detTok, err := s.TokenizeDeterministic(sriracha.RawRecord{})
	require.Error(t, err)
	assert.Empty(t, detTok.FieldSetFingerprint,
		"error path must not stamp fingerprint")

	probTok, err := s.TokenizeProbabilistic(sriracha.RawRecord{})
	require.Error(t, err)
	assert.Empty(t, probTok.FieldSetFingerprint,
		"error path must not stamp fingerprint")
}

func TestSession_NewCopiesFieldSet(t *testing.T) {
	t.Parallel()
	fs := sriracha.FieldSet{
		Version: "v1-test",
		Fields: []sriracha.FieldSpec{
			{Path: sriracha.FieldNameGiven, Required: true, Weight: 2.0},
			{Path: sriracha.FieldNameFamily, Required: false, Weight: 1.0},
		},
		ProbabilisticParams: sriracha.DefaultProbabilisticConfig(),
	}
	originalWeight := fs.Fields[0].Weight
	originalNgram := fs.ProbabilisticParams.NgramSizes[0]

	s, err := New([]byte("secret"), fs, token.WithKeyID("k1"))
	require.NoError(t, err)
	t.Cleanup(s.Destroy)

	fs.Fields[0].Weight = 999.0
	fs.ProbabilisticParams.NgramSizes[0] = 99

	stored := s.FieldSet()
	assert.InDelta(t, originalWeight, stored.Fields[0].Weight, 1e-9,
		"session must not observe caller mutation of Fields after New")
	assert.Equal(t, originalNgram, stored.ProbabilisticParams.NgramSizes[0],
		"session must not observe caller mutation of NgramSizes after New")
}
