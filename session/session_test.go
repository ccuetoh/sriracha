package session

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/token"
)

// testSecret is at least token.MinSecretLen bytes so New accepts it.
const testSecret = "session-test-secret-32-bytes-long"

// testFieldSet is the two-field schema every test session is built from.
// givenWeight is varied to produce a second schema with the same Version and
// field count but a different fingerprint, which is the drift case.
func testFieldSet(givenWeight float64) sriracha.FieldSet {
	return sriracha.FieldSet{
		Version: "v1-test",
		Fields: []sriracha.FieldSpec{
			{Path: sriracha.FieldNameGiven, Required: true, Weight: givenWeight},
			{Path: sriracha.FieldNameFamily, Required: false, Weight: 1.0},
		},
		ProbabilisticParams: sriracha.DefaultProbabilisticConfig(),
	}
}

func newSess(t *testing.T, opts ...Option) *Session {
	t.Helper()
	s, err := New([]byte(testSecret), testFieldSet(2.0), append([]Option{WithKeyID("k1")}, opts...)...)
	require.NoError(t, err)
	t.Cleanup(s.Destroy)
	return s
}

// newDriftSess returns a Session whose schema differs from newSess only in a
// field weight, so its tokens carry a different fingerprint while remaining
// structurally comparable to newSess tokens.
func newDriftSess(t *testing.T) *Session {
	t.Helper()
	s, err := New([]byte(testSecret), testFieldSet(3.0), WithKeyID("k1"))
	require.NoError(t, err)
	t.Cleanup(s.Destroy)
	return s
}

func TestNew(t *testing.T) {
	t.Parallel()

	t.Run("InvalidFieldSetRejected", func(t *testing.T) {
		t.Parallel()
		_, err := New([]byte(testSecret), sriracha.FieldSet{})
		require.Error(t, err, "empty Version must fail validation before allocating locked memory")
	})

	t.Run("EmptySecretRejected", func(t *testing.T) {
		t.Parallel()
		_, err := New(nil, testFieldSet(1.0))
		require.ErrorIs(t, err, token.ErrSecretTooShort)
	})

	t.Run("OptionsApplyBeforeValidation", func(t *testing.T) {
		t.Parallel()
		called := false
		_, err := New([]byte(testSecret), sriracha.FieldSet{}, func(*options) { called = true })
		require.Error(t, err)
		assert.True(t, called, "options must be collected even when the FieldSet is rejected")
	})
}

func TestOptions(t *testing.T) {
	t.Parallel()

	t.Run("WithKeyIDStampsTokens", func(t *testing.T) {
		t.Parallel()
		s := newSess(t)
		tok, err := s.TokenizeDeterministic(sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"})
		require.NoError(t, err)
		assert.Equal(t, "k1", tok.KeyID)
	})

	t.Run("WithTokenOptionsForwards", func(t *testing.T) {
		t.Parallel()
		s, err := New([]byte(testSecret), testFieldSet(2.0), WithTokenOptions(token.WithKeyID("forwarded")))
		require.NoError(t, err)
		t.Cleanup(s.Destroy)
		tok, err := s.TokenizeDeterministic(sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"})
		require.NoError(t, err)
		assert.Equal(t, "forwarded", tok.KeyID)
	})

	t.Run("LaterOptionWins", func(t *testing.T) {
		t.Parallel()
		s, err := New([]byte(testSecret), testFieldSet(2.0), WithKeyID("first"), WithTokenOptions(token.WithKeyID("second")))
		require.NoError(t, err)
		t.Cleanup(s.Destroy)
		tok, err := s.TokenizeDeterministic(sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"})
		require.NoError(t, err)
		assert.Equal(t, "second", tok.KeyID, "options accumulate in order")
	})

	t.Run("WithStrictFingerprintSetsFlag", func(t *testing.T) {
		t.Parallel()
		s := newSess(t, WithStrictFingerprint())
		assert.True(t, s.strictFingerprint)
	})
}

func TestSession_CheckFingerprint(t *testing.T) {
	t.Parallel()

	lax := newSess(t)
	strict := newSess(t, WithStrictFingerprint())

	cases := []struct {
		name    string
		sess    *Session
		fp      string
		wantErr bool
	}{
		{name: "MatchingFingerprintPasses", sess: lax, fp: lax.fingerprint},
		{name: "EmptyFingerprintPassesByDefault", sess: lax, fp: ""},
		{name: "DifferentFingerprintFails", sess: lax, fp: "deadbeef", wantErr: true},
		{name: "MatchingFingerprintPassesUnderStrict", sess: strict, fp: strict.fingerprint},
		{name: "EmptyFingerprintFailsUnderStrict", sess: strict, fp: "", wantErr: true},
		{name: "DifferentFingerprintFailsUnderStrict", sess: strict, fp: "deadbeef", wantErr: true},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.sess.checkFingerprint("a", tc.fp)
			if !tc.wantErr {
				assert.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, ErrFingerprintDrift)
			assert.Contains(t, err.Error(), "token a")
		})
	}

	t.Run("SecondTokenIsReported", func(t *testing.T) {
		t.Parallel()
		err := lax.checkPair(lax.fingerprint, "deadbeef")
		require.ErrorIs(t, err, ErrFingerprintDrift)
		assert.Contains(t, err.Error(), "token b")
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

	t.Run("DefaultPolicyMatches", func(t *testing.T) {
		t.Parallel()
		res, err := s.Match(a, b, token.DefaultMatchPolicy(0.5))
		require.NoError(t, err)
		assert.True(t, res.IsMatch)
		assert.Equal(t, 2, res.ComparableFields)
	})

	t.Run("ZeroPolicyIsThresholdOnly", func(t *testing.T) {
		t.Parallel()
		res, err := s.Match(a, b, token.MatchPolicy{Threshold: 0.5})
		require.NoError(t, err)
		assert.True(t, res.IsMatch)
	})

	t.Run("InvalidPolicyErrors", func(t *testing.T) {
		t.Parallel()
		_, err := s.Match(a, b, token.MatchPolicy{Threshold: 2})
		require.ErrorIs(t, err, token.ErrInvalidThreshold)
	})

	t.Run("DriftedTokensRejected", func(t *testing.T) {
		t.Parallel()
		drift := newDriftSess(t)
		da, err := drift.TokenizeProbabilistic(sriracha.RawRecord{
			sriracha.FieldNameGiven:  "Christopher",
			sriracha.FieldNameFamily: "Smith",
		})
		require.NoError(t, err)
		db, err := drift.TokenizeProbabilistic(sriracha.RawRecord{
			sriracha.FieldNameGiven:  "Cristopher",
			sriracha.FieldNameFamily: "Smith",
		})
		require.NoError(t, err)

		// The two drifted tokens agree with each other, so token.Match
		// scores them without complaint. Only the Session catches it.
		_, err = token.Match(da, db, s.FieldSet(), token.MatchPolicy{})
		require.NoError(t, err)

		_, err = s.Match(da, db, token.MatchPolicy{})
		require.ErrorIs(t, err, ErrFingerprintDrift)
	})

	t.Run("UnstampedTokenRejectedUnderStrict", func(t *testing.T) {
		t.Parallel()
		strict := newSess(t, WithStrictFingerprint())
		bare := a
		bare.FieldSetFingerprint = ""
		_, err := strict.Match(bare, b, token.MatchPolicy{})
		require.ErrorIs(t, err, ErrFingerprintDrift)
	})
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

	t.Run("DriftedTokensRejected", func(t *testing.T) {
		t.Parallel()
		drifted := a
		drifted.FieldSetFingerprint = "deadbeef"
		_, err := s.MatchCLK(drifted, b, 0.9)
		require.ErrorIs(t, err, ErrFingerprintDrift)
	})
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

	t.Run("SameSchemaEqual", func(t *testing.T) {
		t.Parallel()
		eq, err := s.Equal(a, b)
		require.NoError(t, err)
		assert.True(t, eq)
	})

	t.Run("DriftedTokensRejected", func(t *testing.T) {
		t.Parallel()
		drift := newDriftSess(t)
		da, err := drift.TokenizeDeterministic(rec)
		require.NoError(t, err)
		db, err := drift.TokenizeDeterministic(rec)
		require.NoError(t, err)

		// The drifted pair is bit-identical to itself, so token.Equal
		// reports a clean match. Only the Session catches the schema.
		eq, err := token.Equal(da, db)
		require.NoError(t, err)
		assert.True(t, eq)

		eq, err = s.Equal(da, db)
		require.ErrorIs(t, err, ErrFingerprintDrift)
		assert.False(t, eq)
	})

	t.Run("UnstampedTokenPassesByDefault", func(t *testing.T) {
		t.Parallel()
		bareA, bareB := a, b
		bareA.FieldSetFingerprint = ""
		bareB.FieldSetFingerprint = ""
		eq, err := s.Equal(bareA, bareB)
		require.NoError(t, err)
		assert.True(t, eq)
	})

	t.Run("UnstampedTokenRejectedUnderStrict", func(t *testing.T) {
		t.Parallel()
		strict := newSess(t, WithStrictFingerprint())
		bareA := a
		bareA.FieldSetFingerprint = ""
		_, err := strict.Equal(bareA, b)
		require.ErrorIs(t, err, ErrFingerprintDrift)
	})
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
	err := s.ValidateRecord(sriracha.RawRecord{
		sriracha.FieldNameGiven: "Alice",
	})
	assert.NoError(t, err)

	err = s.ValidateRecord(sriracha.RawRecord{
		sriracha.FieldNameFamily: "Smith",
	})
	require.Error(t, err, "missing required field must surface")
	assert.ErrorIs(t, err, sriracha.ErrRequiredFieldMissing)
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

	// Missing required field means token.* returns an error without
	// producing useful token bytes. Session must not stamp the fingerprint
	// onto an error result.
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
	fs := testFieldSet(2.0)
	originalWeight := fs.Fields[0].Weight
	originalNgram := fs.ProbabilisticParams.NgramSizes[0]

	s, err := New([]byte(testSecret), fs, WithKeyID("k1"))
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
