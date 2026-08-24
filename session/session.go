// Package session is the high-level entry point for Sriracha. A Session
// bundles a token.Tokenizer with a FieldSet so callers don't have to thread
// the schema through every tokenize / match call, and so the schema is
// validated up front.
//
// Most callers should reach for session.New rather than constructing the
// underlying Tokenizer directly.
package session

import (
	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/fieldset"
	"github.com/ccuetoh/sriracha/token"
)

// Session pairs a Tokenizer with a validated FieldSet. Construct with New.
//
// Session is safe for concurrent use until Destroy is called; the underlying
// HMAC instances are pooled inside the Tokenizer. Calling any tokenize /
// match method after Destroy is undefined.
type Session interface {
	// FieldSet returns a deep copy of the Session's FieldSet so callers can
	// inspect it without risking mutation of the stored schema.
	FieldSet() sriracha.FieldSet
	// TokenizeDeterministic produces a deterministic token for record using
	// the Session's FieldSet.
	TokenizeDeterministic(record sriracha.RawRecord) (sriracha.DeterministicToken, error)
	// TokenizeProbabilistic produces a per-field probabilistic token for
	// record using the Session's FieldSet. Per-field tokens reveal which
	// fields the record carries and how similar each one is; when per-field
	// scores are not required, prefer TokenizeCLK.
	TokenizeProbabilistic(record sriracha.RawRecord) (sriracha.ProbabilisticToken, error)
	// TokenizeCLK produces a record-level CLK token for record using the
	// Session's FieldSet. CLK is the recommended way to share tokens when
	// per-field scores are not required. See token.Tokenizer.TokenizeCLK.
	TokenizeCLK(record sriracha.RawRecord) (sriracha.CLKToken, error)
	// TokenizeField returns the deterministic 32-byte HMAC for a single
	// (value, path) pair. Useful for stable indexing of one field outside
	// the FieldSet flow; see token.Tokenizer.TokenizeField.
	TokenizeField(value string, path sriracha.FieldPath) ([]byte, error)
	// Equal reports whether a and b are bit-identical. See token.Equal.
	Equal(a, b sriracha.DeterministicToken) bool
	// Match runs the canonical probabilistic comparison against the Session's
	// FieldSet. See token.Match for semantics around absent fields and
	// thresholds.
	Match(a, b sriracha.ProbabilisticToken, threshold float64) (token.MatchResult, error)
	// MatchCLK compares two record-level CLK tokens. See token.MatchCLK for
	// validation and scoring semantics.
	MatchCLK(a, b sriracha.CLKToken, threshold float64) (token.CLKMatchResult, error)
	// ValidateRecord pre-checks record against the Session's FieldSet. See
	// fieldset.ValidateRecord.
	ValidateRecord(record sriracha.RawRecord) []error
	// Destroy wipes the Session's underlying tokenizer. Callers must not
	// share the Session after Destroy.
	Destroy()
}

// session is the default Session implementation backed by a token.Tokenizer
// and a stored FieldSet. The fingerprint of fs is computed once at New time
// and reused on every token; the underlying token.Tokenizer no longer
// recomputes it on each call.
type session struct {
	tok         token.Tokenizer
	fs          sriracha.FieldSet
	fingerprint string
}

// New constructs a Session. It validates fs once with fieldset.Validate and
// returns the resulting validation error (if any) before creating the
// Tokenizer; this lets callers fail fast on a malformed schema without ever
// allocating locked memory. token.New wipes the secret slice when moving it
// into locked memory, so the caller must not reuse it.
//
// fs is deep-copied before being stored, so post-construction mutation of
// the caller's FieldSet (Fields slice or ProbabilisticParams.NgramSizes)
// cannot affect subsequent tokenize / match calls. The FieldSet fingerprint
// is computed once here and stamped onto every token returned by
// TokenizeDeterministic / TokenizeProbabilistic, avoiding a SHA-256 over the
// canonical schema encoding on every call.
//
// opts are forwarded to token.New unchanged.
func New(secret []byte, fs sriracha.FieldSet, opts ...token.Option) (Session, error) {
	if err := fieldset.Validate(fs); err != nil {
		return nil, err
	}
	tok, err := token.New(secret, opts...)
	if err != nil {
		return nil, err
	}
	stored := copyFieldSet(fs)
	return &session{tok: tok, fs: stored, fingerprint: stored.Fingerprint()}, nil
}

func (s *session) FieldSet() sriracha.FieldSet {
	return copyFieldSet(s.fs)
}

// copyFieldSet returns a deep copy of fs. The Fields slice and the
// ProbabilisticParams.NgramSizes slice are freshly allocated so mutation
// of either side leaves the other unaffected; every other FieldSet field
// is a value type and copies via struct assignment.
func copyFieldSet(fs sriracha.FieldSet) sriracha.FieldSet {
	out := sriracha.FieldSet{
		Version:             fs.Version,
		Fields:              append([]sriracha.FieldSpec(nil), fs.Fields...),
		ProbabilisticParams: fs.ProbabilisticParams,
	}
	out.ProbabilisticParams.NgramSizes = append([]int(nil), fs.ProbabilisticParams.NgramSizes...)
	return out
}

func (s *session) TokenizeDeterministic(record sriracha.RawRecord) (sriracha.DeterministicToken, error) {
	tok, err := s.tok.TokenizeDeterministic(record, s.fs)
	if err != nil {
		return tok, err
	}
	tok.FieldSetFingerprint = s.fingerprint
	return tok, nil
}

func (s *session) TokenizeProbabilistic(record sriracha.RawRecord) (sriracha.ProbabilisticToken, error) {
	tok, err := s.tok.TokenizeProbabilistic(record, s.fs)
	if err != nil {
		return tok, err
	}
	tok.FieldSetFingerprint = s.fingerprint
	return tok, nil
}

func (s *session) TokenizeCLK(record sriracha.RawRecord) (sriracha.CLKToken, error) {
	tok, err := s.tok.TokenizeCLK(record, s.fs)
	if err != nil {
		return tok, err
	}
	tok.FieldSetFingerprint = s.fingerprint
	return tok, nil
}

func (s *session) TokenizeField(value string, path sriracha.FieldPath) ([]byte, error) {
	return s.tok.TokenizeField(value, path)
}

func (s *session) Equal(a, b sriracha.DeterministicToken) bool {
	return token.Equal(a, b)
}

func (s *session) Match(a, b sriracha.ProbabilisticToken, threshold float64) (token.MatchResult, error) {
	return token.Match(a, b, s.fs, threshold)
}

func (s *session) MatchCLK(a, b sriracha.CLKToken, threshold float64) (token.CLKMatchResult, error) {
	return token.MatchCLK(a, b, threshold)
}

func (s *session) ValidateRecord(record sriracha.RawRecord) []error {
	return fieldset.ValidateRecord(record, s.fs)
}

func (s *session) Destroy() {
	s.tok.Destroy()
}
