// Package session is the high-level entry point for Sriracha. A Session
// bundles a token.Tokenizer with a FieldSet so callers don't have to thread
// the schema through every Tokenize / Match / Equal call, and so the schema
// is validated up front.
//
// Most callers should reach for session.New rather than constructing the
// underlying Tokenizer directly.
package session

import (
	"go.sriracha.dev/fieldset"
	"go.sriracha.dev/sriracha"
	"go.sriracha.dev/token"
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
	// Tokenize produces a deterministic token for record using the Session's
	// FieldSet.
	Tokenize(record sriracha.RawRecord) (sriracha.DeterministicToken, error)
	// TokenizeBloom produces a probabilistic token for record using the
	// Session's FieldSet.
	TokenizeBloom(record sriracha.RawRecord) (sriracha.BloomToken, error)
	// TokenizeField returns the deterministic 32-byte HMAC for a single
	// (value, path) pair. Useful for stable indexing of one field outside
	// the FieldSet flow; see token.Tokenizer.TokenizeField.
	TokenizeField(value string, path sriracha.FieldPath) ([]byte, error)
	// Equal reports whether a and b are bit-identical. See token.Equal.
	Equal(a, b sriracha.DeterministicToken) bool
	// Match runs the canonical probabilistic comparison against the Session's
	// FieldSet. See token.Match for semantics around absent fields and
	// thresholds.
	Match(a, b sriracha.BloomToken, threshold float64) (token.MatchResult, error)
	// ValidateRecord pre-checks record against the Session's FieldSet. See
	// fieldset.ValidateRecord.
	ValidateRecord(record sriracha.RawRecord) []error
	// Destroy wipes the Session's underlying tokenizer. Callers must not
	// share the Session after Destroy.
	Destroy()
}

// session is the default Session implementation backed by a token.Tokenizer
// and a stored FieldSet.
type session struct {
	tok token.Tokenizer
	fs  sriracha.FieldSet
}

// New constructs a Session. It validates fs once with fieldset.Validate and
// returns the resulting validation error (if any) before creating the
// Tokenizer; this lets callers fail fast on a malformed schema without ever
// allocating locked memory.
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
	return &session{tok: tok, fs: fs}, nil
}

func (s *session) FieldSet() sriracha.FieldSet {
	out := sriracha.FieldSet{
		Version:     s.fs.Version,
		Fields:      append([]sriracha.FieldSpec(nil), s.fs.Fields...),
		BloomParams: s.fs.BloomParams,
	}
	out.BloomParams.NgramSizes = append([]int(nil), s.fs.BloomParams.NgramSizes...)
	return out
}

func (s *session) Tokenize(record sriracha.RawRecord) (sriracha.DeterministicToken, error) {
	return s.tok.TokenizeRecord(record, s.fs)
}

func (s *session) TokenizeBloom(record sriracha.RawRecord) (sriracha.BloomToken, error) {
	return s.tok.TokenizeRecordBloom(record, s.fs)
}

func (s *session) TokenizeField(value string, path sriracha.FieldPath) ([]byte, error) {
	return s.tok.TokenizeField(value, path)
}

func (s *session) Equal(a, b sriracha.DeterministicToken) bool {
	return token.Equal(a, b)
}

func (s *session) Match(a, b sriracha.BloomToken, threshold float64) (token.MatchResult, error) {
	return token.Match(a, b, s.fs, threshold)
}

func (s *session) ValidateRecord(record sriracha.RawRecord) []error {
	return fieldset.ValidateRecord(record, s.fs)
}

func (s *session) Destroy() {
	s.tok.Destroy()
}
