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

// Session pairs a Tokenizer with a validated FieldSet. The zero value is not
// usable; construct with New.
type Session struct {
	tok token.Tokenizer
	fs  sriracha.FieldSet
}

// New constructs a Session. It validates fs once with fieldset.Validate and
// returns the resulting validation error (if any) before creating the
// Tokenizer; this lets callers fail fast on a malformed schema without ever
// allocating locked memory.
//
// opts are forwarded to token.New unchanged.
func New(secret []byte, fs sriracha.FieldSet, opts ...token.Option) (*Session, error) {
	if err := fieldset.Validate(fs); err != nil {
		return nil, err
	}
	tok, err := token.New(secret, opts...)
	if err != nil {
		return nil, err
	}
	return &Session{tok: tok, fs: fs}, nil
}

// FieldSet returns a deep copy of the Session's FieldSet so callers can
// inspect it without risking mutation of the Session's stored schema.
func (s *Session) FieldSet() sriracha.FieldSet {
	out := sriracha.FieldSet{
		Version:     s.fs.Version,
		Fields:      append([]sriracha.FieldSpec(nil), s.fs.Fields...),
		BloomParams: s.fs.BloomParams,
	}
	out.BloomParams.NgramSizes = append([]int(nil), s.fs.BloomParams.NgramSizes...)
	return out
}

// Tokenize produces a deterministic token for record using the Session's
// FieldSet.
func (s *Session) Tokenize(record sriracha.RawRecord) (sriracha.DeterministicToken, error) {
	return s.tok.TokenizeRecord(record, s.fs)
}

// TokenizeBloom produces a probabilistic token for record using the Session's
// FieldSet.
func (s *Session) TokenizeBloom(record sriracha.RawRecord) (sriracha.BloomToken, error) {
	return s.tok.TokenizeRecordBloom(record, s.fs)
}

// TokenizeField returns the deterministic 32-byte HMAC for a single
// (value, path) pair. Useful for stable indexing of one field outside the
// FieldSet flow; see token.Tokenizer.TokenizeField.
func (s *Session) TokenizeField(value string, path sriracha.FieldPath) ([]byte, error) {
	return s.tok.TokenizeField(value, path)
}

// Equal reports whether a and b are bit-identical. See token.Equal.
func (s *Session) Equal(a, b sriracha.DeterministicToken) bool {
	return token.Equal(a, b)
}

// Match runs the canonical probabilistic comparison against the Session's
// FieldSet. See token.Match for semantics around absent fields and
// thresholds.
func (s *Session) Match(a, b sriracha.BloomToken, threshold float64) (token.MatchResult, error) {
	return token.Match(a, b, s.fs, threshold)
}

// ValidateRecord pre-checks record against the Session's FieldSet. See
// fieldset.ValidateRecord.
func (s *Session) ValidateRecord(record sriracha.RawRecord) []error {
	return fieldset.ValidateRecord(record, s.fs)
}

// Destroy wipes the Session's underlying tokenizer. Callers must not share
// the Session after Destroy, and must not call Destroy directly on the
// underlying Tokenizer if they have somehow obtained a reference to it
// (the Session does not expose one).
func (s *Session) Destroy() {
	s.tok.Destroy()
}
