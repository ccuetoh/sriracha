// Package session is the high-level entry point for Sriracha. A Session
// bundles a token.Tokenizer with a FieldSet so callers don't have to thread
// the schema through every tokenize / match call, and so the schema is
// validated up front.
//
// A Session is also the only place the FieldSet fingerprint is checked on the
// way in. Tokens carry the fingerprint of the schema that produced them, and
// a Session compares that against its own before comparing anything else. Two
// tokens minted under an older schema agree with each other, so a plain
// token.Match scores them happily under whatever weights the reader now has;
// a Session refuses them with ErrFingerprintDrift instead.
//
// Most callers should reach for session.New rather than constructing the
// underlying Tokenizer directly.
package session

import (
	"errors"
	"fmt"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/fieldset"
	"github.com/ccuetoh/sriracha/token"
)

// ErrFingerprintDrift reports a token built from a different FieldSet than
// the Session holds. It is a schema error, not a verdict: the tokens were
// never comparable under this schema, so nothing is said about whether the
// two records describe the same person.
var ErrFingerprintDrift = errors.New("session: FieldSetFingerprint drift")

// Option configures a Session at construction time.
type Option func(*options)

// options is the accumulated configuration a Session is built from.
type options struct {
	tokenOpts         []token.Option
	strictFingerprint bool
}

// WithKeyID labels every token the Session emits with the given key
// identifier. Comparison uses it to surface post-rotation mismatches. It
// forwards to token.WithKeyID, so the common case does not need an import of
// package token.
func WithKeyID(id string) Option {
	return func(o *options) { o.tokenOpts = append(o.tokenOpts, token.WithKeyID(id)) }
}

// WithTokenOptions forwards token.Option values to the underlying Tokenizer.
// It is the escape hatch for tokenizer knobs package session does not
// re-export. Options accumulate in the order given, after any earlier
// WithKeyID.
func WithTokenOptions(opts ...token.Option) Option {
	return func(o *options) { o.tokenOpts = append(o.tokenOpts, opts...) }
}

// WithStrictFingerprint makes the Session also reject tokens that carry no
// FieldSetFingerprint at all.
//
// By default an unstamped token passes the drift check. Only a Session stamps
// the fingerprint, so tokens minted by a bare token.Tokenizer carry none, and
// that is a supported flow: an absent fingerprint means unknown, not
// different. Set this when every peer is known to tokenize through a Session,
// which makes an unstamped token evidence of a stale or hand-built producer
// rather than of a low-level caller.
func WithStrictFingerprint() Option {
	return func(o *options) { o.strictFingerprint = true }
}

// Session pairs a Tokenizer with a validated FieldSet. Construct with New.
//
// The fingerprint of the FieldSet is computed once at New time, stamped onto
// every token the Session emits, and checked against every token handed to
// Equal, Match and MatchCLK.
//
// Session is safe for concurrent use until Destroy is called; the underlying
// HMAC instances are pooled inside the Tokenizer. Calling any tokenize /
// match method after Destroy is undefined.
type Session struct {
	tok               *token.Tokenizer
	fs                sriracha.FieldSet
	fingerprint       string
	strictFingerprint bool
}

// New constructs a Session. It validates fs once with FieldSet.Validate and
// returns the resulting validation error (if any) before creating the
// Tokenizer; this lets callers fail fast on a malformed schema without ever
// allocating locked memory. token.New wipes the secret slice when moving it
// into locked memory, so the caller must not reuse it. Building a second
// Session from the same slice needs a copy taken before the first call.
//
// fs is deep-copied before being stored, so post-construction mutation of
// the caller's FieldSet (Fields slice or ProbabilisticParams.NgramSizes)
// cannot affect subsequent tokenize / match calls. The FieldSet fingerprint
// is computed once here and stamped onto every token returned by
// TokenizeDeterministic / TokenizeProbabilistic / TokenizeCLK, avoiding a
// SHA-256 over the canonical schema encoding on every call.
func New(secret []byte, fs sriracha.FieldSet, opts ...Option) (*Session, error) {
	var cfg options
	for _, opt := range opts {
		opt(&cfg)
	}
	if err := fs.Validate(); err != nil {
		return nil, err
	}
	tok, err := token.New(secret, cfg.tokenOpts...)
	if err != nil {
		return nil, err
	}
	stored := copyFieldSet(fs)
	return &Session{
		tok:               tok,
		fs:                stored,
		fingerprint:       stored.Fingerprint(),
		strictFingerprint: cfg.strictFingerprint,
	}, nil
}

// FieldSet returns a deep copy of the Session's FieldSet so callers can
// inspect it without risking mutation of the stored schema.
func (s *Session) FieldSet() sriracha.FieldSet {
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

// checkFingerprint reports whether a token stamped fp may be compared under
// this Session's FieldSet. label names the token in the error so a caller can
// tell which side drifted.
//
// An empty fp passes unless WithStrictFingerprint is set. token.Equal and
// token.Match already reject two tokens whose fingerprints differ from each
// other; this is the check they cannot make, because they have no schema of
// their own to compare against.
func (s *Session) checkFingerprint(label, fp string) error {
	if fp == "" {
		if !s.strictFingerprint {
			return nil
		}
		return fmt.Errorf("%w: token %s carries no fingerprint and the Session is strict", ErrFingerprintDrift, label)
	}
	if fp != s.fingerprint {
		return fmt.Errorf("%w: token %s has %q, Session has %q", ErrFingerprintDrift, label, fp, s.fingerprint)
	}
	return nil
}

// checkPair applies checkFingerprint to both sides of a comparison, a first.
func (s *Session) checkPair(a, b string) error {
	if err := s.checkFingerprint("a", a); err != nil {
		return err
	}
	return s.checkFingerprint("b", b)
}

// TokenizeDeterministic produces a deterministic token for record using the
// Session's FieldSet.
func (s *Session) TokenizeDeterministic(record sriracha.RawRecord) (sriracha.DeterministicToken, error) {
	tok, err := s.tok.TokenizeDeterministic(record, s.fs)
	if err != nil {
		return tok, err
	}
	tok.FieldSetFingerprint = s.fingerprint
	return tok, nil
}

// TokenizeProbabilistic produces a per-field probabilistic token for record
// using the Session's FieldSet. Per-field tokens reveal which fields the
// record carries and how similar each one is; when per-field scores are not
// required, prefer TokenizeCLK.
func (s *Session) TokenizeProbabilistic(record sriracha.RawRecord) (sriracha.ProbabilisticToken, error) {
	tok, err := s.tok.TokenizeProbabilistic(record, s.fs)
	if err != nil {
		return tok, err
	}
	tok.FieldSetFingerprint = s.fingerprint
	return tok, nil
}

// TokenizeCLK produces a record-level CLK token for record using the
// Session's FieldSet. CLK is the recommended way to share tokens when
// per-field scores are not required. See token.Tokenizer.TokenizeCLK.
func (s *Session) TokenizeCLK(record sriracha.RawRecord) (sriracha.CLKToken, error) {
	tok, err := s.tok.TokenizeCLK(record, s.fs)
	if err != nil {
		return tok, err
	}
	tok.FieldSetFingerprint = s.fingerprint
	return tok, nil
}

// TokenizeField returns the deterministic 32-byte HMAC for a single
// (value, path) pair. Useful for stable indexing of one field outside the
// FieldSet flow; see token.Tokenizer.TokenizeField.
func (s *Session) TokenizeField(value string, path sriracha.FieldPath) ([]byte, error) {
	return s.tok.TokenizeField(value, path)
}

// Equal reports whether a and b are bit-identical. Both tokens are first
// checked against the Session's FieldSet fingerprint, which returns
// ErrFingerprintDrift for a token built from a different schema. The
// remaining error cases report tokens that are not comparable for reasons
// visible to token.Equal alone; see token.Equal.
func (s *Session) Equal(a, b sriracha.DeterministicToken) (bool, error) {
	if err := s.checkPair(a.FieldSetFingerprint, b.FieldSetFingerprint); err != nil {
		return false, err
	}
	return token.Equal(a, b)
}

// Match runs the canonical probabilistic comparison against the Session's
// FieldSet. Both tokens are first checked against the Session's FieldSet
// fingerprint, which returns ErrFingerprintDrift for a token built from a
// different schema. See token.Match for semantics around absent fields and
// the policy decision.
//
// policy carries the threshold and the evidence floor together, because a
// threshold alone does not decide a match: a pair that agrees on one field
// out of eight scores 1.000. token.DefaultMatchPolicy(threshold) is the
// recommended value and supplies a floor of two comparable fields. A caller
// that wants nothing but the threshold passes
// token.MatchPolicy{Threshold: threshold}, which applies no floor and matches
// the pre-policy behavior.
func (s *Session) Match(a, b sriracha.ProbabilisticToken, policy token.MatchPolicy) (token.MatchResult, error) {
	if err := s.checkPair(a.FieldSetFingerprint, b.FieldSetFingerprint); err != nil {
		return token.MatchResult{}, err
	}
	return token.Match(a, b, s.fs, policy)
}

// MatchCLK compares two record-level CLK tokens. Both tokens are first
// checked against the Session's FieldSet fingerprint, which returns
// ErrFingerprintDrift for a token built from a different schema. See
// token.MatchCLK for validation and scoring semantics.
//
// The threshold is a bare float64 rather than a token.MatchPolicy because a
// CLK folds the whole record into one filter and keeps no field structure, so
// there is no per-field evidence to floor.
func (s *Session) MatchCLK(a, b sriracha.CLKToken, threshold float64) (token.CLKMatchResult, error) {
	if err := s.checkPair(a.FieldSetFingerprint, b.FieldSetFingerprint); err != nil {
		return token.CLKMatchResult{}, err
	}
	return token.MatchCLK(a, b, threshold)
}

// ValidateRecord pre-checks record against the Session's FieldSet. See
// fieldset.ValidateRecord.
func (s *Session) ValidateRecord(record sriracha.RawRecord) error {
	return fieldset.ValidateRecord(record, s.fs)
}

// Destroy wipes the Session's underlying tokenizer. Callers must not share
// the Session after Destroy.
func (s *Session) Destroy() {
	s.tok.Destroy()
}
