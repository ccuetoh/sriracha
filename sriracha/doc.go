// Package sriracha is a Go library for privacy-preserving person record
// linkage. It produces tokens from raw records that can be compared
// without exposing the underlying PII.
//
// Records are normalized, then tokenized with HMAC-SHA256 (deterministic
// mode) or Bloom filters (probabilistic mode). How the resulting tokens
// are stored or compared is left to the caller.
//
// Import path: go.sriracha.dev/sriracha
//
// # Recommended entry point
//
// Most callers want session.Session — it bundles a token.Tokenizer with a
// FieldSet so you don't have to thread the schema through every Tokenize /
// Match / Equal call:
//
//	s, err := session.New(secret, fieldset.DefaultFieldSet(), token.WithKeyID("k1"))
//	if err != nil { ... }
//	defer s.Destroy()
//	tok, err := s.TokenizeBloom(record)
//
// # Package layout
//
//   - sriracha            — core types (FieldPath, RawRecord, tokens, FieldSet) and canonical field constants
//   - sriracha/normalize  — Unicode normalization pipeline
//   - sriracha/fieldset   — FieldSet validation, record validation, and the canonical v0.1 schema
//   - sriracha/token      — deterministic and probabilistic tokenization, Equal, DicePerField, Score, Match, Calibrate
//   - sriracha/session    — recommended high-level facade bundling a Tokenizer with a FieldSet
//
// Bloom filters are implemented on top of github.com/bits-and-blooms/bitset.
//
// Tokens encode to JSON via the standard encoding/json reflection path.
// The Tokenizer is safe for concurrent use until Destroy is called; if you
// forget Destroy, a runtime cleanup wipes the locked secret buffer once the
// Tokenizer becomes unreachable.
package sriracha
