// Package sriracha is a Go library for privacy-preserving person record
// linkage. It produces tokens from raw records that can be compared
// without exposing the underlying PII.
//
// Records are normalized, then tokenized with HMAC-SHA256 (deterministic
// mode) or Bloom filters (probabilistic mode). How the resulting tokens
// are stored or compared is left to the caller; package token offers
// Equal and DicePerField as ready-made primitives.
//
// Import path: go.sriracha.dev/sriracha
//
// Package layout:
//
//   - sriracha           — core types and field constants
//   - sriracha/normalize — Unicode normalization pipeline
//   - sriracha/token     — deterministic and probabilistic tokenization, comparison helpers
//   - sriracha/fieldset  — FieldSet validation and the canonical v0.1 schema
//
// Bloom filters are implemented on top of github.com/bits-and-blooms/bitset.
//
// Tokens encode to JSON via the encoding/json interface. The Tokenizer is
// safe for concurrent use until Destroy is called.
package sriracha
