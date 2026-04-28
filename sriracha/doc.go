// Package sriracha is a Go library for privacy-preserving person record
// linkage. It produces tokens from raw records that can be compared
// without exposing the underlying PII.
//
// Records are normalized, then tokenized with HMAC-SHA256 (deterministic
// mode) or Bloom filters (probabilistic mode). How the resulting tokens
// are exchanged, stored, or compared is left to the caller.
//
// Import path: go.sriracha.dev/sriracha
//
// Package layout:
//
//   - sriracha           — core types, field constants, errors
//   - sriracha/normalize — Unicode normalization pipeline
//   - sriracha/token     — deterministic and probabilistic tokenization
//   - sriracha/fieldset  — FieldSet validation and the canonical v0.1 schema
//
// Bloom filters are implemented on top of github.com/bits-and-blooms/bitset.
package sriracha
