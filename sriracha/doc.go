// Package sriracha implements the Sriracha open wire protocol for
// privacy-preserving person record linkage.
//
// Sriracha enables two institutions to determine whether their records
// refer to the same individual without exposing the underlying PII.
// Records are normalized, tokenized with HMAC-SHA256 (deterministic mode)
// or Bloom filters (probabilistic mode), and compared via a token index.
//
// Import path: go.sriracha.dev/sriracha
//
// Package layout:
//
//   - sriracha           — core types, field constants, interfaces, errors
//   - sriracha/normalize — Unicode normalization pipeline
//   - sriracha/token     — deterministic and probabilistic tokenization
//   - sriracha/fieldset  — FieldSet validation and semver-based version negotiation
//
// Bloom filters are implemented on top of github.com/bits-and-blooms/bitset.
package sriracha
