// Package sriracha is a Go library for privacy-preserving person record
// linkage. It turns raw person records into tokens that two institutions can
// compare across an organizational boundary without transmitting the raw
// identifiers the tokens were derived from.
//
// Records are normalized, then tokenized with HMAC-SHA256 (deterministic
// mode), per-field Bloom filters (probabilistic mode), or a single
// record-level balanced Bloom filter (CLK mode). CLK tokens are the
// recommended form for sharing when per-field scores are not required. How
// the resulting tokens are stored or transported is left to the caller. Token
// derivation is pinned by golden vector tests.
//
// Import path: github.com/ccuetoh/sriracha
//
// # Recommended entry point
//
// Most callers want session.Session. It bundles a token.Tokenizer with a
// FieldSet so you don't have to thread the schema through every tokenize /
// match call, and it checks the FieldSet fingerprint on both sides of every
// comparison:
//
//	// At least token.MinSecretLen (32) bytes, from crypto/rand or a KMS.
//	// session.New wipes the slice it is handed.
//	s, err := session.New(secret, fieldset.DefaultFieldSet(), session.WithKeyID("k1"))
//	if err != nil { ... }
//	defer s.Destroy()
//
//	tok, err := s.TokenizeProbabilistic(record)
//	if err != nil { ... }
//
//	res, err := s.Match(tok, peerToken, token.DefaultMatchPolicy(0.85))
//
// The policy carries the threshold and the evidence floor together. A
// threshold alone does not decide a match: a pair agreeing on one field out
// of eight scores 1.000, and only the floor rejects it. See token.MatchPolicy.
//
// # Security model
//
// Tokens are pseudonymous, not anonymous. The shared secret is the entire
// privacy barrier. Anyone holding it can re-derive the token for any
// candidate value, and person identifiers (names, dates of birth, national
// IDs) come from a small enough universe to enumerate. Tokens therefore stay
// personal data under regimes such as GDPR and must be handled as such.
//
// Sriracha does not defend against frequency analysis or graph matching by a
// token recipient who holds an auxiliary population sample. These are known,
// published attacks on Bloom filter record linkage and no differential
// privacy is claimed.
//
// Read the full threat model, including what each token form leaks and the
// blast radius of a secret compromise, before deploying:
// https://github.com/ccuetoh/sriracha/blob/main/THREAT_MODEL.md
//
// # Errors
//
// Every error the module returns wraps a sentinel, so callers branch with
// errors.Is rather than matching message text. Sentinels live in the package
// that returns them:
//
//   - module root: schema, record and configuration problems
//     (ErrInvalidFieldPath, ErrUnknownField, ErrDuplicateField,
//     ErrRequiredFieldMissing, ErrEmptyValue, ErrInvalidWeight,
//     ErrMissingVersion, ErrInvalidConfig)
//   - normalize: ErrInvalidValue, for a value the pipeline cannot accept
//   - token: secret, tokenizer lifecycle, comparison and calibration
//     failures (ErrSecretTooShort, ErrDestroyed, ErrFingerprintMismatch,
//     ErrKeyIDMismatch, ErrNoComparableFields and the rest)
//   - session: ErrFingerprintDrift, for a token built under another schema
//
// An error carrying a field path is wrapped in a FieldError, which keeps the
// sentinel reachable through errors.Is and the path through errors.As.
// RecordFromMap and fieldset.ValidateRecord report every problem in one pass
// via errors.Join, so range over the joined error or use errors.Is on it
// directly.
//
// # Package layout
//
//   - (module root): core types (FieldPath, RawRecord, tokens, FieldSet),
//     canonical field constants, and the schema error sentinels
//   - normalize: Unicode normalization pipeline
//   - fieldset: the canonical schema, record validation, and presets
//   - token: deterministic and probabilistic tokenization, Equal,
//     DicePerField, Match, MatchCLK, Calibrate
//   - session: recommended high-level facade bundling a Tokenizer with a
//     FieldSet
//
// Bloom filters are implemented on top of github.com/bits-and-blooms/bitset.
//
// Tokens encode to JSON via the standard encoding/json reflection path.
// The Tokenizer is safe for concurrent use until Destroy is called; if you
// forget Destroy, a runtime cleanup wipes the locked secret buffer once the
// Tokenizer becomes unreachable.
package sriracha
