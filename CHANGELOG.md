# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the project is in the `v0.x` series the public API is unstable and may
change in any release.

## [Unreleased]

### Changed

- `TokenizeDeterministic` now allocates one contiguous backing buffer
  (sized `len(fs.Fields) × sha256.Size`) per token and slices into it
  for each present field, replacing the per-field `h.Sum(nil)`
  allocation in `hmacField`. Output is byte-identical; per-token
  allocations drop by `K - 1` (K = number of present fields). All-
  absent tokens skip the backing allocation entirely.
- `tokenizeFieldBloom` now iterates n-grams via an internal callback
  (`eachNgram`) that writes each gram into a shared scratch buffer,
  eliminating both the per-gram string allocation inside `ngrams` and
  the `[]byte(g)` conversion at the callsite. Output is byte-identical.
- `TokenizeProbabilistic` and the BLIP / balanced-filter HMAC stream
  now pass a stack-allocated scratch buffer to `hash.Hash.Sum` instead
  of `nil`, eliminating one 32-byte allocation per hashed gram (and
  per HMAC-stream refill). Output is byte-identical; per-token
  allocation count drops by `N_grams × cfg.HashCount` per field.
- `TokenizeProbabilistic` now allocates one contiguous backing buffer per
  token instead of one byte slice per field. Field bytes are
  byte-identical to before; the per-token allocation count drops by
  N-1 (where N is the number of fields in the FieldSet).
- Removed the `github.com/bits-and-blooms/bloom/v3` runtime dependency
  (and its transitive `github.com/twmb/murmur3`). Probabilistic
  tokenization used the bloom package only to reach its underlying
  bitset (`bloom.New(...).BitSet()`); switched to `bitset.New` directly.
  Output is byte-identical, the dependency surface is smaller, and one
  allocation per field is eliminated.
- `token.Tokenizer.TokenizeDeterministic` and `TokenizeProbabilistic` no
  longer populate `FieldSetFingerprint` on the returned token. Direct
  `token.Tokenizer` callers must set it themselves if they want downstream
  schema-drift detection. `session.Session.TokenizeDeterministic` and
  `TokenizeProbabilistic` set it automatically using a fingerprint
  computed once at `session.New` time — eliminating a redundant SHA-256
  over the canonical FieldSet encoding on every tokenize call.
- `token.Tokenizer.Destroy` now clears the runtime finalizer registered by
  `New`, so the finalizer cannot re-fire on the already-destroyed locked
  buffer after an explicit `Destroy` call. Defensive only — `memguard`'s
  `Destroy` is idempotent today.

### Fixed

- `session.New` now deep-copies the caller's FieldSet. Previously the stored
  Fields and `ProbabilisticParams.NgramSizes` slices aliased the caller's
  backing arrays, so post-construction mutation by the caller would silently
  affect every subsequent tokenize / match call from that session.
- `DicePerField` and `Match` now reject probabilistic tokens whose
  `ProbabilisticParams` differ in `FlipProbability` or `TargetPopcount`.
  Previously these hardening parameters were ignored by the equality gate, so
  a hardened token compared against a non-hardened token would silently
  produce a meaningless Dice score instead of an error.

### Changed (breaking)

- `FieldSet.Fingerprint()` canonical encoding now incorporates
  `ProbabilisticParams.FlipProbability` and `ProbabilisticParams.TargetPopcount`.
  Fingerprints produced by prior versions will not match values produced by
  this version even for otherwise-unchanged FieldSets. Persisted
  `FieldSetFingerprint` values must be regenerated.

## [0.1.0] - 2026-05-03

Initial public release.

[Unreleased]: https://github.com/ccuetoh/sriracha/compare/v0.1.0...HEAD

[0.1.0]: https://github.com/ccuetoh/sriracha/releases/tag/v0.1.0
