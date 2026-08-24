# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the project is in the `v0.x` series the public API is unstable and may
change in any release.

## [Unreleased]

## [0.2] - 2026-08-13

### Added

- **Breaking wire format, token format v2.** Tokens now carry a `format`
  discriminator (`sriracha/det/2`, `sriracha/bloom/2`, `sriracha/clk/2`)
  that `Equal`, `DicePerField`, `Match`, and `MatchCLK` check before
  comparing. v0.1 tokens and fingerprints are incompatible with this
  release and all parties must re-tokenize with the same version.
- Record-level CLK tokens. `TokenizeCLK` pools every present field into one
  balanced Bloom filter and `MatchCLK` scores two of them. CLK is the
  recommended form for sharing tokens when per-field scores are not
  required, since it exposes no per-field structure, popcounts, or
  presence pattern.
- Golden vector tests that fail on any byte-level drift of normalization
  or token derivation.
- `token.ErrDestroyed`, returned by all tokenize methods once `Destroy` has
  been called. Previously a destroyed tokenizer could silently emit tokens
  keyed with an empty secret.

### Changed (breaking)

- Balanced filters replace BLIP and popcount padding. A balanced filter is
  built at `SizeBits/2`, extended with its complement, and permuted with a
  secret-keyed bijection, so the emitted popcount is exactly `SizeBits/2`
  and leaks nothing about the value. CLK tokens are always balanced.
  Per-field filters default to unbalanced, because balancing compresses
  per-field Dice onto value-dependent baselines and measurably degrades
  weighted multi-field matching; `ProbabilisticConfig.Balanced` opts a
  per-field configuration in. `FlipProbability`, `TargetPopcount`, and
  `HardenedProbabilisticConfig` are removed. Identical values still produce
  identical filters and remain linkable; no differential privacy is
  claimed.
- Presets rebuilt. Fast is 512 bits with 2-grams and 2 hashes, Default is
  1024 bits with 2- and 3-grams and 3 hashes, HighPrecision is 2048 bits
  with 2- and 3-grams and 5 hashes.
- Bloom positions derive from one HMAC per gram using double hashing
  instead of `HashCount` separate HMAC calls, cutting tokenization crypto
  cost by roughly the hash count.
- Q-grams are padded with boundary underscores, so one-rune values now
  produce grams and edge characters carry positional context.
- The HMAC secret is expanded with HKDF-SHA256 into per-purpose subkeys
  for deterministic tokens, Bloom hashing, and the balance permutation.
- Absent optional fields in probabilistic tokens serialize as null instead
  of full-size all-zero filters, shrinking typical token JSON by more than
  half. Asymmetric absence still scores 0 at full weight.
- Normalization strips Unicode format characters (zero width space and
  joiner, bidi marks, soft hyphen) in every field, and name-field
  diacritic stripping now applies only to marks on Latin base characters,
  so Thai, Vietnamese base runes, Arabic, and Indic names keep their
  distinguishing marks.
- `FieldSet.Fingerprint` canonical encoding replaces the hardening
  trailers with a `Balanced` flag. Persisted fingerprints must be
  regenerated.
- The canonical `DefaultFieldSet` version is now `0.2`.

### Changed

- The NCVR and OpenSanctions corpus snapshots moved out of the module onto
  the `testdata-corpus` branch. The bench harness now downloads and caches
  them on first use, so `go get` no longer ships them.
- Values that normalize to the empty string are now treated as absent in both
  tokenize modes. Optional fields keep a nil entry or all-zero filter, and
  required fields return an error. Hardened configs no longer emit noise
  filters for empty values. Tokens produced from records with blank field
  values change as a result.

### Fixed

- `token.New` rejects secrets that are all zero bytes, which usually means the
  slice was wiped by an earlier call and reused. It also returns an error
  instead of panicking when locked memory cannot be allocated.
- `TokenizeProbabilistic` validates `ProbabilisticParams` and returns an error
  for configs that previously caused a divide by zero panic, a makeslice
  panic, or an infinite loop.
- `fieldset.Validate` rejects empty field paths, NaN and infinite weights,
  negative hash counts, and NaN flip probabilities. `ValidateRecord` reports
  required fields that normalize to empty.
- `token.Match` rejects NaN thresholds.
- The README benchmark table had a dangling cell that GitHub did not render.

## [0.1.1] - 2026-08-13

### Changed

- `tokenizeFieldBloom` now reuses `*bitset.BitSet` instances via a
  per-`SizeBits` `sync.Pool` instead of allocating a fresh bitset per
  field. Output is byte-identical; per-token allocations drop by
  `2 × N_fields` after the first call warms the pool.
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

### Security

- Bumped `golang.org/x/text` from v0.31.0 to v0.41.0 to fix
  [GO-2026-5970](https://pkg.go.dev/vuln/GO-2026-5970) (infinite loop on
  invalid input). This raises the minimum Go version to 1.25. Also bumped
  `awnumar/memguard` to v0.23.0 and `bits-and-blooms/bitset` to v1.25.0.

## [0.1.0] - 2026-05-03

Initial public release.

[Unreleased]: https://github.com/ccuetoh/sriracha/compare/v0.1.1...HEAD

[0.1.1]: https://github.com/ccuetoh/sriracha/compare/v0.1.0...v0.1.1

[0.1.0]: https://github.com/ccuetoh/sriracha/releases/tag/v0.1.0
