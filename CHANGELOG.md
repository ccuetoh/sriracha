# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the project is in the `v0.x` series the public API is unstable and may
change in any release.

## [Unreleased]

This is an API break, not a wire break. Token derivation, the format
discriminators (`sriracha/det/2`, `sriracha/bloom/2`, `sriracha/clk/2`), the
HKDF info strings and the `FieldSet.Fingerprint` canonical encoding are all
unchanged, and the golden vectors pass byte-unchanged. Deterministic,
per-field probabilistic and CLK tokens minted by v0.2, and fingerprints
persisted from v0.2, stay valid and comparable. The one exception is a
FieldSet carrying non-`sriracha` org paths; see the normalization entry under
Changed (breaking).

Every signature that moved is listed below with what a v0.2 caller has to do.

### Added

- `token.MatchPolicy` and `token.DefaultMatchPolicy(threshold)`. A policy
  carries the threshold together with an evidence floor
  (`MinComparableFields`, `MinComparableWeight`). The floors gate `IsMatch`
  only: they never change `Score`, and a pair below the floor is a non-match,
  never an error. `DefaultMatchPolicy` sets a floor of two comparable fields.
  The zero `MatchPolicy` applies no floor.
- `MatchResult.ComparableWeight` (JSON `comparable_weight`), the total weight
  of the fields behind `Score`, alongside the existing `ComparableFields`.
- `token.MinSecretLen` (32, the HKDF-SHA256 PRK width), `token.ErrSecretTooShort`
  and `token.ErrSecretAllZero`.
- `token.ErrNoContributingFields`, returned by `TokenizeCLK` when no field
  contributes to the filter.
- Comparison and calibration sentinels in package `token`:
  `ErrFormatMismatch`, `ErrFieldSetVersionMismatch`, `ErrKeyIDMismatch`,
  `ErrFingerprintMismatch`, `ErrParamsMismatch`, `ErrFieldCountMismatch`,
  `ErrFilterLengthMismatch`, `ErrInvalidThreshold`, `ErrNoComparableFields`,
  `ErrNoLabeledPairs`, `ErrAllPairsExcluded`, `ErrNoPositiveF1`.
- `session.Option` with `session.WithKeyID`, `session.WithTokenOptions` and
  `session.WithStrictFingerprint`. `WithKeyID` forwards to `token.WithKeyID`,
  so the common case no longer needs an import of package `token`.
  `WithStrictFingerprint` additionally rejects tokens that carry no
  fingerprint at all, which by default are accepted as unknown.
- `session.ErrFingerprintDrift`.
- Schema, record and configuration sentinels in the module root:
  `ErrInvalidFieldPath`, `ErrUnknownField`, `ErrDuplicateField`,
  `ErrRequiredFieldMissing`, `ErrEmptyValue`, `ErrInvalidWeight`,
  `ErrMissingVersion`, `ErrInvalidConfig`. Errors that concern one field are
  wrapped in the new `sriracha.FieldError{Path, Err}`, which keeps the
  sentinel reachable through `errors.Is` and the path through `errors.As`.
- `normalize.ErrInvalidValue`, wrapped by every normalization format failure.
- `sriracha.FieldSet.Validate()` and `sriracha.ProbabilisticConfig.Validate()`.
- `sriracha.OrgSriracha` and `FieldPath.IsCanonical()`.
- `Calibration.ExcludedPairs`, the number of labeled pairs the evidence floor
  kept out of the sweep.
- Runnable godoc examples for `session`, `token`, `fieldset` and `normalize`,
  so pkg.go.dev renders working code for the entry points.
- `THREAT_MODEL.md` (parties, what each token form leaks, attacks in and out
  of scope, secret custody, blast radius), `RELEASING.md` (tag and Go version
  policy) and `GOVERNANCE.md` (single maintainer, self-review gap,
  succession). `SECURITY.md` now states which tags are supported.

### Changed (breaking)

- `token.New` returns `*token.Tokenizer` instead of the `token.Tokenizer`
  interface, and the interface is deleted. The import-path spelling
  `token.Tokenizer` is unchanged, so most call sites compile as-is; code that
  stored one in an interface-typed field or accepted one as an interface
  parameter now holds a pointer, and code that needed an interface must
  declare its own.
- `session.New(secret, fs, opts ...token.Option) (Session, error)` becomes
  `session.New(secret, fs, opts ...session.Option) (*session.Session, error)`.
  The `session.Session` interface is deleted. Replace
  `token.WithKeyID("k1")` with `session.WithKeyID("k1")`; any other tokenizer
  option goes through `session.WithTokenOptions(token.WithX(...))`.
- The `./mock` package and `.mockery.yml` are deleted along with the two
  interfaces. There is nothing left to mock: construct a real
  `*token.Tokenizer` or `*session.Session` with a test secret. The
  `task generate` and `task generate:check` targets are replaced by
  `task generated`, which checks formatting and `go generate` output.
- `token.New` and `session.New` reject secrets shorter than
  `token.MinSecretLen` (32) with `token.ErrSecretTooShort`. This subsumes the
  v0.2 "secret must not be empty" error. Source 32 bytes from `crypto/rand`,
  a KMS or an env var, never a passphrase literal. Both constructors wipe the
  slice they are handed, so building two objects from one secret needs a
  `bytes.Clone` per constructor.
- `token.Match(a, b, fs, threshold float64)` becomes
  `token.Match(a, b, fs, policy token.MatchPolicy)`, and
  `session.Match(a, b, threshold float64)` becomes
  `session.Match(a, b, policy token.MatchPolicy)`. Pass
  `token.MatchPolicy{Threshold: t}` to reproduce v0.2 behavior exactly.
  Passing `token.DefaultMatchPolicy(t)` is the recommendation and will turn
  some v0.2 matches into non-matches: a pair agreeing on one field out of
  eight scores 1.000, and only the floor rejects it. `Score` is unaffected by
  either floor. `session.MatchCLK` keeps a bare `threshold float64`, because
  a CLK folds the whole record into one filter and keeps no field structure
  to floor.
- `token.Equal` and `session.Session.Equal` return `(bool, error)` instead of
  `bool`. Tokens that are not comparable at all (format, `FieldSetVersion`,
  `KeyID`, fingerprint, or field count disagreement) used to report a plain
  `false`; they now return `(false, <sentinel>)`. A genuine value mismatch is
  still `(false, nil)`. Callers that only want the verdict write
  `eq, _ := ...`.
- `token.Equal` now reports two tokens with no present field on either side as
  `(false, ErrNoComparableFields)`. v0.2 reported `true`, which said any two
  empty records are the same person. This is the one case in the release where
  the bool itself changes, and `eq, _ := ...` absorbs it silently: a token from
  an empty record under `DefaultFieldSet` hits it, since every canonical field
  is optional. Call sites that compare possibly-empty records should check
  `errors.Is(err, token.ErrNoComparableFields)` rather than discard the error.
- `token.Score` is deleted. It had no non-test callers and treated absent
  fields differently from `Match`. Use `token.Match(...)` and read
  `MatchResult.Score`.
- `token.Calibrate(pairs, fs)` becomes
  `token.Calibrate(pairs, fs, policy token.MatchPolicy)`. `policy.Threshold`
  is ignored (Calibrate returns one); the floors exclude low-evidence pairs
  from the sweep and the count lands in `Calibration.ExcludedPairs`. Pass
  `token.MatchPolicy{}` to sweep every pair as v0.2 did.
- `token.ROCPoint` is renamed `token.PRPoint` and `Calibration.ROC` is renamed
  `Calibration.PR` (JSON `roc` becomes `pr`). The curve was always precision
  against recall, never a true ROC.
- `Calibrate` now returns the midpoint of the longest contiguous plateau of
  maximal F1 instead of the lowest threshold reaching it, so the returned
  threshold sits in the middle of the stable region rather than on its edge.
  A threshold pinned from v0.2 `Calibrate` output changes for identical data.
  Recalibrate rather than porting the number across. An all-zero F1 sweep is
  now `ErrNoPositiveF1` instead of a silent 0.
- `sriracha.MustParsePath` is renamed `sriracha.MustParseFieldPath`. Pure
  rename, same panic-at-init contract.
- `sriracha.AnnotatedToken.Version` is renamed `FieldSetVersion` and its JSON
  tag changes from `version` to `field_set_version`, matching the token
  structs. This affects `Annotate` output only, not tokens on the wire.
  `FieldSet.Version` is untouched.
- `sriracha.RecordFromMap(m, fs)` returns `(RawRecord, error)` instead of
  `(RawRecord, []error)`. The error is an `errors.Join` of `FieldError`
  leaves and the partial record is still returned. Replace
  `if len(errs) > 0` with `if err != nil`, and branch with `errors.Is` /
  `errors.As` instead of ranging the slice.
- `fieldset.ValidateRecord(record, fs)` and `session.Session.ValidateRecord`
  return `error` instead of `[]error`, joined the same way. Both still report
  every problem in one pass, now in a deterministic order.
- `fieldset.Validate(fs)` is deleted. Call the method `fs.Validate()` on
  `sriracha.FieldSet`, which is where the checks now live;
  `sriracha.ProbabilisticConfig.Validate()` holds the parameter checks that
  `fieldset` and `token` previously duplicated.
- Field-specific normalization now runs only for paths in the canonical
  `sriracha` org. Any other org gets the shared pipeline (UTF-8 repair,
  format-character stripping, NFKD, lowercase, whitespace collapse) and
  nothing else. A path such as `acme::identifier::mrn` no longer has hyphens,
  dots and spaces stripped, and `acme::date::admission` is no longer required
  to be ISO 8601. Sriracha cannot know what a custom org means by its
  namespaces, and silently rewriting those values was the bug. If your
  FieldSet declares non-`sriracha` paths in canonical-looking namespaces,
  normalize those values yourself before tokenizing, and re-tokenize both
  sides. Canonical `sriracha::` paths are unaffected and produce byte-
  identical tokens.
- `session.Equal`, `session.Match` and `session.MatchCLK` reject tokens whose
  `FieldSetFingerprint` differs from the Session's with
  `ErrFingerprintDrift`. Two tokens minted under an older schema agree with
  each other, so `token.Match` scores them happily under whatever weights the
  reader now holds; a Session refuses them. Comparisons that silently scored
  across a schema change in v0.2 now error. Re-tokenize under the current
  schema, or construct a Session holding the old FieldSet.
- Error messages changed throughout. Every error now wraps a sentinel and
  carries its package prefix (`token: `, `normalize: `). Callers matching on
  message text must switch to `errors.Is`.

### Changed

- `README.md` installs with `go get github.com/ccuetoh/sriracha`. The
  documented `@v0.2` never resolved: Go module versions require
  `vMAJOR.MINOR.PATCH`. See `RELEASING.md`.
- `README.md` and the package documentation no longer describe tokens as
  hiding the underlying PII. Tokens are pseudonymous, not anonymous, and both
  now link the threat model. The package documentation also gained a security
  model section and a map of where the error sentinels live, since pkg.go.dev
  readers never see repository markdown.
- `Calibration` documents that F1, Precision and Recall are in-sample
  training metrics, optimistic by construction, and need a held-out set
  before being quoted.
- The examples now source their secret from `SRIRACHA_SECRET` (plus
  `SRIRACHA_SECRET_OLD` and `SRIRACHA_SECRET_NEW` for key rotation) with a
  `crypto/rand` fallback, instead of shipping short literals.
  `examples/probabilistic` demonstrates the evidence floor,
  `examples/custom-fieldset` demonstrates the org gate in both directions,
  and `examples/calibration` hands one policy to both `Calibrate` and
  `Match`.
- The `fieldset` registry documentation no longer claims per-field filters are
  balanced by default (they are not) or that weights feed `token.Score`
  (which is deleted).
- CI's `generated` job no longer installs mockery; it checks formatting and
  `go generate` output. The job name is unchanged.

### Fixed

- `ProbabilisticToken.String()` counted an empty non-nil field slice as
  present, which is what a JSON round trip produces. Field presence is now
  `len(field) > 0` everywhere, in `String`, `Annotate`, `Equal`,
  `DicePerField` and `Match`.
- `RecordFromMap` and `ValidateRecord` reported problems in map iteration
  order, so the same bad input produced different messages on different runs.
  Both are deterministic now: `RecordFromMap` reports in sorted key order, and
  `ValidateRecord` reports schema problems in `FieldSet` declaration order
  followed by unknown paths in sorted order.
- `examples/deterministic` re-tokenized its records and discarded the errors,
  then printed results in map order. `examples/tokenizer` emitted
  probabilistic tokens with no `FieldSetFingerprint`; a direct
  `token.Tokenizer` caller owns that stamp, and the example now sets it and
  shows drift being caught.

### Security

- Secrets shorter than 32 bytes are rejected. The secret is the entire
  privacy barrier and HKDF-SHA256 extracts a 32-byte PRK, so a shorter secret
  is the weakest link in the system by construction.
- `token.DefaultMatchPolicy` exists because a threshold alone is not a
  decision. Two records agreeing on nothing but a common surname score 1.000;
  the evidence floor is what stops that from being reported as a match.
- A Session refuses to score tokens minted under a schema it does not hold,
  and `WithStrictFingerprint` refuses unstamped tokens, which is the defense
  against a peer downgrading to a bare `token.Tokenizer`.
- `THREAT_MODEL.md` states what each token form leaks, names frequency
  analysis and graph matching against an auxiliary population as known
  undefended attacks, and records that tokens are pseudonymized personal data
  rather than anonymized data.
- No dependency changes in this release.

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

[Unreleased]: https://github.com/ccuetoh/sriracha/compare/v0.2...HEAD

[0.2]: https://github.com/ccuetoh/sriracha/compare/v0.1.1...v0.2

[0.1.1]: https://github.com/ccuetoh/sriracha/compare/v0.1.0...v0.1.1

[0.1.0]: https://github.com/ccuetoh/sriracha/releases/tag/v0.1.0
