# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the project is in the `v0.x` series the public API is unstable and may
change in any release.

## [Unreleased]

### Fixed

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
