# <img height="70" alt="sriracha_logo" src="https://github.com/user-attachments/assets/8932cc91-8d3a-4f16-8b9b-9e8e8b9cecb2" /> Sriracha
## Privacy-preserving person record linkage (PPRL) library

[![CI](https://github.com/ccuetoh/sriracha/actions/workflows/ci.yml/badge.svg)](https://github.com/ccuetoh/sriracha/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/ccuetoh/sriracha/graph/badge.svg?token=1JRW9RH43K)](https://codecov.io/gh/ccuetoh/sriracha)
[![Go Report Card](https://goreportcard.com/badge/go.sriracha.dev)](https://goreportcard.com/report/go.sriracha.dev)
[![pkg.go.dev](https://pkg.go.dev/badge/go.sriracha.dev/sriracha.svg)](https://pkg.go.dev/go.sriracha.dev/sriracha)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

> **Experimental.** API is unstable. Not production-ready.

## What is Sriracha?

Sriracha is a Go library for privacy-preserving record linkage. Institutions in health, government, and research
routinely need to find shared person records across organizational boundaries — without transmitting raw PII.
Sriracha provides the building blocks: records are normalized and tokenized with a shared secret, producing tokens
that can be compared without exposing the underlying identifiers. Storage is left to the consumer; matching is
available via `token.Equal` (deterministic) and `token.Match` (probabilistic).

The recommended entry point is `session.Session` — it bundles a `Tokenizer` with a `FieldSet` so you
don't have to thread the schema through every call.

## Features

- HMAC-SHA256 deterministic tokenization (exact match) with length-prefixed domain separation
- Bloom filter probabilistic tokenization (fuzzy match, typo-tolerant)
- Unicode normalization pipeline (names with diacritic folding, ISO 8601 dates, addresses, identifiers, email; best-effort phone digit-stripping)
- Canonical v0.1 field set with structured `FieldPath` identifiers and `FieldSet.Fingerprint()` for schema-drift detection
- Weighted aggregate `Score` and threshold `Match` over per-field Dice scores; `token.Calibrate` for picking the threshold from labeled data
- Optional `KeyID` on tokens to surface secret-rotation mismatches
- `Tokenizer` is safe for concurrent use; HMAC instances are pooled; a runtime cleanup wipes the locked secret buffer if you forget `Destroy()`
- Tokens marshal to JSON via `encoding/json` for inspection and transport; `Annotate(fs)` returns a redacted, log-safe view

## Picking a threshold

`token.Match` takes a threshold in `[0, 1]`. Pick it from labeled data, not intuition:

```go
cal, err := token.Calibrate(pairs, fs)   // pairs = []token.LabeledPair
if err != nil { /* … */ }
res, err := token.Match(a, b, fs, cal.OptimalThreshold)
```

`Calibration.ROC` carries precision/recall/F1 at every 0.01 step so you can plot or pick a different operating
point (e.g. precision-at-recall ≥ 0.95) than the F1-optimal default.

## Rotating the secret

Sriracha's comparison helpers reject `KeyID` mismatches outright, so you cannot directly compare a token signed
with `k1` against one signed with `k2`. To rotate without dropping inflight matches:

1. Stand up a second `session.Session` (or `token.Tokenizer`) with the new secret and a new `KeyID`.
2. During the overlap window, **dual-tokenize**: every record produces both an old-key and a new-key token. Store
   both shards.
3. Compare on whichever shard the counterpart still uses.
4. When all counterparts have moved over, retire the old key and drop the old shards.

There is no library helper that "tries both keys" automatically — by design, since silently masking a key
mismatch is what `KeyID` exists to prevent.
