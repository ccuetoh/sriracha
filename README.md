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
available via `token.Equal` (deterministic) and `token.DicePerField` (probabilistic).

## Features

- HMAC-SHA256 deterministic tokenization (exact match) with length-prefixed domain separation
- Bloom filter probabilistic tokenization (fuzzy match, typo-tolerant)
- Unicode normalization pipeline (names with diacritic folding, ISO 8601 dates, addresses, identifiers, email; best-effort phone digit-stripping)
- Canonical v0.1 field set with structured `FieldPath` identifiers
- Weighted aggregate `Score` and threshold `Match` over per-field Dice scores
- Optional `KeyID` on tokens to surface secret-rotation mismatches
- `Tokenizer` is safe for concurrent use; HMAC instances are pooled
- Tokens marshal to JSON via `encoding/json` for inspection and transport
