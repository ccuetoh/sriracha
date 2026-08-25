# <img height="70" alt="sriracha_logo" src="https://github.com/user-attachments/assets/8932cc91-8d3a-4f16-8b9b-9e8e8b9cecb2" /> Sriracha
## Privacy-preserving person record linkage (PPRL) library

[![CI](https://github.com/ccuetoh/sriracha/actions/workflows/ci.yml/badge.svg)](https://github.com/ccuetoh/sriracha/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/ccuetoh/sriracha/graph/badge.svg?token=1JRW9RH43K)](https://codecov.io/gh/ccuetoh/sriracha)
[![OpenSSF Best Practices](https://www.bestpractices.dev/projects/12741/badge)](https://www.bestpractices.dev/projects/12741)
[![pkg.go.dev](https://pkg.go.dev/badge/github.com/ccuetoh/sriracha.svg)](https://pkg.go.dev/github.com/ccuetoh/sriracha)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

> **API is unstable**. Not production-ready.


Sriracha is a Go library for privacy-preserving record linkage. It lets institutions
link person records across organizational boundaries without transmitting the raw
identifiers. Records are normalized and tokenized under a shared secret, and the
resulting tokens are compared instead of the source values. Sriracha provides the
building blocks for privacy-first transports; it is not a transport itself.

Tokens are pseudonymous, not anonymous. The shared secret is the whole privacy
barrier: anyone holding it can re-derive the token for any candidate value, so
tokens remain personal data and must be handled as such. Read
[`THREAT_MODEL.md`](THREAT_MODEL.md) before deploying.

## Features

- Deterministic tokenization using HMAC-SHA256
- Probabilistic tokenization with Sørensen–Dice matching
- Record-level CLK tokens, the recommended form for sharing, always balanced so the filter popcount reveals nothing
- Optional balanced mode for per-field filters
- Match policies that pair a threshold with an evidence floor, so a pair agreeing on one field alone is not reported as a match
- Threshold calibration from labeled pairs, reported with the full precision-recall curve
- Schema drift detection: a `Session` refuses tokens minted under a different `FieldSet`
- Unicode normalization pipeline
- Canonical field set with support for extended schemas
- Errors wrap sentinels and carry the field path, so callers branch with `errors.Is` / `errors.As`
- Token derivation pinned by golden vector tests

## Installation

Requires Go 1.25+

```bash
go get github.com/ccuetoh/sriracha
```

## Quickstart
```go
package main

import (
	"fmt"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/fieldset"
	"github.com/ccuetoh/sriracha/session"
	"github.com/ccuetoh/sriracha/token"
)

func main() {
	// At least token.MinSecretLen (32) bytes, from crypto/rand or a KMS.
	secret := []byte("demo-secret-32-bytes-of-key-mats")

	s, _ := session.New(secret, fieldset.DefaultFieldSet())
	defer s.Destroy()

	// Deterministic tokenization
	tokA, _ := s.TokenizeDeterministic(sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Alice",
		sriracha.FieldNameFamily: "Smith",
	})

	tokB, _ := s.TokenizeDeterministic(sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Alice",
		sriracha.FieldNameFamily: "Smith",
	})

	eq, _ := s.Equal(tokA, tokB)
	fmt.Printf("match: %v\n", eq)

	// Probabilistic tokenization
	bloomA, _ := s.TokenizeProbabilistic(sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Alice",
		sriracha.FieldNameFamily: "Smith",
	})

	bloomB, _ := s.TokenizeProbabilistic(sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Alice",
		sriracha.FieldNameFamily: "Smyth", // typo
	})

	result, _ := s.Match(bloomA, bloomB, token.DefaultMatchPolicy(0.85))
	fmt.Printf("match: %v (score: %.2f)\n", result.IsMatch, result.Score)
}
```

## Benchmarks

Live history on [Bencher](https://bencher.dev/perf/sriracha).

| Corpus                                          | Records | Pairs                      | AUROC | Accuracy | Recall |
|-------------------------------------------------|--------:|:---------------------------|------:|---------:|-------:|
| [OpenSanctions](testdata/corpus/opensanctions/) |  26 841 | natural cross-source       |  0.93 |     0.91 |   0.87 |
| [FEBRL4](testdata/corpus/febrl4/)               |  10 000 | synthetic (FEBRL4 noise)   |  1.00 |     1.00 |   1.00 |
| [NCVR](testdata/corpus/ncvr/)                   |   8 848 | synthetic (1–2 char edits) |  1.00 |     1.00 |   1.00 |
