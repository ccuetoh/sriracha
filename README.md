# <img height="70" alt="sriracha_logo" src="https://github.com/user-attachments/assets/8932cc91-8d3a-4f16-8b9b-9e8e8b9cecb2" /> Sriracha
## Privacy-preserving person record linkage (PPRL) library

[![CI](https://github.com/ccuetoh/sriracha/actions/workflows/ci.yml/badge.svg)](https://github.com/ccuetoh/sriracha/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/ccuetoh/sriracha/graph/badge.svg?token=1JRW9RH43K)](https://codecov.io/gh/ccuetoh/sriracha)
[![Go Report Card](https://goreportcard.com/badge/github.com/ccuetoh/sriracha)](https://goreportcard.com/report/github.com/ccuetoh/sriracha)
[![pkg.go.dev](https://pkg.go.dev/badge/github.com/ccuetoh/sriracha.svg)](https://pkg.go.dev/github.com/ccuetoh/sriracha)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

> **Experimental.** API is unstable. Not production-ready.


Sriracha is a Go library for privacy-preserving record linkage. It enables institutions
to share person records across organizational boundaries without transmitting raw PII.
Sriracha provides the building blocks for building privacy-first transports.
Records are normalized and tokenized with a shared secret, producing tokens
that can be compared without exposing the underlying identifiers.

## Features

- Deterministic tokenization using HMAC-SHA256
- Probabilistic tokenization with Bloom filters and Dice coefficient
- Optional BLIP and balanced filter defenses against frequency analysis
- Unicode normalization pipeline
- Canonical field set with support for extended schemas

## Installation

Requires Go 1.22+

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
)

func main() {
	secret := []byte("super-secret-key")

	s, _ := session.New(secret, fieldset.DefaultFieldSet())
	defer s.Destroy()

	// Deterministic tokenization
	tokA, _ := s.Tokenize(sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Alice",
		sriracha.FieldNameFamily: "Smith",
	})

	tokB, _ := s.Tokenize(sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Alice",
		sriracha.FieldNameFamily: "Smith",
	})

	eq := s.Equal(tokA, tokB)
	fmt.Printf("match: %v\n", eq)

	// Probabilistic tokenization
	bloomA, _ := s.TokenizeBloom(sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Alice",
		sriracha.FieldNameFamily: "Smith",
	})

	bloomB, _ := s.TokenizeBloom(sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Alice",
		sriracha.FieldNameFamily: "Smyth", // typo
	})

	result, _ := s.Match(bloomA, bloomB, 0.85)
	fmt.Printf("match: %v (score: %.2f)\n", result.IsMatch, result.Score)
}
```

## Benchmarks

Live history on [Bencher](https://bencher.dev/perf/sriracha).

| Corpus                                          | Records | Pairs                      | AUROC | Accuracy | Recall |
|-------------------------------------------------|--------:|:---------------------------|------:|---------:|-------:|
| [OpenSanctions](testdata/corpus/opensanctions/) |  26 841 | natural cross-source       |  0.93 |     0.91 |   0.87 | 20 k rec/s |
| [FEBRL4](testdata/corpus/febrl4/)               |  10 000 | synthetic (FEBRL4 noise)   |  1.00 |     1.00 |   1.00 |
| [NCVR](testdata/corpus/ncvr/)                   |   8 848 | synthetic (1–2 char edits) |  1.00 |     1.00 |   1.00 |
