# <img height="70" alt="sriracha_logo" src="https://github.com/user-attachments/assets/8932cc91-8d3a-4f16-8b9b-9e8e8b9cecb2" /> Sriracha
## Peer-to-peer secure privacy-preserving person record linkage (PPRL)

[![CI](https://github.com/ccuetoh/sriracha/actions/workflows/ci.yml/badge.svg)](https://github.com/ccuetoh/sriracha/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/ccuetoh/sriracha/graph/badge.svg?token=1JRW9RH43K)](https://codecov.io/gh/ccuetoh/sriracha)
[![Go Report Card](https://goreportcard.com/badge/go.sriracha.dev)](https://goreportcard.com/report/go.sriracha.dev)
[![pkg.go.dev](https://pkg.go.dev/badge/go.sriracha.dev/sriracha.svg)](https://pkg.go.dev/go.sriracha.dev/sriracha)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)

> **Experimental.** Wire format and API are unstable. Not production-ready.

## What is Sriracha?

Institutions in health, government, and research routinely need to find shared person records across organizational
boundaries — without transmitting raw PII. Sriracha enables this: records are normalized, tokenized with a shared
secret, and matched against a token index. No raw identifiers leave the institution, and no central coordinator is
required.

## Features

- HMAC-SHA256 deterministic tokenization (exact match)
- Bloom filter probabilistic tokenization (fuzzy match, typo-tolerant)
- gRPC transport with mTLS
- Unicode normalization pipeline (names, dates, addresses, identifiers)
- Hash-chained audit log for compliance
- Incremental index sync with checkpoint tokens
- Bulk streaming mode
