//go:build bench

// Package bench wires the OpenSanctions person corpus (or any JSONL corpus
// that follows the same shape) into an end-to-end quality and performance
// harness for Sriracha.
//
// This package is gated behind the bench build tag. It is excluded from
// `go test ./...`, golangci-lint, and coverage.out so it cannot dilute the
// signal on the real library; the dedicated bench.yml workflow runs it
// under `-tags=bench` and ships its metrics to Bencher.
//
// Two top-level tests run end-to-end against the corpus:
//
//   - TestQualityBaseline scores 5,000 positive + 5,000 negative pairs
//     under DefaultFieldSet at a fixed threshold (untuned).
//   - TestQualityCalibrated derives the F1-optimal threshold from a held-out
//     calibration sample via token.Calibrate, then scores an independent
//     evaluation sample at that threshold (tuned).
//
// When SRIRACHA_BENCH_OUT is set, both tests append a Bencher Metric Format
// (BMF) block to the named file under their benchmark slug. The CI pipeline
// reads that file with `bencher run --adapter json` so Bencher tracks each
// metric over time and gates regressions.
package bench
