//go:build bench

// Package bench wires labeled person-record corpora into an end-to-end
// quality and performance harness for Sriracha. Today it ships two
// corpora — OpenSanctions (real multi-jurisdiction sanctions data) and
// FEBRL4 (synthetic with controlled noise) — and any JSONL file matching
// the same Record shape can be added to the corpora list. Future FEBRL
// releases live under testdata/corpus/febrl<N>/ and become additional
// entries in that list.
//
// This package is gated behind the bench build tag. It is excluded from
// `go test ./...`, golangci-lint, and coverage.out so it cannot dilute the
// signal on the real library; the dedicated bench.yml workflow runs it
// under `-tags=bench` and ships its metrics to Bencher.
//
// Two top-level tests run end-to-end. Each iterates the corpora list and
// produces one BMF block per (corpus, mode) pair:
//
//   - TestQualityBaseline scores 5,000 positive + 5,000 negative pairs
//     under DefaultFieldSet at a fixed threshold (untuned). BMF slug:
//     <corpus>_untuned.
//   - TestQualityCalibrated derives the F1-optimal threshold from a held-out
//     calibration sample via token.Calibrate, then scores an independent
//     evaluation sample at that threshold (tuned). BMF slug:
//     <corpus>_calibrated.
//
// When SRIRACHA_BENCH_OUT is set, both tests append a Bencher Metric Format
// (BMF) block to the named file under their benchmark slug. The CI pipeline
// reads that file with `bencher run --adapter json` so Bencher tracks each
// metric over time and gates regressions.
package bench
