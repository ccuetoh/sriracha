// Package benchmark wires the OpenSanctions person corpus (or any JSONL
// corpus that follows the same shape) into an end-to-end quality and
// performance harness for Sriracha.
//
// Run loads a labeled corpus, tokenizes every record under a session.Session,
// samples positive and negative pairs from the canonical_id ground truth, and
// reports the metrics that matter for production deployments: precision,
// recall, F1, accuracy, MCC, ROC, AUROC, AUPRC, plus tokenize and match
// latency distributions and throughput.
//
// The harness is deliberately agnostic to the underlying tokenizer
// configuration — pass any session.Session built from any FieldSet and the
// same metrics fall out the other end. Use the cmd/srbench binary for a
// command-line entry point, or call Run directly from a Go test.
package benchmark
