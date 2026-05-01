// Command srbench runs the Sriracha quality and performance benchmark
// against a JSONL corpus shaped like testdata/corpus/opensanctions and
// emits a JSON report on stdout (or a file via -out).
//
// Usage:
//
//	go run ./cmd/srbench -corpus testdata/corpus/opensanctions/open_sanctions.jsonl
//	go run ./cmd/srbench -positives 5000 -negatives 5000 -seed 1 -out report.json
//
// The benchmark uses a fixed dummy secret so it is deterministic across
// invocations; do not point this binary at production data.
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"go.sriracha.dev/benchmark"
	"go.sriracha.dev/fieldset"
	"go.sriracha.dev/session"
)

const benchSecret = "srbench-fixed-secret-not-for-production" //nolint:gosec // G101: not a credential — fixed value documents that this binary is for benchmarking, never production

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "srbench:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr *os.File) error {
	fs := flag.NewFlagSet("srbench", flag.ContinueOnError)
	fs.SetOutput(stderr)
	var (
		corpus    = fs.String("corpus", "testdata/corpus/opensanctions/open_sanctions.jsonl", "path to JSONL corpus")
		out       = fs.String("out", "", "write report JSON to this path (default: stdout)")
		positives = fs.Int("positives", 5000, "max positive pairs to sample")
		negatives = fs.Int("negatives", 5000, "max negative pairs to sample")
		seed      = fs.Uint64("seed", 1, "PRNG seed for pair sampling")
		threshold = fs.Float64("threshold", 0, "evaluate confusion at this threshold (0 disables)")
		summary   = fs.Bool("summary", true, "print a one-line summary to stderr after writing the report")
	)
	if err := fs.Parse(args); err != nil {
		return err
	}

	records, err := benchmark.LoadJSONL(*corpus)
	if err != nil {
		return err
	}

	sess, err := session.New([]byte(benchSecret), fieldset.DefaultFieldSet())
	if err != nil {
		return fmt.Errorf("session: %w", err)
	}
	defer sess.Destroy()

	res, err := benchmark.Run(sess, records, benchmark.Options{
		Pairs: benchmark.PairOptions{
			Positives: *positives,
			Negatives: *negatives,
			Seed:      *seed,
		},
		Threshold: *threshold,
	})
	if err != nil {
		return err
	}

	if err := writeReport(res, *out, stdout); err != nil {
		return err
	}
	if *summary {
		_, _ = fmt.Fprintf(stderr,
			"records=%d positives=%d negatives=%d auroc=%.4f auprc=%.4f best_f1=%.4f@%.2f tokenize_p99=%s match_p99=%s\n",
			res.Counts.Records, res.Counts.Positives, res.Counts.Negatives,
			res.AUROC, res.AUPRC, res.BestF1.F1, res.BestF1.Threshold,
			res.Tokenize.Latency.P99, res.Match.Latency.P99,
		)
	}
	return nil
}

func writeReport(res benchmark.Result, path string, stdout *os.File) error {
	data, err := json.MarshalIndent(res, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal report: %w", err)
	}
	data = append(data, '\n')
	if path == "" {
		_, err := stdout.Write(data)
		return err
	}
	return os.WriteFile(path, data, 0o600)
}
