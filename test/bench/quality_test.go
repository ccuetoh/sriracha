//go:build bench

package bench

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/awnumar/memguard"
	"github.com/stretchr/testify/require"

	"go.sriracha.dev/fieldset"
	"go.sriracha.dev/session"
)

// benchSecret is fixed so harness runs are deterministic across CI
// invocations. It is not a credential — Sriracha's threat model assumes
// the secret is private to one institution; this binary explicitly is
// not.
const benchSecret = "test-bench-fixed-secret-not-for-production" //nolint:gosec // G101: fixed value documents that this harness is for benchmarking only

// expectedFingerprintPrefix pins the SHA-256 fingerprint of
// fieldset.DefaultFieldSet() so an accidental schema change (reordered
// fields, new field, weight tweak) trips a clear test failure rather than
// silently shifting every metric Bencher tracks. Update this constant
// only when DefaultFieldSet changes are intentional.
const expectedFingerprintPrefix = "49ec4861"

// corpus is one labeled benchmark dataset. Both quality tests iterate
// this list, so adding a new corpus is a single struct entry — no per-
// dataset test functions, no per-dataset BMF wiring. The name becomes the
// prefix of the BMF benchmark slug (e.g. "open_sanctions_untuned").
type corpus struct {
	name string // BMF prefix; must be a valid Bencher benchmark slug
	rel  string // relative path under testdata/corpus/, slash-delimited
}

// corpora are evaluated in declaration order. Bencher accepts whatever
// names appear here as new benchmark slugs on first push; renaming a
// corpus later orphans its history, so treat these strings as
// load-bearing.
var corpora = []corpus{
	{name: "open_sanctions", rel: "opensanctions/open_sanctions.jsonl"},
	{name: "febrl", rel: "febrl/febrl.jsonl"},
}

var (
	sharedSession session.Session
	sharedCorpora map[string][]record

	bmfMu      sync.Mutex
	bmfReports = bmfReport{}
)

// TestMain owns the corpora and session lifetimes so each subtest avoids
// reloading the JSONLs and re-allocating the locked secret. Loading every
// corpus up front also fails fast if any file is missing — you find out
// before the first test runs, not three subtests deep.
//
// On exit, if SRIRACHA_BENCH_OUT is set, the accumulated BMF blocks are
// flushed to the named file. CI reads this file via `bencher run --adapter
// json` to ship metrics to Bencher.
func TestMain(m *testing.M) {
	defer memguard.Purge()

	sharedCorpora = make(map[string][]record, len(corpora))
	for _, c := range corpora {
		records, err := loadJSONL(corpusPath(c.rel))
		if err != nil {
			fmt.Fprintf(os.Stderr, "bench: load corpus %q: %v\n", c.name, err)
			os.Exit(1)
		}
		sharedCorpora[c.name] = records
	}

	sess, err := session.New([]byte(benchSecret), fieldset.DefaultFieldSet())
	if err != nil {
		fmt.Fprintln(os.Stderr, "bench: new session:", err)
		os.Exit(1)
	}
	sharedSession = sess

	code := m.Run()

	sess.Destroy()

	if path := os.Getenv("SRIRACHA_BENCH_OUT"); path != "" {
		if err := flushBMF(path); err != nil {
			fmt.Fprintln(os.Stderr, "bench: write BMF:", err)
			if code == 0 {
				code = 1
			}
		}
	}

	os.Exit(code)
}

// corpusPath resolves rel against the module's testdata/corpus/.
// runtime.Caller anchors the lookup to this file's location so the path
// holds whether tests are launched from the repo root, the package
// directory, or an IDE.
func corpusPath(rel string) string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "testdata", "corpus", filepath.FromSlash(rel))
}

// recordReport stashes one benchmark's metrics under name for later BMF
// emission. The mutex guards the shared map across subtests; even though
// the harness runs them serially today, a future t.Parallel sweep would
// otherwise race.
func recordReport(name string, r result) {
	bmfMu.Lock()
	defer bmfMu.Unlock()
	bmfReports[name] = resultMetrics(r)
}

// flushBMF writes whatever the tests recorded to path. Called once at
// process exit from TestMain.
func flushBMF(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create %q: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	return writeBMF(f, bmfReports)
}

// TestQualityBaseline runs the harness with DefaultFieldSet at a fixed
// 0.5 threshold — no calibration. This is the "as-shipped" quality a
// user sees with zero tuning effort, and Bencher tracks it over time
// independent of the calibrated run. One subtest per corpus emits a BMF
// block named "<corpus>_untuned".
//
// Deliberately not t.Parallel — both quality tests share sharedSession
// (locked memory) and the test process owns one session per CI job; the
// CLAUDE.md t.Parallel-first convention applies to ordinary unit tests
// and is documented as waived here.
func TestQualityBaseline(t *testing.T) {
	for _, c := range corpora {
		t.Run(c.name, func(t *testing.T) {
			runBaseline(t, c.name, sharedCorpora[c.name])
		})
	}
}

func runBaseline(t *testing.T, name string, records []record) {
	t.Helper()
	res, err := run(sharedSession, records, options{
		Pairs:     pairOptions{Positives: 5000, Negatives: 5000, Seed: 1},
		Threshold: 0.5,
	})
	require.NoError(t, err)
	requireSchemaPinned(t, res)
	requireFiniteMetrics(t, res)
	recordReport(name+"_untuned", res)
	t.Logf("%s_untuned: auroc=%.4f auprc=%.4f best_f1=%.4f@%.2f tokenize_p99=%s match_p99=%s",
		name, res.AUROC, res.AUPRC, res.BestF1.F1, res.BestF1.Threshold,
		res.Tokenize.Latency.P99, res.Match.Latency.P99)
}

// TestQualityCalibrated derives the F1-optimal threshold from a 1k+1k
// calibration sample (seed=1) via token.Calibrate, then evaluates an
// independent 5k+5k sample (seed=2) at that threshold. The two samples
// are drawn from the same record pool but with different PRNG streams,
// so contamination is bounded — and bounded further by the fact that
// calibration only fits a single scalar (the threshold), not a model.
// One subtest per corpus emits a BMF block named "<corpus>_calibrated".
//
// Deliberately not t.Parallel — see TestQualityBaseline.
func TestQualityCalibrated(t *testing.T) {
	for _, c := range corpora {
		t.Run(c.name, func(t *testing.T) {
			runCalibrated(t, c.name, sharedCorpora[c.name])
		})
	}
}

func runCalibrated(t *testing.T, name string, records []record) {
	t.Helper()
	cal, err := calibrate(sharedSession, records,
		pairOptions{Positives: 1000, Negatives: 1000, Seed: 1})
	require.NoError(t, err)
	t.Logf("%s calibration: optimal_threshold=%.2f F1=%.4f", name, cal.OptimalThreshold, cal.F1)

	res, err := run(sharedSession, records, options{
		Pairs:     pairOptions{Positives: 5000, Negatives: 5000, Seed: 2},
		Threshold: cal.OptimalThreshold,
	})
	require.NoError(t, err)
	requireSchemaPinned(t, res)
	requireFiniteMetrics(t, res)
	recordReport(name+"_calibrated", res)
	t.Logf("%s_calibrated: auroc=%.4f auprc=%.4f best_f1=%.4f@%.2f tokenize_p99=%s match_p99=%s",
		name, res.AUROC, res.AUPRC, res.BestF1.F1, res.BestF1.Threshold,
		res.Tokenize.Latency.P99, res.Match.Latency.P99)
}

// requireSchemaPinned asserts that the FieldSet fingerprint hasn't drifted
// from the value baked into expectedFingerprintPrefix. Catches accidental
// edits to fieldset.DefaultFieldSet weights or ordering that would
// silently shift every Bencher-tracked metric.
func requireSchemaPinned(t *testing.T, r result) {
	t.Helper()
	require.Truef(t, strings.HasPrefix(r.FieldSetFingerprint, expectedFingerprintPrefix),
		"FieldSet fingerprint changed; if intentional, update expectedFingerprintPrefix in quality_test.go. got=%s want_prefix=%s",
		r.FieldSetFingerprint, expectedFingerprintPrefix)
}

// requireFiniteMetrics guards against NaN / Inf creeping into a metric
// slug — Bencher would happily ingest the bad value and the regression
// would be invisible until someone investigated a flatlined chart.
func requireFiniteMetrics(t *testing.T, r result) {
	t.Helper()
	for name, v := range map[string]float64{
		"auroc":     r.AUROC,
		"auprc":     r.AUPRC,
		"best_f1":   r.BestF1.F1,
		"precision": r.BestF1.Precision,
		"recall":    r.BestF1.Recall,
		"accuracy":  r.BestAccuracy.Accuracy,
		"mcc":       r.BestMCC.MCC,
	} {
		require.Falsef(t, math.IsNaN(v) || math.IsInf(v, 0), "%s must be finite, got %v", name, v)
	}
}
