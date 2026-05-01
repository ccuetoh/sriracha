//go:build bench

package bench

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/fieldset"
	"github.com/ccuetoh/sriracha/session"
)

// newRunSession returns a fresh session bound to the default
// FieldSet for unit tests of run / tokenizeAll / matchAll. Each test gets
// its own — sessions hold a locked secret buffer and Destroy invalidates
// them, so sharing across t.Parallel subtests is risky.
func newRunSession(t *testing.T) session.Session {
	t.Helper()
	sess, err := session.New([]byte("run-unit-test-secret"), fieldset.DefaultFieldSet())
	require.NoError(t, err)
	t.Cleanup(sess.Destroy)
	return sess
}

// twinPersonCorpus is a tiny synthetic corpus where canonical_id collapses
// to two persons (alice with two typo variants, bob with a name variant)
// plus a singleton carol. Exercises run end-to-end without touching the
// 26k-record OpenSanctions file.
func twinPersonCorpus() []record {
	return []record{
		{CanonicalID: "alice", EntityID: "a1", Dataset: "ds1", Fields: sriracha.RawRecord{
			sriracha.FieldNameGiven:  "Alice",
			sriracha.FieldNameFamily: "Smith",
			sriracha.FieldDateBirth:  "1980-01-01",
		}},
		{CanonicalID: "alice", EntityID: "a2", Dataset: "ds2", Fields: sriracha.RawRecord{
			sriracha.FieldNameGiven:  "Alyce",
			sriracha.FieldNameFamily: "Smith",
			sriracha.FieldDateBirth:  "1980-01-01",
		}},
		{CanonicalID: "alice", EntityID: "a3", Dataset: "ds3", Fields: sriracha.RawRecord{
			sriracha.FieldNameGiven:  "Alice",
			sriracha.FieldNameFamily: "Smyth",
			sriracha.FieldDateBirth:  "1980-01-01",
		}},
		{CanonicalID: "bob", EntityID: "b1", Dataset: "ds1", Fields: sriracha.RawRecord{
			sriracha.FieldNameGiven:  "Bob",
			sriracha.FieldNameFamily: "Jones",
			sriracha.FieldDateBirth:  "1975-06-15",
		}},
		{CanonicalID: "bob", EntityID: "b2", Dataset: "ds2", Fields: sriracha.RawRecord{
			sriracha.FieldNameGiven:  "Robert",
			sriracha.FieldNameFamily: "Jones",
			sriracha.FieldDateBirth:  "1975-06-15",
		}},
		{CanonicalID: "carol", EntityID: "c1", Dataset: "ds1", Fields: sriracha.RawRecord{
			sriracha.FieldNameGiven:  "Carol",
			sriracha.FieldNameFamily: "Davis",
			sriracha.FieldDateBirth:  "1990-03-20",
		}},
	}
}

func TestRun(t *testing.T) {
	t.Parallel()

	t.Run("ProducesUsableMetrics", func(t *testing.T) {
		t.Parallel()
		sess := newRunSession(t)
		records := twinPersonCorpus()

		res, err := run(sess, records, options{
			Pairs:     pairOptions{Positives: 4, Negatives: 6, Seed: 1},
			Threshold: 0.5,
		})
		require.NoError(t, err)

		assert.Equal(t, "0.1", res.FieldSetVersion)
		assert.NotEmpty(t, res.FieldSetFingerprint)
		assert.Equal(t, len(records), res.Counts.Records)
		assert.Equal(t, 4, res.Counts.Positives)
		assert.Equal(t, 6, res.Counts.Negatives)

		assert.Len(t, res.ROC, 101)
		assert.GreaterOrEqual(t, res.AUROC, 0.0)
		assert.LessOrEqual(t, res.AUROC, 1.0)
		assert.GreaterOrEqual(t, res.AUPRC, 0.0)
		assert.LessOrEqual(t, res.AUPRC, 1.0)

		assert.NotNil(t, res.AtThreshold)
		assert.InDelta(t, 0.5, res.AtThreshold.Threshold, 1e-9)

		assert.Equal(t, len(records), res.Tokenize.Latency.Count)
		assert.Equal(t, 10, res.Match.Latency.Count)
		assert.Greater(t, res.Tokenize.TotalDuration.Nanoseconds(), int64(0))
		assert.Greater(t, res.Match.TotalDuration.Nanoseconds(), int64(0))
	})

	t.Run("DeterministicAcrossRuns", func(t *testing.T) {
		t.Parallel()
		records := twinPersonCorpus()

		sess1 := newRunSession(t)
		res1, err := run(sess1, records, options{Pairs: pairOptions{Positives: 4, Negatives: 4, Seed: 99}})
		require.NoError(t, err)

		sess2 := newRunSession(t)
		res2, err := run(sess2, records, options{Pairs: pairOptions{Positives: 4, Negatives: 4, Seed: 99}})
		require.NoError(t, err)

		assert.InDelta(t, res1.AUROC, res2.AUROC, 1e-9)
		assert.InDelta(t, res1.AUPRC, res2.AUPRC, 1e-9)
		assert.Equal(t, res1.BestF1.TP, res2.BestF1.TP)
		assert.Equal(t, res1.BestF1.FP, res2.BestF1.FP)
		assert.Equal(t, res1.BestF1.TN, res2.BestF1.TN)
		assert.Equal(t, res1.BestF1.FN, res2.BestF1.FN)
	})

	t.Run("MarshalsToJSON", func(t *testing.T) {
		t.Parallel()
		sess := newRunSession(t)
		records := twinPersonCorpus()

		res, err := run(sess, records, options{Pairs: pairOptions{Positives: 2, Negatives: 2, Seed: 1}})
		require.NoError(t, err)

		data, err := json.Marshal(res)
		require.NoError(t, err)
		assert.Contains(t, string(data), `"auroc"`)
		assert.Contains(t, string(data), `"best_f1"`)
		assert.Contains(t, string(data), `"tokenize"`)
		assert.Contains(t, string(data), `"throughput_per_sec"`)
	})

	t.Run("DroppedFieldsReportedNotFatal", func(t *testing.T) {
		t.Parallel()
		sess := newRunSession(t)
		records := twinPersonCorpus()
		records[0].Fields[sriracha.FieldAddressCountry] = "USA"

		res, err := run(sess, records, options{Pairs: pairOptions{Positives: 2, Negatives: 2, Seed: 1}})
		require.NoError(t, err)
		require.NotNil(t, res.Counts.DroppedFields)
		assert.Equal(t, 1, res.Counts.DroppedFields[sriracha.FieldAddressCountry.String()])
	})

	t.Run("RejectsNilSession", func(t *testing.T) {
		t.Parallel()
		_, err := run(nil, twinPersonCorpus(), options{Pairs: pairOptions{Positives: 1, Negatives: 1, Seed: 1}})
		require.Error(t, err)
	})

	t.Run("RejectsTinyCorpus", func(t *testing.T) {
		t.Parallel()
		sess := newRunSession(t)
		_, err := run(sess, []record{{CanonicalID: "a"}}, options{Pairs: pairOptions{Positives: 1, Seed: 1}})
		require.Error(t, err)
	})

	t.Run("RejectsOutOfRangeThreshold", func(t *testing.T) {
		t.Parallel()
		sess := newRunSession(t)
		_, err := run(sess, twinPersonCorpus(), options{
			Pairs:     pairOptions{Positives: 1, Negatives: 1, Seed: 1},
			Threshold: 1.5,
		})
		require.Error(t, err)
	})

	t.Run("PropagatesPairSamplingError", func(t *testing.T) {
		t.Parallel()
		sess := newRunSession(t)
		_, err := run(sess, twinPersonCorpus(), options{Pairs: pairOptions{Seed: 1}})
		require.Error(t, err)
	})
}

func TestCalibrate(t *testing.T) {
	t.Parallel()

	t.Run("ProducesOptimalThreshold", func(t *testing.T) {
		t.Parallel()
		sess := newRunSession(t)
		cal, err := calibrate(sess, twinPersonCorpus(),
			pairOptions{Positives: 2, Negatives: 2, Seed: 1})
		require.NoError(t, err)
		assert.GreaterOrEqual(t, cal.OptimalThreshold, 0.0)
		assert.LessOrEqual(t, cal.OptimalThreshold, 1.0)
		assert.Len(t, cal.ROC, 101)
	})

	t.Run("RejectsNilSession", func(t *testing.T) {
		t.Parallel()
		_, err := calibrate(nil, twinPersonCorpus(),
			pairOptions{Positives: 1, Negatives: 1, Seed: 1})
		require.Error(t, err)
	})

	t.Run("PropagatesPairSamplingError", func(t *testing.T) {
		t.Parallel()
		sess := newRunSession(t)
		_, err := calibrate(sess, twinPersonCorpus(), pairOptions{Seed: 1})
		require.Error(t, err)
	})
}

func TestMatchAllRejectsOutOfRangePair(t *testing.T) {
	t.Parallel()
	sess := newRunSession(t)
	records := twinPersonCorpus()

	tokens, _, _, err := tokenizeAll(sess, records)
	require.NoError(t, err)

	_, _, _, err = matchAll(sess, []pair{{A: 0, B: 99}}, tokens)
	require.Error(t, err)
}

func TestTokenizeAllSurfacesRequiredFieldError(t *testing.T) {
	t.Parallel()
	fs := fieldset.DefaultFieldSet()
	for i := range fs.Fields {
		if fs.Fields[i].Path == sriracha.FieldNameFamily {
			fs.Fields[i].Required = true
		}
	}
	sess, err := session.New([]byte("required-field-secret"), fs)
	require.NoError(t, err)
	t.Cleanup(sess.Destroy)

	records := []record{
		{CanonicalID: "x", EntityID: "x1", Fields: sriracha.RawRecord{
			sriracha.FieldNameGiven: "OnlyGiven",
		}},
	}
	_, _, _, err = tokenizeAll(sess, records)
	require.Error(t, err)
}

func TestMatchAllSurfacesIncompatibleTokens(t *testing.T) {
	t.Parallel()
	sess := newRunSession(t)
	records := twinPersonCorpus()
	tokens, _, _, err := tokenizeAll(sess, records)
	require.NoError(t, err)

	tokens[1].FieldSetVersion = "different-version"
	_, _, _, err = matchAll(sess, []pair{{A: 0, B: 1}}, tokens)
	require.Error(t, err)
}
