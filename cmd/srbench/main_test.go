package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fixtureCorpus is a 4-record JSONL document that exercises the runner
// end-to-end without depending on the full OpenSanctions corpus.
const fixtureCorpus = `{"canonical_id":"alice","entity_id":"a1","record":{"sriracha::name::given":"Alice","sriracha::name::family":"Smith","sriracha::date::birth":"1980-01-01"}}
{"canonical_id":"alice","entity_id":"a2","record":{"sriracha::name::given":"Alyce","sriracha::name::family":"Smith","sriracha::date::birth":"1980-01-01"}}
{"canonical_id":"bob","entity_id":"b1","record":{"sriracha::name::given":"Bob","sriracha::name::family":"Jones","sriracha::date::birth":"1975-06-15"}}
{"canonical_id":"bob","entity_id":"b2","record":{"sriracha::name::given":"Robert","sriracha::name::family":"Jones","sriracha::date::birth":"1975-06-15"}}
`

// writeFixture lays the fixtureCorpus down at path, returning the path so
// tests can chain it into the -corpus flag.
func writeFixture(t *testing.T, dir string) string {
	t.Helper()
	path := filepath.Join(dir, "corpus.jsonl")
	require.NoError(t, os.WriteFile(path, []byte(fixtureCorpus), 0o600))
	return path
}

// devNull is a write-only file used so run() can scribble its summary
// somewhere without polluting the test output.
func devNull(t *testing.T) *os.File {
	t.Helper()
	f, err := os.OpenFile(os.DevNull, os.O_WRONLY, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = f.Close() })
	return f
}

func TestRun(t *testing.T) {
	t.Parallel()

	t.Run("WritesReportToFile", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		corpus := writeFixture(t, dir)
		out := filepath.Join(dir, "report.json")

		err := run([]string{
			"-corpus", corpus,
			"-out", out,
			"-positives", "1",
			"-negatives", "1",
			"-seed", "1",
			"-summary=false",
		}, devNull(t), devNull(t))
		require.NoError(t, err)

		data, err := os.ReadFile(out)
		require.NoError(t, err)
		var parsed map[string]any
		require.NoError(t, json.Unmarshal(data, &parsed))
		assert.Contains(t, parsed, "auroc")
		assert.Contains(t, parsed, "best_f1")
	})

	t.Run("RejectsBadFlag", func(t *testing.T) {
		t.Parallel()
		err := run([]string{"-not-a-real-flag"}, devNull(t), devNull(t))
		require.Error(t, err)
	})

	t.Run("RejectsMissingCorpus", func(t *testing.T) {
		t.Parallel()
		err := run([]string{"-corpus", filepath.Join(t.TempDir(), "missing.jsonl")}, devNull(t), devNull(t))
		require.Error(t, err)
	})

	t.Run("RejectsBadThreshold", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		corpus := writeFixture(t, dir)
		err := run([]string{
			"-corpus", corpus,
			"-positives", "1",
			"-negatives", "1",
			"-threshold", "1.5",
		}, devNull(t), devNull(t))
		require.Error(t, err)
	})

	t.Run("WritesReportToStdoutWithSummary", func(t *testing.T) {
		t.Parallel()
		dir := t.TempDir()
		corpus := writeFixture(t, dir)

		stdout, err := os.CreateTemp(dir, "stdout-*")
		require.NoError(t, err)
		stderr, err := os.CreateTemp(dir, "stderr-*")
		require.NoError(t, err)

		err = run([]string{
			"-corpus", corpus,
			"-positives", "1",
			"-negatives", "1",
			"-seed", "1",
		}, stdout, stderr)
		require.NoError(t, err)
		require.NoError(t, stdout.Close())
		require.NoError(t, stderr.Close())

		stdoutBytes, err := os.ReadFile(stdout.Name())
		require.NoError(t, err)
		var parsed map[string]any
		require.NoError(t, json.Unmarshal(stdoutBytes, &parsed))

		stderrBytes, err := os.ReadFile(stderr.Name())
		require.NoError(t, err)
		assert.Contains(t, string(stderrBytes), "auroc=")
	})
}
