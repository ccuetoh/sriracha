package file

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.sriracha.dev/sriracha"
)

// errReader is an io.Reader that always returns an error.
type errReader struct{ err error }

func (e *errReader) Read(_ []byte) (int, error) { return 0, e.err }

// newForTest opens (or creates) the log at path and registers a cleanup that
// closes the underlying file handle before t.TempDir() removes the directory.
// On Windows, open handles prevent directory removal, so this is required.
func newForTest(t *testing.T, path string) *log {
	t.Helper()
	al, err := New(path)
	require.NoError(t, err)
	l, ok := al.(*log)
	require.True(t, ok, "New must return *log")
	t.Cleanup(func() { _ = l.f.Close() })
	return l
}

func TestNewEmptyFile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l := newForTest(t, path)
	var zero [32]byte
	assert.Equal(t, zero, l.prevHash)
}

func TestNewOpenError(t *testing.T) {
	t.Parallel()
	_, err := New("/nonexistent/dir/audit.jsonl")
	require.Error(t, err)
}

func TestNewSeedsHashFromExistingFile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")

	l1 := newForTest(t, path)
	require.NoError(t, l1.Append(context.Background(), sriracha.AuditEvent{EventType: sriracha.EventCapabilities}))
	prevHash := l1.prevHash

	l2 := newForTest(t, path)
	assert.Equal(t, prevHash, l2.prevHash)
}

func TestAppendSetsEventID(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l := newForTest(t, path)

	require.NoError(t, l.Append(context.Background(), sriracha.AuditEvent{}))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var ev sriracha.AuditEvent
	require.NoError(t, json.Unmarshal([]byte(strings.TrimRight(string(data), "\n")), &ev))
	assert.NotEmpty(t, ev.EventID)
	_, parseErr := ksuid.Parse(ev.EventID)
	assert.NoError(t, parseErr)
}

func TestAppendFirstEventHasZeroPreviousHash(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l := newForTest(t, path)

	require.NoError(t, l.Append(context.Background(), sriracha.AuditEvent{}))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var ev sriracha.AuditEvent
	require.NoError(t, json.Unmarshal([]byte(strings.TrimRight(string(data), "\n")), &ev))
	assert.Equal(t, [32]byte{}, ev.PreviousHash)
}

func TestAppendChainIntegrity(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l := newForTest(t, path)

	for i := range 5 {
		require.NoError(t, l.Append(context.Background(), sriracha.AuditEvent{RecordCount: i}))
	}

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	lines := strings.Split(strings.TrimRight(string(data), "\n"), "\n")
	require.Len(t, lines, 5)

	var prevHash [32]byte
	for _, line := range lines {
		var ev sriracha.AuditEvent
		require.NoError(t, json.Unmarshal([]byte(line), &ev))
		assert.Equal(t, prevHash, ev.PreviousHash)
		prevHash = sha256.Sum256([]byte(line))
	}
}

func TestAppendUpdatesInMemoryHash(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l := newForTest(t, path)

	require.NoError(t, l.Append(context.Background(), sriracha.AuditEvent{SessionID: "s1"}))
	hash1 := l.prevHash
	require.NoError(t, l.Append(context.Background(), sriracha.AuditEvent{SessionID: "s2"}))
	assert.NotEqual(t, hash1, l.prevHash)
}

func TestAppendWriteError(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l := newForTest(t, path)

	require.NoError(t, l.f.Close())
	err := l.Append(context.Background(), sriracha.AuditEvent{})
	assert.Error(t, err)
}

func TestVerifyEmptyFile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l := newForTest(t, path)
	assert.NoError(t, l.Verify(context.Background()))
}

func TestVerifyValidChain(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l := newForTest(t, path)

	for i := range 3 {
		require.NoError(t, l.Append(context.Background(), sriracha.AuditEvent{RecordCount: i}))
	}
	assert.NoError(t, l.Verify(context.Background()))
}

func TestVerifyBrokenChain(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l := newForTest(t, path)

	require.NoError(t, l.Append(context.Background(), sriracha.AuditEvent{SessionID: "first"}))
	require.NoError(t, l.Append(context.Background(), sriracha.AuditEvent{SessionID: "second"}))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	lines := strings.SplitN(string(data), "\n", 2)

	var ev sriracha.AuditEvent
	require.NoError(t, json.Unmarshal([]byte(lines[0]), &ev))
	ev.RecordCount = 999
	tampered, err := json.Marshal(ev)
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(path, []byte(string(tampered)+"\n"+lines[1]), 0600))

	assert.Error(t, l.Verify(context.Background()))
}

func TestVerifyEmptyEventID(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")

	ev := sriracha.AuditEvent{EventType: sriracha.EventQuery}
	raw, err := json.Marshal(ev)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append(raw, '\n'), 0600))

	l := newForTest(t, path)
	assert.Error(t, l.Verify(context.Background()))
}

func TestVerifyMissingFile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l := newForTest(t, path)

	// Close the write handle before removing; on Windows an open handle
	// prevents deletion even when FILE_SHARE_DELETE is set.
	require.NoError(t, l.f.Close())
	require.NoError(t, os.Remove(path))
	assert.Error(t, l.Verify(context.Background()))
}

func TestReopenAndExtend(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")

	l1 := newForTest(t, path)
	for i := range 3 {
		require.NoError(t, l1.Append(context.Background(), sriracha.AuditEvent{RecordCount: i}))
	}

	l2 := newForTest(t, path)
	for i := range 2 {
		require.NoError(t, l2.Append(context.Background(), sriracha.AuditEvent{RecordCount: 100 + i}))
	}

	assert.NoError(t, l2.Verify(context.Background()))
}

// TestNewLogSeedHashError exercises the error-cleanup path in newLog (and
// transitively the os.Open error branch in seedHash) by supplying a valid
// write handle with a seedPath that does not exist.
func TestNewLogSeedHashError(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	f, err := os.OpenFile(filepath.Join(dir, "write.jsonl"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	require.NoError(t, err)

	_, err = newLog(f, filepath.Join(dir, "nonexistent", "seed.jsonl"))
	require.Error(t, err)
}

// TestVerifySkipsEmptyLines verifies that blank lines in the file are ignored
// and do not break the hash chain. This covers the len(raw)==0 continue branch.
func TestVerifySkipsEmptyLines(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l := newForTest(t, path)

	require.NoError(t, l.Append(context.Background(), sriracha.AuditEvent{SessionID: "s1"}))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append(data, '\n'), 0600))

	assert.NoError(t, l.Verify(context.Background()))
}

// TestVerifyInvalidJSON checks that a line containing non-JSON data causes
// Verify to return an error, covering the json.Unmarshal failure branch.
func TestVerifyInvalidJSON(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	require.NoError(t, os.WriteFile(path, []byte("not-json\n"), 0600))

	l := newForTest(t, path)
	assert.Error(t, l.Verify(context.Background()))
}

// TestNewTightensPreExistingMode verifies that opening an audit log file with
// looser permissions tightens it to 0600. O_CREATE alone does not do this for
// pre-existing files.
func TestNewTightensPreExistingMode(t *testing.T) {
	t.Parallel()
	if runtime.GOOS == "windows" {
		t.Skip("POSIX file modes do not apply on Windows")
	}

	path := filepath.Join(t.TempDir(), "audit.jsonl")
	require.NoError(t, os.WriteFile(path, []byte{}, 0o644))

	l := newForTest(t, path)
	_ = l // used via cleanup

	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())
}

// TestScanSeedError covers the sc.Err() branch in scanSeed.
func TestScanSeedError(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l := newForTest(t, path)

	err := l.scanSeed(&errReader{err: errors.New("read error")})
	require.Error(t, err)
	assert.ErrorContains(t, err, "seed scan")
}
