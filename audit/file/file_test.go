package file

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.sriracha.dev/sriracha"
)

func TestNewEmptyFile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l, err := New(path)
	require.NoError(t, err)
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

	l1, err := New(path)
	require.NoError(t, err)
	require.NoError(t, l1.Append(context.Background(), sriracha.AuditEvent{EventType: sriracha.EventCapabilities}))
	prevHash := l1.prevHash

	l2, err := New(path)
	require.NoError(t, err)
	assert.Equal(t, prevHash, l2.prevHash)
}

func TestAppendSetsEventID(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l, err := New(path)
	require.NoError(t, err)

	require.NoError(t, l.Append(context.Background(), sriracha.AuditEvent{}))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var ev sriracha.AuditEvent
	require.NoError(t, json.Unmarshal([]byte(strings.TrimRight(string(data), "\n")), &ev))
	assert.NotEmpty(t, ev.EventID)
}

func TestAppendFirstEventHasZeroPreviousHash(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l, err := New(path)
	require.NoError(t, err)

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
	l, err := New(path)
	require.NoError(t, err)

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
	l, err := New(path)
	require.NoError(t, err)

	require.NoError(t, l.Append(context.Background(), sriracha.AuditEvent{SessionID: "s1"}))
	hash1 := l.prevHash
	require.NoError(t, l.Append(context.Background(), sriracha.AuditEvent{SessionID: "s2"}))
	assert.NotEqual(t, hash1, l.prevHash)
}

func TestAppendWriteError(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l, err := New(path)
	require.NoError(t, err)

	require.NoError(t, l.f.Close())
	err = l.Append(context.Background(), sriracha.AuditEvent{})
	assert.Error(t, err)
}

func TestVerifyEmptyFile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l, err := New(path)
	require.NoError(t, err)
	assert.NoError(t, l.Verify(context.Background()))
}

func TestVerifyValidChain(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l, err := New(path)
	require.NoError(t, err)

	for i := range 3 {
		require.NoError(t, l.Append(context.Background(), sriracha.AuditEvent{RecordCount: i}))
	}
	assert.NoError(t, l.Verify(context.Background()))
}

func TestVerifyBrokenChain(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l, err := New(path)
	require.NoError(t, err)

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

	l, err := New(path)
	require.NoError(t, err)
	assert.Error(t, l.Verify(context.Background()))
}

func TestVerifyMissingFile(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	l, err := New(path)
	require.NoError(t, err)

	require.NoError(t, os.Remove(path))
	assert.Error(t, l.Verify(context.Background()))
}

func TestReopenAndExtend(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")

	l1, err := New(path)
	require.NoError(t, err)
	for i := range 3 {
		require.NoError(t, l1.Append(context.Background(), sriracha.AuditEvent{RecordCount: i}))
	}

	l2, err := New(path)
	require.NoError(t, err)
	for i := range 2 {
		require.NoError(t, l2.Append(context.Background(), sriracha.AuditEvent{RecordCount: 100 + i}))
	}

	assert.NoError(t, l2.Verify(context.Background()))
}

func TestNewEventIDFormat(t *testing.T) {
	t.Parallel()
	id, err := newEventID()
	require.NoError(t, err)

	parts := strings.Split(id, "-")
	require.Len(t, parts, 5)
	assert.Len(t, parts[0], 8)
	assert.Len(t, parts[1], 4)
	assert.Len(t, parts[2], 4)
	assert.Len(t, parts[3], 4)
	assert.Len(t, parts[4], 12)
}

func TestNewEventIDUnique(t *testing.T) {
	t.Parallel()
	id1, err := newEventID()
	require.NoError(t, err)
	id2, err := newEventID()
	require.NoError(t, err)
	assert.NotEqual(t, id1, id2)
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
	l, err := New(path)
	require.NoError(t, err)

	require.NoError(t, l.Append(context.Background(), sriracha.AuditEvent{SessionID: "s1"}))

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	// Insert a blank line after the first event.
	require.NoError(t, os.WriteFile(path, append(data, '\n'), 0600))

	assert.NoError(t, l.Verify(context.Background()))
}

// TestVerifyInvalidJSON checks that a line containing non-JSON data causes
// Verify to return an error, covering the json.Unmarshal failure branch.
func TestVerifyInvalidJSON(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "audit.jsonl")
	require.NoError(t, os.WriteFile(path, []byte("not-json\n"), 0600))

	l, err := New(path)
	require.NoError(t, err)
	assert.Error(t, l.Verify(context.Background()))
}
