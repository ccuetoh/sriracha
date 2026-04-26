package file

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"go.sriracha.dev/sriracha"
)

var _ sriracha.AuditLog = (*Log)(nil)

// Log is an append-only JSONL audit log with SHA-256 hash chaining.
type Log struct {
	mu          sync.Mutex
	f           *os.File
	path        string
	prevHash    [32]byte
	randRead    func([]byte) (int, error) // nil → crypto/rand.Read; overridable in tests
	marshalJSON func(any) ([]byte, error) // nil → json.Marshal; overridable in tests
}

// New opens or creates the JSONL audit log at path.
// If the file already contains events, the in-memory previous hash is seeded
// from the last event so that further appends extend the chain correctly.
func New(path string) (*Log, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		return nil, fmt.Errorf("audit/file: open %s: %w", path, err)
	}
	return newLog(f, path)
}

// newLog initialises a Log from an already-open write handle. It seeds the
// previous hash from the file at seedPath and closes f if seeding fails.
func newLog(f *os.File, seedPath string) (*Log, error) {
	l := &Log{f: f, path: seedPath}
	if err := l.seedHash(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return l, nil
}

// seedHash opens the log file for reading and delegates to scanSeed.
func (l *Log) seedHash() error {
	rf, err := os.Open(l.path)
	if err != nil {
		return fmt.Errorf("audit/file: seed open: %w", err)
	}
	defer rf.Close()
	return l.scanSeed(rf)
}

// scanSeed reads r line by line and sets prevHash to SHA-256 of the last
// non-empty line, so subsequent appends extend the chain correctly.
func (l *Log) scanSeed(r io.Reader) error {
	var last string
	sc := bufio.NewScanner(r)
	for sc.Scan() {
		if line := sc.Text(); line != "" {
			last = line
		}
	}
	if err := sc.Err(); err != nil {
		return fmt.Errorf("audit/file: seed scan: %w", err)
	}
	if last != "" {
		l.prevHash = sha256.Sum256([]byte(last))
	}
	return nil
}

// newEventID returns a UUID v4 string using readFn as the entropy source.
func newEventID(readFn func([]byte) (int, error)) (string, error) {
	var b [16]byte
	if _, err := readFn(b[:]); err != nil {
		return "", err
	}
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16]), nil
}

// Append writes ev to the log as a JSON line. It sets EventID (UUID v4) and
// PreviousHash (SHA-256 of the previous event's raw JSON) before writing.
// The caller must not set EventID or PreviousHash; the implementation owns them.
func (l *Log) Append(_ context.Context, ev sriracha.AuditEvent) error {
	readFn := l.randRead
	if readFn == nil {
		readFn = rand.Read
	}
	id, err := newEventID(readFn)
	if err != nil {
		return fmt.Errorf("audit/file: event ID: %w", err)
	}

	marshalFn := l.marshalJSON
	if marshalFn == nil {
		marshalFn = json.Marshal
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	ev.EventID = id
	ev.PreviousHash = l.prevHash

	raw, err := marshalFn(ev)
	if err != nil {
		return fmt.Errorf("audit/file: marshal: %w", err)
	}

	if _, err := l.f.Write(append(raw, '\n')); err != nil {
		return fmt.Errorf("audit/file: write: %w", err)
	}

	l.prevHash = sha256.Sum256(raw)
	return nil
}

// Verify re-reads the file from the beginning and checks that every event's
// PreviousHash equals SHA-256 of the preceding event's raw JSON bytes, and
// that no EventID is empty. Returns nil for an empty log or a valid chain.
func (l *Log) Verify(_ context.Context) error {
	rf, err := os.Open(l.path)
	if err != nil {
		return fmt.Errorf("audit/file: verify open: %w", err)
	}
	defer rf.Close()

	var prevHash [32]byte

	sc := bufio.NewScanner(rf)
	for sc.Scan() {
		raw := []byte(sc.Text())
		if len(raw) == 0 {
			continue
		}

		var ev sriracha.AuditEvent
		if err := json.Unmarshal(raw, &ev); err != nil {
			return fmt.Errorf("audit/file: verify unmarshal: %w", err)
		}
		if ev.EventID == "" {
			return errors.New("audit/file: empty event ID")
		}
		if ev.PreviousHash != prevHash {
			return errors.New("audit/file: broken hash chain")
		}

		prevHash = sha256.Sum256(raw)
	}
	return sc.Err()
}
