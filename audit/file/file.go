package file

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/segmentio/ksuid"

	"go.sriracha.dev/sriracha"
)

var _ sriracha.AuditLog = (*log)(nil)

// auditFileMode is the permission mode enforced on the audit log file. The
// log may contain identifier hashes and policy IDs; world/group access would
// leak that even if the contents are integrity-protected by the hash chain.
const auditFileMode = 0o600

// scannerMaxBytes caps the per-line buffer used when seeding the hash chain
// or verifying the log. A single AuditEvent that serialises to more than this
// would otherwise be silently truncated by bufio.Scanner's 64 KiB default.
const scannerMaxBytes = 16 * 1024 * 1024

// log is an append-only JSONL audit log with SHA-256 hash chaining.
type log struct {
	mu       sync.Mutex
	f        *os.File
	path     string
	prevHash [32]byte
}

// New opens or creates the JSONL audit log at path.
// If the file already contains events, the in-memory previous hash is seeded
// from the last event so that further appends extend the chain correctly.
func New(path string) (sriracha.AuditLog, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, auditFileMode) //nolint:gosec // path is caller-supplied by design
	if err != nil {
		return nil, fmt.Errorf("audit/file: open %s: %w", path, err)
	}
	// O_CREATE only honors the mode for new files; tighten any pre-existing
	// looser mode so a deployment never leaks the audit log via group/world bits.
	if err := f.Chmod(auditFileMode); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("audit/file: chmod %s: %w", path, err)
	}
	// fsync the parent directory so the file's existence survives a crash
	// even before the first Append.
	if err := syncDir(filepath.Dir(path)); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("audit/file: sync dir: %w", err)
	}
	return newLog(f, path)
}

// newLog initialises a log from an already-open write handle. It seeds the
// previous hash from the file at seedPath and closes f if seeding fails.
func newLog(f *os.File, seedPath string) (*log, error) {
	l := &log{f: f, path: seedPath}
	if err := l.seedHash(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return l, nil
}

// seedHash opens the log file for reading and delegates to scanSeed.
func (l *log) seedHash() error {
	rf, err := os.Open(l.path)
	if err != nil {
		return fmt.Errorf("audit/file: seed open: %w", err)
	}
	defer rf.Close() //nolint:errcheck // read-only; close error is not actionable
	return l.scanSeed(rf)
}

// scanSeed reads r line by line and sets prevHash to SHA-256 of the last
// non-empty line, so subsequent appends extend the chain correctly.
func (l *log) scanSeed(r io.Reader) error {
	var last string
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 64*1024), scannerMaxBytes)
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

// Append writes ev to the log as a JSON line. It sets EventID (KSUID) and
// PreviousHash (SHA-256 of the previous event's raw JSON) before writing,
// then fsyncs the file before returning so the chain advances only after the
// event is durable on disk.
// The caller must not set EventID or PreviousHash; the implementation owns them.
func (l *log) Append(_ context.Context, ev sriracha.AuditEvent) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	ev.EventID = ksuid.New().String()
	ev.PreviousHash = l.prevHash

	raw, err := json.Marshal(ev)
	if err != nil {
		return fmt.Errorf("audit/file: marshal: %w", err)
	}

	if _, err := l.f.Write(append(raw, '\n')); err != nil {
		return fmt.Errorf("audit/file: write: %w", err)
	}
	if err := l.f.Sync(); err != nil {
		return fmt.Errorf("audit/file: sync: %w", err)
	}

	l.prevHash = sha256.Sum256(raw)
	return nil
}

// Verify re-reads the file from the beginning and checks that every event's
// PreviousHash equals SHA-256 of the preceding event's raw JSON bytes, and
// that no EventID is empty. Returns nil for an empty log or a valid chain.
func (l *log) Verify(_ context.Context) error {
	rf, err := os.Open(l.path)
	if err != nil {
		return fmt.Errorf("audit/file: verify open: %w", err)
	}
	defer rf.Close() //nolint:errcheck // read-only; close error is not actionable

	var prevHash [32]byte

	sc := bufio.NewScanner(rf)
	sc.Buffer(make([]byte, 0, 64*1024), scannerMaxBytes)
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
