//go:build !windows

package file

import "os"

// syncDir fsyncs the directory at path so the audit-log file's existence
// survives a crash even before the first Append. Errors are propagated to keep
// durability guarantees explicit rather than best-effort.
func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	return syncOpenDir(d)
}

// syncOpenDir fsyncs and closes the already-open directory handle. Split out
// from syncDir so the inner Sync error path is reachable in tests by passing a
// closed *os.File without altering the syncDir public contract.
func syncOpenDir(d *os.File) error {
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}
