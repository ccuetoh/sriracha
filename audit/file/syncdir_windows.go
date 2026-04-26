//go:build windows

package file

import "os"

// syncDir is a best-effort no-op on Windows. FlushFileBuffers on a directory
// handle requires GENERIC_WRITE access, which os.Open(dir) does not grant, so
// every attempt returns "Access is denied". Audit-log durability on Windows
// therefore depends on the per-file Sync that Append performs after each event.
// The Open is still attempted so that a missing path is reported, matching the
// Unix behaviour for that error.
func syncDir(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	return d.Close()
}

// syncOpenDir mirrors the Unix helper so internal tests build and run on
// Windows. Production code on Windows does not invoke this path.
func syncOpenDir(d *os.File) error {
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return err
	}
	return d.Close()
}
