//go:build bench

package bench

import (
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"time"
)

// downloadTimeout bounds one corpus download end to end. The snapshots
// are tens of megabytes, so a couple of minutes is generous even on a
// slow CI link.
const downloadTimeout = 2 * time.Minute

// remoteCorpus pins one downloadable corpus snapshot. Both digests are
// lowercase hex SHA-256. The gzip digest covers the file as served, the
// plain digest covers the decompressed JSONL. A snapshot change on the
// data branch must update both.
type remoteCorpus struct {
	rel         string // relative path under testdata/corpus/, slash-delimited
	url         string
	gzipSHA256  string
	plainSHA256 string
}

// remoteCorpora lists the corpora that are not stored in the source tree.
// Their frozen gzip snapshots live on the testdata-corpus branch of this
// repository.
var remoteCorpora = []remoteCorpus{
	{
		rel:         "ncvr/ncvr.jsonl",
		url:         "https://raw.githubusercontent.com/ccuetoh/sriracha/testdata-corpus/ncvr.jsonl.gz",
		gzipSHA256:  "7d1ae7084e1329c25a74e2372ffdc2c15873869157e05e1532d718d029261611",
		plainSHA256: "58bbc6e9c7e188cdc725e51d78409c418356884b19728ad1ea72eecdc03f3b7f",
	},
	{
		rel:         "opensanctions/open_sanctions.jsonl",
		url:         "https://raw.githubusercontent.com/ccuetoh/sriracha/testdata-corpus/open_sanctions.jsonl.gz",
		gzipSHA256:  "8ec089d67f4412c706cfa1bf7885c5ded9a8a389707bbed09743e0bab9f51c22",
		plainSHA256: "804e5d20c81956139d02c4bc54f9e04f5197b83dde557f5ab1ae39d7677fbb71",
	},
}

// corpusPath resolves rel against the module's testdata/corpus/.
// runtime.Caller anchors the lookup to this file's location so the path
// holds whether tests are launched from the repo root, the package
// directory, or an IDE.
func corpusPath(rel string) string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..", "testdata", "corpus", filepath.FromSlash(rel))
}

// errCorpusOffline reports that a corpus is absent locally and
// SRIRACHA_CORPUS_OFFLINE forbids downloading it. Callers detect it with
// errors.Is and skip the corpus instead of failing.
var errCorpusOffline = errors.New("bench: corpus download disabled by SRIRACHA_CORPUS_OFFLINE")

// ensureCorpus returns a path to the corpus file for rel, a slash-delimited
// path under testdata/corpus/. A file present in the tree wins. Otherwise
// the pinned snapshot is served from the cache dir, downloading and
// verifying it first when needed. SRIRACHA_CORPUS_CACHE overrides the
// cache dir, which defaults to testdata/corpus/.cache in the module.
// A non-empty SRIRACHA_CORPUS_OFFLINE turns a would-be download into
// errCorpusOffline.
func ensureCorpus(rel string) (string, error) {
	cacheDir := os.Getenv("SRIRACHA_CORPUS_CACHE")
	if cacheDir == "" {
		cacheDir = corpusPath(".cache")
	}
	return ensureCorpusIn(rel, cacheDir, remoteCorpora, os.Getenv("SRIRACHA_CORPUS_OFFLINE") != "")
}

// ensureCorpusIn is the injectable core of ensureCorpus. Tests supply
// their own table (with httptest URLs) and cache dir so no environment
// mutation is needed.
func ensureCorpusIn(rel, cacheDir string, table []remoteCorpus, offline bool) (string, error) {
	treePath := corpusPath(rel)
	if _, err := os.Stat(treePath); err == nil {
		return treePath, nil
	}

	var remote *remoteCorpus
	for i := range table {
		if table[i].rel == rel {
			remote = &table[i]
			break
		}
	}
	if remote == nil {
		return "", fmt.Errorf("bench: corpus %q is not in the tree and has no remote snapshot entry", rel)
	}

	cached := filepath.Join(cacheDir, filepath.FromSlash(rel))
	ok, err := fileMatchesSHA256(cached, remote.plainSHA256)
	if err != nil {
		return "", err
	}
	if ok {
		return cached, nil
	}

	if offline {
		return "", fmt.Errorf("bench: corpus %q: %w", rel, errCorpusOffline)
	}

	if err := downloadCorpus(remote, cached); err != nil {
		return "", fmt.Errorf("bench: download corpus %q: %w", rel, err)
	}
	return cached, nil
}

// fileMatchesSHA256 reports whether the file at path exists and hashes to
// wantHex. A missing file is a plain false, not an error, so callers fall
// through to the download path.
func fileMatchesSHA256(path, wantHex string) (bool, error) {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("bench: open cached corpus %q: %w", path, err)
	}
	defer func() { _ = f.Close() }()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return false, fmt.Errorf("bench: hash cached corpus %q: %w", path, err)
	}
	return hex.EncodeToString(h.Sum(nil)) == wantHex, nil
}

// downloadCorpus fetches remote's gzip snapshot, verifies both pinned
// digests, and installs the decompressed JSONL at dest atomically. The
// gzip bytes are verified before any decompression happens, and every
// intermediate lives in a temp file that is removed on failure, so a bad
// download never leaves a usable-looking cache entry behind.
func downloadCorpus(remote *remoteCorpus, dest string) error {
	dir := filepath.Dir(dest)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create cache dir %q: %w", dir, err)
	}

	gzPath, err := fetchGzip(remote.url, dir, remote.gzipSHA256)
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(gzPath) }()

	return decompressVerified(gzPath, dest, remote.plainSHA256)
}

// fetchGzip streams url into a temp file under dir, hashing as it goes,
// and returns the temp path once the digest matches wantHex. The caller
// owns the returned file.
func fetchGzip(url, dir, wantHex string) (string, error) {
	client := &http.Client{Timeout: downloadTimeout}
	resp, err := client.Get(url)
	if err != nil {
		return "", fmt.Errorf("get %q: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("get %q: unexpected status %s", url, resp.Status)
	}

	tmp, err := os.CreateTemp(dir, "corpus-*.gz.tmp")
	if err != nil {
		return "", fmt.Errorf("create temp file in %q: %w", dir, err)
	}
	tmpPath := tmp.Name()

	h := sha256.New()
	_, err = io.Copy(io.MultiWriter(tmp, h), resp.Body)
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("stream %q: %w", url, err)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != wantHex {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("gzip digest mismatch for %q: got %s, want %s", url, got, wantHex)
	}
	return tmpPath, nil
}

// decompressVerified decompresses the gzip file at gzPath into a temp
// file next to dest, hashing the plain bytes as they stream, and renames
// the temp file onto dest only after the digest matches wantHex.
func decompressVerified(gzPath, dest, wantHex string) error {
	in, err := os.Open(gzPath)
	if err != nil {
		return fmt.Errorf("open %q: %w", gzPath, err)
	}
	defer func() { _ = in.Close() }()

	zr, err := gzip.NewReader(in)
	if err != nil {
		return fmt.Errorf("gzip reader for %q: %w", gzPath, err)
	}

	tmp, err := os.CreateTemp(filepath.Dir(dest), "corpus-*.jsonl.tmp")
	if err != nil {
		return fmt.Errorf("create temp file for %q: %w", dest, err)
	}
	tmpPath := tmp.Name()

	h := sha256.New()
	_, err = io.Copy(io.MultiWriter(tmp, h), zr)
	if closeErr := zr.Close(); err == nil {
		err = closeErr
	}
	if closeErr := tmp.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("decompress %q: %w", gzPath, err)
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != wantHex {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("uncompressed digest mismatch for %q: got %s, want %s", dest, got, wantHex)
	}
	if err := os.Rename(tmpPath, dest); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("install %q: %w", dest, err)
	}
	return nil
}
