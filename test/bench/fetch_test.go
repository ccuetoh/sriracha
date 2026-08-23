//go:build bench

package bench

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fetchTestRel is a corpus path that exists neither in the tree nor in
// the real remote table, so every test below controls its fate entirely
// through the injected table.
const fetchTestRel = "fetchtest/fetch_test_corpus.jsonl"

var fetchTestCorpus = []byte(`{"canonical_id":"c1","record":{"sriracha::name::given":"Alice"}}
{"canonical_id":"c1","record":{"sriracha::name::given":"Alicia"}}
`)

func gzipBytes(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := zw.Write(data)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

// serveBytes starts an httptest server that serves body on every request
// and returns the server URL plus a hit counter.
func serveBytes(t *testing.T, body []byte) (string, *atomic.Int64) {
	t.Helper()
	var hits atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return srv.URL, &hits
}

func TestEnsureCorpusTreeFile(t *testing.T) {
	t.Parallel()

	t.Run("InjectedHelperShortCircuits", func(t *testing.T) {
		t.Parallel()
		path, err := ensureCorpusIn("febrl4/febrl4.jsonl", t.TempDir(), nil, true)
		require.NoError(t, err)
		assert.Equal(t, corpusPath("febrl4/febrl4.jsonl"), path)
		_, err = os.Stat(path)
		require.NoError(t, err)
	})

	t.Run("EnsureCorpusShortCircuits", func(t *testing.T) {
		t.Parallel()
		path, err := ensureCorpus("febrl4/febrl4.jsonl")
		require.NoError(t, err)
		assert.Equal(t, corpusPath("febrl4/febrl4.jsonl"), path)
	})
}

func TestEnsureCorpusCacheHit(t *testing.T) {
	t.Parallel()

	cacheDir := t.TempDir()
	cached := filepath.Join(cacheDir, filepath.FromSlash(fetchTestRel))
	require.NoError(t, os.MkdirAll(filepath.Dir(cached), 0o755))
	require.NoError(t, os.WriteFile(cached, fetchTestCorpus, 0o600))

	table := []remoteCorpus{{
		rel:         fetchTestRel,
		url:         "http://127.0.0.1:0/never-reached",
		gzipSHA256:  sha256Hex(gzipBytes(t, fetchTestCorpus)),
		plainSHA256: sha256Hex(fetchTestCorpus),
	}}

	path, err := ensureCorpusIn(fetchTestRel, cacheDir, table, false)
	require.NoError(t, err)
	assert.Equal(t, cached, path)
}

func TestEnsureCorpusDownload(t *testing.T) {
	t.Parallel()

	gz := gzipBytes(t, fetchTestCorpus)
	url, hits := serveBytes(t, gz)
	cacheDir := t.TempDir()
	table := []remoteCorpus{{
		rel:         fetchTestRel,
		url:         url,
		gzipSHA256:  sha256Hex(gz),
		plainSHA256: sha256Hex(fetchTestCorpus),
	}}

	path, err := ensureCorpusIn(fetchTestRel, cacheDir, table, false)
	require.NoError(t, err)
	assert.Equal(t, filepath.Join(cacheDir, filepath.FromSlash(fetchTestRel)), path)

	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, fetchTestCorpus, got)
	assert.EqualValues(t, 1, hits.Load())

	again, err := ensureCorpusIn(fetchTestRel, cacheDir, table, false)
	require.NoError(t, err)
	assert.Equal(t, path, again)
	assert.EqualValues(t, 1, hits.Load(), "second call must be served from cache")
}

func TestEnsureCorpusStaleCacheRedownloads(t *testing.T) {
	t.Parallel()

	gz := gzipBytes(t, fetchTestCorpus)
	url, hits := serveBytes(t, gz)
	cacheDir := t.TempDir()
	cached := filepath.Join(cacheDir, filepath.FromSlash(fetchTestRel))
	require.NoError(t, os.MkdirAll(filepath.Dir(cached), 0o755))
	require.NoError(t, os.WriteFile(cached, []byte("stale bytes"), 0o600))

	table := []remoteCorpus{{
		rel:         fetchTestRel,
		url:         url,
		gzipSHA256:  sha256Hex(gz),
		plainSHA256: sha256Hex(fetchTestCorpus),
	}}

	path, err := ensureCorpusIn(fetchTestRel, cacheDir, table, false)
	require.NoError(t, err)
	got, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, fetchTestCorpus, got)
	assert.EqualValues(t, 1, hits.Load())
}

func TestEnsureCorpusErrors(t *testing.T) {
	t.Parallel()

	gz := gzipBytes(t, fetchTestCorpus)
	goodGzip := sha256Hex(gz)
	goodPlain := sha256Hex(fetchTestCorpus)
	wrong := sha256Hex([]byte("something else"))

	cases := []struct {
		name        string
		gzipSHA256  string
		plainSHA256 string
		offline     bool
		wantOffline bool
		wantInErr   string
	}{
		{
			name:        "GzipDigestMismatch",
			gzipSHA256:  wrong,
			plainSHA256: goodPlain,
			wantInErr:   "gzip digest mismatch",
		},
		{
			name:        "UncompressedDigestMismatch",
			gzipSHA256:  goodGzip,
			plainSHA256: wrong,
			wantInErr:   "uncompressed digest mismatch",
		},
		{
			name:        "OfflineWithoutCache",
			gzipSHA256:  goodGzip,
			plainSHA256: goodPlain,
			offline:     true,
			wantOffline: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			url, _ := serveBytes(t, gz)
			cacheDir := t.TempDir()
			table := []remoteCorpus{{
				rel:         fetchTestRel,
				url:         url,
				gzipSHA256:  tc.gzipSHA256,
				plainSHA256: tc.plainSHA256,
			}}

			_, err := ensureCorpusIn(fetchTestRel, cacheDir, table, tc.offline)
			require.Error(t, err)
			if tc.wantOffline {
				assert.ErrorIs(t, err, errCorpusOffline)
			}
			if tc.wantInErr != "" {
				assert.Contains(t, err.Error(), tc.wantInErr)
			}

			cached := filepath.Join(cacheDir, filepath.FromSlash(fetchTestRel))
			_, statErr := os.Stat(cached)
			assert.True(t, os.IsNotExist(statErr), "no cache file may remain after failure")
			assert.Empty(t, leftoverFiles(t, cacheDir), "no temp files may remain after failure")
		})
	}

	t.Run("UnknownRel", func(t *testing.T) {
		t.Parallel()
		_, err := ensureCorpusIn("nope/missing.jsonl", t.TempDir(), nil, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no remote snapshot entry")
	})

	t.Run("HTTPErrorStatus", func(t *testing.T) {
		t.Parallel()
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "gone", http.StatusNotFound)
		}))
		t.Cleanup(srv.Close)
		table := []remoteCorpus{{
			rel:         fetchTestRel,
			url:         srv.URL,
			gzipSHA256:  goodGzip,
			plainSHA256: goodPlain,
		}}
		_, err := ensureCorpusIn(fetchTestRel, t.TempDir(), table, false)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unexpected status")
	})

	t.Run("CorruptGzipWithMatchingDigest", func(t *testing.T) {
		t.Parallel()
		notGzip := []byte("plain bytes, not a gzip stream")
		url, _ := serveBytes(t, notGzip)
		table := []remoteCorpus{{
			rel:         fetchTestRel,
			url:         url,
			gzipSHA256:  sha256Hex(notGzip),
			plainSHA256: goodPlain,
		}}
		cacheDir := t.TempDir()
		_, err := ensureCorpusIn(fetchTestRel, cacheDir, table, false)
		require.Error(t, err)
		assert.Empty(t, leftoverFiles(t, cacheDir))
	})
}

// leftoverFiles walks dir and returns every regular file found, so tests
// can assert that failed downloads clean up both the final path and any
// temp files.
func leftoverFiles(t *testing.T, dir string) []string {
	t.Helper()
	var files []string
	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	require.NoError(t, err)
	return files
}
