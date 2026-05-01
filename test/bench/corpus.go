//go:build bench

package bench

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/normalize"
)

// record is one row of a Sriracha benchmark corpus. CanonicalID is the
// ground-truth person identity: two records with the same CanonicalID are a
// positive pair, two with different CanonicalIDs are a negative pair.
// EntityID and Dataset are kept for diagnostic output (which row failed,
// where did it come from) and play no role in scoring.
type record struct {
	CanonicalID string             `json:"canonical_id"`
	EntityID    string             `json:"entity_id"`
	Dataset     string             `json:"dataset"`
	Fields      sriracha.RawRecord `json:"record"`
	Aliases     []string           `json:"aliases,omitempty"`
}

// loadJSONL parses a newline-delimited JSON corpus from path. Each line must
// decode into a record. Empty lines are skipped. The first malformed line
// short-circuits with an error annotated by its 1-based line number so a
// human can find the offending row.
func loadJSONL(path string) ([]record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("bench: open corpus %q: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	return readJSONL(f, path)
}

// readJSONL is the io.Reader-friendly half of loadJSONL, factored out so
// tests can feed a bytes.Reader without touching the filesystem.
func readJSONL(r io.Reader, source string) ([]record, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	var records []record
	line := 0
	for scanner.Scan() {
		line++
		raw := scanner.Bytes()
		if len(raw) == 0 {
			continue
		}
		var rec record
		if err := json.Unmarshal(raw, &rec); err != nil {
			return nil, fmt.Errorf("bench: %s line %d: %w", source, line, err)
		}
		if rec.CanonicalID == "" {
			return nil, fmt.Errorf("bench: %s line %d: missing canonical_id", source, line)
		}
		records = append(records, rec)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("bench: read corpus %s: %w", source, err)
	}
	if len(records) == 0 {
		return nil, errors.New("bench: corpus is empty")
	}
	return records, nil
}

// sanitize returns a copy of in with every field that fails Sriracha's
// normalization pipeline dropped. Real-world corpora carry plenty of values
// the normalizer rejects on principle (3-letter country codes, non-ISO-8601
// dates, malformed phones); without this pre-pass a single bad field would
// abort the whole record.
//
// dropped maps the dropped field path to the first normalization error for
// that path on this record — handy for reporting rather than swallowing.
func sanitize(in sriracha.RawRecord) (clean sriracha.RawRecord, dropped map[sriracha.FieldPath]error) {
	clean = make(sriracha.RawRecord, len(in))
	for path, val := range in {
		if _, err := normalize.Normalize(val, path); err != nil {
			if dropped == nil {
				dropped = make(map[sriracha.FieldPath]error)
			}
			dropped[path] = err
			continue
		}
		clean[path] = val
	}
	return clean, dropped
}

// groupByCanonical buckets records by CanonicalID, preserving order within
// each bucket. The values are indices into records (not copies of record),
// so callers can drive sampling without duplicating data.
func groupByCanonical(records []record) map[string][]int {
	groups := make(map[string][]int)
	for i, r := range records {
		groups[r.CanonicalID] = append(groups[r.CanonicalID], i)
	}
	return groups
}
