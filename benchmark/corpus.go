package benchmark

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"

	"go.sriracha.dev/normalize"
	"go.sriracha.dev/sriracha"
)

// Record is one row of a Sriracha benchmark corpus. CanonicalID is the
// ground-truth person identity: two records with the same CanonicalID are a
// positive pair, two with different CanonicalIDs are a negative pair.
// EntityID and Dataset are kept for diagnostic output (which row failed,
// where did it come from) and play no role in scoring.
type Record struct {
	CanonicalID string             `json:"canonical_id"`
	EntityID    string             `json:"entity_id"`
	Dataset     string             `json:"dataset"`
	Fields      sriracha.RawRecord `json:"record"`
	Aliases     []string           `json:"aliases,omitempty"`
}

// LoadJSONL parses a newline-delimited JSON corpus from path. Each line must
// decode into a Record. Empty lines are skipped. The first malformed line
// short-circuits with an error annotated by its 1-based line number so a
// human can find the offending row.
func LoadJSONL(path string) ([]Record, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("benchmark: open corpus %q: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	return readJSONL(f, path)
}

// readJSONL is the io.Reader-friendly half of LoadJSONL, factored out so
// tests can feed a bytes.Reader without touching the filesystem.
func readJSONL(r io.Reader, source string) ([]Record, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)
	var records []Record
	line := 0
	for scanner.Scan() {
		line++
		raw := scanner.Bytes()
		if len(raw) == 0 {
			continue
		}
		var rec Record
		if err := json.Unmarshal(raw, &rec); err != nil {
			return nil, fmt.Errorf("benchmark: %s line %d: %w", source, line, err)
		}
		if rec.CanonicalID == "" {
			return nil, fmt.Errorf("benchmark: %s line %d: missing canonical_id", source, line)
		}
		records = append(records, rec)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("benchmark: read corpus %s: %w", source, err)
	}
	if len(records) == 0 {
		return nil, errors.New("benchmark: corpus is empty")
	}
	return records, nil
}

// Sanitize returns a copy of in with every field that fails Sriracha's
// normalization pipeline dropped. Real-world corpora carry plenty of values
// the normalizer rejects on principle (3-letter country codes, non-ISO-8601
// dates, malformed phones); without this pre-pass a single bad field would
// abort the whole record.
//
// dropped maps the dropped field path to the first normalization error for
// that path on this record — handy for reporting rather than swallowing.
func Sanitize(in sriracha.RawRecord) (clean sriracha.RawRecord, dropped map[sriracha.FieldPath]error) {
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

// GroupByCanonical buckets records by CanonicalID, preserving order within
// each bucket. The values are indices into records (not copies of Record),
// so callers can drive sampling without duplicating data.
func GroupByCanonical(records []Record) map[string][]int {
	groups := make(map[string][]int)
	for i, r := range records {
		groups[r.CanonicalID] = append(groups[r.CanonicalID], i)
	}
	return groups
}
