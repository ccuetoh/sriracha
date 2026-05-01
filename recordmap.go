package sriracha

import "fmt"

// RecordFromMap converts a string-keyed map (CSV row, JSON object, SQL row,
// etc.) into a RawRecord, resolving each key as a fully-qualified FieldPath
// (org::namespace::name) and rejecting any path that does not appear in fs.
//
// Resolution is strict: short names like "given" are not auto-prefixed with
// "sriracha::name::". This avoids ambiguity when callers mix custom-org paths
// that happen to share local names with canonical fields.
//
// Both a partially-constructed record and a slice of every error encountered
// are returned; the record contains the entries that resolved cleanly so
// callers can choose to proceed with the valid subset. Returns a nil error
// slice on full success.
func RecordFromMap(m map[string]string, fs FieldSet) (RawRecord, []error) {
	known := make(map[FieldPath]struct{}, len(fs.Fields))
	for _, spec := range fs.Fields {
		known[spec.Path] = struct{}{}
	}

	record := make(RawRecord, len(m))
	var errs []error
	for key, value := range m {
		path, err := ParseFieldPath(key)
		if err != nil {
			errs = append(errs, fmt.Errorf("recordmap: key %q: %w", key, err))
			continue
		}
		if _, ok := known[path]; !ok {
			errs = append(errs, fmt.Errorf("recordmap: key %q is not in FieldSet", key))
			continue
		}
		record[path] = value
	}
	return record, errs
}
