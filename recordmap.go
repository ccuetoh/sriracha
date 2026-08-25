package sriracha

import (
	"errors"
	"maps"
	"slices"
)

// RecordFromMap converts a string-keyed map (CSV row, JSON object, SQL row,
// etc.) into a RawRecord, resolving each key as a fully-qualified FieldPath
// (org::namespace::name) and rejecting any path that does not appear in fs.
//
// Resolution is strict: short names like "given" are not auto-prefixed with
// "sriracha::name::". This avoids ambiguity when callers mix custom-org paths
// that happen to share local names with canonical fields.
//
// Both a partially-constructed record and the joined error are returned; the
// record contains the entries that resolved cleanly so callers can choose to
// proceed with the valid subset. The error is an errors.Join of one
// FieldError per rejected key, wrapping ErrInvalidFieldPath for a malformed
// key and ErrUnknownField for a well-formed key that fs does not declare.
// Keys are processed in sorted order, so the joined message is stable.
// Returns a nil error on full success.
func RecordFromMap(m map[string]string, fs FieldSet) (RawRecord, error) {
	known := make(map[FieldPath]struct{}, len(fs.Fields))
	for _, spec := range fs.Fields {
		known[spec.Path] = struct{}{}
	}

	record := make(RawRecord, len(m))
	var errs []error
	for _, key := range slices.Sorted(maps.Keys(m)) {
		path, err := ParseFieldPath(key)
		if err != nil {
			errs = append(errs, FieldError{Path: FieldPath{raw: key}, Err: err})
			continue
		}
		if _, ok := known[path]; !ok {
			errs = append(errs, FieldError{Path: path, Err: ErrUnknownField})
			continue
		}
		record[path] = m[key]
	}
	return record, errors.Join(errs...)
}
