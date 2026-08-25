// Package fieldset validates records against a Sriracha FieldSet and exposes
// the canonical default schema.
//
// ValidateRecord runs the same per-field normalization as the tokenizer and
// reports every required-but-missing field, unknown path, and normalization
// failure in a single pass, useful for batch-ingest pre-flight checks.
// Schema validation itself lives on the type; see sriracha.FieldSet.Validate.
// DefaultFieldSet returns a deep copy of the canonical 16-field schema with
// conservative weight defaults; tune via token.Calibrate against a labeled
// pair set.
package fieldset

import (
	"errors"
	"slices"
	"strings"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/normalize"
)

// ValidateRecord reports every problem with record relative to fs in one
// pass: required-but-missing fields, required fields that normalize to the
// empty string, unknown paths in record, and per-field normalization
// failures. Returns nil when record is fully valid.
//
// Problems are reported as a single error joined with errors.Join. Every
// leaf is a sriracha.FieldError, so errors.Is finds the sentinel
// (sriracha.ErrRequiredFieldMissing, sriracha.ErrEmptyValue,
// sriracha.ErrUnknownField, or whatever normalize returned) and errors.As
// recovers the offending path. Unknown paths are reported in sorted order so
// the joined message does not depend on map iteration order.
//
// This is a pre-flight check. Calling it followed by tokenization runs the
// normalizer twice, acceptable for batch ingest where surfacing all problems
// at once is worth the cost.
func ValidateRecord(record sriracha.RawRecord, fs sriracha.FieldSet) error {
	var errs []error

	known := make(map[sriracha.FieldPath]struct{}, len(fs.Fields))
	for _, spec := range fs.Fields {
		known[spec.Path] = struct{}{}
		raw, ok := record[spec.Path]
		if !ok {
			if spec.Required {
				errs = append(errs, sriracha.FieldError{Path: spec.Path, Err: sriracha.ErrRequiredFieldMissing})
			}
			continue
		}
		norm, err := normalize.Normalize(raw, spec.Path)
		if err != nil {
			errs = append(errs, sriracha.FieldError{Path: spec.Path, Err: err})
			continue
		}
		if spec.Required && norm == "" {
			errs = append(errs, sriracha.FieldError{Path: spec.Path, Err: sriracha.ErrEmptyValue})
		}
	}

	unknown := make([]sriracha.FieldPath, 0, len(record))
	for path := range record {
		if _, ok := known[path]; !ok {
			unknown = append(unknown, path)
		}
	}
	slices.SortFunc(unknown, func(a, b sriracha.FieldPath) int {
		return strings.Compare(a.String(), b.String())
	})
	for _, path := range unknown {
		errs = append(errs, sriracha.FieldError{Path: path, Err: sriracha.ErrUnknownField})
	}

	return errors.Join(errs...)
}
