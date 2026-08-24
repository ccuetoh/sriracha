// Package fieldset validates Sriracha FieldSet schemas and the records
// produced against them, and exposes the canonical default schema.
//
// Validate rejects malformed FieldSets: empty version, duplicate paths,
// negative weights, or a probabilistic configuration that would crash or
// produce degenerate (all-zero) filters at tokenization time.
// ValidateRecord runs the same per-field normalization as the tokenizer
// and reports every required-but-missing field, unknown path, and
// normalization failure in a single pass — useful for batch-ingest
// pre-flight checks. DefaultFieldSet returns a deep copy of the canonical
// 16-field schema with conservative weight defaults; tune via
// token.Calibrate against a labeled pair set.
package fieldset

import (
	"errors"
	"fmt"
	"math"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/normalize"
)

// Validate checks that fs is a well-formed FieldSet.
// Returns an error if:
//   - Version is empty
//   - Any Path is the zero value
//   - Any Path appears more than once
//   - Any Weight is negative, NaN, or infinite
//   - ProbabilisticParams is invalid (zero size, odd size with Balanced set,
//     non-positive hash count, or empty/non-positive ngram sizes)
func Validate(fs sriracha.FieldSet) error {
	if fs.Version == "" {
		return errors.New("fieldset: version must not be empty")
	}

	seen := make(map[sriracha.FieldPath]struct{}, len(fs.Fields))
	for i, f := range fs.Fields {
		if f.Path.String() == "" {
			return fmt.Errorf("fieldset: field %d has empty path", i)
		}
		if _, dup := seen[f.Path]; dup {
			return fmt.Errorf("fieldset: duplicate field path %q", f.Path)
		}

		seen[f.Path] = struct{}{}
		if f.Weight < 0 {
			return fmt.Errorf("fieldset: field %q has negative weight %f", f.Path, f.Weight)
		}
		if math.IsNaN(f.Weight) || math.IsInf(f.Weight, 0) {
			return fmt.Errorf("fieldset: field %q has non-finite weight %f", f.Path, f.Weight)
		}
	}

	return validateProbabilisticParams(fs.ProbabilisticParams)
}

// ValidateRecord reports every problem with record relative to fs in one
// pass: required-but-missing fields, required fields that normalize to the
// empty string, unknown paths in record, and per-field normalization
// failures. Returns nil when record is fully valid.
//
// This is a pre-flight check. Calling it followed by tokenization runs the
// normalizer twice — acceptable for batch ingest where surfacing all errors
// at once is worth the cost.
func ValidateRecord(record sriracha.RawRecord, fs sriracha.FieldSet) []error {
	var errs []error

	known := make(map[sriracha.FieldPath]struct{}, len(fs.Fields))
	for _, spec := range fs.Fields {
		known[spec.Path] = struct{}{}
		raw, ok := record[spec.Path]
		if !ok {
			if spec.Required {
				errs = append(errs, fmt.Errorf("fieldset: required field %q missing", spec.Path))
			}
			continue
		}
		norm, err := normalize.Normalize(raw, spec.Path)
		if err != nil {
			errs = append(errs, fmt.Errorf("fieldset: field %q: %w", spec.Path, err))
			continue
		}
		if spec.Required && norm == "" {
			errs = append(errs, fmt.Errorf("fieldset: required field %q is empty", spec.Path))
		}
	}

	for path := range record {
		if _, ok := known[path]; !ok {
			errs = append(errs, fmt.Errorf("fieldset: unknown field %q (not in FieldSet)", path))
		}
	}

	return errs
}

// validateProbabilisticParams rejects ProbabilisticConfig values that would crash or produce
// degenerate (all-zero) filters at tokenization time. The checks mirror the
// token package's internal validator.
func validateProbabilisticParams(cfg sriracha.ProbabilisticConfig) error {
	if cfg.SizeBits == 0 {
		return errors.New("fieldset: ProbabilisticParams.SizeBits must be > 0")
	}
	if cfg.Balanced && cfg.SizeBits%2 != 0 {
		return fmt.Errorf("fieldset: ProbabilisticParams.SizeBits must be even when Balanced, got %d", cfg.SizeBits)
	}
	if cfg.HashCount <= 0 {
		return errors.New("fieldset: ProbabilisticParams.HashCount must be > 0")
	}
	if len(cfg.NgramSizes) == 0 {
		return errors.New("fieldset: ProbabilisticParams.NgramSizes must not be empty")
	}
	for i, sz := range cfg.NgramSizes {
		if sz <= 0 {
			return fmt.Errorf("fieldset: ProbabilisticParams.NgramSizes[%d] must be > 0, got %d", i, sz)
		}
	}
	return nil
}
