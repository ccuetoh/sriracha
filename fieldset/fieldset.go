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

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/normalize"
)

// Validate checks that fs is a well-formed FieldSet.
// Returns an error if:
//   - Version is empty
//   - Any Path appears more than once
//   - Any Weight is negative
//   - ProbabilisticParams is invalid (zero size, zero hash count, or empty/non-positive ngram sizes)
func Validate(fs sriracha.FieldSet) error {
	if fs.Version == "" {
		return errors.New("fieldset: version must not be empty")
	}

	seen := make(map[sriracha.FieldPath]struct{}, len(fs.Fields))
	for _, f := range fs.Fields {
		if _, dup := seen[f.Path]; dup {
			return fmt.Errorf("fieldset: duplicate field path %q", f.Path)
		}

		seen[f.Path] = struct{}{}
		if f.Weight < 0 {
			return fmt.Errorf("fieldset: field %q has negative weight %f", f.Path, f.Weight)
		}
	}

	return validateProbabilisticParams(fs.ProbabilisticParams)
}

// ValidateRecord reports every problem with record relative to fs in one
// pass: required-but-missing fields, unknown paths in record, and per-field
// normalization failures. Returns nil when record is fully valid.
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
		if _, err := normalize.Normalize(raw, spec.Path); err != nil {
			errs = append(errs, fmt.Errorf("fieldset: field %q: %w", spec.Path, err))
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
// degenerate (all-zero) filters at tokenization time.
func validateProbabilisticParams(cfg sriracha.ProbabilisticConfig) error {
	if cfg.SizeBits == 0 {
		return errors.New("fieldset: ProbabilisticParams.SizeBits must be > 0")
	}
	if cfg.HashCount == 0 {
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
	if cfg.FlipProbability < 0 || cfg.FlipProbability >= 1 {
		return fmt.Errorf("fieldset: ProbabilisticParams.FlipProbability must be in [0, 1), got %v", cfg.FlipProbability)
	}
	if cfg.TargetPopcount >= cfg.SizeBits {
		return fmt.Errorf("fieldset: ProbabilisticParams.TargetPopcount must be < SizeBits, got %d (size %d)", cfg.TargetPopcount, cfg.SizeBits)
	}
	return nil
}
