package fieldset

import (
	"errors"
	"fmt"

	"go.sriracha.dev/sriracha"
)

// Validate checks that fs is a well-formed FieldSet.
// Returns an error if:
//   - Version is empty
//   - Any Path appears more than once
//   - Any Weight is negative
//   - BloomParams is invalid (zero size, zero hash count, or empty/non-positive ngram sizes)
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

	return validateBloomParams(fs.BloomParams)
}

// validateBloomParams rejects BloomConfig values that would crash or produce
// degenerate (all-zero) filters at tokenization time.
func validateBloomParams(cfg sriracha.BloomConfig) error {
	if cfg.SizeBits == 0 {
		return errors.New("fieldset: BloomParams.SizeBits must be > 0")
	}
	if cfg.HashCount == 0 {
		return errors.New("fieldset: BloomParams.HashCount must be > 0")
	}
	if len(cfg.NgramSizes) == 0 {
		return errors.New("fieldset: BloomParams.NgramSizes must not be empty")
	}
	for i, sz := range cfg.NgramSizes {
		if sz <= 0 {
			return fmt.Errorf("fieldset: BloomParams.NgramSizes[%d] must be > 0, got %d", i, sz)
		}
	}
	return nil
}
