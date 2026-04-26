package fieldset

import (
	"errors"
	"fmt"

	"github.com/Masterminds/semver/v3"

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

// Compatible reports whether a and b can be used together.
// Version strings must match exactly.
func Compatible(a, b sriracha.FieldSet) bool {
	return a.Version == b.Version
}

// NegotiateVersion returns the highest semver-compatible version in supported
// that matches requested by major version. Both supported entries and
// requested are parsed as semver; entries that fail to parse are skipped.
//
// "Top-level match" means the major component must equal requested's major;
// among matching entries, the one with the highest (major, minor, patch) is
// returned.
//
// Returns an error if supported is empty, requested is empty, requested does
// not parse as semver, or no entry has a matching major version.
func NegotiateVersion(supported []string, requested string) (string, error) {
	if len(supported) == 0 {
		return "", errors.New("fieldset: no supported versions")
	}
	if requested == "" {
		return "", errors.New("fieldset: requested version is empty")
	}

	reqV, err := semver.NewVersion(requested)
	if err != nil {
		return "", fmt.Errorf("fieldset: requested version %q is not valid semver: %w", requested, err)
	}

	var (
		bestRaw string
		bestVer *semver.Version
	)
	for _, s := range supported {
		v, err := semver.NewVersion(s)
		if err != nil {
			continue
		}
		if v.Major() != reqV.Major() {
			continue
		}
		if bestVer == nil || v.GreaterThan(bestVer) {
			bestVer = v
			bestRaw = s
		}
	}

	if bestVer == nil {
		return "", fmt.Errorf("fieldset: no compatible version: requested %q (major %d) not in %v", requested, reqV.Major(), supported)
	}
	return bestRaw, nil
}
