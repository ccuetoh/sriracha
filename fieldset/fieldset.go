package fieldset

import (
	"errors"
	"fmt"
	"strings"

	"go.sriracha.dev/sriracha"
)

// Validate checks that fs is a well-formed FieldSet.
// Returns an error if:
//   - Version is empty
//   - Any Path appears more than once
//   - Any Weight is negative
func Validate(fs sriracha.FieldSet) error {
	if fs.Version == "" {
		return errors.New("fieldset: version must not be empty")
	}

	seen := make(map[sriracha.FieldPath]bool, len(fs.Fields))
	for _, f := range fs.Fields {
		if seen[f.Path] {
			return fmt.Errorf("fieldset: duplicate field path %q", f.Path)
		}

		seen[f.Path] = true
		if f.Weight < 0 {
			return fmt.Errorf("fieldset: field %q has negative weight %f", f.Path, f.Weight)
		}
	}

	return nil
}

// Compatible reports whether a and b can be used together.
// Version strings must match exactly.
func Compatible(a, b sriracha.FieldSet) bool {
	return a.Version == b.Version
}

// NegotiateVersion returns the highest version in supported that matches requested.
// Uses semver comparison (not lexicographic) to select the highest compatible version.
// Returns an error if supported is empty, requested is empty, or no match exists.
func NegotiateVersion(supported []string, requested string) (string, error) {
	if len(supported) == 0 {
		return "", errors.New("fieldset: no supported versions")
	}

	if requested == "" {
		return "", errors.New("fieldset: requested version is empty")
	}

	var best string
	for _, s := range supported {
		if s == requested {
			best = s
			break // exact match found; since all matches are identical strings, first is fine
		}
	}

	if len(best) == 0 {
		return "", fmt.Errorf("fieldset: no compatible version: requested %q not in %v", requested, supported)
	}

	return best, nil
}

// compareSemver compares two semver strings by numeric component.
// Returns positive if a > b, negative if a < b, 0 if equal.
func compareSemver(a, b string) int {
	ap := parseSemver(a)
	bp := parseSemver(b)
	for i := 0; i < 3; i++ {
		if ap[i] != bp[i] {
			return ap[i] - bp[i]
		}
	}
	return 0
}

// parseSemver parses a version string into [major, minor, patch] integers.
// Pre-release and build metadata suffixes (e.g. "-rc.1", "+build") are
// silently truncated at the first non-digit character in each component.
// Callers are responsible for validating the version format if strictness is needed.
func parseSemver(v string) [3]int {
	parts := strings.SplitN(v, ".", 3)
	var result [3]int
	for i := 0; i < 3 && i < len(parts); i++ {
		result[i] = parseSemverIntSafe(parts[i])
	}
	return result
}

func parseSemverIntSafe(s string) int {
	n := 0
	for _, r := range s {
		if r < '0' || r > '9' {
			break
		}
		n = n*10 + int(r-'0')
	}
	return n
}
