package normalize

import (
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"go.sriracha.dev/sriracha"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"golang.org/x/text/unicode/norm"
)

// Normalize applies the standard Sriracha normalization pipeline to value
// for the given field path. Returns an error if the value is invalid for
// the field's expected format.
func Normalize(value string, path sriracha.FieldPath) (string, error) {
	// Replace invalid UTF-8 bytes with U+FFFD before any transformation.
	// Without this, raw invalid bytes pass through norm.NFKD and cases.Lower
	// unchanged but get decoded as RuneError by range loops (e.g. in
	// normalizeIdentifier), producing inconsistent byte representations across
	// successive calls and breaking idempotency.
	value = strings.ToValidUTF8(value, "�")
	// Step 1: NFKD decomposition
	value = nfkdDecompose(value)
	// Step 2: Unicode-correct lowercasing (language.Und = deterministic, locale-independent)
	value = unicodeLower(value)
	// Step 3: Collapse whitespace (handles U+00A0 and other Unicode spaces)
	value = collapseWhitespace(value)
	// Step 4: Trim leading/trailing whitespace
	value = trimWhitespace(value)

	// Step 5: Field-specific normalization
	switch {
	case path.InNamespace("date"):
		return normalizeDate(value)
	case path.InNamespace("identifier"):
		return normalizeIdentifier(value), nil
	// Country is the only address field with special normalization;
	// other address fields fall through to default (steps 1-4 only).
	case path.String() == sriracha.FieldAddressCountry.String():
		return normalizeCountry(value)
	default:
		return value, nil
	}
}

// nfkdDecompose applies Unicode NFKD decomposition.
func nfkdDecompose(s string) string {
	return norm.NFKD.String(s)
}

// unicodeLower applies Unicode-correct lowercasing with language.Und
// (language-independent, deterministic across all institutions).
func unicodeLower(s string) string {
	return cases.Lower(language.Und).String(s)
}

// collapseWhitespace replaces runs of Unicode whitespace (including U+00A0)
// with a single ASCII space.
func collapseWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// trimWhitespace removes leading and trailing whitespace.
func trimWhitespace(s string) string {
	return strings.TrimSpace(s)
}

// normalizeDate accepts only ISO 8601 YYYY-MM-DD format.
// Any other format returns an error to preserve determinism.
func normalizeDate(s string) (string, error) {
	if s == "" {
		return "", fmt.Errorf("date value is empty")
	}
	_, err := time.Parse("2006-01-02", s)
	if err != nil {
		return "", fmt.Errorf("date must be ISO 8601 YYYY-MM-DD, got %q", s)
	}
	return s, nil
}

// normalizeIdentifier strips hyphens, dots, and spaces from identifier fields.
// Uses a rune loop (no regex) for performance on this hot path.
func normalizeIdentifier(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if r != '-' && r != '.' && r != ' ' {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// normalizeCountry validates and uppercases a 2-letter ISO 3166-1 alpha-2 code.
func normalizeCountry(s string) (string, error) {
	upper := strings.ToUpper(s)
	if utf8.RuneCountInString(upper) != 2 {
		return "", fmt.Errorf("country code must be 2 characters, got %q", s)
	}

	for _, r := range upper {
		if r > 127 || !unicode.IsLetter(r) {
			return "", fmt.Errorf("country code must be 2 ASCII letters, got %q", s)
		}
	}

	return upper, nil
}
