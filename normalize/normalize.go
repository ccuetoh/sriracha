package normalize

import (
	"errors"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/ccuetoh/sriracha"
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
	case path.InNamespace(sriracha.NamespaceDate):
		return normalizeDate(value)
	case path.InNamespace(sriracha.NamespaceIdentifier):
		// Re-apply NFKD after stripping: removing separators (e.g. a space
		// produced by NFKD decomposition of U+00B8 CEDILLA) can leave adjacent
		// combining marks in non-canonical combining-class order, which NFKD
		// would reorder on the next call and break idempotency.
		return nfkdDecompose(normalizeIdentifier(value)), nil
	case path.InNamespace(sriracha.NamespaceName):
		return normalizeName(value), nil
	case path == sriracha.FieldContactEmail:
		return normalizeEmail(value)
	case path == sriracha.FieldContactPhone:
		return normalizePhone(value)
	// Country is the only address field with special normalization;
	// other address fields fall through to default (steps 1-4 only).
	case path == sriracha.FieldAddressCountry:
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
		return "", errors.New("date value is empty")
	}
	_, err := time.Parse("2006-01-02", s)
	if err != nil {
		return "", errors.New("date must be ISO 8601 YYYY-MM-DD")
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

// normalizeName strips Unicode combining marks (category Mn) so that
// "José" and "Jose" produce the same output. Re-applies NFKD afterwards for
// the same idempotency reason as normalizeIdentifier, and re-collapses /
// trims whitespace because stripping a Mn-only run between spaces (e.g.
// "x ݈" → "x ") would otherwise leave a trailing space that the next
// call would trim, breaking idempotency.
func normalizeName(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if !unicode.Is(unicode.Mn, r) {
			b.WriteRune(r)
		}
	}
	out := nfkdDecompose(b.String())
	return strings.TrimSpace(strings.Join(strings.Fields(out), " "))
}

// normalizeEmail splits the address on its single '@', strips any trailing
// dots from the domain (FQDN canonicalisation), and rejects internal
// whitespace or empty parts. The default pipeline has already lowercased,
// NFKD-decomposed, and trimmed leading/trailing whitespace.
func normalizeEmail(s string) (string, error) {
	if strings.ContainsAny(s, " \t\r\n") {
		return "", errors.New("email must not contain whitespace")
	}
	at := strings.IndexByte(s, '@')
	if at < 0 || strings.IndexByte(s[at+1:], '@') >= 0 {
		return "", errors.New("email must contain exactly one '@'")
	}
	local, domain := s[:at], s[at+1:]
	domain = strings.TrimRight(domain, ".")
	if local == "" || domain == "" {
		return "", errors.New("email must have non-empty local and domain parts")
	}
	return local + "@" + domain, nil
}

// normalizePhone keeps only digits and a single leading '+'. Errors when the
// final digit count is below 7. Best-effort: no country awareness, no E.164
// validation.
func normalizePhone(s string) (string, error) {
	var b strings.Builder
	b.Grow(len(s))
	digits := 0
	for i, r := range s {
		switch {
		case r >= '0' && r <= '9':
			b.WriteRune(r)
			digits++
		case r == '+' && i == 0:
			b.WriteRune(r)
		}
	}
	if digits < 7 {
		return "", errors.New("phone must contain at least 7 digits")
	}
	return b.String(), nil
}

// normalizeCountry validates and uppercases a 2-letter ISO 3166-1 alpha-2 code.
func normalizeCountry(s string) (string, error) {
	upper := strings.ToUpper(s)
	if utf8.RuneCountInString(upper) != 2 {
		return "", errors.New("country code must be 2 characters")
	}

	for _, r := range upper {
		if r > 127 || !unicode.IsLetter(r) {
			return "", errors.New("country code must be 2 ASCII letters")
		}
	}

	return upper, nil
}
