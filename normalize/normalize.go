// Package normalize implements the Unicode normalization pipeline applied
// to every field value before tokenization.
//
// Every value runs the shared pipeline first. Invalid UTF-8 bytes are
// replaced with U+FFFD, Unicode format characters (category Cf) are
// stripped, NFKD decomposition is applied, the value is lowercased with
// language.Und, and whitespace is collapsed and trimmed.
//
// A value on a canonical path (org sriracha) then runs one field-specific
// normalizer, chosen by namespace first and exact path second:
//
//	identifier::*     hyphens, dots and spaces stripped
//	name::*           combining marks stripped after a Latin base rune
//	date::*           must already be ISO 8601 YYYY-MM-DD, error otherwise
//	contact::email    exactly one '@' required, trailing domain dots stripped
//	contact::phone    digits plus a single leading '+', at least 7 digits
//	address::country  2-letter ISO 3166-1 alpha-2, uppercased
//	address::*        shared pipeline only
//
// A path carrying any other org gets the shared pipeline and nothing else.
// Sriracha does not know what a custom org means by its namespaces, so
// applying the canonical rules would silently rewrite values (separator
// stripping on an identifier-shaped path, ISO 8601 rejection on a
// date-shaped one). A custom org that wants identifier or date handling
// should normalize before calling.
//
// Both institutions in a linkage must normalize identically for their tokens
// to match, so these rules are part of the wire contract. Changing them
// changes every token derived from them.
//
// Calling Normalize directly is rarely needed. token.Tokenizer runs it as
// the first stage of every tokenize call. The surface is exported so callers
// can pre-validate input or build custom indexing pipelines that share the
// canonical form.
package normalize

import (
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/ccuetoh/sriracha"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"golang.org/x/text/unicode/norm"
)

// ErrInvalidValue reports a value that does not match the format its field
// requires. Every format failure Normalize returns wraps it, so callers can
// branch with errors.Is instead of matching message text. A field that
// requires a value but receives an empty one wraps sriracha.ErrEmptyValue
// instead.
var ErrInvalidValue = errors.New("invalid value")

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
	// Step 1: Strip Unicode format characters (category Cf)
	value = stripFormatChars(value)
	// Step 2: NFKD decomposition
	value = nfkdDecompose(value)
	// Step 3: Unicode-correct lowercasing (language.Und = deterministic, locale-independent)
	value = unicodeLower(value)
	// Step 4: Collapse whitespace (handles U+00A0 and other Unicode spaces)
	value = collapseWhitespace(value)
	// Step 5: Trim leading/trailing whitespace
	value = trimWhitespace(value)

	// Step 6: Field-specific normalization, canonical paths only. A custom
	// org's namespaces mean whatever that org decided, so inheriting the
	// canonical rules would silently rewrite its values.
	if !path.IsCanonical() {
		return value, nil
	}

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

// stripFormatChars removes every Unicode format character (category Cf),
// which covers zero width spaces and joiners, bidi marks, and soft hyphens.
// These are invisible, so visually identical values would otherwise tokenize
// apart. Returns the input unchanged when it contains no Cf rune, so the
// common case does not allocate.
func stripFormatChars(s string) string {
	if !strings.ContainsFunc(s, isFormatChar) {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		if !isFormatChar(r) {
			b.WriteRune(r)
		}
	}
	return b.String()
}

// isFormatChar reports whether r is a Unicode format character (category Cf).
func isFormatChar(r rune) bool {
	return unicode.Is(unicode.Cf, r)
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
		return "", fmt.Errorf("normalize: date %w", sriracha.ErrEmptyValue)
	}
	_, err := time.Parse("2006-01-02", s)
	if err != nil {
		return "", fmt.Errorf("normalize: %w: date must be ISO 8601 YYYY-MM-DD", ErrInvalidValue)
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

// normalizeName strips a combining mark (category Mn) only when the most
// recent preceding non-mark rune is in the Latin script, so "José" and "Jose"
// both become "jose" while Thai, Vietnamese-adjacent scripts, Arabic, and
// Indic marks that carry meaning are preserved. A mark with no preceding base
// rune is kept. Re-applies NFKD afterwards for the same idempotency reason as
// normalizeIdentifier, and re-collapses and trims whitespace so stripping can
// never leave a leading or trailing space for the next call to trim.
func normalizeName(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	latinBase := false
	for _, r := range s {
		if unicode.Is(unicode.Mn, r) {
			if latinBase {
				continue
			}
			b.WriteRune(r)
			continue
		}
		latinBase = unicode.Is(unicode.Latin, r)
		b.WriteRune(r)
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
		return "", fmt.Errorf("normalize: %w: email must not contain whitespace", ErrInvalidValue)
	}
	at := strings.IndexByte(s, '@')
	if at < 0 || strings.IndexByte(s[at+1:], '@') >= 0 {
		return "", fmt.Errorf("normalize: %w: email must contain exactly one '@'", ErrInvalidValue)
	}
	local, domain := s[:at], s[at+1:]
	domain = strings.TrimRight(domain, ".")
	if local == "" || domain == "" {
		return "", fmt.Errorf("normalize: %w: email must have non-empty local and domain parts", ErrInvalidValue)
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
		return "", fmt.Errorf("normalize: %w: phone must contain at least 7 digits", ErrInvalidValue)
	}
	return b.String(), nil
}

// normalizeCountry validates and uppercases a 2-letter ISO 3166-1 alpha-2 code.
func normalizeCountry(s string) (string, error) {
	upper := strings.ToUpper(s)
	if utf8.RuneCountInString(upper) != 2 {
		return "", fmt.Errorf("normalize: %w: country code must be 2 characters", ErrInvalidValue)
	}

	for _, r := range upper {
		if r > 127 || !unicode.IsLetter(r) {
			return "", fmt.Errorf("normalize: %w: country code must be 2 ASCII letters", ErrInvalidValue)
		}
	}

	return upper, nil
}
