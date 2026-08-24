package normalize_test

import (
	"errors"
	"fmt"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/normalize"
)

// Normalization is the wire contract. Two institutions only produce equal
// tokens for the same person if they first fold the value the same way, so
// casing, diacritics and stray whitespace all collapse before hashing.
func ExampleNormalize() {
	out, err := normalize.Normalize("  José   MARTÍNEZ ", sriracha.FieldNameFull)
	if err != nil {
		fmt.Println("normalize:", err)
		return
	}
	fmt.Printf("%q\n", out)
	// Output:
	// "jose martinez"
}

// Identifier paths additionally lose hyphens, dots and spaces, so the same
// national ID punctuated two ways tokenizes to one value.
func ExampleNormalize_identifier() {
	a, err := normalize.Normalize("12.345.678-9", sriracha.FieldIdentifierNationalID)
	if err != nil {
		fmt.Println("normalize:", err)
		return
	}
	b, err := normalize.Normalize("12 345 678 9", sriracha.FieldIdentifierNationalID)
	if err != nil {
		fmt.Println("normalize:", err)
		return
	}
	fmt.Printf("%q %q %v\n", a, b, a == b)
	// Output:
	// "123456789" "123456789" true
}

// Date fields must already be ISO 8601. Guessing at an ambiguous format would
// make the token depend on the reader's locale, so Normalize refuses instead.
// Every format failure wraps ErrInvalidValue.
func ExampleNormalize_invalidValue() {
	_, err := normalize.Normalize("15/01/1990", sriracha.FieldDateBirth)
	fmt.Println(errors.Is(err, normalize.ErrInvalidValue))
	fmt.Println(err)
	// Output:
	// true
	// normalize: invalid value: date must be ISO 8601 YYYY-MM-DD
}

// A path outside the sriracha org gets the shared pipeline and nothing else.
// Sriracha does not know what another org means by a namespace called
// "identifier", so it does not apply the canonical identifier rules to it.
func ExampleNormalize_customOrg() {
	custom, err := sriracha.ParseFieldPath("acme::identifier::member_no")
	if err != nil {
		fmt.Println("path:", err)
		return
	}
	out, err := normalize.Normalize("12.345.678-9", custom)
	if err != nil {
		fmt.Println("normalize:", err)
		return
	}
	fmt.Printf("%q\n", out)
	// Output:
	// "12.345.678-9"
}
