package normalize_test

import (
	"fmt"

	"go.sriracha.dev/normalize"
	"go.sriracha.dev/sriracha"
)

func ExampleNormalize() {
	out, _ := normalize.Normalize("  José  ", sriracha.FieldNameGiven)
	fmt.Printf("%q\n", out)
	// Output: "jose"
}

func ExampleNormalize_email() {
	out, _ := normalize.Normalize("  Hello@Example.COM.", sriracha.FieldContactEmail)
	fmt.Println(out)
	// Output: hello@example.com
}

func ExampleNormalize_phone() {
	out, _ := normalize.Normalize("+1 (800) 555-1234", sriracha.FieldContactPhone)
	fmt.Println(out)
	// Output: +18005551234
}

func ExampleNormalize_identifier() {
	out, _ := normalize.Normalize("123-456.789 0", sriracha.FieldIdentifierNationalID)
	fmt.Println(out)
	// Output: 1234567890
}
