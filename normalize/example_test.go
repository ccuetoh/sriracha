package normalize_test

import (
	"fmt"

	"go.sriracha.dev/normalize"
	"go.sriracha.dev/sriracha"
)

func ExampleNormalize() {
	out, err := normalize.Normalize("  José  ", sriracha.FieldNameGiven)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%q\n", out)
	// Output: "jose"
}
