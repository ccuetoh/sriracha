package fieldset_test

import (
	"fmt"

	"go.sriracha.dev/fieldset"
	"go.sriracha.dev/sriracha"
)

func ExampleDefaultFieldSet() {
	fs := fieldset.DefaultFieldSet()
	fmt.Println(fs.Version, len(fs.Fields))
	// Output: 0.1 16
}

func ExampleRegister() {
	custom := sriracha.FieldSet{
		Version: "example-1",
		Fields: []sriracha.FieldSpec{
			{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0},
		},
		BloomParams: sriracha.DefaultBloomConfig(),
	}
	if err := fieldset.Register(custom); err != nil {
		fmt.Println(err)
		return
	}
	got, ok := fieldset.Lookup("example-1")
	fmt.Println(ok, got.Version)
	// Output: true example-1
}

func ExampleLookup() {
	got, ok := fieldset.Lookup("0.1")
	fmt.Println(ok, got.Version, len(got.Fields))
	// Output: true 0.1 16
}
