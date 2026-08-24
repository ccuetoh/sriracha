package fieldset_test

import (
	"errors"
	"fmt"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/fieldset"
)

// The canonical schema covers the 16 fields most linkage projects share.
// Weights are relative magnitudes, so only their ratios matter.
func ExampleDefaultFieldSet() {
	fs := fieldset.DefaultFieldSet()
	fmt.Println("version:", fs.Version)
	fmt.Println("fields:", len(fs.Fields))
	fmt.Printf("first: %s weight %.1f\n", fs.Fields[0].Path, fs.Fields[0].Weight)
	// Output:
	// version: 0.2
	// fields: 16
	// first: sriracha::identifier::national_id weight 3.0
}

// ValidateRecord is the batch-ingest pre-flight check. It reports every
// problem in one pass rather than stopping at the first, joined with
// errors.Join so each leaf stays reachable.
func ExampleValidateRecord() {
	fs := sriracha.FieldSet{
		Version: "example/1",
		Fields: []sriracha.FieldSpec{
			{Path: sriracha.FieldNameFamily, Required: true, Weight: 2.5},
			{Path: sriracha.FieldDateBirth, Weight: 2.0},
		},
		ProbabilisticParams: sriracha.DefaultProbabilisticConfig(),
	}

	record := sriracha.RawRecord{
		sriracha.FieldDateBirth:    "15/01/1990",
		sriracha.FieldContactEmail: "alice@example.com",
	}

	err := fieldset.ValidateRecord(record, fs)
	fmt.Println("required missing:", errors.Is(err, sriracha.ErrRequiredFieldMissing))
	fmt.Println("unknown field:", errors.Is(err, sriracha.ErrUnknownField))
	fmt.Println(err)
	// Output:
	// required missing: true
	// unknown field: true
	// field "sriracha::name::family": required field missing
	// field "sriracha::date::birth": normalize: invalid value: date must be ISO 8601 YYYY-MM-DD
	// field "sriracha::contact::email": unknown field
}

// Every leaf is a sriracha.FieldError, so errors.As recovers which path
// failed without parsing the message.
func ExampleValidateRecord_fieldPath() {
	fs := fieldset.DefaultFieldSet()
	record := sriracha.RawRecord{sriracha.FieldContactPhone: "+1 555"}

	err := fieldset.ValidateRecord(record, fs)

	var fieldErr sriracha.FieldError
	if errors.As(err, &fieldErr) {
		fmt.Println("path:", fieldErr.Path)
		fmt.Println("namespace:", fieldErr.Path.Namespace())
	}
	// Output:
	// path: sriracha::contact::phone
	// namespace: contact
}

// A record that satisfies the schema returns nil.
func ExampleValidateRecord_valid() {
	fs := fieldset.DefaultFieldSet()
	record := sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Alice",
		sriracha.FieldNameFamily: "Smith",
		sriracha.FieldDateBirth:  "1990-01-15",
	}
	fmt.Println(fieldset.ValidateRecord(record, fs))
	// Output:
	// <nil>
}
