// Custom fieldset shows how to extend the canonical schema with an
// organisation-scoped field path and ship the result as a versioned FieldSet.
// Both sides of the comparison must agree on the schema (and key) for tokens
// to be comparable.
//
// Run with:
//
//	go run ./examples/custom-fieldset
package main

import (
	"fmt"
	"log"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/session"
)

// fieldHospitalMRN is an org-scoped field outside the canonical "sriracha"
// namespace. ParseFieldPath enforces the <org>::<namespace>::<name> shape; we
// use MustParsePath here because the input is a compile-time constant.
var fieldHospitalMRN = sriracha.MustParsePath("acme-health::identifier::mrn")

func customFieldSet() sriracha.FieldSet {
	return sriracha.FieldSet{
		Version: "acme-health/v1",
		Fields: []sriracha.FieldSpec{
			{Path: fieldHospitalMRN, Required: true, Weight: 4.0},
			{Path: sriracha.FieldNameGiven, Required: true, Weight: 2.0},
			{Path: sriracha.FieldNameFamily, Required: true, Weight: 2.5},
			{Path: sriracha.FieldDateBirth, Required: true, Weight: 2.0},
			{Path: sriracha.FieldContactEmail, Required: false, Weight: 1.5},
		},
		ProbabilisticParams: sriracha.DefaultProbabilisticConfig(),
	}
}

func main() {
	secret := []byte("acme-health-shared-secret")
	fs := customFieldSet()

	s, err := session.New(secret, fs)
	if err != nil {
		log.Fatalf("session.New: %v", err)
	}
	defer s.Destroy()

	complete := sriracha.RawRecord{
		fieldHospitalMRN:         "MRN-00012345",
		sriracha.FieldNameGiven:  "Alice",
		sriracha.FieldNameFamily: "Smith",
		sriracha.FieldDateBirth:  "1990-01-15",
	}
	missingMRN := sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Alice",
		sriracha.FieldNameFamily: "Smith",
		sriracha.FieldDateBirth:  "1990-01-15",
	}

	if errs := s.ValidateRecord(missingMRN); len(errs) > 0 {
		fmt.Println("validation errors for missingMRN:")
		for _, e := range errs {
			fmt.Printf("  - %v\n", e)
		}
	}

	tok, err := s.TokenizeProbabilistic(complete)
	if err != nil {
		log.Fatalf("tokenize complete: %v", err)
	}
	fmt.Printf("complete record token: %s\n", tok)

	annotated := tok.Annotate(fs)
	fmt.Println("per-field presence (safe to log):")
	for _, f := range annotated.Fields {
		fmt.Printf("  %-40s present=%v bytes=%d\n", f.Path, f.Present, f.ByteCount)
	}
}
