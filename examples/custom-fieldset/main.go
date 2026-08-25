// Custom fieldset shows how to extend the canonical schema with
// organisation-scoped field paths and ship the result as a versioned
// FieldSet. Both sides of the comparison must agree on the schema (and key)
// for tokens to be comparable.
//
// It also shows the org gate in the normalization pipeline: values on a path
// outside the canonical "sriracha" org get the shared pipeline and nothing
// else.
//
// Run with:
//
//	go run ./examples/custom-fieldset
//
// Set SRIRACHA_SECRET to use your own secret; the example generates an
// ephemeral one otherwise.
package main

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"log"
	"maps"
	"os"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/fieldset"
	"github.com/ccuetoh/sriracha/session"
	"github.com/ccuetoh/sriracha/token"
)

// Org-scoped fields live outside the canonical "sriracha" namespace.
// ParseFieldPath enforces the <org>::<namespace>::<name> shape; we use
// MustParseFieldPath here because the input is a compile-time constant.
//
// Normalization dispatches on the org. A canonical path gets one
// field-specific normalizer after the shared pipeline: identifier paths lose
// their separators, date paths must be ISO 8601, email and phone get their
// own rules. A path on any other org gets the shared pipeline only (invalid
// UTF-8 repair, format-character stripping, NFKD, lowercasing, whitespace
// collapse) and stops there, because Sriracha does not know what acme-health
// means by "identifier" or "date" and guessing would silently rewrite values.
// An org that wants those rules normalizes before handing the value over.
var (
	fieldHospitalMRN       = sriracha.MustParseFieldPath("acme-health::identifier::mrn")
	fieldHospitalAdmission = sriracha.MustParseFieldPath("acme-health::date::admission")
)

func customFieldSet() sriracha.FieldSet {
	return sriracha.FieldSet{
		Version: "acme-health/v1",
		Fields: []sriracha.FieldSpec{
			{Path: fieldHospitalMRN, Required: true, Weight: 4.0},
			{Path: fieldHospitalAdmission, Required: false, Weight: 0.5},
			{Path: sriracha.FieldNameGiven, Required: true, Weight: 2.0},
			{Path: sriracha.FieldNameFamily, Required: true, Weight: 2.5},
			{Path: sriracha.FieldDateBirth, Required: true, Weight: 2.0},
			{Path: sriracha.FieldContactEmail, Required: false, Weight: 1.5},
		},
		ProbabilisticParams: sriracha.DefaultProbabilisticConfig(),
	}
}

func main() {
	secret, err := loadSecret("SRIRACHA_SECRET")
	if err != nil {
		log.Fatalf("load secret: %v", err)
	}

	fs := customFieldSet()

	// session.New wipes the slice it is handed, so each Session gets its own
	// copy and the caller's copy is cleared once both exist.
	s, err := session.New(bytes.Clone(secret), fs)
	if err != nil {
		log.Fatalf("session.New: %v", err)
	}
	defer s.Destroy()

	canonical, err := session.New(bytes.Clone(secret), fieldset.DefaultFieldSet())
	if err != nil {
		log.Fatalf("session.New canonical: %v", err)
	}
	defer canonical.Destroy()
	clear(secret)

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

	if err := s.ValidateRecord(missingMRN); err != nil {
		fmt.Printf("validation errors for missingMRN:\n  - %v\n", err)
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

	fmt.Println("\norg gate:")
	reportMRNSeparators(s, complete)
	reportNationalIDSeparators(canonical)
	reportDateFormats(s, complete)
}

// reportMRNSeparators shows that two spellings of one MRN do not collapse.
// The path carries a custom org, so the canonical identifier rule that would
// have stripped the hyphen and the space never runs.
func reportMRNSeparators(s *session.Session, base sriracha.RawRecord) {
	hyphen := withField(base, fieldHospitalMRN, "MRN-00012345")
	spaced := withField(base, fieldHospitalMRN, "MRN 00012345")

	a, err := s.TokenizeDeterministic(hyphen)
	if err != nil {
		log.Fatalf("tokenize hyphenated MRN: %v", err)
	}
	b, err := s.TokenizeDeterministic(spaced)
	if err != nil {
		log.Fatalf("tokenize spaced MRN: %v", err)
	}
	eq, err := s.Equal(a, b)
	if err != nil {
		log.Fatalf("Equal MRN spellings: %v", err)
	}
	fmt.Printf("  %-46s equal=%v (separators survive off-org)\n",
		`"MRN-00012345" vs "MRN 00012345"`, eq)
}

// reportNationalIDSeparators is the canonical contrast: the same shape of
// difference on sriracha::identifier::national_id does collapse, because that
// path gets the identifier normalizer.
func reportNationalIDSeparators(s *session.Session) {
	a, err := s.TokenizeDeterministic(sriracha.RawRecord{
		sriracha.FieldIdentifierNationalID: "12.345.678-9",
	})
	if err != nil {
		log.Fatalf("tokenize dotted national id: %v", err)
	}
	b, err := s.TokenizeDeterministic(sriracha.RawRecord{
		sriracha.FieldIdentifierNationalID: "12345678-9",
	})
	if err != nil {
		log.Fatalf("tokenize plain national id: %v", err)
	}
	eq, err := s.Equal(a, b)
	if err != nil {
		log.Fatalf("Equal national id spellings: %v", err)
	}
	fmt.Printf("  %-46s equal=%v (canonical identifier path strips them)\n",
		`"12.345.678-9" vs "12345678-9"`, eq)
}

// reportDateFormats shows the other half of the gate. A canonical date path
// rejects anything that is not ISO 8601; the org-scoped one takes the value
// as written, so acme-health owns the format contract for it.
func reportDateFormats(s *session.Session, base sriracha.RawRecord) {
	offOrg := withField(base, fieldHospitalAdmission, "05/03/2024")
	if _, err := s.TokenizeDeterministic(offOrg); err != nil {
		log.Fatalf("tokenize org-scoped date: %v", err)
	}
	fmt.Printf("  %-46s accepted on acme-health::date::admission\n", `"05/03/2024"`)

	canonicalDate := withField(base, sriracha.FieldDateBirth, "05/03/2024")
	_, err := s.TokenizeDeterministic(canonicalDate)
	if err == nil {
		log.Fatal("expected sriracha::date::birth to reject a non-ISO 8601 value")
	}
	fmt.Printf("  %-46s rejected on sriracha::date::birth: %v\n", `"05/03/2024"`, err)
}

// withField returns a copy of r with path set to value.
func withField(r sriracha.RawRecord, path sriracha.FieldPath, value string) sriracha.RawRecord {
	out := maps.Clone(r)
	out[path] = value
	return out
}

// loadSecret returns the tokenization secret. The secret is the whole privacy
// barrier: anyone holding it can re-derive every token from a guessed record,
// so a real deployment pulls it from a KMS or a secrets manager at startup and
// never commits it to source or config. It must be at least
// token.MinSecretLen bytes of key material, not a passphrase.
//
// The random fallback keeps the example runnable with no setup. It works here
// because both sides of every comparison live in this process; two
// institutions that need to link records must share one secret out of band.
func loadSecret(env string) ([]byte, error) {
	if v := os.Getenv(env); v != "" {
		if len(v) < token.MinSecretLen {
			return nil, fmt.Errorf("%s must be at least %d bytes, got %d", env, token.MinSecretLen, len(v))
		}
		return []byte(v), nil
	}
	secret := make([]byte, token.MinSecretLen)
	if _, err := rand.Read(secret); err != nil {
		return nil, fmt.Errorf("generate ephemeral secret: %w", err)
	}
	return secret, nil
}
