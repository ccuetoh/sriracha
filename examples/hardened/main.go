// Hardened compares the default probabilistic configuration against the
// hardened one (BLIP bit-flipping + balanced-filter popcount padding). The
// hardened config trades a small amount of recall for resistance to
// frequency-analysis attacks against the published filters.
//
// Run with:
//
//	go run ./examples/hardened
package main

import (
	"fmt"
	"log"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/fieldset"
	"github.com/ccuetoh/sriracha/session"
)

func main() {
	pairs := []struct {
		label string
		a, b  sriracha.RawRecord
	}{
		{
			"identical",
			rec("Alice", "Smith", "1990-01-15"),
			rec("Alice", "Smith", "1990-01-15"),
		},
		{
			"typo",
			rec("Alice", "Smith", "1990-01-15"),
			rec("Alice", "Smyth", "1990-01-15"),
		},
		{
			"different",
			rec("Alice", "Smith", "1990-01-15"),
			rec("Bob", "Jones", "1955-03-02"),
		},
	}

	defaultFS := fieldset.DefaultFieldSet()

	hardenedFS := fieldset.DefaultFieldSet()
	hardenedFS.Version = "0.1-hardened"
	hardenedFS.ProbabilisticParams = sriracha.HardenedProbabilisticConfig()

	fmt.Printf("%-12s %12s %12s\n", "pair", "default", "hardened")
	for _, p := range pairs {
		def := score(defaultFS, p.a, p.b)
		hard := score(hardenedFS, p.a, p.b)
		fmt.Printf("%-12s %12.3f %12.3f\n", p.label, def, hard)
	}
}

func score(fs sriracha.FieldSet, a, b sriracha.RawRecord) float64 {
	s, err := session.New([]byte("super-secret-key"), fs)
	if err != nil {
		log.Fatalf("session.New: %v", err)
	}
	defer s.Destroy()

	tokA, err := s.TokenizeProbabilistic(a)
	if err != nil {
		log.Fatalf("tokenize a: %v", err)
	}
	tokB, err := s.TokenizeProbabilistic(b)
	if err != nil {
		log.Fatalf("tokenize b: %v", err)
	}
	res, err := s.Match(tokA, tokB, 0)
	if err != nil {
		log.Fatalf("match: %v", err)
	}
	return res.Score
}

func rec(given, family, dob string) sriracha.RawRecord {
	return sriracha.RawRecord{
		sriracha.FieldNameGiven:  given,
		sriracha.FieldNameFamily: family,
		sriracha.FieldDateBirth:  dob,
	}
}
