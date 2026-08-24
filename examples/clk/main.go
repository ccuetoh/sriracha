// CLK demonstrates record-level tokens. A CLK token pools every present
// field into one balanced Bloom filter, so a token holder sees no per-field
// structure, no per-field popcounts, and no field presence pattern. CLK is
// the recommended form for sharing tokens when per-field scores are not
// required.
//
// Note balanced filters concentrate unrelated pairs near 0.5, so CLK
// thresholds sit higher than per-field ones. Calibrate against labeled
// pairs for production use.
//
// Run with:
//
//	go run ./examples/clk
package main

import (
	"fmt"
	"log"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/fieldset"
	"github.com/ccuetoh/sriracha/session"
)

func main() {
	s, err := session.New([]byte("0123456789abcdef0123456789abcdef"), fieldset.DefaultFieldSet())
	if err != nil {
		log.Fatalf("session.New: %v", err)
	}
	defer s.Destroy()

	pairs := []struct {
		label string
		a, b  sriracha.RawRecord
	}{
		{"identical", rec("Alice", "Smith", "1990-01-15"), rec("Alice", "Smith", "1990-01-15")},
		{"typo", rec("Alice", "Smith", "1990-01-15"), rec("Alice", "Smyth", "1990-01-15")},
		{"different", rec("Alice", "Smith", "1990-01-15"), rec("Bob", "Jones", "1955-03-02")},
	}

	for _, p := range pairs {
		tokA, err := s.TokenizeCLK(p.a)
		if err != nil {
			log.Fatalf("TokenizeCLK: %v", err)
		}
		tokB, err := s.TokenizeCLK(p.b)
		if err != nil {
			log.Fatalf("TokenizeCLK: %v", err)
		}
		result, err := s.MatchCLK(tokA, tokB, 0.8)
		if err != nil {
			log.Fatalf("MatchCLK: %v", err)
		}
		fmt.Printf("%-12s score=%.3f match=%v\n", p.label, result.Score, result.IsMatch)
	}
}

func rec(given, family, birth string) sriracha.RawRecord {
	return sriracha.RawRecord{
		sriracha.FieldNameGiven:  given,
		sriracha.FieldNameFamily: family,
		sriracha.FieldDateBirth:  birth,
	}
}
