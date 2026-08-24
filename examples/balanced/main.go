// Balanced compares balanced and unbalanced per-field configurations.
// Balanced filters always emit exactly SizeBits/2 set bits, so an observer
// cannot infer value length from the popcount, at the cost of compressed
// per-field Dice scores. Per-field filters default to unbalanced; CLK
// tokens are always balanced. Identical values still produce identical
// filters and remain linkable.
//
// Run with:
//
//	go run ./examples/balanced
package main

import (
	"fmt"
	"log"
	"math/bits"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/fieldset"
	"github.com/ccuetoh/sriracha/session"
)

func main() {
	balancedFS := fieldset.DefaultFieldSet()
	balancedFS.Version = "0.2-balanced"
	balancedFS.ProbabilisticParams.Balanced = true

	plainFS := fieldset.DefaultFieldSet()

	fmt.Println("family name popcounts (balanced hides value length):")
	for _, name := range []string{"Li", "Smith", "Papadopoulos"} {
		rec := sriracha.RawRecord{sriracha.FieldNameFamily: name}
		fmt.Printf("%-14s balanced=%-4d unbalanced=%d\n",
			name, popcount(balancedFS, rec), popcount(plainFS, rec))
	}

	pairs := []struct {
		label string
		a, b  sriracha.RawRecord
	}{
		{"identical", rec("Alice", "Smith"), rec("Alice", "Smith")},
		{"typo", rec("Alice", "Smith"), rec("Alice", "Smyth")},
		{"different", rec("Alice", "Smith"), rec("Bob", "Jones")},
	}

	fmt.Println("\nmatch scores:")
	fmt.Printf("%-12s %10s %12s\n", "pair", "balanced", "unbalanced")
	for _, p := range pairs {
		fmt.Printf("%-12s %10.3f %12.3f\n", p.label,
			score(balancedFS, p.a, p.b), score(plainFS, p.a, p.b))
	}
}

func rec(given, family string) sriracha.RawRecord {
	return sriracha.RawRecord{
		sriracha.FieldNameGiven:  given,
		sriracha.FieldNameFamily: family,
	}
}

func popcount(fs sriracha.FieldSet, r sriracha.RawRecord) int {
	s, err := session.New([]byte("0123456789abcdef0123456789abcdef"), fs)
	if err != nil {
		log.Fatalf("session.New: %v", err)
	}
	defer s.Destroy()

	tok, err := s.TokenizeProbabilistic(r)
	if err != nil {
		log.Fatalf("TokenizeProbabilistic: %v", err)
	}
	n := 0
	for i, spec := range fs.Fields {
		if spec.Path == sriracha.FieldNameFamily {
			for _, b := range tok.Fields[i] {
				n += bits.OnesCount8(b)
			}
		}
	}
	return n
}

func score(fs sriracha.FieldSet, a, b sriracha.RawRecord) float64 {
	s, err := session.New([]byte("0123456789abcdef0123456789abcdef"), fs)
	if err != nil {
		log.Fatalf("session.New: %v", err)
	}
	defer s.Destroy()

	tokA, err := s.TokenizeProbabilistic(a)
	if err != nil {
		log.Fatalf("TokenizeProbabilistic: %v", err)
	}
	tokB, err := s.TokenizeProbabilistic(b)
	if err != nil {
		log.Fatalf("TokenizeProbabilistic: %v", err)
	}
	result, err := s.Match(tokA, tokB, 0.5)
	if err != nil {
		log.Fatalf("Match: %v", err)
	}
	return result.Score
}
