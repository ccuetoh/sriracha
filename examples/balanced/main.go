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
//
// Set SRIRACHA_SECRET to use your own secret; the example generates an
// ephemeral one otherwise.
package main

import (
	"bytes"
	"crypto/rand"
	"fmt"
	"log"
	"math/bits"
	"os"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/fieldset"
	"github.com/ccuetoh/sriracha/session"
	"github.com/ccuetoh/sriracha/token"
)

func main() {
	secret, err := loadSecret("SRIRACHA_SECRET")
	if err != nil {
		log.Fatalf("load secret: %v", err)
	}

	balancedFS := fieldset.DefaultFieldSet()
	balancedFS.Version = "0.2-balanced"
	balancedFS.ProbabilisticParams.Balanced = true

	// Two Sessions over the same secret and different schemas. Their
	// FieldSets fingerprint differently, so a token from one is never scored
	// against a token from the other.
	//
	// session.New wipes the slice it is handed once the key material is in
	// locked memory, so each Session gets its own copy and the caller's copy
	// is cleared as soon as both exist.
	balancedS, err := session.New(bytes.Clone(secret), balancedFS)
	if err != nil {
		log.Fatalf("session.New balanced: %v", err)
	}
	defer balancedS.Destroy()

	plainS, err := session.New(bytes.Clone(secret), fieldset.DefaultFieldSet())
	if err != nil {
		log.Fatalf("session.New unbalanced: %v", err)
	}
	defer plainS.Destroy()
	clear(secret)

	fmt.Println("family name popcounts (balanced hides value length):")
	for _, name := range []string{"Li", "Smith", "Papadopoulos"} {
		r := sriracha.RawRecord{sriracha.FieldNameFamily: name}
		fmt.Printf("%-14s balanced=%-4d unbalanced=%d\n",
			name, popcount(balancedS, r), popcount(plainS, r))
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
			score(balancedS, p.a, p.b), score(plainS, p.a, p.b))
	}
	fmt.Println("\nBalanced scores compress towards the middle, so the two columns\n" +
		"do not share a threshold. Calibrate each configuration separately\n" +
		"and pair the threshold with an evidence floor before deciding a match.")
}

func rec(given, family string) sriracha.RawRecord {
	return sriracha.RawRecord{
		sriracha.FieldNameGiven:  given,
		sriracha.FieldNameFamily: family,
	}
}

// popcount returns the number of set bits in the family-name filter of r.
func popcount(s *session.Session, r sriracha.RawRecord) int {
	tok, err := s.TokenizeProbabilistic(r)
	if err != nil {
		log.Fatalf("TokenizeProbabilistic: %v", err)
	}
	n := 0
	for i, spec := range s.FieldSet().Fields {
		if spec.Path == sriracha.FieldNameFamily {
			for _, b := range tok.Fields[i] {
				n += bits.OnesCount8(b)
			}
		}
	}
	return n
}

// score returns the weighted average similarity of a and b. The policy is the
// zero value because this table reports raw scores rather than decisions:
// with no threshold and no floor, only MatchResult.Score is meaningful.
func score(s *session.Session, a, b sriracha.RawRecord) float64 {
	tokA, err := s.TokenizeProbabilistic(a)
	if err != nil {
		log.Fatalf("TokenizeProbabilistic: %v", err)
	}
	tokB, err := s.TokenizeProbabilistic(b)
	if err != nil {
		log.Fatalf("TokenizeProbabilistic: %v", err)
	}
	result, err := s.Match(tokA, tokB, token.MatchPolicy{})
	if err != nil {
		log.Fatalf("Match: %v", err)
	}
	return result.Score
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
