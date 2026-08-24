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
//
// Set SRIRACHA_SECRET to use your own secret; the example generates an
// ephemeral one otherwise.
package main

import (
	"crypto/rand"
	"fmt"
	"log"
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

	s, err := session.New(secret, fieldset.DefaultFieldSet())
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

	// MatchCLK takes a bare threshold, not a token.MatchPolicy. A CLK folds
	// the whole record into one filter and keeps no field structure, so there
	// is no per-field evidence to floor: a one-field record and a ten-field
	// record are indistinguishable once tokenized. Whatever guarantee the
	// per-field evidence floor gives has to come from validating records
	// before tokenizing them.
	const threshold = 0.80
	fmt.Printf("threshold=%.2f (no evidence floor applies to CLK)\n", threshold)

	for _, p := range pairs {
		tokA, err := s.TokenizeCLK(p.a)
		if err != nil {
			log.Fatalf("TokenizeCLK %s a: %v", p.label, err)
		}
		tokB, err := s.TokenizeCLK(p.b)
		if err != nil {
			log.Fatalf("TokenizeCLK %s b: %v", p.label, err)
		}
		result, err := s.MatchCLK(tokA, tokB, threshold)
		if err != nil {
			log.Fatalf("MatchCLK %s: %v", p.label, err)
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
