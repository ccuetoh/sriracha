// Quickstart shows the minimum end-to-end flow: build a Session, tokenize
// two records both deterministically and probabilistically, and compare.
//
// Run with:
//
//	go run ./examples/quickstart
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

	alice := sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Alice",
		sriracha.FieldNameFamily: "Smith",
		sriracha.FieldDateBirth:  "1990-01-15",
	}
	aliceTypo := sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Alice",
		sriracha.FieldNameFamily: "Smyth",
		sriracha.FieldDateBirth:  "1990-01-15",
	}

	detA, err := s.TokenizeDeterministic(alice)
	if err != nil {
		log.Fatalf("TokenizeDeterministic A: %v", err)
	}
	detB, err := s.TokenizeDeterministic(aliceTypo)
	if err != nil {
		log.Fatalf("TokenizeDeterministic B: %v", err)
	}
	detEq, err := s.Equal(detA, detB)
	if err != nil {
		log.Fatalf("Equal: %v", err)
	}
	fmt.Printf("deterministic equal (typo): %v\n", detEq)

	probA, err := s.TokenizeProbabilistic(alice)
	if err != nil {
		log.Fatalf("TokenizeProbabilistic A: %v", err)
	}
	probB, err := s.TokenizeProbabilistic(aliceTypo)
	if err != nil {
		log.Fatalf("TokenizeProbabilistic B: %v", err)
	}

	// A threshold alone does not decide a match: a pair agreeing on one field
	// out of sixteen scores 1.000. DefaultMatchPolicy pairs the threshold with
	// an evidence floor of two comparable fields, and IsMatch reports both.
	policy := token.DefaultMatchPolicy(0.70)
	res, err := s.Match(probA, probB, policy)
	if err != nil {
		log.Fatalf("Match: %v", err)
	}
	fmt.Printf("probabilistic match: %v (score %.3f >= threshold %.2f, comparable fields %d >= floor %d)\n",
		res.IsMatch, res.Score, policy.Threshold, res.ComparableFields, policy.MinComparableFields)
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
