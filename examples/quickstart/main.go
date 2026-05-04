// Quickstart shows the minimum end-to-end flow: build a Session, tokenize
// two records both deterministically and probabilistically, and compare.
//
// Run with:
//
//	go run ./examples/quickstart
package main

import (
	"fmt"
	"log"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/fieldset"
	"github.com/ccuetoh/sriracha/session"
)

func main() {
	secret := []byte("super-secret-key")

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
	fmt.Printf("deterministic equal (typo): %v\n", s.Equal(detA, detB))

	probA, err := s.TokenizeProbabilistic(alice)
	if err != nil {
		log.Fatalf("TokenizeProbabilistic A: %v", err)
	}
	probB, err := s.TokenizeProbabilistic(aliceTypo)
	if err != nil {
		log.Fatalf("TokenizeProbabilistic B: %v", err)
	}

	const threshold = 0.70
	res, err := s.Match(probA, probB, threshold)
	if err != nil {
		log.Fatalf("Match: %v", err)
	}
	fmt.Printf("probabilistic match (threshold %.2f): %v (score: %.3f, fields: %d)\n",
		threshold, res.IsMatch, res.Score, res.ComparableFields)
}
