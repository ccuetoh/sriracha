// Probabilistic demonstrates Bloom-filter tokenization, which tolerates typos
// and noisy data. The example breaks down the per-field Dice scores so you
// can see which fields drove (or fought) the aggregate.
//
// Run with:
//
//	go run ./examples/probabilistic
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

	clean := sriracha.RawRecord{
		sriracha.FieldNameGiven:    "Jonathan",
		sriracha.FieldNameFamily:   "O'Brien",
		sriracha.FieldDateBirth:    "1985-07-22",
		sriracha.FieldContactEmail: "jonathan.obrien@example.com",
	}
	noisy := sriracha.RawRecord{
		sriracha.FieldNameGiven:    "Jonathon",
		sriracha.FieldNameFamily:   "OBrien",
		sriracha.FieldDateBirth:    "1985-07-22",
		sriracha.FieldContactEmail: "j.obrien@example.com",
	}
	different := sriracha.RawRecord{
		sriracha.FieldNameGiven:    "Sarah",
		sriracha.FieldNameFamily:   "Connor",
		sriracha.FieldDateBirth:    "1972-04-09",
		sriracha.FieldContactEmail: "sarah@example.com",
	}
	// Two records whose only field is a common surname. They agree perfectly
	// on everything they share, which is exactly the case a bare threshold
	// gets wrong.
	surnameOnlyA := sriracha.RawRecord{
		sriracha.FieldNameFamily: "O'Brien",
	}
	surnameOnlyB := sriracha.RawRecord{
		sriracha.FieldNameFamily: "  o'brien",
	}

	cleanTok, err := s.TokenizeProbabilistic(clean)
	if err != nil {
		log.Fatalf("tokenize clean: %v", err)
	}
	noisyTok, err := s.TokenizeProbabilistic(noisy)
	if err != nil {
		log.Fatalf("tokenize noisy: %v", err)
	}
	differentTok, err := s.TokenizeProbabilistic(different)
	if err != nil {
		log.Fatalf("tokenize different: %v", err)
	}
	surnameTokA, err := s.TokenizeProbabilistic(surnameOnlyA)
	if err != nil {
		log.Fatalf("tokenize surnameOnlyA: %v", err)
	}
	surnameTokB, err := s.TokenizeProbabilistic(surnameOnlyB)
	if err != nil {
		log.Fatalf("tokenize surnameOnlyB: %v", err)
	}

	// The policy carries the threshold and the evidence floor together. Score
	// is a weighted average over the fields both records carry, so it says
	// nothing about how much evidence went into it; the floor is what stops a
	// one-field agreement being reported as a match.
	policy := token.DefaultMatchPolicy(0.80)
	fmt.Printf("policy: threshold=%.2f min comparable fields=%d\n\n",
		policy.Threshold, policy.MinComparableFields)

	report(s, "clean vs noisy", cleanTok, noisyTok, policy)
	report(s, "clean vs different", cleanTok, differentTok, policy)
	report(s, "surname only", surnameTokA, surnameTokB, policy)
}

func report(s *session.Session, label string, a, b sriracha.ProbabilisticToken, policy token.MatchPolicy) {
	res, err := s.Match(a, b, policy)
	if err != nil {
		log.Fatalf("%s: %v", label, err)
	}
	fmt.Printf("%-18s -> match=%-5v score=%.3f comparable fields=%d weight=%.1f\n",
		label, res.IsMatch, res.Score, res.ComparableFields, res.ComparableWeight)
	if !res.IsMatch && res.Score >= policy.Threshold {
		fmt.Printf("%-18s    score cleared the threshold but only %d field(s) backed it\n",
			"", res.ComparableFields)
	}
	for path, score := range res.ByPath() {
		if score == 0 {
			continue
		}
		fmt.Printf("    %-40s %.3f\n", path, score)
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
