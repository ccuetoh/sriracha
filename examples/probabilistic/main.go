// Probabilistic demonstrates Bloom-filter tokenization, which tolerates typos
// and noisy data. The example breaks down the per-field Dice scores so you
// can see which fields drove (or fought) the aggregate.
//
// Run with:
//
//	go run ./examples/probabilistic
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

	const threshold = 0.80

	report(s, "clean vs noisy    ", cleanTok, noisyTok, threshold)
	report(s, "clean vs different", cleanTok, differentTok, threshold)
}

func report(s session.Session, label string, a, b sriracha.ProbabilisticToken, threshold float64) {
	res, err := s.Match(a, b, threshold)
	if err != nil {
		log.Fatalf("%s: %v", label, err)
	}
	fmt.Printf("%s -> match=%v score=%.3f (threshold=%.2f, comparable=%d)\n",
		label, res.IsMatch, res.Score, threshold, res.ComparableFields)
	for path, score := range res.ByPath() {
		if score == 0 {
			continue
		}
		fmt.Printf("    %-40s %.3f\n", path, score)
	}
}
