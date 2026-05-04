// Tokenizer shows the low-level flow without session.Session: construct a
// token.Tokenizer directly, validate the FieldSet yourself, and pass it
// explicitly into every tokenize / match call. Useful when one Tokenizer
// needs to serve multiple FieldSets (a Session locks itself to one).
//
// Run with:
//
//	go run ./examples/tokenizer
package main

import (
	"fmt"
	"log"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/fieldset"
	"github.com/ccuetoh/sriracha/token"
)

func main() {
	fs := fieldset.DefaultFieldSet()
	if err := fieldset.Validate(fs); err != nil {
		log.Fatalf("Validate: %v", err)
	}

	tok, err := token.New([]byte("super-secret-key"))
	if err != nil {
		log.Fatalf("token.New: %v", err)
	}
	defer tok.Destroy()

	a := sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Alice",
		sriracha.FieldNameFamily: "Smith",
		sriracha.FieldDateBirth:  "1990-01-15",
	}
	b := sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Alice",
		sriracha.FieldNameFamily: "Smyth",
		sriracha.FieldDateBirth:  "1990-01-15",
	}

	probA, err := tok.TokenizeProbabilistic(a, fs)
	if err != nil {
		log.Fatalf("TokenizeProbabilistic a: %v", err)
	}
	probB, err := tok.TokenizeProbabilistic(b, fs)
	if err != nil {
		log.Fatalf("TokenizeProbabilistic b: %v", err)
	}

	res, err := token.Match(probA, probB, fs, 0.70)
	if err != nil {
		log.Fatalf("Match: %v", err)
	}
	fmt.Printf("match=%v score=%.3f comparable=%d\n", res.IsMatch, res.Score, res.ComparableFields)

	// TokenizeField produces a stable HMAC for a single (value, path) pair —
	// useful for building an index outside the FieldSet flow.
	digest, err := tok.TokenizeField("alice@example.com", sriracha.FieldContactEmail)
	if err != nil {
		log.Fatalf("TokenizeField: %v", err)
	}
	fmt.Printf("email index key (first 8 bytes hex): %x\n", digest[:8])
}
