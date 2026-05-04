// Deterministic shows HMAC-SHA256 tokenization where two records match only
// when every present field is byte-identical after normalization. Use this
// mode when records are already cleaned (e.g. a national-ID join key).
//
// Run with:
//
//	go run ./examples/deterministic
package main

import (
	"fmt"
	"log"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/fieldset"
	"github.com/ccuetoh/sriracha/session"
)

func main() {
	secret := []byte("shared-secret-between-institutions")

	s, err := session.New(secret, fieldset.DefaultFieldSet())
	if err != nil {
		log.Fatalf("session.New: %v", err)
	}
	defer s.Destroy()

	left := sriracha.RawRecord{
		sriracha.FieldIdentifierNationalID: "12.345.678-9",
		sriracha.FieldNameGiven:            "  José ",
		sriracha.FieldNameFamily:           "García",
	}
	right := sriracha.RawRecord{
		sriracha.FieldIdentifierNationalID: "12345678-9",
		sriracha.FieldNameGiven:            "JOSE",
		sriracha.FieldNameFamily:           "Garcia",
	}
	stranger := sriracha.RawRecord{
		sriracha.FieldIdentifierNationalID: "98.765.432-1",
		sriracha.FieldNameGiven:            "Maria",
		sriracha.FieldNameFamily:           "Lopez",
	}

	for name, rec := range map[string]sriracha.RawRecord{"left": left, "right": right, "stranger": stranger} {
		tok, err := s.TokenizeDeterministic(rec)
		if err != nil {
			log.Fatalf("tokenize %s: %v", name, err)
		}
		fmt.Printf("%-9s -> %s\n", name, tok)
	}

	leftTok, _ := s.TokenizeDeterministic(left)
	rightTok, _ := s.TokenizeDeterministic(right)
	strangerTok, _ := s.TokenizeDeterministic(stranger)

	fmt.Printf("left == right    (normalized identical): %v\n", s.Equal(leftTok, rightTok))
	fmt.Printf("left == stranger (different person):     %v\n", s.Equal(leftTok, strangerTok))
}
