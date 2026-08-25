// Deterministic shows HMAC-SHA256 tokenization where two records match only
// when every present field is byte-identical after normalization. Use this
// mode when records are already cleaned (e.g. a national-ID join key).
//
// Run with:
//
//	go run ./examples/deterministic
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

	// Tokenize once and keep the tokens. Re-tokenizing to compare would work
	// (the mode is deterministic) but it costs a second pass and invites
	// dropping the error the second time round.
	leftTok, err := s.TokenizeDeterministic(left)
	if err != nil {
		log.Fatalf("tokenize left: %v", err)
	}
	rightTok, err := s.TokenizeDeterministic(right)
	if err != nil {
		log.Fatalf("tokenize right: %v", err)
	}
	strangerTok, err := s.TokenizeDeterministic(stranger)
	if err != nil {
		log.Fatalf("tokenize stranger: %v", err)
	}

	fmt.Printf("%-9s -> %s\n", "left", leftTok)
	fmt.Printf("%-9s -> %s\n", "right", rightTok)
	fmt.Printf("%-9s -> %s\n", "stranger", strangerTok)

	// Equal returns an error, not false, when two tokens are not comparable
	// at all (different schema, different key, different format). Treating
	// that as "different person" is the bug the error exists to prevent.
	sameEq, err := s.Equal(leftTok, rightTok)
	if err != nil {
		log.Fatalf("Equal(left, right): %v", err)
	}
	strangerEq, err := s.Equal(leftTok, strangerTok)
	if err != nil {
		log.Fatalf("Equal(left, stranger): %v", err)
	}
	fmt.Printf("left == right    (normalized identical): %v\n", sameEq)
	fmt.Printf("left == stranger (different person):     %v\n", strangerEq)
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
