// Key rotation shows session.WithKeyID. Tagging every token with the secret's
// identifier means a post-rotation comparison surfaces as an explicit
// mismatch (different KeyIDs) instead of silently producing two unrelated
// HMACs that look like a "different person" result.
//
// Run with:
//
//	go run ./examples/key-rotation
//
// Set SRIRACHA_SECRET_OLD and SRIRACHA_SECRET_NEW to use your own secrets;
// the example generates ephemeral ones otherwise.
package main

import (
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/fieldset"
	"github.com/ccuetoh/sriracha/session"
	"github.com/ccuetoh/sriracha/token"
)

func main() {
	fs := fieldset.DefaultFieldSet()

	// Rotation means two secrets are live at once: the retired one still
	// covers tokens already in storage, the new one covers everything minted
	// from now on. Both come from the same place as any other secret, a KMS
	// or a secrets manager, and neither is ever written into source.
	retired, err := loadSecret("SRIRACHA_SECRET_OLD")
	if err != nil {
		log.Fatalf("load retired secret: %v", err)
	}
	rotated, err := loadSecret("SRIRACHA_SECRET_NEW")
	if err != nil {
		log.Fatalf("load rotated secret: %v", err)
	}

	oldSession, err := session.New(retired, fs, session.WithKeyID("k-2025-01"))
	if err != nil {
		log.Fatalf("oldSession: %v", err)
	}
	defer oldSession.Destroy()

	newSession, err := session.New(rotated, fs, session.WithKeyID("k-2026-01"))
	if err != nil {
		log.Fatalf("newSession: %v", err)
	}
	defer newSession.Destroy()

	record := sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Alice",
		sriracha.FieldNameFamily: "Smith",
		sriracha.FieldDateBirth:  "1990-01-15",
	}

	oldTok, err := oldSession.TokenizeDeterministic(record)
	if err != nil {
		log.Fatalf("tokenize old: %v", err)
	}
	newTok, err := newSession.TokenizeDeterministic(record)
	if err != nil {
		log.Fatalf("tokenize new: %v", err)
	}

	fmt.Printf("old token: %s\n", oldTok)
	fmt.Printf("new token: %s\n", newTok)

	// Both Sessions hold the same FieldSet, so the fingerprints agree and the
	// comparison gets as far as the KeyIDs. Equal reports the mismatch as an
	// error rather than returning false: the same person under two keys is a
	// rotation event, and reporting it as "not the same person" would be a
	// silent false negative.
	crossEq, crossErr := newSession.Equal(oldTok, newTok)
	switch {
	case errors.Is(crossErr, token.ErrKeyIDMismatch):
		fmt.Printf("Equal (cross-key): %v (%v)\n", crossEq, crossErr)
	case crossErr != nil:
		log.Fatalf("Equal (cross-key): %v", crossErr)
	default:
		log.Fatalf("Equal (cross-key): expected ErrKeyIDMismatch, got %v", crossEq)
	}

	// Same secret, same KeyID -> bit-identical token, Equal returns true.
	sameTok, err := newSession.TokenizeDeterministic(record)
	if err != nil {
		log.Fatalf("tokenize same: %v", err)
	}
	sameEq, err := newSession.Equal(newTok, sameTok)
	if err != nil {
		log.Fatalf("Equal (same key): %v", err)
	}
	fmt.Printf("Equal (same key):  %v\n", sameEq)

	// Re-tokenizing the source records under the new key is the only way to
	// link across a rotation. Tokens are one way, so stored tokens minted
	// under the retired key cannot be converted.
	fmt.Println("to link pre-rotation tokens, re-tokenize the source records under the new key")
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
