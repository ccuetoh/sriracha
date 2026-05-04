// Key rotation shows token.WithKeyID. Tagging every token with the secret's
// identifier means a post-rotation comparison surfaces as an explicit
// mismatch (different KeyIDs) instead of silently producing two unrelated
// HMACs that look like a "different person" result.
//
// Run with:
//
//	go run ./examples/key-rotation
package main

import (
	"fmt"
	"log"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/fieldset"
	"github.com/ccuetoh/sriracha/session"
	"github.com/ccuetoh/sriracha/token"
)

func main() {
	fs := fieldset.DefaultFieldSet()

	oldSession, err := session.New([]byte("retired-secret"), fs, token.WithKeyID("k-2025-01"))
	if err != nil {
		log.Fatalf("oldSession: %v", err)
	}
	defer oldSession.Destroy()

	newSession, err := session.New([]byte("rotated-secret"), fs, token.WithKeyID("k-2026-01"))
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

	// Equal returns false because KeyIDs differ — the caller can see this
	// is a rotation event, not a person mismatch.
	fmt.Printf("Equal (cross-key): %v\n", token.Equal(oldTok, newTok))

	// Same secret, same KeyID -> bit-identical token, Equal returns true.
	sameTok, err := newSession.TokenizeDeterministic(record)
	if err != nil {
		log.Fatalf("tokenize same: %v", err)
	}
	fmt.Printf("Equal (same key):  %v\n", token.Equal(newTok, sameTok))
}
