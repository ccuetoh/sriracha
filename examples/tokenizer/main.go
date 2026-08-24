// Tokenizer shows the low-level flow without session.Session: construct a
// token.Tokenizer directly, validate the FieldSet yourself, stamp the
// FieldSet fingerprint yourself, and pass the FieldSet explicitly into every
// tokenize / match call. Useful when one Tokenizer needs to serve multiple
// FieldSets (a Session locks itself to one).
//
// Run with:
//
//	go run ./examples/tokenizer
//
// Set SRIRACHA_SECRET to use your own secret; the example generates an
// ephemeral one otherwise.
package main

import (
	"crypto/rand"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/fieldset"
	"github.com/ccuetoh/sriracha/token"
)

func main() {
	secret, err := loadSecret("SRIRACHA_SECRET")
	if err != nil {
		log.Fatalf("load secret: %v", err)
	}

	fs := fieldset.DefaultFieldSet()
	if err := fs.Validate(); err != nil {
		log.Fatalf("Validate: %v", err)
	}

	tok, err := token.New(secret)
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

	// A Tokenizer serves any FieldSet handed to it, so it cannot know which
	// schema a token belongs to and leaves FieldSetFingerprint empty. Stamping
	// it is the direct caller's job. An unstamped token compares against
	// anything, including a token built from a schema whose weights have since
	// been retuned, and a session.Session built WithStrictFingerprint refuses
	// it outright. session.Session stamps this for you.
	fingerprint := fs.Fingerprint()
	probA.FieldSetFingerprint = fingerprint
	probB.FieldSetFingerprint = fingerprint
	fmt.Printf("fieldset fingerprint: %s\n", fingerprint[:8])

	// The policy is the threshold plus the evidence floor. Score alone does
	// not decide a match: agreement on a single field scores 1.000.
	policy := token.DefaultMatchPolicy(0.70)
	res, err := token.Match(probA, probB, fs, policy)
	if err != nil {
		log.Fatalf("Match: %v", err)
	}
	fmt.Printf("match=%v score=%.3f comparable fields=%d (threshold %.2f, floor %d)\n",
		res.IsMatch, res.Score, res.ComparableFields, policy.Threshold, policy.MinComparableFields)

	// What the stamp buys: retune one weight and the fingerprint changes, so
	// the comparison fails loudly instead of scoring two schemas against each
	// other under whichever weights the caller happened to pass.
	retuned := fieldset.DefaultFieldSet()
	retuned.Fields[0].Weight = 5.0
	drifted := probB
	drifted.FieldSetFingerprint = retuned.Fingerprint()
	if _, err := token.Match(probA, drifted, fs, policy); !errors.Is(err, token.ErrFingerprintMismatch) {
		log.Fatalf("Match (drifted): expected ErrFingerprintMismatch, got %v", err)
	}
	fmt.Printf("retuned weights (%s) against %s: %v\n",
		drifted.FieldSetFingerprint[:8], fingerprint[:8], token.ErrFingerprintMismatch)

	// TokenizeField produces a stable HMAC for a single (value, path) pair,
	// useful for building an index outside the FieldSet flow.
	digest, err := tok.TokenizeField("alice@example.com", sriracha.FieldContactEmail)
	if err != nil {
		log.Fatalf("TokenizeField: %v", err)
	}
	fmt.Printf("email index key (first 8 bytes hex): %x\n", digest[:8])
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
