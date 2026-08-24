package session_test

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/fieldset"
	"github.com/ccuetoh/sriracha/session"
	"github.com/ccuetoh/sriracha/token"
)

// exampleSecret is a literal so these examples produce stable output. The
// secret is the entire privacy barrier: a real deployment reads at least
// token.MinSecretLen bytes from crypto/rand or a KMS and never commits one to
// source. Both institutions must hold the same secret, shared out of band.
// session.New wipes the slice it is given, so each call converts the constant
// afresh.
const exampleSecret = "sriracha-example-secret-32-byte!"

// exampleRecord builds a small record on the canonical schema.
func exampleRecord(given, family, dob string) sriracha.RawRecord {
	return sriracha.RawRecord{
		sriracha.FieldNameGiven:  given,
		sriracha.FieldNameFamily: family,
		sriracha.FieldDateBirth:  dob,
	}
}

// Example is the scenario the library exists for. Two institutions hold
// records about the same person and neither may reveal them. Each side
// tokenizes locally, ships JSON over the wire, and the receiver scores the
// pair without ever seeing a name.
//
// The last step is the failure a Session exists to catch. A pair of tokens
// minted under an older schema agree with each other, so token.Match, which
// holds no schema of its own, scores them happily under whatever weights the
// reader now has. A Session knows which schema it holds and refuses.
func Example() {
	fs := fieldset.DefaultFieldSet()

	// Institution A, in its own process.
	a, err := session.New([]byte(exampleSecret), fs)
	if err != nil {
		fmt.Println("session A:", err)
		return
	}
	defer a.Destroy()

	sent, err := a.TokenizeProbabilistic(exampleRecord("Alice", "Smith", "1990-01-15"))
	if err != nil {
		fmt.Println("tokenize A:", err)
		return
	}
	wire, err := json.Marshal(sent)
	if err != nil {
		fmt.Println("marshal:", err)
		return
	}

	// Institution B, on the other side of the wire.
	b, err := session.New([]byte(exampleSecret), fs)
	if err != nil {
		fmt.Println("session B:", err)
		return
	}
	defer b.Destroy()

	var received sriracha.ProbabilisticToken
	if err := json.Unmarshal(wire, &received); err != nil {
		fmt.Println("unmarshal:", err)
		return
	}
	fmt.Println("format:", received.Format)
	fmt.Println("schema:", received.FieldSetVersion, received.FieldSetFingerprint[:8])

	local, err := b.TokenizeProbabilistic(exampleRecord("Alice", "Smyth", "1990-01-15"))
	if err != nil {
		fmt.Println("tokenize B:", err)
		return
	}
	res, err := b.Match(received, local, token.DefaultMatchPolicy(0.70))
	if err != nil {
		fmt.Println("match:", err)
		return
	}
	fmt.Printf("match=%v score=%.3f fields=%d\n", res.IsMatch, res.Score, res.ComparableFields)

	// A reweights one field and redeploys. Nothing about the wire format
	// changes, only the meaning of the numbers.
	drifted := fieldset.DefaultFieldSet()
	drifted.Fields[3].Weight = 2.25
	c, err := session.New([]byte(exampleSecret), drifted)
	if err != nil {
		fmt.Println("session C:", err)
		return
	}
	defer c.Destroy()

	// B re-scores an archived batch that A sent before the change. Both
	// tokens in the pair carry the old fingerprint, so they are consistent
	// with each other and only disagree with the schema B holds now.
	staleA, err := c.TokenizeProbabilistic(exampleRecord("Alice", "Smith", "1990-01-15"))
	if err != nil {
		fmt.Println("tokenize stale A:", err)
		return
	}
	staleB, err := c.TokenizeProbabilistic(exampleRecord("Alice", "Smyth", "1990-01-15"))
	if err != nil {
		fmt.Println("tokenize stale B:", err)
		return
	}

	blind, err := token.Match(staleA, staleB, fs, token.DefaultMatchPolicy(0.70))
	if err != nil {
		fmt.Println("blind match:", err)
		return
	}
	fmt.Println("token.Match scores it anyway:", blind.IsMatch)

	_, err = b.Match(staleA, staleB, token.DefaultMatchPolicy(0.70))
	fmt.Println("session rejects the drift:", errors.Is(err, session.ErrFingerprintDrift))
	// Output:
	// format: sriracha/bloom/2
	// schema: 0.2 7fa7fbca
	// match=true score=0.855 fields=3
	// token.Match scores it anyway: true
	// session rejects the drift: true
}

// New validates the schema once and caches its fingerprint, so callers do not
// thread the FieldSet through every call and a malformed schema fails before
// any locked memory is allocated.
func ExampleNew() {
	s, err := session.New([]byte(exampleSecret), fieldset.DefaultFieldSet())
	if err != nil {
		fmt.Println("session.New:", err)
		return
	}
	defer s.Destroy()

	a, err := s.TokenizeProbabilistic(exampleRecord("Alice", "Smith", "1990-01-15"))
	if err != nil {
		fmt.Println("tokenize a:", err)
		return
	}
	b, err := s.TokenizeProbabilistic(exampleRecord("Alice", "Smyth", "1990-01-15"))
	if err != nil {
		fmt.Println("tokenize b:", err)
		return
	}

	res, err := s.Match(a, b, token.DefaultMatchPolicy(0.70))
	if err != nil {
		fmt.Println("match:", err)
		return
	}
	fmt.Printf("match=%v score=%.3f fields=%d\n", res.IsMatch, res.Score, res.ComparableFields)
	// Output:
	// match=true score=0.855 fields=3
}

// A malformed schema is reported at construction, not at the first tokenize
// call.
func ExampleNew_invalidFieldSet() {
	_, err := session.New([]byte(exampleSecret), sriracha.FieldSet{})
	fmt.Println(errors.Is(err, sriracha.ErrMissingVersion))
	fmt.Println(err)
	// Output:
	// true
	// field set version must not be empty
}

// WithKeyID labels every token the Session emits, so a comparison across a
// key rotation reports a mismatch instead of a quiet non-match.
func ExampleWithKeyID() {
	s, err := session.New([]byte(exampleSecret), fieldset.DefaultFieldSet(), session.WithKeyID("2024-07"))
	if err != nil {
		fmt.Println("session.New:", err)
		return
	}
	defer s.Destroy()

	tok, err := s.TokenizeDeterministic(exampleRecord("Alice", "Smith", "1990-01-15"))
	if err != nil {
		fmt.Println("tokenize:", err)
		return
	}
	fmt.Println("key id:", tok.KeyID)
	// Output:
	// key id: 2024-07
}

// By default a token carrying no fingerprint passes the drift check, because
// only a Session stamps one and tokens from a bare token.Tokenizer are a
// supported flow. WithStrictFingerprint rejects them, which is the defence
// against a peer quietly downgrading to a hand-built producer.
func ExampleWithStrictFingerprint() {
	fs := fieldset.DefaultFieldSet()
	record := exampleRecord("Alice", "Smith", "1990-01-15")

	tk, err := token.New([]byte(exampleSecret))
	if err != nil {
		fmt.Println("token.New:", err)
		return
	}
	defer tk.Destroy()
	unstamped, err := tk.TokenizeDeterministic(record, fs)
	if err != nil {
		fmt.Println("tokenize unstamped:", err)
		return
	}

	lax, err := session.New([]byte(exampleSecret), fs)
	if err != nil {
		fmt.Println("session lax:", err)
		return
	}
	defer lax.Destroy()
	strict, err := session.New([]byte(exampleSecret), fs, session.WithStrictFingerprint())
	if err != nil {
		fmt.Println("session strict:", err)
		return
	}
	defer strict.Destroy()

	stamped, err := lax.TokenizeDeterministic(record)
	if err != nil {
		fmt.Println("tokenize stamped:", err)
		return
	}

	// Same secret, same schema, so the field HMACs are identical either way.
	eq, err := lax.Equal(unstamped, stamped)
	fmt.Println("lax:", eq, err)

	_, err = strict.Equal(unstamped, stamped)
	fmt.Println("strict rejects unstamped:", errors.Is(err, session.ErrFingerprintDrift))
	// Output:
	// lax: true <nil>
	// strict rejects unstamped: true
}

// MatchCLK compares record-level tokens, which carry no per-field structure at
// all. It takes a bare threshold rather than a token.MatchPolicy because there
// is no per-field evidence left to floor. Balanced filters lift unrelated
// records well above 0, so CLK thresholds need their own calibration.
func ExampleSession_MatchCLK() {
	s, err := session.New([]byte(exampleSecret), fieldset.DefaultFieldSet())
	if err != nil {
		fmt.Println("session.New:", err)
		return
	}
	defer s.Destroy()

	a, err := s.TokenizeCLK(exampleRecord("Alice", "Smith", "1990-01-15"))
	if err != nil {
		fmt.Println("tokenize a:", err)
		return
	}
	typo, err := s.TokenizeCLK(exampleRecord("Alice", "Smyth", "1990-01-15"))
	if err != nil {
		fmt.Println("tokenize typo:", err)
		return
	}
	other, err := s.TokenizeCLK(exampleRecord("Bob", "Jones", "1955-03-02"))
	if err != nil {
		fmt.Println("tokenize other:", err)
		return
	}

	near, err := s.MatchCLK(a, typo, 0.85)
	if err != nil {
		fmt.Println("match near:", err)
		return
	}
	far, err := s.MatchCLK(a, other, 0.85)
	if err != nil {
		fmt.Println("match far:", err)
		return
	}
	fmt.Printf("typo:      score=%.3f match=%v\n", near.Score, near.IsMatch)
	fmt.Printf("different: score=%.3f match=%v\n", far.Score, far.IsMatch)
	// Output:
	// typo:      score=0.955 match=true
	// different: score=0.664 match=false
}

// ValidateRecord pre-checks a record against the Session's schema before
// tokenization, reporting every problem in one pass.
func ExampleSession_ValidateRecord() {
	s, err := session.New([]byte(exampleSecret), fieldset.DefaultFieldSet())
	if err != nil {
		fmt.Println("session.New:", err)
		return
	}
	defer s.Destroy()

	err = s.ValidateRecord(sriracha.RawRecord{
		sriracha.FieldNameGiven: "Alice",
		sriracha.FieldDateBirth: "15/01/1990",
	})
	fmt.Println(err)
	// Output:
	// field "sriracha::date::birth": normalize: invalid value: date must be ISO 8601 YYYY-MM-DD
}
