package token_test

import (
	"errors"
	"fmt"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/fieldset"
	"github.com/ccuetoh/sriracha/token"
)

// exampleSecret is a literal so these examples produce stable output. The
// secret is the entire privacy barrier: a real deployment reads at least
// token.MinSecretLen bytes from crypto/rand or a KMS and never commits one to
// source. token.New wipes the slice it is given, so each call converts the
// constant afresh.
const exampleSecret = "sriracha-example-secret-32-byte!"

// exampleRecord builds a small record on the canonical schema.
func exampleRecord(given, family, dob string) sriracha.RawRecord {
	return sriracha.RawRecord{
		sriracha.FieldNameGiven:  given,
		sriracha.FieldNameFamily: family,
		sriracha.FieldDateBirth:  dob,
	}
}

// Deterministic tokens are exact-match only. Identical records agree
// bit-for-bit; a one-letter typo produces an unrelated HMAC and no signal
// about how close the two values were.
func ExampleEqual() {
	tk, err := token.New([]byte(exampleSecret))
	if err != nil {
		fmt.Println("new:", err)
		return
	}
	defer tk.Destroy()

	fs := fieldset.DefaultFieldSet()
	a, err := tk.TokenizeDeterministic(exampleRecord("Alice", "Smith", "1990-01-15"), fs)
	if err != nil {
		fmt.Println("tokenize a:", err)
		return
	}
	same, err := tk.TokenizeDeterministic(exampleRecord("ALICE", " Smith ", "1990-01-15"), fs)
	if err != nil {
		fmt.Println("tokenize same:", err)
		return
	}
	typo, err := tk.TokenizeDeterministic(exampleRecord("Alice", "Smyth", "1990-01-15"), fs)
	if err != nil {
		fmt.Println("tokenize typo:", err)
		return
	}

	eqSame, err := token.Equal(a, same)
	fmt.Println("normalized duplicate:", eqSame, err)

	eqTypo, err := token.Equal(a, typo)
	fmt.Println("one-letter typo:", eqTypo, err)
	// Output:
	// normalized duplicate: true <nil>
	// one-letter typo: false <nil>
}

// Equal returns (bool, error) because "not equal" and "not comparable" are
// different answers. Tokens minted under different secrets say nothing about
// whether the records describe the same person, so Equal reports
// ErrKeyIDMismatch rather than a quiet false.
func ExampleEqual_keyRotation() {
	fs := fieldset.DefaultFieldSet()
	record := exampleRecord("Alice", "Smith", "1990-01-15")

	oldKey, err := token.New([]byte(exampleSecret), token.WithKeyID("2024-01"))
	if err != nil {
		fmt.Println("new old:", err)
		return
	}
	defer oldKey.Destroy()
	newKey, err := token.New([]byte("sriracha-rotated-secret-32-byte!"), token.WithKeyID("2024-07"))
	if err != nil {
		fmt.Println("new new:", err)
		return
	}
	defer newKey.Destroy()

	a, err := oldKey.TokenizeDeterministic(record, fs)
	if err != nil {
		fmt.Println("tokenize a:", err)
		return
	}
	b, err := newKey.TokenizeDeterministic(record, fs)
	if err != nil {
		fmt.Println("tokenize b:", err)
		return
	}

	eq, err := token.Equal(a, b)
	fmt.Println("equal:", eq)
	fmt.Println("rotation detected:", errors.Is(err, token.ErrKeyIDMismatch))
	fmt.Println(err)
	// Output:
	// equal: false
	// rotation detected: true
	// token: KeyID mismatch: "2024-01" vs "2024-07"
}

// Probabilistic tokens keep a similarity signal, so a typo still scores high.
// Score is a weighted average over the fields the pair could be compared on;
// the policy carries the threshold and the evidence floor together.
func ExampleMatch() {
	tk, err := token.New([]byte(exampleSecret))
	if err != nil {
		fmt.Println("new:", err)
		return
	}
	defer tk.Destroy()

	fs := fieldset.DefaultFieldSet()
	a, err := tk.TokenizeProbabilistic(exampleRecord("Alice", "Smith", "1990-01-15"), fs)
	if err != nil {
		fmt.Println("tokenize a:", err)
		return
	}
	b, err := tk.TokenizeProbabilistic(exampleRecord("Alice", "Smyth", "1990-01-15"), fs)
	if err != nil {
		fmt.Println("tokenize b:", err)
		return
	}

	res, err := token.Match(a, b, fs, token.DefaultMatchPolicy(0.70))
	if err != nil {
		fmt.Println("match:", err)
		return
	}
	fmt.Printf("match=%v score=%.3f fields=%d weight=%.1f\n",
		res.IsMatch, res.Score, res.ComparableFields, res.ComparableWeight)
	// Output:
	// match=true score=0.855 fields=3 weight=6.5
}

// The evidence floor is why a threshold alone is not a decision. Two records
// that share nothing but a country code agree perfectly on the one field they
// can be compared on and score 1.000, the same number a full name plus date of
// birth agreement produces. DefaultMatchPolicy refuses to call that a match;
// the zero MatchPolicy applies no floor and accepts it.
func ExampleMatch_evidenceFloor() {
	tk, err := token.New([]byte(exampleSecret))
	if err != nil {
		fmt.Println("new:", err)
		return
	}
	defer tk.Destroy()

	fs := fieldset.DefaultFieldSet()
	thin := sriracha.RawRecord{sriracha.FieldAddressCountry: "CL"}
	a, err := tk.TokenizeProbabilistic(thin, fs)
	if err != nil {
		fmt.Println("tokenize a:", err)
		return
	}
	b, err := tk.TokenizeProbabilistic(thin, fs)
	if err != nil {
		fmt.Println("tokenize b:", err)
		return
	}

	floored, err := token.Match(a, b, fs, token.DefaultMatchPolicy(0.70))
	if err != nil {
		fmt.Println("match:", err)
		return
	}
	bare, err := token.Match(a, b, fs, token.MatchPolicy{Threshold: 0.70})
	if err != nil {
		fmt.Println("match:", err)
		return
	}

	fmt.Printf("score=%.3f fields=%d\n", floored.Score, floored.ComparableFields)
	fmt.Println("with floor:", floored.IsMatch)
	fmt.Println("without floor:", bare.IsMatch)
	// Output:
	// score=1.000 fields=1
	// with floor: false
	// without floor: true
}

// MatchResult keeps the per-field scores alongside the aggregate, so a caller
// can see which field carried the decision.
func ExampleMatchResult_ScoreFor() {
	tk, err := token.New([]byte(exampleSecret))
	if err != nil {
		fmt.Println("new:", err)
		return
	}
	defer tk.Destroy()

	fs := fieldset.DefaultFieldSet()
	a, err := tk.TokenizeProbabilistic(exampleRecord("Alice", "Smith", "1990-01-15"), fs)
	if err != nil {
		fmt.Println("tokenize a:", err)
		return
	}
	b, err := tk.TokenizeProbabilistic(exampleRecord("Alice", "Smyth", "1990-01-15"), fs)
	if err != nil {
		fmt.Println("tokenize b:", err)
		return
	}

	res, err := token.Match(a, b, fs, token.DefaultMatchPolicy(0.70))
	if err != nil {
		fmt.Println("match:", err)
		return
	}

	for _, path := range []sriracha.FieldPath{
		sriracha.FieldNameGiven,
		sriracha.FieldNameFamily,
		sriracha.FieldContactEmail,
	} {
		score, ok := res.ScoreFor(path)
		fmt.Printf("%s %.3f %v\n", path.LocalName(), score, ok)
	}
	// Output:
	// given 1.000 true
	// family 0.623 true
	// email 0.000 true
}

// A CLK folds every present field into one filter, so it leaks neither which
// fields the record carries nor how similar each one is. That is the
// recommended token to share when per-field scores are not needed. Balanced
// filters lift unrelated records toward a floor near 0.5, so CLK thresholds
// must be calibrated separately.
func ExampleMatchCLK() {
	tk, err := token.New([]byte(exampleSecret))
	if err != nil {
		fmt.Println("new:", err)
		return
	}
	defer tk.Destroy()

	fs := fieldset.DefaultFieldSet()
	a, err := tk.TokenizeCLK(exampleRecord("Alice", "Smith", "1990-01-15"), fs)
	if err != nil {
		fmt.Println("tokenize a:", err)
		return
	}
	typo, err := tk.TokenizeCLK(exampleRecord("Alice", "Smyth", "1990-01-15"), fs)
	if err != nil {
		fmt.Println("tokenize typo:", err)
		return
	}
	other, err := tk.TokenizeCLK(exampleRecord("Bob", "Jones", "1955-03-02"), fs)
	if err != nil {
		fmt.Println("tokenize other:", err)
		return
	}

	near, err := token.MatchCLK(a, typo, 0.85)
	if err != nil {
		fmt.Println("match near:", err)
		return
	}
	far, err := token.MatchCLK(a, other, 0.85)
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

// Calibrate picks the threshold from labeled pairs instead of a guess. It
// returns the midpoint of the longest plateau of maximum F1, not the bottom
// edge, because the bottom edge sits right against the non-match distribution.
// F1, precision and recall are in-sample: measure the chosen threshold on a
// held-out set before quoting them.
func ExampleCalibrate() {
	tk, err := token.New([]byte(exampleSecret))
	if err != nil {
		fmt.Println("new:", err)
		return
	}
	defer tk.Destroy()

	fs := fieldset.DefaultFieldSet()
	labeled := []struct {
		a, b  sriracha.RawRecord
		match bool
	}{
		{exampleRecord("Alice", "Smith", "1990-01-15"), exampleRecord("Alice", "Smith", "1990-01-15"), true},
		{exampleRecord("Alice", "Smith", "1990-01-15"), exampleRecord("Alice", "Smyth", "1990-01-15"), true},
		{exampleRecord("Jonathan", "OBrien", "1985-07-22"), exampleRecord("Jonathon", "O'Brien", "1985-07-22"), true},
		{exampleRecord("Maria", "Lopez", "1972-04-09"), exampleRecord("Maria", "Lopes", "1972-04-09"), true},
		{exampleRecord("Alice", "Smith", "1990-01-15"), exampleRecord("Bob", "Jones", "1955-03-02"), false},
		{exampleRecord("Alice", "Smith", "1990-01-15"), exampleRecord("Alice", "Johnson", "1991-08-04"), false},
		{exampleRecord("Maria", "Lopez", "1972-04-09"), exampleRecord("Carlos", "Garcia", "1972-04-09"), false},
		{exampleRecord("Robert", "Singh", "2001-12-30"), exampleRecord("Sara", "Connor", "1980-05-11"), false},
	}

	pairs := make([]token.LabeledPair, len(labeled))
	for i, p := range labeled {
		a, err := tk.TokenizeProbabilistic(p.a, fs)
		if err != nil {
			fmt.Println("tokenize a:", err)
			return
		}
		b, err := tk.TokenizeProbabilistic(p.b, fs)
		if err != nil {
			fmt.Println("tokenize b:", err)
			return
		}
		pairs[i] = token.LabeledPair{A: a, B: b, Match: p.match}
	}

	cal, err := token.Calibrate(pairs, fs, token.MatchPolicy{MinComparableFields: 2})
	if err != nil {
		fmt.Println("calibrate:", err)
		return
	}
	fmt.Printf("threshold=%.2f F1=%.3f precision=%.3f recall=%.3f excluded=%d\n",
		cal.OptimalThreshold, cal.F1, cal.Precision, cal.Recall, cal.ExcludedPairs)
	// Output:
	// threshold=0.64 F1=1.000 precision=1.000 recall=1.000 excluded=0
}
