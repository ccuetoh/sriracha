// Calibration shows how to pick a Match threshold from labeled pairs using
// token.Calibrate. The example builds a tiny synthetic ground-truth set,
// sweeps thresholds, and prints the best F1 / precision / recall plus a few
// nearby precision-recall points.
//
// Run with:
//
//	go run ./examples/calibration
//
// Set SRIRACHA_SECRET to use your own secret; the example generates an
// ephemeral one otherwise.
package main

import (
	"crypto/rand"
	"fmt"
	"log"
	"math"
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

	fs := fieldset.DefaultFieldSet()
	s, err := session.New(secret, fs)
	if err != nil {
		log.Fatalf("session.New: %v", err)
	}
	defer s.Destroy()

	type rawPair struct {
		a, b  sriracha.RawRecord
		match bool
	}
	raws := []rawPair{
		{rec("Alice", "Smith", "1990-01-15"), rec("Alice", "Smith", "1990-01-15"), true},
		{rec("Alice", "Smith", "1990-01-15"), rec("Alice", "Smyth", "1990-01-15"), true},
		{rec("Jonathan", "OBrien", "1985-07-22"), rec("Jonathon", "O'Brien", "1985-07-22"), true},
		{rec("Maria", "Lopez", "1972-04-09"), rec("Maria", "Lopes", "1972-04-09"), true},
		{rec("Robert", "Singh", "2001-12-30"), rec("Rob", "Singh", "2001-12-30"), true},
		{rec("Alice", "Smith", "1990-01-15"), rec("Bob", "Jones", "1955-03-02"), false},
		{rec("Alice", "Smith", "1990-01-15"), rec("Alice", "Johnson", "1991-08-04"), false},
		{rec("Maria", "Lopez", "1972-04-09"), rec("Carlos", "Garcia", "1972-04-09"), false},
		{rec("Robert", "Singh", "2001-12-30"), rec("Sara", "Connor", "1980-05-11"), false},
		{rec("Jonathan", "OBrien", "1985-07-22"), rec("Sarah", "OBrien", "1962-11-02"), false},
	}

	pairs := make([]token.LabeledPair, len(raws))
	for i, p := range raws {
		a, err := s.TokenizeProbabilistic(p.a)
		if err != nil {
			log.Fatalf("tokenize pair %d a: %v", i, err)
		}
		b, err := s.TokenizeProbabilistic(p.b)
		if err != nil {
			log.Fatalf("tokenize pair %d b: %v", i, err)
		}
		pairs[i] = token.LabeledPair{A: a, B: b, Match: p.match}
	}

	// Calibrate against the policy the deployment will actually match with.
	// The floors drop pairs that carry too little evidence to decide, so a
	// threshold tuned here is tuned on the same population it will be applied
	// to. Calibrate ignores policy.Threshold; that is the value it returns.
	policy := token.DefaultMatchPolicy(0)
	cal, err := token.Calibrate(pairs, fs, policy)
	if err != nil {
		log.Fatalf("Calibrate: %v", err)
	}

	fmt.Printf("labeled pairs: %d (excluded by the evidence floor: %d)\n", len(pairs), cal.ExcludedPairs)
	fmt.Printf("optimal threshold: %.2f  F1=%.3f  precision=%.3f  recall=%.3f\n",
		cal.OptimalThreshold, cal.F1, cal.Precision, cal.Recall)
	fmt.Println("These are in-sample numbers on ten hand-written pairs. A real")
	fmt.Println("calibration needs held-out pairs drawn from the population being")
	fmt.Println("linked, or it reports how well the threshold memorised its input.")

	fmt.Println("\nnearby PR points:")
	for _, p := range cal.PR {
		if math.Abs(p.Threshold-cal.OptimalThreshold) > 0.05 {
			continue
		}
		fmt.Printf("    t=%.2f  P=%.3f  R=%.3f  F1=%.3f\n", p.Threshold, p.Precision, p.Recall, p.F1)
	}

	// The calibrated threshold is only half a decision. Hand it back with the
	// same floors to get the policy that Match should run with.
	policy.Threshold = cal.OptimalThreshold
	res, err := s.Match(pairs[1].A, pairs[1].B, policy)
	if err != nil {
		log.Fatalf("Match: %v", err)
	}
	fmt.Printf("\ncalibrated policy applied to the typo pair: match=%v score=%.3f comparable fields=%d\n",
		res.IsMatch, res.Score, res.ComparableFields)
}

func rec(given, family, dob string) sriracha.RawRecord {
	return sriracha.RawRecord{
		sriracha.FieldNameGiven:  given,
		sriracha.FieldNameFamily: family,
		sriracha.FieldDateBirth:  dob,
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
