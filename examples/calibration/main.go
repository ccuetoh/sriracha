// Calibration shows how to pick a Match threshold from labeled pairs using
// token.Calibrate. The example builds a tiny synthetic ground-truth set,
// sweeps thresholds, and prints the best F1 / precision / recall plus a few
// nearby ROC points.
//
// Run with:
//
//	go run ./examples/calibration
package main

import (
	"fmt"
	"log"
	"math"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/fieldset"
	"github.com/ccuetoh/sriracha/session"
	"github.com/ccuetoh/sriracha/token"
)

func main() {
	secret := []byte("super-secret-key")
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

	cal, err := token.Calibrate(pairs, fs)
	if err != nil {
		log.Fatalf("Calibrate: %v", err)
	}

	fmt.Printf("optimal threshold: %.2f  F1=%.3f  precision=%.3f  recall=%.3f\n",
		cal.OptimalThreshold, cal.F1, cal.Precision, cal.Recall)
	fmt.Println("nearby ROC points:")
	for _, p := range cal.ROC {
		if math.Abs(p.Threshold-cal.OptimalThreshold) > 0.05 {
			continue
		}
		fmt.Printf("    t=%.2f  P=%.3f  R=%.3f  F1=%.3f\n", p.Threshold, p.Precision, p.Recall, p.F1)
	}
}

func rec(given, family, dob string) sriracha.RawRecord {
	return sriracha.RawRecord{
		sriracha.FieldNameGiven:  given,
		sriracha.FieldNameFamily: family,
		sriracha.FieldDateBirth:  dob,
	}
}
