package token_test

import (
	"fmt"

	"go.sriracha.dev/sriracha"
	"go.sriracha.dev/token"
)

func ExampleTokenizer_TokenizeRecord() {
	tok, err := token.New([]byte("shared-secret"), token.WithKeyID("key-2026-04"))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer tok.Destroy()

	fs := sriracha.FieldSet{
		Version: "demo-1",
		Fields: []sriracha.FieldSpec{
			{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0},
		},
	}
	rec := sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"}
	tr, err := tok.TokenizeRecord(rec, fs)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%s\n", tr)
	// Output: DeterministicToken{v=demo-1 key=key-2026-04 fields=1/1 bytes=32}
}

func ExampleMatch() {
	tok, err := token.New([]byte("shared-secret"))
	if err != nil {
		fmt.Println(err)
		return
	}
	defer tok.Destroy()

	fs := sriracha.FieldSet{
		Version: "demo-1",
		Fields: []sriracha.FieldSpec{
			{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0},
		},
		BloomParams: sriracha.DefaultBloomConfig(),
	}

	a, _ := tok.TokenizeRecordBloom(sriracha.RawRecord{sriracha.FieldNameGiven: "Christopher"}, fs)
	b, _ := tok.TokenizeRecordBloom(sriracha.RawRecord{sriracha.FieldNameGiven: "Christopher"}, fs)

	res, err := token.Match(a, b, fs, 0.9)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(res.IsMatch, res.Score == 1.0)
	// Output: true true
}
