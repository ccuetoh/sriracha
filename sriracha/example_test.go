package sriracha_test

import (
	"encoding/json"
	"fmt"

	"go.sriracha.dev/sriracha"
)

func ExampleParseFieldPath() {
	fp, err := sriracha.ParseFieldPath("sriracha::name::given")
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(fp.Org(), fp.Namespace(), fp.LocalName())
	// Output: sriracha name given
}

func ExampleDeterministicToken_jsonRoundTrip() {
	orig := sriracha.DeterministicToken{
		FieldSetVersion: "0.1",
		KeyID:           "k1",
		Fields:          [][]byte{{0x01, 0x02}, nil},
	}
	data, _ := json.Marshal(orig)

	var decoded sriracha.DeterministicToken
	_ = json.Unmarshal(data, &decoded)
	fmt.Println(decoded.FieldSetVersion, decoded.KeyID, len(decoded.Fields))
	// Output: 0.1 k1 2
}

func ExampleBloomToken_jsonRoundTrip() {
	orig := sriracha.BloomToken{
		FieldSetVersion: "0.1",
		BloomParams:     sriracha.DefaultBloomConfig(),
		Fields:          [][]byte{{0xff}},
	}
	data, _ := json.Marshal(orig)

	var decoded sriracha.BloomToken
	_ = json.Unmarshal(data, &decoded)
	fmt.Println(decoded.FieldSetVersion, decoded.BloomParams.SizeBits)
	// Output: 0.1 1024
}
