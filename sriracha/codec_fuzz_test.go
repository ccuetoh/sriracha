package sriracha

import (
	"encoding/json"
	"testing"
)

// FuzzUnmarshalDeterministicJSON verifies that DeterministicToken.UnmarshalJSON
// never panics on arbitrary input.
func FuzzUnmarshalDeterministicJSON(f *testing.F) {
	f.Add([]byte(`{"field_set_version":"0.1","fields":["AQID"]}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`null`))
	f.Add([]byte(`{"field_set_version":"0.1","fields":[null]}`))

	f.Fuzz(func(t *testing.T, data []byte) {
		var tr DeterministicToken
		_ = json.Unmarshal(data, &tr)
	})
}

// FuzzUnmarshalBloomJSON verifies that BloomToken.UnmarshalJSON never panics
// on arbitrary input.
func FuzzUnmarshalBloomJSON(f *testing.F) {
	f.Add([]byte(`{"field_set_version":"0.1","bloom_params":{"SizeBits":1024,"NgramSizes":[2,3],"HashCount":2},"fields":["AQID"]}`))
	f.Add([]byte(`{}`))
	f.Add([]byte(`null`))

	f.Fuzz(func(t *testing.T, data []byte) {
		var tr BloomToken
		_ = json.Unmarshal(data, &tr)
	})
}
