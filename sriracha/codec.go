package sriracha

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
)

// jsonField represents an aligned token field on the wire: nil entries (absent
// optional fields) marshal to JSON null, populated entries to a base64 string.
type jsonField struct {
	bytes []byte
	null  bool
}

func (f jsonField) MarshalJSON() ([]byte, error) {
	if f.null {
		return []byte("null"), nil
	}
	return json.Marshal(base64.StdEncoding.EncodeToString(f.bytes))
}

func (f *jsonField) UnmarshalJSON(data []byte) error {
	if string(data) == "null" {
		f.null = true
		f.bytes = nil
		return nil
	}
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return fmt.Errorf("sriracha: token field is not a base64 string: %w", err)
	}
	b, err := base64.StdEncoding.DecodeString(s)
	if err != nil {
		return fmt.Errorf("sriracha: token field base64 decode: %w", err)
	}
	f.null = false
	f.bytes = b
	return nil
}

func encodeFields(in [][]byte) []jsonField {
	out := make([]jsonField, len(in))
	for i, b := range in {
		if b == nil {
			out[i] = jsonField{null: true}
		} else {
			out[i] = jsonField{bytes: b}
		}
	}
	return out
}

func decodeFields(in []jsonField) [][]byte {
	out := make([][]byte, len(in))
	for i, f := range in {
		if f.null {
			out[i] = nil
		} else {
			out[i] = f.bytes
		}
	}
	return out
}

type detTokenJSON struct {
	FieldSetVersion string      `json:"field_set_version"`
	KeyID           string      `json:"key_id,omitempty"`
	Fields          []jsonField `json:"fields"`
}

// MarshalJSON encodes a DeterministicToken with base64-encoded field bytes.
// Absent optional fields encode as JSON null, preserving positional alignment
// with the FieldSet on the receiving side.
func (t DeterministicToken) MarshalJSON() ([]byte, error) {
	return json.Marshal(detTokenJSON{
		FieldSetVersion: t.FieldSetVersion,
		KeyID:           t.KeyID,
		Fields:          encodeFields(t.Fields),
	})
}

// UnmarshalJSON decodes a DeterministicToken produced by MarshalJSON. Returns
// an error if FieldSetVersion is empty or any field is not valid base64.
func (t *DeterministicToken) UnmarshalJSON(data []byte) error {
	var raw detTokenJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.FieldSetVersion == "" {
		return errors.New("sriracha: DeterministicToken missing field_set_version")
	}
	t.FieldSetVersion = raw.FieldSetVersion
	t.KeyID = raw.KeyID
	t.Fields = decodeFields(raw.Fields)
	return nil
}

type bloomTokenJSON struct {
	FieldSetVersion string      `json:"field_set_version"`
	KeyID           string      `json:"key_id,omitempty"`
	BloomParams     BloomConfig `json:"bloom_params"`
	Fields          []jsonField `json:"fields"`
}

// MarshalJSON encodes a BloomToken with base64-encoded field bytes and the
// nested BloomParams. Absent optional fields encode as JSON null.
func (t BloomToken) MarshalJSON() ([]byte, error) {
	return json.Marshal(bloomTokenJSON{
		FieldSetVersion: t.FieldSetVersion,
		KeyID:           t.KeyID,
		BloomParams:     t.BloomParams,
		Fields:          encodeFields(t.Fields),
	})
}

// UnmarshalJSON decodes a BloomToken produced by MarshalJSON. Returns an
// error if FieldSetVersion is empty or any field is not valid base64.
func (t *BloomToken) UnmarshalJSON(data []byte) error {
	var raw bloomTokenJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.FieldSetVersion == "" {
		return errors.New("sriracha: BloomToken missing field_set_version")
	}
	t.FieldSetVersion = raw.FieldSetVersion
	t.KeyID = raw.KeyID
	t.BloomParams = raw.BloomParams
	t.Fields = decodeFields(raw.Fields)
	return nil
}
