package token

import (
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"fmt"

	"go.sriracha.dev/normalize"
	sriracha "go.sriracha.dev/sriracha"
)

// Tokenizer produces TokenRecords from RawRecords using a shared secret.
type Tokenizer struct {
	Secret []byte
}

// New creates a Tokenizer with the given HMAC secret.
// Returns an error if secret is empty.
func New(secret []byte) (*Tokenizer, error) {
	if len(secret) == 0 {
		return nil, fmt.Errorf("token: secret must not be empty")
	}
	return &Tokenizer{Secret: secret}, nil
}

// TokenizeRecord tokenizes a RawRecord in deterministic mode (HMAC-SHA256 per field).
// Fields are processed in FieldSet order.
// Missing required fields return an error.
// Missing optional fields are silently skipped (no bytes added to Payload).
func (t *Tokenizer) TokenizeRecord(record sriracha.RawRecord, fs sriracha.FieldSet) (sriracha.TokenRecord, error) {
	var buf []byte
	for _, spec := range fs.Fields {
		raw, ok := record[spec.Path]
		if !ok || sriracha.IsNotFound(raw) || sriracha.IsNotHeld(raw) {
			if spec.Required {
				return sriracha.TokenRecord{}, fmt.Errorf("token: required field %q missing", spec.Path)
			}
			continue
		}
		normalized, err := normalize.Normalize(raw, spec.Path)
		if err != nil {
			return sriracha.TokenRecord{}, fmt.Errorf("token: normalization failed for field %q: %w", spec.Path, err)
		}
		fieldToken := t.tokenizeField(normalized, spec.Path)
		buf = append(buf, fieldToken...)
	}
	checksum := sha256.Sum256(buf)
	return sriracha.TokenRecord{
		FieldSetVersion: fs.Version,
		Mode:            sriracha.Deterministic,
		Payload:         buf,
		Checksum:        checksum,
	}, nil
}

// ValidateTokenRecord verifies that the Payload matches the Checksum.
// Uses constant-time comparison to prevent timing attacks.
func ValidateTokenRecord(tr sriracha.TokenRecord) error {
	expected := sha256.Sum256(tr.Payload)
	if subtle.ConstantTimeCompare(expected[:], tr.Checksum[:]) != 1 {
		return fmt.Errorf("token: checksum mismatch")
	}
	return nil
}

// tokenizeField computes a 32-byte HMAC-SHA256 token for a single normalized field value.
// The field path is included to prevent cross-field collisions.
func (t *Tokenizer) tokenizeField(normalizedValue string, path sriracha.FieldPath) []byte {
	h := hmac.New(sha256.New, t.Secret)
	h.Write([]byte(normalizedValue + ":" + path.String()))
	return h.Sum(nil)
}
