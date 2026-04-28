package token

import (
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/awnumar/memguard"

	"go.sriracha.dev/normalize"
	"go.sriracha.dev/sriracha"
)

// Tokenizer produces tokens from RawRecords using a shared secret.
// Call Destroy when finished to wipe the key from memory.
type Tokenizer interface {
	// TokenizeRecord tokenizes a RawRecord in deterministic mode (HMAC-SHA256
	// per field). The returned token's Fields slice is aligned with fs.Fields:
	// each entry is a 32-byte HMAC for a present field, or nil for an absent
	// optional field. Missing required fields return an error.
	TokenizeRecord(record sriracha.RawRecord, fs sriracha.FieldSet) (sriracha.DeterministicToken, error)
	// TokenizeRecordBloom tokenizes a RawRecord in probabilistic (Bloom filter)
	// mode. The returned token's Fields slice is aligned with fs.Fields:
	// present fields contain the populated filter, absent optional fields
	// contain an all-zero filter of the same length. Missing required fields
	// return an error.
	TokenizeRecordBloom(record sriracha.RawRecord, fs sriracha.FieldSet) (sriracha.BloomToken, error)
	// Destroy wipes the HMAC key from memory. The Tokenizer must not be used
	// after this call.
	Destroy()
}

// tokenizer is the default Tokenizer implementation backed by a memguard-locked
// secret.
type tokenizer struct {
	secret *memguard.LockedBuffer
}

// New creates a Tokenizer with the given HMAC secret.
// The secret is copied into a locked, non-swappable memory region and the
// source slice is wiped. Returns an error if secret is empty.
func New(secret []byte) (Tokenizer, error) {
	if len(secret) == 0 {
		return nil, errors.New("token: secret must not be empty")
	}

	return &tokenizer{secret: memguard.NewBufferFromBytes(secret)}, nil
}

func (t *tokenizer) Destroy() {
	t.secret.Destroy()
}

func (t *tokenizer) TokenizeRecord(record sriracha.RawRecord, fs sriracha.FieldSet) (sriracha.DeterministicToken, error) {
	fields := make([][]byte, len(fs.Fields))
	h := hmac.New(sha256.New, t.secret.Bytes())

	for i, spec := range fs.Fields {
		raw, ok := record[spec.Path]
		if !ok {
			if spec.Required {
				return sriracha.DeterministicToken{}, fmt.Errorf("token: required field %q missing", spec.Path)
			}
			continue
		}
		normalized, err := normalize.Normalize(raw, spec.Path)
		if err != nil {
			return sriracha.DeterministicToken{}, fmt.Errorf("token: normalization failed for field %q: %w", spec.Path, err)
		}
		h.Reset()
		h.Write([]byte(normalized))
		h.Write([]byte{':'})
		h.Write([]byte(spec.Path.String()))
		fields[i] = h.Sum(nil)
	}

	return sriracha.DeterministicToken{
		FieldSetVersion: fs.Version,
		Fields:          fields,
	}, nil
}
