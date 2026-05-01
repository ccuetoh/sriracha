// Package token implements deterministic (HMAC-SHA256) and probabilistic
// (Bloom filter) tokenization plus the comparison primitives Equal,
// DicePerField, Score, and Match.
//
// Most callers want Match — it wraps DicePerField + Score and returns the
// thresholded decision in one call. Even simpler: package session bundles a
// Tokenizer with a FieldSet so you don't have to thread the schema through
// every call.
package token

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"runtime"
	"sync"

	"github.com/awnumar/memguard"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/normalize"
)

// Tokenizer produces tokens from RawRecords using a shared secret.
// Call Destroy when finished to wipe the source secret buffer; if you forget,
// a runtime cleanup wipes it once the Tokenizer becomes unreachable.
//
// Tokenizer is safe for concurrent use by multiple goroutines until Destroy
// is called; HMAC instances are pooled internally. Calling any tokenize
// method after Destroy is undefined.
//
// Most callers want a session.Session — it bundles a Tokenizer with a
// FieldSet so you don't have to thread the schema through every call.
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
	// TokenizeField returns the deterministic 32-byte HMAC for a single
	// (value, path) pair, after running the same normalization pipeline
	// TokenizeRecord uses. Useful for stable indexing of one field outside
	// the FieldSet flow.
	TokenizeField(value string, path sriracha.FieldPath) ([]byte, error)
	// Destroy wipes the secret buffer that backs this Tokenizer. Pooled HMAC
	// instances created from the secret may still hold derived key material
	// (inner/outer pad) on the heap until garbage-collected. The Tokenizer
	// must not be used after this call.
	Destroy()
}

// Option configures a Tokenizer at construction time.
type Option func(*tokenizerOpts)

type tokenizerOpts struct {
	keyID string
}

// WithKeyID labels every token emitted by the Tokenizer with the given key
// identifier. Comparison helpers use it to surface post-rotation mismatches.
func WithKeyID(id string) Option {
	return func(o *tokenizerOpts) { o.keyID = id }
}

// tokenizer is the default Tokenizer implementation backed by a memguard-locked
// secret. HMAC instances are pooled so concurrent callers do not race on the
// underlying hash state.
type tokenizer struct {
	secret *memguard.LockedBuffer
	keyID  string
	pool   sync.Pool
}

// New creates a Tokenizer with the given HMAC secret.
// The secret is copied into a locked, non-swappable memory region and the
// source slice is wiped. Returns an error if secret is empty.
//
// A runtime finalizer wipes the locked buffer if the returned Tokenizer
// becomes unreachable without an explicit Destroy call.
func New(secret []byte, opts ...Option) (Tokenizer, error) {
	if len(secret) == 0 {
		return nil, errors.New("token: secret must not be empty")
	}

	var o tokenizerOpts
	for _, opt := range opts {
		opt(&o)
	}

	locked := memguard.NewBufferFromBytes(secret)
	t := &tokenizer{secret: locked, keyID: o.keyID}
	t.pool.New = func() any { return hmac.New(sha256.New, locked.Bytes()) }
	runtime.SetFinalizer(t, func(t *tokenizer) { t.secret.Destroy() })
	return t, nil
}

func (t *tokenizer) Destroy() {
	t.secret.Destroy()
}

func (t *tokenizer) acquire() hash.Hash {
	h, _ := t.pool.Get().(hash.Hash)
	return h
}

func (t *tokenizer) release(h hash.Hash) {
	h.Reset()
	t.pool.Put(h)
}

func (t *tokenizer) TokenizeRecord(record sriracha.RawRecord, fs sriracha.FieldSet) (sriracha.DeterministicToken, error) {
	fields := make([][]byte, len(fs.Fields))
	h := t.acquire()
	defer t.release(h)

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
		fields[i] = hmacField(h, normalized, spec.Path)
	}

	return sriracha.DeterministicToken{
		FieldSetVersion:     fs.Version,
		KeyID:               t.keyID,
		FieldSetFingerprint: fs.Fingerprint(),
		Fields:              fields,
	}, nil
}

func (t *tokenizer) TokenizeField(value string, path sriracha.FieldPath) ([]byte, error) {
	normalized, err := normalize.Normalize(value, path)
	if err != nil {
		return nil, fmt.Errorf("token: normalization failed for field %q: %w", path, err)
	}
	h := t.acquire()
	defer t.release(h)
	return hmacField(h, normalized, path), nil
}

// hmacField writes the canonical length-prefixed (value, path) preimage into
// h and returns the digest. Length-prefixing is what prevents
// (value="ab", path="c") from colliding with (value="a", path="bc").
func hmacField(h hash.Hash, normalizedValue string, path sriracha.FieldPath) []byte {
	h.Reset()
	var lp [4]byte
	nv := []byte(normalizedValue)
	binary.BigEndian.PutUint32(lp[:], uint32(len(nv))) //nolint:gosec // G115: normalized value length bounded by input
	h.Write(lp[:])
	h.Write(nv)
	pb := []byte(path.String())
	binary.BigEndian.PutUint32(lp[:], uint32(len(pb))) //nolint:gosec // G115: field path length bounded by parser
	h.Write(lp[:])
	h.Write(pb)
	return h.Sum(nil)
}
