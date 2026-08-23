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
	"sync/atomic"

	"github.com/awnumar/memguard"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/normalize"
)

// ErrDestroyed is returned by tokenize methods called after Destroy.
var ErrDestroyed = errors.New("token: tokenizer has been destroyed")

// Tokenizer produces tokens from RawRecords using a shared secret.
// Call Destroy when finished to wipe the source secret buffer; if you forget,
// a runtime cleanup wipes it once the Tokenizer becomes unreachable.
//
// Tokenizer is safe for concurrent use by multiple goroutines until Destroy
// is called; HMAC instances are pooled internally. Tokenize methods called
// after Destroy return ErrDestroyed.
//
// Most callers want a session.Session — it bundles a Tokenizer with a
// FieldSet so you don't have to thread the schema through every call.
type Tokenizer interface {
	// TokenizeDeterministic tokenizes a RawRecord in deterministic mode (HMAC-SHA256
	// per field). The returned token's Fields slice is aligned with fs.Fields:
	// each entry is a 32-byte HMAC for a present field, or nil for an absent
	// optional field. Missing required fields return an error. A value that
	// normalizes to the empty string is treated as absent, so an optional
	// field keeps a nil entry and a required field returns an error.
	//
	// The returned token has FieldSetFingerprint left empty — fingerprint
	// management is the caller's responsibility, so a session can cache
	// fs.Fingerprint() once at construction time rather than re-running it on
	// every tokenize call. session.Session.TokenizeDeterministic stamps the
	// cached value automatically.
	TokenizeDeterministic(record sriracha.RawRecord, fs sriracha.FieldSet) (sriracha.DeterministicToken, error)
	// TokenizeProbabilistic tokenizes a RawRecord in probabilistic (Bloom filter)
	// mode. The returned token's Fields slice is aligned with fs.Fields:
	// present fields contain the populated filter, absent optional fields
	// contain an all-zero filter of the same length. Missing required fields
	// return an error. fs.ProbabilisticParams is validated first and an
	// invalid config returns an error. A value that normalizes to the empty
	// string is treated as absent, so an optional field keeps the all-zero
	// filter and a required field returns an error.
	//
	// As with TokenizeDeterministic, FieldSetFingerprint is left empty on the
	// returned token; the caller (typically session.Session) stamps it.
	TokenizeProbabilistic(record sriracha.RawRecord, fs sriracha.FieldSet) (sriracha.ProbabilisticToken, error)
	// TokenizeField returns the deterministic 32-byte HMAC for a single
	// (value, path) pair, after running the same normalization pipeline
	// TokenizeDeterministic uses. Useful for stable indexing of one field outside
	// the FieldSet flow. Returns an error when the value normalizes to the
	// empty string.
	TokenizeField(value string, path sriracha.FieldPath) ([]byte, error)
	// Destroy wipes the secret buffer that backs this Tokenizer. Pooled HMAC
	// instances created from the secret may still hold derived key material
	// (inner/outer pad) on the heap until garbage-collected. Tokenize methods
	// called after Destroy return ErrDestroyed.
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
	secret    *memguard.LockedBuffer
	keyID     string
	pool      sync.Pool
	destroyed atomic.Bool
}

// New creates a Tokenizer with the given HMAC secret.
// The secret is copied into a locked, non-swappable memory region and the
// source slice is wiped. Returns an error if secret is empty or all zero
// bytes, or if allocating the locked memory region fails. An all-zero
// secret usually means the slice was already wiped by an earlier New call
// and reused by mistake.
//
// A runtime finalizer wipes the locked buffer if the returned Tokenizer
// becomes unreachable without an explicit Destroy call.
func New(secret []byte, opts ...Option) (Tokenizer, error) {
	return newTokenizer(secret, memguard.NewBufferFromBytes, opts...)
}

// newTokenizer implements New with the locked buffer allocator injected so
// tests can exercise the allocation failure path.
func newTokenizer(secret []byte, alloc func([]byte) *memguard.LockedBuffer, opts ...Option) (Tokenizer, error) {
	if len(secret) == 0 {
		return nil, errors.New("token: secret must not be empty")
	}
	if isAllZero(secret) {
		return nil, errors.New("token: secret must not be all zero bytes")
	}

	var o tokenizerOpts
	for _, opt := range opts {
		opt(&o)
	}

	var locked *memguard.LockedBuffer
	if err := recoverToError(func() { locked = alloc(secret) }); err != nil {
		return nil, err
	}
	t := &tokenizer{secret: locked, keyID: o.keyID}
	t.pool.New = func() any { return hmac.New(sha256.New, locked.Bytes()) }
	runtime.SetFinalizer(t, func(t *tokenizer) { t.secret.Destroy() })
	return t, nil
}

// isAllZero reports whether every byte of b is zero.
func isAllZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

// recoverToError runs fn and converts a panic inside it into an error.
// memguard panics when locking or allocating protected memory fails.
func recoverToError(fn func()) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("token: locked buffer allocation failed: %v", r)
		}
	}()
	fn()
	return nil
}

func (t *tokenizer) Destroy() {
	t.destroyed.Store(true)
	// Destroy has no error return. The recover wrapper only keeps a
	// memguard panic from escaping.
	_ = recoverToError(t.secret.Destroy)
	runtime.SetFinalizer(t, nil)
}

func (t *tokenizer) acquire() hash.Hash {
	h, _ := t.pool.Get().(hash.Hash)
	return h
}

func (t *tokenizer) release(h hash.Hash) {
	h.Reset()
	t.pool.Put(h)
}

func (t *tokenizer) TokenizeDeterministic(record sriracha.RawRecord, fs sriracha.FieldSet) (sriracha.DeterministicToken, error) {
	if t.destroyed.Load() {
		return sriracha.DeterministicToken{}, ErrDestroyed
	}
	fields := make([][]byte, len(fs.Fields))
	// Lazy-allocated on first present field. Skipping the alloc for
	// all-absent records preserves the existing one-alloc footprint of
	// that case; the per-present-field nil check is negligible.
	var backing []byte
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
		if normalized == "" {
			if spec.Required {
				return sriracha.DeterministicToken{}, fmt.Errorf("token: required field %q is empty", spec.Path)
			}
			continue
		}
		if backing == nil {
			backing = make([]byte, len(fs.Fields)*sha256.Size)
		}
		out := backing[i*sha256.Size : (i+1)*sha256.Size]
		hmacField(h, out, normalized, spec.Path)
		fields[i] = out
	}

	return sriracha.DeterministicToken{
		FieldSetVersion: fs.Version,
		KeyID:           t.keyID,
		Fields:          fields,
	}, nil
}

func (t *tokenizer) TokenizeField(value string, path sriracha.FieldPath) ([]byte, error) {
	if t.destroyed.Load() {
		return nil, ErrDestroyed
	}
	normalized, err := normalize.Normalize(value, path)
	if err != nil {
		return nil, fmt.Errorf("token: normalization failed for field %q: %w", path, err)
	}
	if normalized == "" {
		return nil, fmt.Errorf("token: value for field %q normalizes to empty", path)
	}
	h := t.acquire()
	defer t.release(h)
	out := make([]byte, sha256.Size)
	hmacField(h, out, normalized, path)
	return out, nil
}

// hmacField writes the canonical length-prefixed (value, path) preimage into
// h and writes the digest into out. out must be exactly sha256.Size bytes;
// the caller owns the buffer. Length-prefixing is what prevents
// (value="ab", path="c") from colliding with (value="a", path="bc").
func hmacField(h hash.Hash, out []byte, normalizedValue string, path sriracha.FieldPath) {
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
	h.Sum(out[:0])
}
