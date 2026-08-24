// Package token implements deterministic (HMAC-SHA256) and probabilistic
// (Bloom filter) tokenization plus the comparison primitives Equal,
// DicePerField, Score, Match, and MatchCLK.
//
// Most callers want Match — it wraps DicePerField + Score and returns the
// thresholded decision in one call. When per-field scores are not required,
// TokenizeCLK plus MatchCLK is the recommended way to share tokens: a CLK
// folds the whole record into one filter and reveals no per-field structure.
// Even simpler: package session bundles a Tokenizer with a FieldSet so you
// don't have to thread the schema through every call.
package token

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"io"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/awnumar/memguard"
	"golang.org/x/crypto/hkdf"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/normalize"
)

// MinSecretLen is the shortest secret New accepts, in bytes. HKDF-SHA256
// extracts a 32-byte pseudorandom key from the secret, so 32 bytes is the
// point past which a longer secret stops adding strength and below which the
// secret is the weakest part of the derivation.
const MinSecretLen = 32

// Sentinel errors returned by the tokenizer. Errors that name a field wrap
// a sriracha.FieldError around a root sentinel instead.
var (
	// ErrDestroyed is returned by tokenize methods called after Destroy.
	ErrDestroyed = errors.New("token: tokenizer has been destroyed")

	// ErrSecretTooShort reports a secret shorter than MinSecretLen, the
	// empty secret included.
	ErrSecretTooShort = fmt.Errorf("token: secret must be at least %d bytes", MinSecretLen)

	// ErrSecretAllZero reports a secret whose bytes are all zero. That
	// usually means the slice was wiped by an earlier New call and reused
	// by mistake, since New wipes the secret it is handed.
	ErrSecretAllZero = errors.New("token: secret must not be all zero bytes")
)

// fieldErr tags err with the field that produced it and the package prefix.
// The sriracha.FieldError leaf keeps the underlying sentinel reachable
// through errors.Is and the path through errors.As.
func fieldErr(path sriracha.FieldPath, err error) error {
	return fmt.Errorf("token: %w", sriracha.FieldError{Path: path, Err: err})
}

// HKDF info strings for the three subkeys derived from the caller's secret.
// Each mode reads only its own subkey, so learning one subkey does not
// expose the others or the source secret.
const (
	infoDeterministic = "sriracha/v2/deterministic"
	infoBloom         = "sriracha/v2/bloom"
	infoPermutation   = "sriracha/v2/permutation"
)

// subkeySize is the byte length of each derived subkey.
const subkeySize = 32

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

// Tokenizer produces tokens from RawRecords using a shared secret. It is
// backed by memguard-locked buffers holding the source secret and the three
// derived subkeys, with pooled HMAC instances so concurrent callers do not
// race on the underlying hash state. Construct one with New.
//
// Call Destroy when finished to wipe the secret and subkey buffers; if you
// forget, a runtime cleanup wipes them once the Tokenizer becomes
// unreachable.
//
// Tokenizer is safe for concurrent use by multiple goroutines until Destroy
// is called. Tokenize methods called after Destroy return ErrDestroyed.
//
// Most callers want a session.Session, which bundles a Tokenizer with a
// FieldSet so you don't have to thread the schema through every call.
type Tokenizer struct {
	secret    *memguard.LockedBuffer
	detKey    *memguard.LockedBuffer
	bloomKey  *memguard.LockedBuffer
	permKey   *memguard.LockedBuffer
	keyID     string
	detPool   sync.Pool
	bloomPool sync.Pool
	perms     sync.Map
	destroyed atomic.Bool
}

// New creates a Tokenizer with the given secret.
// Three 32-byte subkeys (deterministic, bloom, permutation) are derived from
// the secret with HKDF-SHA256 before the secret is copied into a locked,
// non-swappable memory region and the source slice is wiped. Each subkey is
// stored in its own locked buffer.
//
// The secret is the entire privacy barrier. Tokens are unforgeable and
// unlinkable only to a party that does not hold it; anyone who does can
// tokenize a guessed record and confirm the match. HKDF-SHA256 extracts a
// 32-byte pseudorandom key from the secret, so New requires at least
// MinSecretLen bytes and returns ErrSecretTooShort otherwise. Source those
// bytes from crypto/rand, a KMS, or an environment variable holding
// generated key material. Never use a passphrase literal: a memorable
// string has far less entropy than its length suggests, and the field
// values being tokenized come from a small, guessable space.
//
// Returns ErrSecretTooShort if the secret is shorter than MinSecretLen,
// ErrSecretAllZero if every byte is zero, or an error if subkey derivation
// or locked memory allocation fails.
//
// A runtime finalizer wipes the locked buffers if the returned Tokenizer
// becomes unreachable without an explicit Destroy call.
func New(secret []byte, opts ...Option) (*Tokenizer, error) {
	return newTokenizer(secret, memguard.NewBufferFromBytes, hkdfSubkey, opts...)
}

// hkdfSubkey derives one 32-byte subkey from secret with HKDF-SHA256, a nil
// salt, and the given info string.
func hkdfSubkey(secret []byte, info string) ([]byte, error) {
	return hkdfDerive(secret, info, subkeySize)
}

// hkdfDerive expands size bytes of key material from secret under info.
// HKDF-SHA256 caps expansion at 255 blocks of 32 bytes; requests beyond the
// cap return an error.
func hkdfDerive(secret []byte, info string, size int) ([]byte, error) {
	key := make([]byte, size)
	if _, err := io.ReadFull(hkdf.New(sha256.New, secret, nil, []byte(info)), key); err != nil {
		return nil, fmt.Errorf("token: subkey derivation failed for %q: %w", info, err)
	}
	return key, nil
}

// newTokenizer implements New with the locked buffer allocator and the
// subkey derivation function injected so tests can exercise the failure
// paths.
func newTokenizer(secret []byte, alloc func([]byte) *memguard.LockedBuffer, derive func(secret []byte, info string) ([]byte, error), opts ...Option) (*Tokenizer, error) {
	if len(secret) < MinSecretLen {
		return nil, fmt.Errorf("%w, got %d", ErrSecretTooShort, len(secret))
	}
	if isAllZero(secret) {
		return nil, ErrSecretAllZero
	}

	var o tokenizerOpts
	for _, opt := range opts {
		opt(&o)
	}

	// Subkeys must be derived before alloc moves the secret into locked
	// memory, because the allocator wipes its source slice.
	detKey, err := derive(secret, infoDeterministic)
	if err != nil {
		return nil, err
	}
	bloomKey, err := derive(secret, infoBloom)
	if err != nil {
		return nil, err
	}
	permKey, err := derive(secret, infoPermutation)
	if err != nil {
		return nil, err
	}

	var allocated []*memguard.LockedBuffer
	lock := func(b []byte) (*memguard.LockedBuffer, error) {
		var locked *memguard.LockedBuffer
		if err := recoverToError(func() { locked = alloc(b) }); err != nil {
			for _, prev := range allocated {
				_ = recoverToError(prev.Destroy)
			}
			return nil, err
		}
		allocated = append(allocated, locked)
		return locked, nil
	}

	lockedSecret, err := lock(secret)
	if err != nil {
		return nil, err
	}
	lockedDet, err := lock(detKey)
	if err != nil {
		return nil, err
	}
	lockedBloom, err := lock(bloomKey)
	if err != nil {
		return nil, err
	}
	lockedPerm, err := lock(permKey)
	if err != nil {
		return nil, err
	}

	t := &Tokenizer{
		secret:   lockedSecret,
		detKey:   lockedDet,
		bloomKey: lockedBloom,
		permKey:  lockedPerm,
		keyID:    o.keyID,
	}
	t.detPool.New = func() any { return newHMACSHA256(lockedDet.Bytes()) }
	t.bloomPool.New = func() any { return newHMACSHA256(lockedBloom.Bytes()) }
	runtime.SetFinalizer(t, func(t *Tokenizer) { t.wipe() })
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

// wipe destroys the secret and subkey buffers. Destroy has no error return,
// so the recover wrappers only keep a memguard panic from escaping.
func (t *Tokenizer) wipe() {
	_ = recoverToError(t.secret.Destroy)
	_ = recoverToError(t.detKey.Destroy)
	_ = recoverToError(t.bloomKey.Destroy)
	_ = recoverToError(t.permKey.Destroy)
}

// Destroy wipes the secret and subkey buffers that back this Tokenizer.
// Pooled HMAC instances created from the subkeys may still hold derived key
// material (inner/outer pad) on the heap until garbage-collected. Tokenize
// methods called after Destroy return ErrDestroyed.
func (t *Tokenizer) Destroy() {
	t.destroyed.Store(true)
	t.wipe()
	runtime.SetFinalizer(t, nil)
}

// newHMACSHA256 returns an HMAC-SHA256 instance keyed by key.
func newHMACSHA256(key []byte) hash.Hash {
	return hmac.New(sha256.New, key)
}

// acquireDet returns a pooled HMAC keyed by the deterministic subkey.
func (t *Tokenizer) acquireDet() hash.Hash {
	h, _ := t.detPool.Get().(hash.Hash)
	return h
}

func (t *Tokenizer) releaseDet(h hash.Hash) {
	h.Reset()
	t.detPool.Put(h)
}

// acquireBloom returns a pooled HMAC keyed by the bloom subkey.
func (t *Tokenizer) acquireBloom() hash.Hash {
	h, _ := t.bloomPool.Get().(hash.Hash)
	return h
}

func (t *Tokenizer) releaseBloom(h hash.Hash) {
	h.Reset()
	t.bloomPool.Put(h)
}

// TokenizeDeterministic tokenizes a RawRecord in deterministic mode
// (HMAC-SHA256 per field). The returned token's Fields slice is aligned with
// fs.Fields: each entry is a 32-byte HMAC for a present field, or nil for an
// absent optional field. A missing required field returns an error wrapping
// sriracha.ErrRequiredFieldMissing. A value that normalizes to the empty
// string is treated as absent, so an optional field keeps a nil entry and a
// required field returns an error wrapping sriracha.ErrEmptyValue.
//
// The returned token has FieldSetFingerprint left empty. Fingerprint
// management is the caller's responsibility, so a session can cache
// fs.Fingerprint() once at construction time rather than re-running it on
// every tokenize call. session.Session.TokenizeDeterministic stamps the
// cached value automatically.
func (t *Tokenizer) TokenizeDeterministic(record sriracha.RawRecord, fs sriracha.FieldSet) (sriracha.DeterministicToken, error) {
	if t.destroyed.Load() {
		return sriracha.DeterministicToken{}, ErrDestroyed
	}
	fields := make([][]byte, len(fs.Fields))
	// Lazy-allocated on first present field. Skipping the alloc for
	// all-absent records preserves the existing one-alloc footprint of
	// that case; the per-present-field nil check is negligible.
	var backing []byte
	h := t.acquireDet()
	defer t.releaseDet(h)

	for i, spec := range fs.Fields {
		raw, ok := record[spec.Path]
		if !ok {
			if spec.Required {
				return sriracha.DeterministicToken{}, fieldErr(spec.Path, sriracha.ErrRequiredFieldMissing)
			}
			continue
		}
		normalized, err := normalize.Normalize(raw, spec.Path)
		if err != nil {
			return sriracha.DeterministicToken{}, fieldErr(spec.Path, err)
		}
		if normalized == "" {
			if spec.Required {
				return sriracha.DeterministicToken{}, fieldErr(spec.Path, sriracha.ErrEmptyValue)
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
		Format:          sriracha.TokenFormatDeterministic,
		FieldSetVersion: fs.Version,
		KeyID:           t.keyID,
		Fields:          fields,
	}, nil
}

// TokenizeField returns the deterministic 32-byte HMAC for a single
// (value, path) pair, after running the same normalization pipeline
// TokenizeDeterministic uses. Useful for stable indexing of one field
// outside the FieldSet flow. A value that normalizes to the empty string
// returns an error wrapping sriracha.ErrEmptyValue.
func (t *Tokenizer) TokenizeField(value string, path sriracha.FieldPath) ([]byte, error) {
	if t.destroyed.Load() {
		return nil, ErrDestroyed
	}
	normalized, err := normalize.Normalize(value, path)
	if err != nil {
		return nil, fieldErr(path, err)
	}
	if normalized == "" {
		return nil, fieldErr(path, sriracha.ErrEmptyValue)
	}
	h := t.acquireDet()
	defer t.releaseDet(h)
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
