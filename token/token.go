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

// ErrDestroyed is returned by tokenize methods called after Destroy.
var ErrDestroyed = errors.New("token: tokenizer has been destroyed")

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

// Tokenizer produces tokens from RawRecords using a shared secret.
// Call Destroy when finished to wipe the secret and subkey buffers; if you
// forget, a runtime cleanup wipes them once the Tokenizer becomes
// unreachable.
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
	// contain a nil entry. Missing required fields return an error.
	// fs.ProbabilisticParams is validated first and an invalid config returns
	// an error. A value that normalizes to the empty string is treated as
	// absent, so an optional field keeps a nil entry and a required field
	// returns an error.
	//
	// Per-field tokens reveal per-field structure: which fields the record
	// carries and how similar each one is. When per-field scores are not
	// required, prefer TokenizeCLK.
	//
	// As with TokenizeDeterministic, FieldSetFingerprint is left empty on the
	// returned token; the caller (typically session.Session) stamps it.
	TokenizeProbabilistic(record sriracha.RawRecord, fs sriracha.FieldSet) (sriracha.ProbabilisticToken, error)
	// TokenizeCLK tokenizes a RawRecord into a single record-level CLK
	// filter. Every present field contributes its q-grams (with the field
	// path in each gram's preimage) to one shared filter, which then
	// receives the same balanced and permutation treatment as per-field
	// filters. Missing required fields and required fields that normalize to
	// the empty string return an error, as in TokenizeProbabilistic; absent
	// optional fields simply do not contribute. A record where no field
	// contributes returns an error.
	//
	// CLK is the recommended way to share tokens when per-field scores are
	// not required, because per-field tokens reveal per-field structure.
	// FieldSetFingerprint is left empty; the caller stamps it.
	TokenizeCLK(record sriracha.RawRecord, fs sriracha.FieldSet) (sriracha.CLKToken, error)
	// TokenizeField returns the deterministic 32-byte HMAC for a single
	// (value, path) pair, after running the same normalization pipeline
	// TokenizeDeterministic uses. Useful for stable indexing of one field outside
	// the FieldSet flow. Returns an error when the value normalizes to the
	// empty string.
	TokenizeField(value string, path sriracha.FieldPath) ([]byte, error)
	// Destroy wipes the secret and subkey buffers that back this Tokenizer.
	// Pooled HMAC instances created from the subkeys may still hold derived
	// key material (inner/outer pad) on the heap until garbage-collected.
	// Tokenize methods called after Destroy return ErrDestroyed.
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

// tokenizer is the default Tokenizer implementation backed by memguard-locked
// buffers holding the source secret and the three derived subkeys. HMAC
// instances are pooled so concurrent callers do not race on the underlying
// hash state.
type tokenizer struct {
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
// stored in its own locked buffer. Returns an error if secret is empty or
// all zero bytes, if subkey derivation fails, or if allocating a locked
// memory region fails. An all-zero secret usually means the slice was
// already wiped by an earlier New call and reused by mistake.
//
// A runtime finalizer wipes the locked buffers if the returned Tokenizer
// becomes unreachable without an explicit Destroy call.
func New(secret []byte, opts ...Option) (Tokenizer, error) {
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
func newTokenizer(secret []byte, alloc func([]byte) *memguard.LockedBuffer, derive func(secret []byte, info string) ([]byte, error), opts ...Option) (Tokenizer, error) {
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

	t := &tokenizer{
		secret:   lockedSecret,
		detKey:   lockedDet,
		bloomKey: lockedBloom,
		permKey:  lockedPerm,
		keyID:    o.keyID,
	}
	t.detPool.New = func() any { return newHMACSHA256(lockedDet.Bytes()) }
	t.bloomPool.New = func() any { return newHMACSHA256(lockedBloom.Bytes()) }
	runtime.SetFinalizer(t, func(t *tokenizer) { t.wipe() })
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
func (t *tokenizer) wipe() {
	_ = recoverToError(t.secret.Destroy)
	_ = recoverToError(t.detKey.Destroy)
	_ = recoverToError(t.bloomKey.Destroy)
	_ = recoverToError(t.permKey.Destroy)
}

func (t *tokenizer) Destroy() {
	t.destroyed.Store(true)
	t.wipe()
	runtime.SetFinalizer(t, nil)
}

// newHMACSHA256 returns an HMAC-SHA256 instance keyed by key.
func newHMACSHA256(key []byte) hash.Hash {
	return hmac.New(sha256.New, key)
}

// acquireDet returns a pooled HMAC keyed by the deterministic subkey.
func (t *tokenizer) acquireDet() hash.Hash {
	h, _ := t.detPool.Get().(hash.Hash)
	return h
}

func (t *tokenizer) releaseDet(h hash.Hash) {
	h.Reset()
	t.detPool.Put(h)
}

// acquireBloom returns a pooled HMAC keyed by the bloom subkey.
func (t *tokenizer) acquireBloom() hash.Hash {
	h, _ := t.bloomPool.Get().(hash.Hash)
	return h
}

func (t *tokenizer) releaseBloom(h hash.Hash) {
	h.Reset()
	t.bloomPool.Put(h)
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
	h := t.acquireDet()
	defer t.releaseDet(h)

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
		Format:          sriracha.TokenFormatDeterministic,
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
