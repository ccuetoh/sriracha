package token

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"math"
	"sync"
	"unicode/utf8"

	"github.com/bits-and-blooms/bitset"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/normalize"
)

// ErrNoContributingFields reports a CLK request where no field contributed
// any q-gram, so the filter would carry no information about the record.
var ErrNoContributingFields = errors.New("token: CLK requires at least one contributing field")

// TokenizeProbabilistic tokenizes a RawRecord in probabilistic (Bloom
// filter) mode. The returned token's Fields slice is aligned with fs.Fields:
// present fields contain the populated filter, absent optional fields
// contain a nil entry. fs.ProbabilisticParams is validated first and an
// invalid config returns an error wrapping sriracha.ErrInvalidConfig. A
// missing required field returns an error wrapping
// sriracha.ErrRequiredFieldMissing. A value that normalizes to the empty
// string is treated as absent, so an optional field keeps a nil entry and a
// required field returns an error wrapping sriracha.ErrEmptyValue.
//
// Per-field tokens reveal per-field structure: which fields the record
// carries and how similar each one is. When per-field scores are not
// required, prefer TokenizeCLK.
//
// As with TokenizeDeterministic, FieldSetFingerprint is left empty on the
// returned token; the caller (typically session.Session) stamps it.
func (t *Tokenizer) TokenizeProbabilistic(record sriracha.RawRecord, fs sriracha.FieldSet) (sriracha.ProbabilisticToken, error) {
	if t.destroyed.Load() {
		return sriracha.ProbabilisticToken{}, ErrDestroyed
	}
	cfg := fs.ProbabilisticParams
	if err := validateBloomConfig(cfg); err != nil {
		return sriracha.ProbabilisticToken{}, err
	}
	fieldBytes := filterBytes(cfg.SizeBits)
	fields := make([][]byte, len(fs.Fields))
	// One contiguous backing slab per token, lazily allocated on the first
	// present field; each present field's bytes is a sub-slice. Absent
	// optional fields keep a nil entry and consume no bytes.
	var backing []byte

	// Reuse a single pooled HMAC across every field/gram in this record.
	h := t.acquireBloom()
	defer t.releaseBloom(h)

	for i, spec := range fs.Fields {
		raw, ok := record[spec.Path]
		if !ok {
			if spec.Required {
				return sriracha.ProbabilisticToken{}, fieldErr(spec.Path, sriracha.ErrRequiredFieldMissing)
			}
			continue
		}

		normalized, err := normalize.Normalize(raw, spec.Path)
		if err != nil {
			return sriracha.ProbabilisticToken{}, fieldErr(spec.Path, err)
		}
		if normalized == "" {
			if spec.Required {
				return sriracha.ProbabilisticToken{}, fieldErr(spec.Path, sriracha.ErrEmptyValue)
			}
			// The value is treated as absent and keeps a nil entry.
			continue
		}
		if backing == nil {
			backing = make([]byte, len(fs.Fields)*fieldBytes)
		}
		out := backing[i*fieldBytes : (i+1)*fieldBytes]
		t.tokenizeFieldBloom(h, out, normalized, spec.Path, cfg)
		fields[i] = out
	}

	return sriracha.ProbabilisticToken{
		Format:              sriracha.TokenFormatProbabilistic,
		FieldSetVersion:     fs.Version,
		KeyID:               t.keyID,
		ProbabilisticParams: cfg,
		Fields:              fields,
	}, nil
}

// TokenizeCLK tokenizes a RawRecord into a single record-level CLK filter.
// Every present field contributes its q-grams (with the field path in each
// gram's preimage) to one shared filter, so the same gram from two different
// fields lands at different positions. The filter then receives the same
// balanced and permutation treatment as per-field filters. CLK filters are
// always balanced regardless of cfg.Balanced, so the emitted popcount is
// exactly SizeBits/2 and reveals nothing about the record, and SizeBits must
// be even.
//
// Missing required fields and required fields that normalize to the empty
// string return an error, as in TokenizeProbabilistic; absent optional
// fields simply do not contribute. A record where no field contributes
// returns ErrNoContributingFields, because an empty CLK would otherwise be
// indistinguishable from a real filter after balancing.
//
// CLK is the recommended way to share tokens when per-field scores are not
// required, because per-field tokens reveal per-field structure.
// FieldSetFingerprint is left empty; the caller stamps it.
func (t *Tokenizer) TokenizeCLK(record sriracha.RawRecord, fs sriracha.FieldSet) (sriracha.CLKToken, error) {
	if t.destroyed.Load() {
		return sriracha.CLKToken{}, ErrDestroyed
	}
	cfg := fs.ProbabilisticParams
	if err := validateBloomConfig(cfg); err != nil {
		return sriracha.CLKToken{}, err
	}
	if cfg.SizeBits%2 != 0 {
		return sriracha.CLKToken{}, fmt.Errorf("token: %w: SizeBits must be even for CLK, got %d", sriracha.ErrInvalidConfig, cfg.SizeBits)
	}
	base := cfg.SizeBits / 2
	bs := acquireBitset(base)
	defer releaseBitset(base, bs)
	h := t.acquireBloom()
	defer t.releaseBloom(h)

	contributing := 0
	for _, spec := range fs.Fields {
		raw, ok := record[spec.Path]
		if !ok {
			if spec.Required {
				return sriracha.CLKToken{}, fieldErr(spec.Path, sriracha.ErrRequiredFieldMissing)
			}
			continue
		}
		normalized, err := normalize.Normalize(raw, spec.Path)
		if err != nil {
			return sriracha.CLKToken{}, fieldErr(spec.Path, err)
		}
		if normalized == "" {
			if spec.Required {
				return sriracha.CLKToken{}, fieldErr(spec.Path, sriracha.ErrEmptyValue)
			}
			continue
		}
		setGramBits(h, bs, normalized, spec.Path, cfg, base)
		contributing++
	}
	if contributing == 0 {
		return sriracha.CLKToken{}, ErrNoContributingFields
	}

	out := make([]byte, filterBytes(cfg.SizeBits))
	t.balanceInto(out, bs, cfg.SizeBits)
	return sriracha.CLKToken{
		Format:              sriracha.TokenFormatCLK,
		FieldSetVersion:     fs.Version,
		KeyID:               t.keyID,
		ProbabilisticParams: cfg,
		Filter:              out,
	}, nil
}

// filterBytes returns the serialized byte length of a filter with sizeBits
// bits (whole little-endian uint64 words).
func filterBytes(sizeBits uint32) int {
	return int(((sizeBits + 63) / 64) * 8)
}

// validateBloomConfig rejects ProbabilisticConfig values that would divide
// by zero at position selection or allocate a degenerate filter. FieldSets
// built through FieldSet.Validate are already checked; this guards direct
// Tokenizer callers. The rules live on the config itself, so this only adds
// the package prefix; every error wraps sriracha.ErrInvalidConfig.
func validateBloomConfig(cfg sriracha.ProbabilisticConfig) error {
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("token: %w", err)
	}
	return nil
}

// tokenizeFieldBloom writes the serialized filter bytes for a single
// normalized field value into out, using h (which is reset between hashes).
// out must be exactly filterBytes(cfg.SizeBits) bytes — the caller owns it
// and slices into the per-token backing buffer.
//
// The base Bloom filter uses cfg.SizeBits bits, or cfg.SizeBits/2 when
// cfg.Balanced. Balanced filters then append the complement of the base
// filter and apply the tokenizer's secret permutation, so the emitted
// popcount is exactly cfg.SizeBits/2 for every present field.
func (t *Tokenizer) tokenizeFieldBloom(h hash.Hash, out []byte, normalizedValue string, path sriracha.FieldPath, cfg sriracha.ProbabilisticConfig) {
	base := cfg.SizeBits
	if cfg.Balanced {
		base = cfg.SizeBits / 2
	}
	bs := acquireBitset(base)
	defer releaseBitset(base, bs)
	setGramBits(h, bs, normalizedValue, path, cfg, base)
	if cfg.Balanced {
		t.balanceInto(out, bs, cfg.SizeBits)
		return
	}
	bitsetToBytes(bs, out)
}

// setGramBits sets the bit positions for every q-gram of normalizedValue
// into bs, a base filter of baseBits bits. One HMAC runs per gram; the
// message is len(gram)||gram||len(path)||path with 4-byte big-endian length
// prefixes so distinct (gram, path) pairs cannot collide via concatenation.
// HashCount positions are derived from the single digest by double hashing:
// h1 is the big-endian uint64 of digest bytes 0..8, h2 is the big-endian
// uint64 of bytes 8..16 with the low bit forced to 1 (so h2 is odd and
// strides visit distinct positions on power-of-two sizes), and position i is
// (h1 + i*h2) mod baseBits under wrapping uint64 arithmetic.
func setGramBits(h hash.Hash, bs *bitset.BitSet, normalizedValue string, path sriracha.FieldPath, cfg sriracha.ProbabilisticConfig, baseBits uint32) {
	pathBytes := []byte(path.String())
	eachNgram(normalizedValue, cfg.NgramSizes, func(gb []byte) {
		h1, h2 := gramDoubleHash(h, gb, pathBytes)
		for i := range cfg.HashCount {
			pos := (h1 + uint64(i)*h2) % uint64(baseBits) //nolint:gosec // i bounded by cfg.HashCount
			bs.Set(uint(pos))
		}
	})
}

// gramDoubleHash runs one HMAC over the length-prefixed (gram, path) message
// and returns the double-hashing pair (h1, h2) described in setGramBits.
func gramDoubleHash(h hash.Hash, gram, pathBytes []byte) (h1, h2 uint64) {
	h.Reset()
	var lp [4]byte
	binary.BigEndian.PutUint32(lp[:], uint32(len(gram))) //nolint:gosec // G115: gram length bounded by ngram size
	h.Write(lp[:])
	h.Write(gram)
	binary.BigEndian.PutUint32(lp[:], uint32(len(pathBytes))) //nolint:gosec // G115: field path length bounded by parser
	h.Write(lp[:])
	h.Write(pathBytes)
	var sumBuf [32]byte
	sum := h.Sum(sumBuf[:0])
	h1 = binary.BigEndian.Uint64(sum[:8])
	h2 = binary.BigEndian.Uint64(sum[8:16]) | 1
	return h1, h2
}

// balanceInto emits the balanced form of the base filter bs into out. The
// extended filter E doubles bs with its complement (E[j] = B[j],
// E[half+j] = NOT B[j]), then each extended bit j lands at position perm[j]
// of the output, where perm is the tokenizer's secret permutation for
// sizeBits. Exactly one of each (j, half+j) pair is set, so the emitted
// popcount is exactly sizeBits/2 regardless of the value.
func (t *Tokenizer) balanceInto(out []byte, bs *bitset.BitSet, sizeBits uint32) {
	perm := t.permutation(sizeBits)
	half := sizeBits / 2
	ext := acquireBitset(sizeBits)
	defer releaseBitset(sizeBits, ext)
	for j := uint32(0); j < half; j++ {
		if bs.Test(uint(j)) {
			ext.Set(uint(perm[j]))
		} else {
			ext.Set(uint(perm[half+j]))
		}
	}
	bitsetToBytes(ext, out)
}

// permutation returns the tokenizer's bijection on [0, sizeBits), generating
// and caching it on first use for each sizeBits. The permutation is a
// Fisher-Yates shuffle driven by an HMAC counter stream keyed by the
// permutation subkey, with message BE32(sizeBits) || BE32(counter), so it is
// deterministic per (secret, sizeBits) and unbiased via rejection sampling.
// Concurrent first calls may both generate; the shuffle is deterministic so
// either result is identical and LoadOrStore keeps one.
func (t *Tokenizer) permutation(sizeBits uint32) []uint32 {
	if cached, ok := t.perms.Load(sizeBits); ok {
		return cached.([]uint32) //nolint:forcetypeassert // only []uint32 is ever stored
	}
	perm := make([]uint32, sizeBits)
	for i := range perm {
		perm[i] = uint32(i) //nolint:gosec // G115: i bounded by sizeBits
	}
	s := newPermStream(t.permKey.Bytes(), sizeBits)
	for i := int(sizeBits) - 1; i >= 1; i-- {
		j := uniformIndex(s.next64, uint64(i)+1)
		perm[i], perm[j] = perm[j], perm[i]
	}
	stored, _ := t.perms.LoadOrStore(sizeBits, perm)
	return stored.([]uint32) //nolint:forcetypeassert // only []uint32 is ever stored
}

// uniformIndex draws an unbiased uniform value in [0, bound) from a stream
// of uniform uint64 samples via rejection sampling: samples in the partial
// final multiple of bound are discarded so every residue is equally likely.
func uniformIndex(next func() uint64, bound uint64) uint64 {
	// rem is 2^64 mod bound; when it is zero, bound divides 2^64 and every
	// sample is accepted.
	rem := (math.MaxUint64%bound + 1) % bound
	for {
		v := next()
		if rem == 0 || v <= math.MaxUint64-rem {
			return v % bound
		}
	}
}

// permStream is a deterministic uint64 stream produced by HMAC-counter mode
// under the permutation subkey. Each block hashes
// BE32(sizeBits) || BE32(counter) and yields four big-endian uint64 samples.
type permStream struct {
	h    hash.Hash
	size [4]byte
	ctr  uint32
	buf  [32]byte
	pos  int
}

func newPermStream(key []byte, sizeBits uint32) *permStream {
	s := &permStream{h: newHMACSHA256(key)}
	binary.BigEndian.PutUint32(s.size[:], sizeBits)
	s.pos = len(s.buf)
	return s
}

func (s *permStream) next64() uint64 {
	if s.pos+8 > len(s.buf) {
		s.refill()
	}
	v := binary.BigEndian.Uint64(s.buf[s.pos:])
	s.pos += 8
	return v
}

func (s *permStream) refill() {
	s.h.Reset()
	s.h.Write(s.size[:])
	var ctrBuf [4]byte
	binary.BigEndian.PutUint32(ctrBuf[:], s.ctr)
	s.h.Write(ctrBuf[:])
	// Sum into the stream's own buffer; no per-refill allocation.
	s.h.Sum(s.buf[:0])
	s.pos = 0
	s.ctr++
}

// bitsetToBytes serialises a BitSet as little-endian uint64 words into out.
// out must be exactly len(bs.Words())*8 bytes; the caller owns the buffer.
func bitsetToBytes(bs *bitset.BitSet, out []byte) {
	words := bs.Words()
	for i, w := range words {
		binary.LittleEndian.PutUint64(out[i*8:], w)
	}
}

// bitsetPools keys a sync.Pool by bit size so reuse only matches bitsets
// of the same word count. The typical workload is one Tokenizer with one
// FieldSet (a single SizeBits), so at most two pools end up hot: the base
// size and, when Balanced, the extended size.
var bitsetPools sync.Map // map[uint32]*sync.Pool

// acquireBitset returns a zeroed BitSet sized for sizeBits, drawn from a
// per-size pool. The caller must hand it back via releaseBitset.
// ClearAll runs on acquire (not release) so the caller always sees a
// zeroed filter regardless of whether the pool returned a freshly-
// allocated bitset or a reused one whose words were left dirty by the
// previous user.
func acquireBitset(sizeBits uint32) *bitset.BitSet {
	// Fast path: most calls land here once the pool exists. Going through
	// LoadOrStore on every call would force the &sync.Pool{...} literal
	// (and its closure) to be allocated on the heap before LoadOrStore
	// can decide to discard it; the Load gate skips that.
	p, ok := bitsetPools.Load(sizeBits)
	if !ok {
		p, _ = bitsetPools.LoadOrStore(sizeBits, &sync.Pool{
			New: func() any { return bitset.New(uint(sizeBits)) },
		})
	}
	bs, _ := p.(*sync.Pool).Get().(*bitset.BitSet)
	bs.ClearAll()
	return bs
}

// releaseBitset returns bs to the pool for the given sizeBits. The bitset's
// dirty state is wiped lazily on the next acquireBitset. Must be paired
// with an acquireBitset call for the same sizeBits; mismatched usage
// panics on the unchecked type assertion below, which is the intended
// failure mode for an internal helper.
func releaseBitset(sizeBits uint32, bs *bitset.BitSet) {
	p, _ := bitsetPools.Load(sizeBits)
	p.(*sync.Pool).Put(bs) //nolint:forcetypeassert // acquireBitset always stores a *sync.Pool first
}

// eachNgram invokes fn for each n-gram (across all sizes in sizes) extracted
// from s, decoded as runes (not bytes) so multi-byte UTF-8 input produces
// correct gram boundaries. For each gram size q the value is padded with
// q-1 underscore runes on each side before extraction, so boundary
// characters appear in as many grams as interior characters and even a
// one-rune value produces grams for every size. The byte slice passed to fn
// aliases an internal scratch buffer; fn must not retain it past the call.
// Iteration order is: for each size in sizes (in order), all grams of that
// size left-to-right over the padded value. No-ops when s is empty or sizes
// is empty.
func eachNgram(s string, sizes []int, fn func(gram []byte)) {
	runes := []rune(s)
	n := len(runes)
	if n == 0 || len(sizes) == 0 {
		return
	}
	maxSize := sizes[0]
	for _, sz := range sizes[1:] {
		if sz > maxSize {
			maxSize = sz
		}
	}
	// One padded buffer covers every size: maxSize-1 underscores on each
	// side, with smaller sizes reading a centered sub-slice.
	maxPad := maxSize - 1
	padded := make([]rune, 0, n+2*maxPad)
	for range maxPad {
		padded = append(padded, '_')
	}
	padded = append(padded, runes...)
	for range maxPad {
		padded = append(padded, '_')
	}
	// Scratch is sized to the worst case (largest gram, all 4-byte runes).
	scratch := make([]byte, 0, maxSize*utf8.UTFMax)
	for _, sz := range sizes {
		pad := sz - 1
		window := padded[maxPad-pad : maxPad+n+pad]
		for i := 0; i+sz <= len(window); i++ {
			scratch = scratch[:0]
			for _, r := range window[i : i+sz] {
				scratch = utf8.AppendRune(scratch, r)
			}
			fn(scratch)
		}
	}
}

// ngrams collects the output of eachNgram into a string slice. Used by
// tests and any caller that needs random access; the per-field hot path
// uses eachNgram directly to avoid the gram-string allocation.
func ngrams(s string, sizes []int) []string {
	var result []string
	eachNgram(s, sizes, func(g []byte) {
		result = append(result, string(g))
	})
	return result
}
