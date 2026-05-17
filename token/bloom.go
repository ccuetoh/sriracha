package token

import (
	"encoding/binary"
	"fmt"
	"hash"
	"sync"
	"unicode/utf8"

	"github.com/bits-and-blooms/bitset"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/normalize"
)

func (t *tokenizer) TokenizeProbabilistic(record sriracha.RawRecord, fs sriracha.FieldSet) (sriracha.ProbabilisticToken, error) {
	cfg := fs.ProbabilisticParams
	fieldBytes := int(((cfg.SizeBits + 63) / 64) * 8)
	fields := make([][]byte, len(fs.Fields))
	// One contiguous backing slab per token; each field's bytes is a
	// sub-slice. Drops the per-field allocation that the previous
	// implementation paid for both absent and present fields.
	backing := make([]byte, len(fs.Fields)*fieldBytes)

	// Reuse a single pooled HMAC across every field/gram in this record.
	h := t.acquire()
	defer t.release(h)

	for i, spec := range fs.Fields {
		out := backing[i*fieldBytes : (i+1)*fieldBytes]
		raw, ok := record[spec.Path]
		if !ok {
			if spec.Required {
				return sriracha.ProbabilisticToken{}, fmt.Errorf("token: required field %q missing", spec.Path)
			}
			// out is zero-initialised by make; absent optional fields
			// match the previous all-zero-filter convention.
			fields[i] = out
			continue
		}

		normalized, err := normalize.Normalize(raw, spec.Path)
		if err != nil {
			return sriracha.ProbabilisticToken{}, fmt.Errorf("token: normalization failed for field %q: %w", spec.Path, err)
		}
		tokenizeFieldBloom(h, out, normalized, spec.Path, cfg)
		fields[i] = out
	}

	return sriracha.ProbabilisticToken{
		FieldSetVersion:     fs.Version,
		KeyID:               t.keyID,
		ProbabilisticParams: cfg,
		Fields:              fields,
	}, nil
}

// tokenizeFieldBloom writes the serialized Bloom filter bytes for a single
// normalized field value into out, using h (which is reset between hashes).
// out must be exactly ((cfg.SizeBits + 63) / 64) * 8 bytes — the caller
// owns it and slices into the per-token backing buffer.
//
// Bit positions are derived from HMAC-SHA256 over (gram, path, counter), each
// component written separately and length-prefixed so distinct (gram, path)
// or counter values cannot collide via concatenation. Without separators,
// (gram="ab", path="c.d") and (gram="a", path="bc.d") would hash the same input.
//
// If cfg.FlipProbability > 0, each bit of the constructed filter is flipped
// with that probability (BLIP) using a deterministic HMAC-derived stream
// keyed by ("blip:", path, normalizedValue). If cfg.TargetPopcount > 0 and
// the post-BLIP popcount is below the target, additional zero bits are set
// (chosen via a separate "balance:" stream) until the popcount reaches the
// target. Both transforms are deterministic — identical inputs produce
// identical filters.
func tokenizeFieldBloom(h hash.Hash, out []byte, normalizedValue string, path sriracha.FieldPath, cfg sriracha.ProbabilisticConfig) {
	bs := acquireBitset(cfg.SizeBits)
	defer releaseBitset(cfg.SizeBits, bs)
	pathBytes := []byte(path.String())

	var lp [4]byte
	var ib [4]byte
	var sumBuf [32]byte
	eachNgram(normalizedValue, cfg.NgramSizes, func(gb []byte) {
		for i := range cfg.HashCount {
			h.Reset()
			binary.BigEndian.PutUint32(lp[:], uint32(len(gb))) //nolint:gosec // G115: gram length bounded by ngram size
			h.Write(lp[:])
			h.Write(gb)
			binary.BigEndian.PutUint32(lp[:], uint32(len(pathBytes))) //nolint:gosec // G115: field path length bounded by parser
			h.Write(lp[:])
			h.Write(pathBytes)
			binary.BigEndian.PutUint32(ib[:], uint32(i)) //nolint:gosec // i bounded by cfg.HashCount
			h.Write(ib[:])
			sum := h.Sum(sumBuf[:0])
			pos := binary.BigEndian.Uint64(sum[:8]) % uint64(cfg.SizeBits)
			bs.Set(uint(pos))
		}
	})

	if cfg.FlipProbability > 0 {
		applyBLIP(h, bs, cfg.SizeBits, cfg.FlipProbability, path, normalizedValue)
	}
	if cfg.TargetPopcount > 0 {
		applyBalance(h, bs, cfg.SizeBits, cfg.TargetPopcount, path, normalizedValue)
	}

	bitsetToBytes(bs, out)
}

// applyBLIP flips each of the first sizeBits bits of bs with probability p,
// using a deterministic HMAC-keyed stream so the same (secret, path,
// normalizedValue) always produces the same flip pattern.
func applyBLIP(h hash.Hash, bs *bitset.BitSet, sizeBits uint32, p float64, path sriracha.FieldPath, normalizedValue string) {
	threshold := uint64(p * float64(uint64(1)<<53))
	s := newHMACStream(h, "blip:", path, normalizedValue)
	for i := uint32(0); i < sizeBits; i++ {
		if s.uint64()>>11 < threshold {
			bs.Flip(uint(i))
		}
	}
}

// applyBalance pads bs with additional set bits until its popcount reaches
// target, drawing bit indices from a deterministic HMAC-keyed stream. If the
// current popcount is already at or above target, bs is left unchanged.
func applyBalance(h hash.Hash, bs *bitset.BitSet, sizeBits, target uint32, path sriracha.FieldPath, normalizedValue string) {
	pop := uint32(bs.Count()) //nolint:gosec // G115: bs has at most cfg.SizeBits set bits, which fits in uint32
	if pop >= target {
		return
	}
	needed := target - pop
	s := newHMACStream(h, "balance:", path, normalizedValue)
	for needed > 0 {
		idx := uint(s.uint64() % uint64(sizeBits))
		if !bs.Test(idx) {
			bs.Set(idx)
			needed--
		}
	}
}

// hmacStream is a deterministic byte stream produced by HMAC-counter mode
// over a fixed label. It provides 64-bit samples used for BLIP flipping and
// balanced-filter index selection. The stream is reproducible from
// (secret, label, path, normalizedValue) alone.
type hmacStream struct {
	h     hash.Hash
	label []byte
	buf   [32]byte
	pos   int
	ctr   uint32
}

func newHMACStream(h hash.Hash, prefix string, path sriracha.FieldPath, normalizedValue string) *hmacStream {
	pathBytes := []byte(path.String())
	nv := []byte(normalizedValue)
	label := make([]byte, 0, len(prefix)+8+len(pathBytes)+len(nv))
	var lp [4]byte
	label = append(label, prefix...)
	binary.BigEndian.PutUint32(lp[:], uint32(len(pathBytes))) //nolint:gosec // G115: path length bounded by parser
	label = append(label, lp[:]...)
	label = append(label, pathBytes...)
	binary.BigEndian.PutUint32(lp[:], uint32(len(nv))) //nolint:gosec // G115: value length bounded by input
	label = append(label, lp[:]...)
	label = append(label, nv...)
	s := &hmacStream{h: h, label: label}
	s.pos = len(s.buf)
	return s
}

func (s *hmacStream) refill() {
	s.h.Reset()
	s.h.Write(s.label)
	var ctrBuf [4]byte
	binary.BigEndian.PutUint32(ctrBuf[:], s.ctr)
	s.h.Write(ctrBuf[:])
	// Sum into the stream's own buffer; no per-refill allocation.
	s.h.Sum(s.buf[:0])
	s.pos = 0
	s.ctr++
}

func (s *hmacStream) uint64() uint64 {
	if s.pos+8 > len(s.buf) {
		s.refill()
	}
	v := binary.BigEndian.Uint64(s.buf[s.pos:])
	s.pos += 8
	return v
}

// bitsetToBytes serialises a BitSet as little-endian uint64 words into out.
// out must be exactly len(bs.Words())*8 bytes; the caller owns the buffer.
func bitsetToBytes(bs *bitset.BitSet, out []byte) {
	words := bs.Words()
	for i, w := range words {
		binary.LittleEndian.PutUint64(out[i*8:], w)
	}
}

// bitsetPools keys a sync.Pool by SizeBits so reuse only matches bitsets
// of the same word count. The typical workload is one Tokenizer with one
// FieldSet (a single SizeBits), so a single pool ends up hot.
var bitsetPools sync.Map // map[uint32]*sync.Pool

// acquireBitset returns a zeroed BitSet sized for sizeBits, drawn from a
// per-SizeBits pool. The caller must hand it back via releaseBitset.
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
// panics on the nil type assertion below, which is the intended failure
// mode for an internal helper.
func releaseBitset(sizeBits uint32, bs *bitset.BitSet) {
	p, _ := bitsetPools.Load(sizeBits)
	p.(*sync.Pool).Put(bs)
}

// eachNgram invokes fn for each n-gram (across all sizes in sizes) extracted
// from s, decoded as runes (not bytes) so multi-byte UTF-8 input produces
// correct gram boundaries. The byte slice passed to fn aliases an internal
// scratch buffer; fn must not retain it past the call. Iteration order is:
// for each size in sizes (in order), all grams of that size left-to-right.
// No-ops when s has fewer runes than the smallest size, or sizes is empty.
func eachNgram(s string, sizes []int, fn func(gram []byte)) {
	runes := []rune(s)
	n := len(runes)
	if n == 0 || len(sizes) == 0 {
		return
	}
	minSize, maxSize := sizes[0], sizes[0]
	for _, sz := range sizes[1:] {
		if sz < minSize {
			minSize = sz
		}
		if sz > maxSize {
			maxSize = sz
		}
	}
	if n < minSize {
		return
	}
	// Scratch is sized to the worst case (largest gram, all 4-byte runes).
	scratch := make([]byte, 0, maxSize*utf8.UTFMax)
	for _, sz := range sizes {
		for i := 0; i+sz <= n; i++ {
			scratch = scratch[:0]
			for _, r := range runes[i : i+sz] {
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
