package token

import (
	"encoding/binary"
	"fmt"
	"hash"

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
	bs := bitset.New(uint(cfg.SizeBits))
	grams := ngrams(normalizedValue, cfg.NgramSizes)
	pathBytes := []byte(path.String())

	var lp [4]byte
	var ib [4]byte
	for _, g := range grams {
		gb := []byte(g)
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
			sum := h.Sum(nil)
			pos := binary.BigEndian.Uint64(sum[:8]) % uint64(cfg.SizeBits)
			bs.Set(uint(pos))
		}
	}

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
	sum := s.h.Sum(nil)
	copy(s.buf[:], sum)
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

// ngrams returns all n-grams of the given sizes from s.
// Iterates Unicode runes (not bytes) to correctly handle multi-byte UTF-8.
// Returns nil if s has fewer runes than the smallest requested size, or if sizes is empty.
func ngrams(s string, sizes []int) []string {
	runes := []rune(s)
	n := len(runes)
	if n == 0 || len(sizes) == 0 {
		return nil
	}
	minSize := sizes[0]
	for _, sz := range sizes[1:] {
		if sz < minSize {
			minSize = sz
		}
	}
	if n < minSize {
		return nil
	}
	var result []string
	for _, sz := range sizes {
		for i := 0; i+sz <= n; i++ {
			result = append(result, string(runes[i:i+sz]))
		}
	}
	return result
}
