package token

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"hash"

	"github.com/bits-and-blooms/bitset"
	bloom "github.com/bits-and-blooms/bloom/v3"

	"go.sriracha.dev/normalize"
	"go.sriracha.dev/sriracha"
)

func (t *tokenizer) TokenizeRecordBloom(record sriracha.RawRecord, fs sriracha.FieldSet) (sriracha.BloomToken, error) {
	cfg := fs.BloomParams
	fieldBytes := int(((cfg.SizeBits + 63) / 64) * 8)
	fields := make([][]byte, len(fs.Fields))

	// Reuse a single HMAC instance across every field/gram in this record.
	h := hmac.New(sha256.New, t.secret.Bytes())

	for i, spec := range fs.Fields {
		raw, ok := record[spec.Path]
		if !ok {
			if spec.Required {
				return sriracha.BloomToken{}, fmt.Errorf("token: required field %q missing", spec.Path)
			}
			fields[i] = make([]byte, fieldBytes)
			continue
		}

		normalized, err := normalize.Normalize(raw, spec.Path)
		if err != nil {
			return sriracha.BloomToken{}, fmt.Errorf("token: normalization failed for field %q: %w", spec.Path, err)
		}
		fields[i] = tokenizeFieldBloom(h, normalized, spec.Path, cfg)
	}

	return sriracha.BloomToken{
		FieldSetVersion: fs.Version,
		BloomParams:     cfg,
		Fields:          fields,
	}, nil
}

// tokenizeFieldBloom returns serialized Bloom filter bytes for a single
// normalized field value, using h (which is reset between hashes).
//
// Bit positions are derived from HMAC-SHA256 over (gram, path, counter), each
// component written separately and length-prefixed so distinct (gram, path)
// or counter values cannot collide via concatenation. Without separators,
// (gram="ab", path="c.d") and (gram="a", path="bc.d") would hash the same input.
func tokenizeFieldBloom(h hash.Hash, normalizedValue string, path sriracha.FieldPath, cfg sriracha.BloomConfig) []byte {
	f := bloom.New(uint(cfg.SizeBits), uint(cfg.HashCount))
	bs := f.BitSet()
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

	return bitsetToBytes(bs)
}

// bitsetToBytes serialises a BitSet as little-endian uint64 words.
func bitsetToBytes(bs *bitset.BitSet) []byte {
	words := bs.Words()
	out := make([]byte, len(words)*8)
	for i, w := range words {
		binary.LittleEndian.PutUint64(out[i*8:], w)
	}
	return out
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
