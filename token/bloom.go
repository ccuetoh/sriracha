package token

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/bits-and-blooms/bitset"
	bloom "github.com/bits-and-blooms/bloom/v3"

	"go.sriracha.dev/normalize"
	"go.sriracha.dev/sriracha"
)

// TokenizeRecordBloom tokenizes a RawRecord in probabilistic (Bloom filter) mode.
// Each field gets its own Bloom filter of BloomParams.SizeBits bits.
// The Payload is the concatenation of all per-field filters in FieldSet order.
// Missing optional fields contribute an all-zero filter (preserving field layout).
// Missing required fields return an error.
func (t *Tokenizer) TokenizeRecordBloom(record sriracha.RawRecord, fs sriracha.FieldSet) (sriracha.TokenRecord, error) {
	cfg := fs.BloomParams
	fieldBytes := int(((cfg.SizeBits + 63) / 64) * 8)
	var payload []byte
	for _, spec := range fs.Fields {
		raw, ok := record[spec.Path]
		if !ok || sriracha.IsNotFound(raw) || sriracha.IsNotHeld(raw) {
			if spec.Required {
				return sriracha.TokenRecord{}, fmt.Errorf("token: required field %q missing", spec.Path)
			}
			// Absent optional field: zero-filled filter preserves field layout for weighted scoring
			payload = append(payload, make([]byte, fieldBytes)...)
			continue
		}

		normalized, err := normalize.Normalize(raw, spec.Path)
		if err != nil {
			return sriracha.TokenRecord{}, fmt.Errorf("token: normalization failed for field %q: %w", spec.Path, err)
		}
		payload = append(payload, t.tokenizeFieldBloom(normalized, spec.Path, cfg)...)
	}

	return sriracha.TokenRecord{
		FieldSetVersion: fs.Version,
		Mode:            sriracha.Probabilistic,
		Algo:            sriracha.AlgoBloomV1,
		Payload:         payload,
		Checksum:        sha256.Sum256(payload),
	}, nil
}

// tokenizeFieldBloom returns serialized Bloom filter bytes for a single normalized field value.
// Bit positions are determined by HMAC-SHA256 keyed with the Tokenizer secret, preventing
// n-gram inference without access to the key.
func (t *Tokenizer) tokenizeFieldBloom(normalizedValue string, path sriracha.FieldPath, cfg sriracha.BloomConfig) []byte {
	f := bloom.New(uint(cfg.SizeBits), uint(cfg.HashCount))
	bs := f.BitSet()
	grams := ngrams(normalizedValue, cfg.NgramSizes)
	pathStr := path.String()

	// Allocate one HMAC instance and reset it between uses to avoid the cost
	// of re-keying (hmac.New allocates a new underlying hash each call).
	h := hmac.New(sha256.New, t.secret.Bytes())

	for _, g := range grams {
		prefix := []byte(g + pathStr)
		for i := range cfg.HashCount {
			h.Reset()
			h.Write(prefix)
			h.Write([]byte(strconv.Itoa(i)))
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
