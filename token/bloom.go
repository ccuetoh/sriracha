package token

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"fmt"

	"go.sriracha.dev/internal/bitset"
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
	var payload []byte
	for _, spec := range fs.Fields {
		raw, ok := record[spec.Path]
		if !ok || sriracha.IsNotFound(raw) || sriracha.IsNotHeld(raw) {
			if spec.Required {
				return sriracha.TokenRecord{}, fmt.Errorf("token: required field %q missing", spec.Path)
			}
			// Absent optional field: zero-filled filter preserves field layout for weighted scoring
			zeroFilter := bitset.New(int(cfg.SizeBits))
			payload = append(payload, zeroFilter.ToBytes()...)
			continue
		}

		normalized, err := normalize.Normalize(raw, spec.Path)
		if err != nil {
			return sriracha.TokenRecord{}, fmt.Errorf("token: normalization failed for field %q: %w", spec.Path, err)
		}
		filter, err := t.tokenizeFieldBloom(normalized, spec.Path, cfg)
		if err != nil {
			return sriracha.TokenRecord{}, err
		}
		payload = append(payload, filter.ToBytes()...)
	}

	return sriracha.TokenRecord{
		FieldSetVersion: fs.Version,
		Mode:            sriracha.Probabilistic,
		Algo:            sriracha.AlgoBloomV1,
		Payload:         payload,
		Checksum:        sha256.Sum256(payload),
	}, nil
}

// tokenizeFieldBloom returns a Bloom filter bitset for a single normalized field value.
// For each n-gram, cfg.HashCount HMAC-SHA256 outputs determine bit positions to set.
func (t *Tokenizer) tokenizeFieldBloom(normalizedValue string, path sriracha.FieldPath, cfg sriracha.BloomConfig) (*bitset.Bitset, error) {
	var (
		b       = bitset.New(int(cfg.SizeBits))
		grams   = ngrams(normalizedValue, cfg.NgramSizes)
		pathStr = path.String()
	)

	for _, g := range grams {
		for i := range cfg.HashCount {
			suffix := fmt.Sprintf("%d", i)
			h := hmacSum(t.secret, []byte(g+pathStr+suffix))
			pos := int(binary.BigEndian.Uint64(h[:8]) % uint64(cfg.SizeBits))
			if err := b.Set(pos); err != nil {
				return nil, fmt.Errorf("token: bloom set failed: %w", err)
			}
		}
	}

	return b, nil
}

// hmacSum returns HMAC-SHA256(key, data).
func hmacSum(key, data []byte) []byte {
	h := hmac.New(sha256.New, key)
	h.Write(data)
	return h.Sum(nil)
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
