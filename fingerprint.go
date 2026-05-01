package sriracha

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"hash"
	"math"
)

// Fingerprint returns the lowercase hex-encoded SHA-256 of a canonical encoding
// of fs. Two FieldSets with identical schemas produce identical fingerprints;
// any change to Version, field path, ordering, Required, Weight, or any
// BloomParams component changes it.
//
// The canonical encoding (big-endian throughout, matching the length-prefix
// convention used by the token package) is:
//
//	u32(len(Version)) || Version
//	u32(len(Fields))
//	for each FieldSpec in declared order:
//	    u32(len(Path.String())) || Path.String()
//	    u8(Required: 0 or 1)
//	    u64(math.Float64bits(Weight))
//	u32(BloomParams.SizeBits)
//	u32(uint32(BloomParams.HashCount))
//	u32(len(BloomParams.NgramSizes))
//	for each NgramSize: u32(int32(size))
//
// This spec is locked: any change to the encoding is a breaking on-the-wire
// change, must bump the FieldSet Version.
func (fs FieldSet) Fingerprint() string {
	h := sha256.New()
	var b [8]byte

	writeU32Len(h, b[:4], len(fs.Version))
	h.Write([]byte(fs.Version))

	writeU32Len(h, b[:4], len(fs.Fields))
	for _, spec := range fs.Fields {
		path := spec.Path.String()
		writeU32Len(h, b[:4], len(path))
		h.Write([]byte(path))
		if spec.Required {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}
		binary.BigEndian.PutUint64(b[:8], math.Float64bits(spec.Weight))
		h.Write(b[:8])
	}

	binary.BigEndian.PutUint32(b[:4], fs.BloomParams.SizeBits)
	h.Write(b[:4])
	binary.BigEndian.PutUint32(b[:4], uint32(fs.BloomParams.HashCount)) //nolint:gosec // G115: HashCount bounded at validation time
	h.Write(b[:4])
	writeU32Len(h, b[:4], len(fs.BloomParams.NgramSizes))
	for _, sz := range fs.BloomParams.NgramSizes {
		binary.BigEndian.PutUint32(b[:4], uint32(sz)) //nolint:gosec // G115: NgramSizes bounded at validation time
		h.Write(b[:4])
	}

	return hex.EncodeToString(h.Sum(nil))
}

// writeU32Len writes the big-endian uint32 length prefix used throughout the
// fingerprint canonical encoding.
func writeU32Len(h hash.Hash, scratch []byte, n int) {
	binary.BigEndian.PutUint32(scratch, uint32(n)) //nolint:gosec // G115: length bounded by input
	h.Write(scratch)
}
