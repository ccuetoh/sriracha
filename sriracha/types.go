package sriracha

import "fmt"

// RawRecord is the input type institutions populate before tokenization.
// Keys are FieldPath values; values are the raw string for that field.
// Fields the institution does not have should simply be omitted from the map.
type RawRecord map[FieldPath]string

// DeterministicToken is the output of HMAC-SHA256 tokenization.
// Fields[i] is the 32-byte HMAC for FieldSet.Fields[i]. Absent optional fields
// produce a nil entry, preserving positional alignment with the FieldSet.
// KeyID is the optional identifier of the secret used to produce the token; it
// surfaces post-rotation mismatches that would otherwise be silent.
type DeterministicToken struct {
	FieldSetVersion string
	KeyID           string
	Fields          [][]byte
}

// BloomToken is the output of probabilistic (Bloom filter) tokenization.
// Fields[i] is the serialized Bloom filter (little-endian uint64 words) for
// FieldSet.Fields[i]. Absent optional fields produce an all-zero filter,
// preserving positional alignment with the FieldSet.
// KeyID is the optional identifier of the secret used to produce the token; it
// surfaces post-rotation mismatches that would otherwise be silent.
type BloomToken struct {
	FieldSetVersion string
	KeyID           string
	BloomParams     BloomConfig
	Fields          [][]byte
}

// FieldSpec describes one field within a FieldSet.
type FieldSpec struct {
	Path     FieldPath
	Required bool
	Weight   float64
}

// BloomConfig holds parameters for Bloom filter tokenization.
type BloomConfig struct {
	SizeBits   uint32
	NgramSizes []int
	HashCount  int
}

// DefaultBloomConfig returns the standard Bloom filter configuration.
func DefaultBloomConfig() BloomConfig {
	return BloomConfig{
		SizeBits:   1024,
		NgramSizes: []int{2, 3},
		HashCount:  2,
	}
}

// FieldSet describes the schema used for tokenization.
type FieldSet struct {
	Version     string
	Fields      []FieldSpec
	BloomParams BloomConfig
}

// String returns a redacted summary of the token: counts and metadata only,
// never any byte from Fields. Safe for logging.
func (t DeterministicToken) String() string {
	present, total, bytes := summariseFields(t.Fields)
	return fmt.Sprintf("DeterministicToken{v=%s key=%s fields=%d/%d bytes=%d}",
		t.FieldSetVersion, t.KeyID, present, total, bytes)
}

// String returns a redacted summary of the token: counts and metadata only,
// never any byte from Fields. Safe for logging.
func (t BloomToken) String() string {
	present, total, bytes := summariseFields(t.Fields)
	return fmt.Sprintf("BloomToken{v=%s key=%s size=%db fields=%d/%d bytes=%d}",
		t.FieldSetVersion, t.KeyID, t.BloomParams.SizeBits, present, total, bytes)
}

func summariseFields(fields [][]byte) (present, total, totalBytes int) {
	total = len(fields)
	for _, f := range fields {
		if f != nil {
			present++
		}
		totalBytes += len(f)
	}
	return
}
