package sriracha

// RawRecord is the input type institutions populate before tokenization.
// Keys are FieldPath values; values are the raw string for that field.
// Fields the institution does not have should simply be omitted from the map.
type RawRecord map[FieldPath]string

// DeterministicToken is the output of HMAC-SHA256 tokenization.
// Fields[i] is the 32-byte HMAC for FieldSet.Fields[i]. Absent optional fields
// produce a nil entry, preserving positional alignment with the FieldSet.
type DeterministicToken struct {
	FieldSetVersion string
	Fields          [][]byte
}

// BloomToken is the output of probabilistic (Bloom filter) tokenization.
// Fields[i] is the serialized Bloom filter (little-endian uint64 words) for
// FieldSet.Fields[i]. Absent optional fields produce an all-zero filter,
// preserving positional alignment with the FieldSet.
type BloomToken struct {
	FieldSetVersion string
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
