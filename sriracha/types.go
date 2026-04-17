package sriracha

import "time"

// MatchMode indicates whether tokens use deterministic or probabilistic encoding.
type MatchMode int

const (
	Deterministic MatchMode = 1
	Probabilistic MatchMode = 2
)

// RawRecord is the input type institutions populate before tokenization.
// Keys are FieldPath values; values are raw strings or the NotFound/NotHeld sentinels.
type RawRecord map[FieldPath]string

// TokenRecord is the wire-format output of tokenization.
type TokenRecord struct {
	FieldSetVersion string
	Mode            MatchMode
	Payload         []byte
	Checksum        [32]byte
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

// FieldWeight overrides the weight of a field at query time.
type FieldWeight struct {
	Path   FieldPath
	Weight float64
}

// MatchConfig controls matching behavior at query time.
type MatchConfig struct {
	Threshold    float32
	MaxResults   uint32
	FieldWeights []FieldWeight
}

// Candidate is a matching record returned by the index.
type Candidate struct {
	RecordID   string
	Confidence float64
}

// IndexStats describes the current state of a token index.
type IndexStats struct {
	RecordCount    int64
	LastRebuild    time.Time
	LastSync       time.Time
	IndexSizeBytes int64
}
