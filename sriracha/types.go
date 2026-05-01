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
// FieldSetFingerprint is the hex SHA-256 of the FieldSet canonical encoding
// (see FieldSet.Fingerprint); when both sides of a comparison have it set, it
// surfaces silent schema drift (e.g. reordered fields) that the user-set
// FieldSetVersion alone would miss.
type DeterministicToken struct {
	FieldSetVersion     string   `json:"field_set_version"`
	KeyID               string   `json:"key_id,omitempty"`
	FieldSetFingerprint string   `json:"field_set_fingerprint,omitempty"`
	Fields              [][]byte `json:"fields"`
}

// BloomToken is the output of probabilistic (Bloom filter) tokenization.
// Fields[i] is the serialized Bloom filter (little-endian uint64 words) for
// FieldSet.Fields[i]. Absent optional fields produce an all-zero filter,
// preserving positional alignment with the FieldSet.
// KeyID is the optional identifier of the secret used to produce the token; it
// surfaces post-rotation mismatches that would otherwise be silent.
// FieldSetFingerprint is the hex SHA-256 of the FieldSet canonical encoding
// (see FieldSet.Fingerprint); when both sides of a comparison have it set, it
// surfaces silent schema drift (e.g. reordered fields) that the user-set
// FieldSetVersion alone would miss.
type BloomToken struct {
	FieldSetVersion     string      `json:"field_set_version"`
	KeyID               string      `json:"key_id,omitempty"`
	FieldSetFingerprint string      `json:"field_set_fingerprint,omitempty"`
	BloomParams         BloomConfig `json:"bloom_params"`
	Fields              [][]byte    `json:"fields"`
}

// FieldSpec describes one field within a FieldSet.
type FieldSpec struct {
	Path     FieldPath `json:"path"`
	Required bool      `json:"required"`
	Weight   float64   `json:"weight"`
}

// BloomConfig holds parameters for Bloom filter tokenization.
type BloomConfig struct {
	SizeBits   uint32 `json:"size_bits"`
	NgramSizes []int  `json:"ngram_sizes"`
	HashCount  int    `json:"hash_count"`
}

// FastBloomConfig returns a lightweight Bloom filter configuration optimised
// for throughput over precision. Suitable for large-scale screening passes
// where a secondary verification step follows.
func FastBloomConfig() BloomConfig {
	return BloomConfig{
		SizeBits:   1024,
		NgramSizes: []int{2, 3},
		HashCount:  2,
	}
}

// DefaultBloomConfig returns the standard Bloom filter configuration.
// It balances precision and token size for most production workloads.
func DefaultBloomConfig() BloomConfig {
	return BloomConfig{
		SizeBits:   2048,
		NgramSizes: []int{2, 3},
		HashCount:  3,
	}
}

// HighPrecisionBloomConfig returns a Bloom filter configuration tuned for
// maximum matching accuracy at the cost of larger tokens.
func HighPrecisionBloomConfig() BloomConfig {
	return BloomConfig{
		SizeBits:   4096,
		NgramSizes: []int{2, 3},
		HashCount:  5,
	}
}

// FieldSet describes the schema used for tokenization.
type FieldSet struct {
	Version     string      `json:"version"`
	Fields      []FieldSpec `json:"fields"`
	BloomParams BloomConfig `json:"bloom_params"`
}

// String returns a redacted summary of the token: counts and metadata only,
// never any byte from Fields. Safe for logging.
func (t DeterministicToken) String() string {
	present, total, bytes := summariseFields(t.Fields)
	return fmt.Sprintf("DeterministicToken{v=%s key=%s fp=%s fields=%d/%d bytes=%d}",
		t.FieldSetVersion, t.KeyID, shortFingerprint(t.FieldSetFingerprint), present, total, bytes)
}

// String returns a redacted summary of the token: counts and metadata only,
// never any byte from Fields. Safe for logging.
func (t BloomToken) String() string {
	present, total, bytes := summariseFields(t.Fields)
	return fmt.Sprintf("BloomToken{v=%s key=%s fp=%s size=%db fields=%d/%d bytes=%d}",
		t.FieldSetVersion, t.KeyID, shortFingerprint(t.FieldSetFingerprint), t.BloomParams.SizeBits, present, total, bytes)
}

// shortFingerprint returns the first 8 hex chars of fp, or "" if fp is empty.
// Eight hex chars (4 bytes) are enough to spot drift in logs without bloating
// every line.
func shortFingerprint(fp string) string {
	if len(fp) <= 8 {
		return fp
	}
	return fp[:8]
}

// AnnotatedField is the per-field summary returned by Annotate. It carries
// the field path and presence flags but never the raw token bytes.
type AnnotatedField struct {
	Path      FieldPath `json:"path"`
	Present   bool      `json:"present"`
	ByteCount int       `json:"byte_count"`
}

// AnnotatedToken is a safe-to-log view of a token: token-level metadata plus
// per-field presence and byte counts, with the raw HMAC / Bloom bytes stripped.
type AnnotatedToken struct {
	Version             string           `json:"version"`
	KeyID               string           `json:"key_id,omitempty"`
	FieldSetFingerprint string           `json:"field_set_fingerprint,omitempty"`
	Fields              []AnnotatedField `json:"fields"`
}

// Annotate returns a redacted view of t paired with fs. The result holds the
// path and presence flag for every field in fs (truncated or extended to
// match) but never the raw HMAC bytes — safe for logs, dashboards, and audit
// trails.
//
// Presence is defined as len(field) > 0; nil and empty slices both register
// as absent.
func (t DeterministicToken) Annotate(fs FieldSet) AnnotatedToken {
	return annotateFields(t.FieldSetVersion, t.KeyID, t.FieldSetFingerprint, t.Fields, fs, presentByLength)
}

// Annotate returns a redacted view of t paired with fs. The result holds the
// path and presence flag for every field in fs but never the raw filter
// bytes. Presence is defined as "any bit set" — an all-zero filter is treated
// as absent, matching the convention used by Match.
func (t BloomToken) Annotate(fs FieldSet) AnnotatedToken {
	return annotateFields(t.FieldSetVersion, t.KeyID, t.FieldSetFingerprint, t.Fields, fs, presentByAnyBit)
}

func presentByLength(b []byte) bool { return len(b) > 0 }

func presentByAnyBit(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return true
		}
	}
	return false
}

func annotateFields(version, keyID, fingerprint string, fields [][]byte, fs FieldSet, present func([]byte) bool) AnnotatedToken {
	n := len(fs.Fields)
	if len(fields) > n {
		n = len(fields)
	}
	annotated := make([]AnnotatedField, n)
	for i := range annotated {
		var path FieldPath
		if i < len(fs.Fields) {
			path = fs.Fields[i].Path
		}
		annotated[i].Path = path
		if i < len(fields) {
			annotated[i].ByteCount = len(fields[i])
			annotated[i].Present = present(fields[i])
		}
	}
	return AnnotatedToken{
		Version:             version,
		KeyID:               keyID,
		FieldSetFingerprint: fingerprint,
		Fields:              annotated,
	}
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
