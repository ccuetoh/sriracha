package sriracha

import "fmt"

// RawRecord is the input type institutions populate before tokenization.
// Keys are FieldPath values; values are the raw string for that field.
// Fields the institution does not have should simply be omitted from the map.
type RawRecord map[FieldPath]string

// Token format discriminators. The tokenizer stamps these onto every token it
// emits; comparison helpers refuse to compare tokens whose formats differ.
// The trailing number is the wire-format generation, bumped on every breaking
// change to the token construction.
const (
	// TokenFormatDeterministic marks v2 deterministic (HMAC-SHA256) tokens.
	TokenFormatDeterministic = "sriracha/det/2" //nolint:gosec // G101: wire-format discriminator, not a credential
	// TokenFormatProbabilistic marks v2 per-field Bloom filter tokens.
	TokenFormatProbabilistic = "sriracha/bloom/2" //nolint:gosec // G101: wire-format discriminator, not a credential
	// TokenFormatCLK marks v2 record-level CLK tokens.
	TokenFormatCLK = "sriracha/clk/2" //nolint:gosec // G101: wire-format discriminator, not a credential
)

// DeterministicToken is the output of HMAC-SHA256 tokenization.
// Format identifies the wire format that produced the token; tokens with
// different formats are never comparable.
// Fields[i] is the 32-byte HMAC for FieldSet.Fields[i]. Absent optional fields
// produce a nil entry, preserving positional alignment with the FieldSet.
// KeyID is the optional identifier of the secret used to produce the token; it
// surfaces post-rotation mismatches that would otherwise be silent.
// FieldSetFingerprint is the hex SHA-256 of the FieldSet canonical encoding
// (see FieldSet.Fingerprint); when both sides of a comparison have it set, it
// surfaces silent schema drift (e.g. reordered fields) that the user-set
// FieldSetVersion alone would miss. session.Session populates this field
// automatically using a value cached at session.New time; direct
// token.Tokenizer callers must set it themselves if they want drift
// detection.
type DeterministicToken struct {
	Format              string   `json:"format"`
	FieldSetVersion     string   `json:"field_set_version"`
	KeyID               string   `json:"key_id,omitempty"`
	FieldSetFingerprint string   `json:"field_set_fingerprint,omitempty"`
	Fields              [][]byte `json:"fields"`
}

// ProbabilisticToken is the output of probabilistic (Bloom filter) tokenization.
// Format identifies the wire format that produced the token; tokens with
// different formats are never comparable.
// Fields[i] is the serialized Bloom filter (little-endian uint64 words) for
// FieldSet.Fields[i]. Absent optional fields produce a nil entry, preserving
// positional alignment with the FieldSet.
// KeyID is the optional identifier of the secret used to produce the token; it
// surfaces post-rotation mismatches that would otherwise be silent.
// FieldSetFingerprint is the hex SHA-256 of the FieldSet canonical encoding
// (see FieldSet.Fingerprint); when both sides of a comparison have it set, it
// surfaces silent schema drift (e.g. reordered fields) that the user-set
// FieldSetVersion alone would miss. session.Session populates this field
// automatically using a value cached at session.New time; direct
// token.Tokenizer callers must set it themselves if they want drift
// detection.
//
// A per-field token reveals which fields a record carries and how similar
// each field is. When per-field scores are not required, prefer CLKToken,
// which folds the whole record into one filter.
type ProbabilisticToken struct {
	Format              string              `json:"format"`
	FieldSetVersion     string              `json:"field_set_version"`
	KeyID               string              `json:"key_id,omitempty"`
	FieldSetFingerprint string              `json:"field_set_fingerprint,omitempty"`
	ProbabilisticParams ProbabilisticConfig `json:"probabilistic_params"`
	Fields              [][]byte            `json:"fields"`
}

// CLKToken is the output of record-level CLK (cryptographic long-term key)
// tokenization. Every present field of the record contributes its q-grams to
// the single Filter, so the token carries one whole-record similarity signal
// and no per-field structure. CLK is the recommended way to share tokens when
// per-field scores are not required, because per-field tokens reveal which
// fields are present and how similar each one is.
// Format, KeyID, and FieldSetFingerprint have the same meaning as on
// ProbabilisticToken. Filter is the serialized filter bytes (little-endian
// uint64 words).
type CLKToken struct {
	Format              string              `json:"format"`
	FieldSetVersion     string              `json:"field_set_version"`
	KeyID               string              `json:"key_id,omitempty"`
	FieldSetFingerprint string              `json:"field_set_fingerprint,omitempty"`
	ProbabilisticParams ProbabilisticConfig `json:"probabilistic_params"`
	Filter              []byte              `json:"filter"`
}

// FieldSpec describes one field within a FieldSet.
type FieldSpec struct {
	Path     FieldPath `json:"path"`
	Required bool      `json:"required"`
	Weight   float64   `json:"weight"`
}

// ProbabilisticConfig holds parameters for Bloom filter tokenization.
//
// SizeBits is the size of the emitted filter. When Balanced is set, the
// underlying Bloom filter uses SizeBits/2 bits (SizeBits must be even); the
// emitted filter appends the bitwise complement of the base filter and then
// applies a secret permutation to the combined bits. A balanced filter has a
// popcount of exactly SizeBits/2, which removes the popcount length leak of
// plain Bloom filters.
//
// Balanced applies to per-field filters only; CLK tokens are always
// balanced. It defaults to false for per-field filters because balancing
// compresses per-field Dice scores onto value-dependent baselines and
// degrades weighted multi-field matching. Enable it only when per-field
// popcounts must be hidden and thresholds are calibrated for it. Balanced
// filters are not differential privacy: identical values still produce
// identical filters and remain linkable by anyone who can submit chosen
// values under the same secret.
type ProbabilisticConfig struct {
	SizeBits   uint32 `json:"size_bits"`
	NgramSizes []int  `json:"ngram_sizes"`
	HashCount  int    `json:"hash_count"`
	Balanced   bool   `json:"balanced,omitempty"`
}

// FastProbabilisticConfig returns a lightweight filter configuration
// optimised for throughput and token size over precision. Suitable for
// large-scale screening passes where a secondary verification step follows.
func FastProbabilisticConfig() ProbabilisticConfig {
	return ProbabilisticConfig{
		SizeBits:   512,
		NgramSizes: []int{2},
		HashCount:  2,
	}
}

// DefaultProbabilisticConfig returns the standard filter configuration. It
// balances precision and token size for per-field matching. Per-field
// filters expose per-field popcounts and presence; prefer CLK tokens when
// sharing tokens without needing per-field scores.
func DefaultProbabilisticConfig() ProbabilisticConfig {
	return ProbabilisticConfig{
		SizeBits:   1024,
		NgramSizes: []int{2, 3},
		HashCount:  3,
	}
}

// HighPrecisionProbabilisticConfig returns a filter configuration tuned for
// maximum matching accuracy at the cost of larger tokens.
func HighPrecisionProbabilisticConfig() ProbabilisticConfig {
	return ProbabilisticConfig{
		SizeBits:   2048,
		NgramSizes: []int{2, 3},
		HashCount:  5,
	}
}

// FieldSet describes the schema used for tokenization.
type FieldSet struct {
	Version             string              `json:"version"`
	Fields              []FieldSpec         `json:"fields"`
	ProbabilisticParams ProbabilisticConfig `json:"probabilistic_params"`
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
func (t ProbabilisticToken) String() string {
	present, total, bytes := summariseFields(t.Fields)
	return fmt.Sprintf("ProbabilisticToken{v=%s key=%s fp=%s size=%db fields=%d/%d bytes=%d}",
		t.FieldSetVersion, t.KeyID, shortFingerprint(t.FieldSetFingerprint), t.ProbabilisticParams.SizeBits, present, total, bytes)
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
// bytes. Presence is defined as len(field) > 0; absent optional fields carry
// a nil filter, matching the convention used by Match.
func (t ProbabilisticToken) Annotate(fs FieldSet) AnnotatedToken {
	return annotateFields(t.FieldSetVersion, t.KeyID, t.FieldSetFingerprint, t.Fields, fs, presentByLength)
}

func presentByLength(b []byte) bool { return len(b) > 0 }

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
