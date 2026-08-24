package token

import (
	"crypto/subtle"
	"errors"
	"fmt"
	"math/bits"
	"slices"

	"github.com/ccuetoh/sriracha"
)

// Sentinel errors returned by the comparison primitives. Every one of them
// means the two tokens are not comparable, which is a different outcome from
// "these are different people": key rotation, schema drift and wire-format
// drift all land here instead of returning a quiet false.
var (
	// ErrFormatMismatch reports two tokens from different wire generations.
	ErrFormatMismatch = errors.New("token: Format mismatch")

	// ErrFieldSetVersionMismatch reports two tokens built from different
	// FieldSet versions.
	ErrFieldSetVersionMismatch = errors.New("token: FieldSetVersion mismatch")

	// ErrKeyIDMismatch reports two tokens derived under different secrets.
	ErrKeyIDMismatch = errors.New("token: KeyID mismatch")

	// ErrFingerprintMismatch reports two tokens built from schemas with the
	// same version but different contents. Only checked when both sides
	// carry a fingerprint.
	ErrFingerprintMismatch = errors.New("token: FieldSetFingerprint mismatch")

	// ErrParamsMismatch reports two probabilistic tokens built with
	// different Bloom filter parameters.
	ErrParamsMismatch = errors.New("token: ProbabilisticParams mismatch")

	// ErrFieldCountMismatch reports tokens whose field counts disagree with
	// each other or with the FieldSet used to compare them.
	ErrFieldCountMismatch = errors.New("token: field count mismatch")

	// ErrFilterLengthMismatch reports two filters of different byte length,
	// either a per-field filter or a record-level CLK filter.
	ErrFilterLengthMismatch = errors.New("token: filter byte length mismatch")

	// ErrInvalidThreshold reports a threshold outside [0,1], NaN included.
	ErrInvalidThreshold = errors.New("token: threshold must be in [0,1]")

	// ErrNoComparableFields reports a comparison with no evidence on either
	// side: every field is absent from both tokens, so there is nothing to
	// decide on.
	ErrNoComparableFields = errors.New("token: no comparable fields")
)

// present reports whether a token field carries data. A field is absent when
// it has zero length, which covers both a nil slice and an empty non-nil one.
// JSON round-trips turn nil into null or "" depending on the peer, and that
// choice must not flip a verdict, so this is the single absence predicate for
// the whole package.
func present(field []byte) bool {
	return len(field) > 0
}

// MatchPolicy is the decision policy for a comparison. The zero value is
// threshold 0 with no evidence floor, which is exactly the old
// Match(a, b, fs, 0) behavior.
//
// The two floors gate IsMatch only. They never change Score, and a pair that
// falls below them is a non-match, never an error.
type MatchPolicy struct {
	// Threshold is the minimum Score for a match, in [0,1].
	Threshold float64
	// MinComparableFields is the minimum number of fields present on at
	// least one side and carrying positive weight.
	MinComparableFields int
	// MinComparableWeight is the minimum total weight of those fields.
	MinComparableWeight float64
}

// DefaultMatchPolicy returns the recommended policy for threshold: a floor of
// two comparable fields. One field is not evidence of identity. Two records
// that share only a country code agree perfectly on everything they can be
// compared on, and a single-field agreement scoring 1.000 is the shape of a
// false positive, not of a match.
//
// Raise MinComparableFields, or set MinComparableWeight, when the population
// has many sparse records.
func DefaultMatchPolicy(threshold float64) MatchPolicy {
	return MatchPolicy{Threshold: threshold, MinComparableFields: 2}
}

// validate checks the threshold and both floors.
func (p MatchPolicy) validate() error {
	if !(p.Threshold >= 0 && p.Threshold <= 1) {
		return fmt.Errorf("%w, got %v", ErrInvalidThreshold, p.Threshold)
	}
	return p.validateFloors()
}

// validateFloors checks the evidence floors alone. Calibrate uses it because
// it supplies its own thresholds and ignores p.Threshold.
func (p MatchPolicy) validateFloors() error {
	if p.MinComparableFields < 0 {
		return fmt.Errorf("token: %w: MinComparableFields must not be negative, got %d",
			sriracha.ErrInvalidConfig, p.MinComparableFields)
	}
	if !(p.MinComparableWeight >= 0) {
		return fmt.Errorf("token: %w: MinComparableWeight must not be negative or NaN, got %v",
			sriracha.ErrInvalidConfig, p.MinComparableWeight)
	}
	return nil
}

// MatchResult holds the output of Match: per-field Dice scores, the weighted
// aggregate Score in [0, 1], the threshold decision, the FieldSet paths in the
// same order as PerField, and the evidence the decision rests on.
//
// ComparableFields counts the fields that contributed to the weighted average
// (the union of fields present on either side, excluding fields with
// non-positive weight) and ComparableWeight is their total weight. Both are
// reported so a caller can see how much evidence produced Score, and why the
// policy floors did or did not admit the pair.
type MatchResult struct {
	Score            float64              `json:"score"`
	PerField         []float64            `json:"per_field"`
	Paths            []sriracha.FieldPath `json:"paths"`
	IsMatch          bool                 `json:"is_match"`
	ComparableFields int                  `json:"comparable_fields"`
	ComparableWeight float64              `json:"comparable_weight"`
}

// CLKMatchResult holds the output of MatchCLK: the Dice score between the
// two record-level filters and the threshold decision.
type CLKMatchResult struct {
	Score   float64 `json:"score"`
	IsMatch bool    `json:"is_match"`
}

// ScoreFor returns the per-field Dice score for path along with true if the
// path appears in the result. Paths with zero or negative weight that were
// dropped from the weighted average still appear here with their raw Dice
// score.
func (r MatchResult) ScoreFor(path sriracha.FieldPath) (float64, bool) {
	for i, p := range r.Paths {
		if p == path {
			return r.PerField[i], true
		}
	}
	return 0, false
}

// ByPath returns a fresh map keyed by FieldPath with each path's Dice score.
// Useful for downstream code that wants to look up scores without scanning.
func (r MatchResult) ByPath() map[sriracha.FieldPath]float64 {
	out := make(map[sriracha.FieldPath]float64, len(r.Paths))
	for i, p := range r.Paths {
		out[p] = r.PerField[i]
	}
	return out
}

// tokenMeta is the metadata every token kind carries, pulled out so the three
// comparison entry points apply one rule instead of three copies.
type tokenMeta struct {
	format      string
	version     string
	keyID       string
	fingerprint string
}

// compareMeta applies the rule documented on Equal. It returns a
// sentinel-wrapped error on the first difference, in a fixed order: Format
// first, so tokens from a different wire generation fail before anything else
// is examined.
func compareMeta(a, b tokenMeta) error {
	if a.format != b.format {
		return fmt.Errorf("%w: %q vs %q", ErrFormatMismatch, a.format, b.format)
	}
	if a.version != b.version {
		return fmt.Errorf("%w: %q vs %q", ErrFieldSetVersionMismatch, a.version, b.version)
	}
	if a.keyID != b.keyID {
		return fmt.Errorf("%w: %q vs %q", ErrKeyIDMismatch, a.keyID, b.keyID)
	}
	if a.fingerprint != "" && b.fingerprint != "" && a.fingerprint != b.fingerprint {
		return fmt.Errorf("%w: %q vs %q", ErrFingerprintMismatch, a.fingerprint, b.fingerprint)
	}
	return nil
}

// Equal reports whether a and b are bit-identical across every field.
//
// The bool answers "same person"; the error answers "were these two tokens
// even comparable". Metadata drift returns (false, sentinel) rather than a
// bare false, so a rotated key or a changed schema cannot be mistaken for two
// different people. A field count that disagrees does the same.
//
// This is the metadata rule for the whole package; DicePerField and MatchCLK
// apply it unchanged. Required metadata is compared strictly: Format,
// FieldSetVersion and KeyID must be equal on both sides. Optional metadata is
// compared only when both sides set it, which today means FieldSetFingerprint
// alone. A peer that omits the fingerprint is not claiming a different schema,
// so an absent fingerprint means unknown, not different.
//
// KeyID is required rather than optional even though it is often empty,
// because it binds the token to the secret it was derived under. Treating an
// empty KeyID as unknown would let a token from a rotated key compare against
// one from the old key, and every field would then differ for a reason that
// has nothing to do with identity. An empty KeyID on both sides still passes;
// the asymmetry only bites when one side labels its key and the other does
// not, which is the case worth failing loudly.
//
// Two tokens whose fields are all absent on both sides return
// (false, ErrNoComparableFields). There is no evidence that they describe the
// same person, and reporting them equal would say that any two empty records
// are the same person.
//
// A field is absent when it has zero length, nil or not. A field present on
// one side and absent on the other, or present on both with different byte
// lengths, is an ordinary false: those are records that disagree, not tokens
// that cannot be compared. Per-field byte comparison is constant-time.
func Equal(a, b sriracha.DeterministicToken) (bool, error) {
	if err := compareMeta(
		tokenMeta{a.Format, a.FieldSetVersion, a.KeyID, a.FieldSetFingerprint},
		tokenMeta{b.Format, b.FieldSetVersion, b.KeyID, b.FieldSetFingerprint},
	); err != nil {
		return false, err
	}
	if len(a.Fields) != len(b.Fields) {
		return false, fmt.Errorf("%w: %d vs %d", ErrFieldCountMismatch, len(a.Fields), len(b.Fields))
	}

	evidence := 0
	for i := range a.Fields {
		if present(a.Fields[i]) || present(b.Fields[i]) {
			evidence++
		}
	}
	if evidence == 0 {
		return false, ErrNoComparableFields
	}

	for i := range a.Fields {
		ai, bi := a.Fields[i], b.Fields[i]
		if len(ai) != len(bi) {
			return false, nil
		}
		if len(ai) == 0 {
			continue
		}
		if subtle.ConstantTimeCompare(ai, bi) != 1 {
			return false, nil
		}
	}
	return true, nil
}

// DicePerField returns the Sørensen–Dice coefficient between corresponding
// fields of a and b. The result is one score in [0, 1] per field, in FieldSet
// order. A field that is absent on either side scores 0: absent on both sides
// means both records lack the field, absent on one side is an asymmetric
// absence, and either way there is no overlap to score. A field is absent when
// it has zero length, nil or not.
//
// Returns a sentinel-wrapped error if the tokens are not comparable: metadata
// drift under the rule documented on Equal, ProbabilisticParams drift, or a
// field count disagreement. Byte lengths are compared only when both sides of a
// field are present.
//
// Most callers want Match, which adds the weighted aggregate and the policy
// decision.
func DicePerField(a, b sriracha.ProbabilisticToken) ([]float64, error) {
	if err := compareMeta(
		tokenMeta{a.Format, a.FieldSetVersion, a.KeyID, a.FieldSetFingerprint},
		tokenMeta{b.Format, b.FieldSetVersion, b.KeyID, b.FieldSetFingerprint},
	); err != nil {
		return nil, err
	}
	if !bloomParamsEqual(a.ProbabilisticParams, b.ProbabilisticParams) {
		return nil, ErrParamsMismatch
	}
	if len(a.Fields) != len(b.Fields) {
		return nil, fmt.Errorf("%w: %d vs %d", ErrFieldCountMismatch, len(a.Fields), len(b.Fields))
	}
	scores := make([]float64, len(a.Fields))
	for i := range a.Fields {
		ai, bi := a.Fields[i], b.Fields[i]
		if !present(ai) || !present(bi) {
			continue
		}
		if len(ai) != len(bi) {
			return nil, fmt.Errorf("%w: field %d: %d vs %d", ErrFilterLengthMismatch, i, len(ai), len(bi))
		}
		scores[i] = dice(ai, bi)
	}
	return scores, nil
}

// bloomParamsEqual reports whether two ProbabilisticConfig values are field-for-field
// identical. ProbabilisticConfig contains a []int (NgramSizes) and so is not comparable
// with ==. Balanced changes the filter construction entirely, so tokens
// produced with different values are not comparable and must match here.
func bloomParamsEqual(a, b sriracha.ProbabilisticConfig) bool {
	return a.SizeBits == b.SizeBits &&
		a.HashCount == b.HashCount &&
		a.Balanced == b.Balanced &&
		slices.Equal(a.NgramSizes, b.NgramSizes)
}

// Match is the canonical entry point for per-field probabilistic comparison.
// It compares a and b under fs and returns the per-field Dice scores, the
// weighted aggregate Score, the evidence the pair supplied, and the decision
// under policy. When per-field scores are not required, prefer MatchCLK over
// CLK tokens, which reveal no per-field structure.
//
// Fields absent on both sides drop from the weighted average; asymmetric
// absence (present on one side, absent on the other) keeps its score of 0 at
// full weight and counts as a real mismatch signal. A field is absent when it
// has zero length, nil or not.
//
// Score is a weighted average over the fields the pair could be compared on,
// so it is only comparable across pairs with similar evidence mass. Two
// records sharing nothing but a country code agree perfectly on the one field
// they both carry and score 1.000, the same number a full name plus date of
// birth agreement produces. That is why policy carries an evidence floor:
// IsMatch is Score >= policy.Threshold AND ComparableFields >=
// policy.MinComparableFields AND ComparableWeight >=
// policy.MinComparableWeight. The floor gates IsMatch only. Score is never
// altered by it, and a pair that falls below it is a non-match, not an error.
// DefaultMatchPolicy supplies a floor of two fields; the zero MatchPolicy
// applies none.
//
// If every field is both-absent (or zero-weighted), the returned MatchResult
// has Score=0, IsMatch=false, ComparableFields=0, never an error. The error
// return is reserved for tokens that are not comparable: an invalid policy, or
// the drift conditions listed on DicePerField, plus a field count that
// disagrees with fs.
func Match(a, b sriracha.ProbabilisticToken, fs sriracha.FieldSet, policy MatchPolicy) (MatchResult, error) {
	if err := policy.validate(); err != nil {
		return MatchResult{}, err
	}
	perField, err := DicePerField(a, b)
	if err != nil {
		return MatchResult{}, err
	}
	if len(perField) != len(fs.Fields) {
		return MatchResult{}, fmt.Errorf("%w: %d fields vs FieldSet length %d",
			ErrFieldCountMismatch, len(perField), len(fs.Fields))
	}

	paths := make([]sriracha.FieldPath, len(fs.Fields))
	var sum, totalW float64
	comparable := 0
	for i, spec := range fs.Fields {
		paths[i] = spec.Path
		w := spec.Weight
		if w <= 0 || (!present(a.Fields[i]) && !present(b.Fields[i])) {
			continue
		}
		sum += w * perField[i]
		totalW += w
		comparable++
	}

	if comparable == 0 {
		return MatchResult{PerField: perField, Paths: paths}, nil
	}
	score := sum / totalW
	return MatchResult{
		Score:            score,
		PerField:         perField,
		Paths:            paths,
		IsMatch:          score >= policy.Threshold && comparable >= policy.MinComparableFields && totalW >= policy.MinComparableWeight,
		ComparableFields: comparable,
		ComparableWeight: totalW,
	}, nil
}

// MatchCLK compares two record-level CLK tokens and returns the Dice score
// with a threshold decision. It validates the threshold (NaN and
// out-of-range values are rejected), then the metadata under the rule
// documented on Equal, then ProbabilisticParams and filter byte length. Any
// difference returns a sentinel-wrapped error because the scores would not be
// comparable.
//
// The threshold is a bare float64 rather than a MatchPolicy because a CLK
// folds the whole record into one filter and has no field structure left. There
// is no per-field evidence to count, so there is no evidence floor to apply.
// A caller who needs one has to keep the field-level structure and use Match.
//
// Note that balanced filters lift the Dice score of unrelated records well
// above 0, toward a floor of about 0.5, so thresholds calibrated for
// unbalanced or per-field tokens do not transfer directly.
func MatchCLK(a, b sriracha.CLKToken, threshold float64) (CLKMatchResult, error) {
	if !(threshold >= 0 && threshold <= 1) {
		return CLKMatchResult{}, fmt.Errorf("%w, got %v", ErrInvalidThreshold, threshold)
	}
	if err := compareMeta(
		tokenMeta{a.Format, a.FieldSetVersion, a.KeyID, a.FieldSetFingerprint},
		tokenMeta{b.Format, b.FieldSetVersion, b.KeyID, b.FieldSetFingerprint},
	); err != nil {
		return CLKMatchResult{}, err
	}
	if !bloomParamsEqual(a.ProbabilisticParams, b.ProbabilisticParams) {
		return CLKMatchResult{}, ErrParamsMismatch
	}
	if len(a.Filter) != len(b.Filter) {
		return CLKMatchResult{}, fmt.Errorf("%w: %d vs %d", ErrFilterLengthMismatch, len(a.Filter), len(b.Filter))
	}
	score := dice(a.Filter, b.Filter)
	return CLKMatchResult{Score: score, IsMatch: score >= threshold}, nil
}

// dice computes the Sørensen–Dice coefficient over two equal-length bit-packed
// byte slices. Endianness of the underlying word layout is irrelevant: set
// bits remain set regardless of word order.
func dice(a, b []byte) float64 {
	var inter, ca, cb int
	for i := range a {
		inter += bits.OnesCount8(a[i] & b[i])
		ca += bits.OnesCount8(a[i])
		cb += bits.OnesCount8(b[i])
	}
	total := ca + cb
	if total == 0 {
		return 0
	}
	return 2.0 * float64(inter) / float64(total)
}
