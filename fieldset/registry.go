package fieldset

import "go.sriracha.dev/sriracha"

var defaultV01 = sriracha.FieldSet{
	Version: "0.1",
	Fields: []sriracha.FieldSpec{
		{Path: sriracha.FieldIdentifierNationalID, Required: false, Weight: 3.0},
		{Path: sriracha.FieldIdentifierPassport, Required: false, Weight: 3.0},
		{Path: sriracha.FieldIdentifierTaxID, Required: false, Weight: 2.5},
		{Path: sriracha.FieldNameGiven, Required: false, Weight: 2.0},
		{Path: sriracha.FieldNameFamily, Required: false, Weight: 2.5},
		{Path: sriracha.FieldNameFull, Required: false, Weight: 1.5},
		{Path: sriracha.FieldNameMiddle, Required: false, Weight: 1.0},
		{Path: sriracha.FieldDateBirth, Required: false, Weight: 2.0},
		{Path: sriracha.FieldDateDeath, Required: false, Weight: 1.0},
		{Path: sriracha.FieldDateRegistration, Required: false, Weight: 0.5},
		{Path: sriracha.FieldAddressCountry, Required: false, Weight: 0.5},
		{Path: sriracha.FieldAddressAdminArea, Required: false, Weight: 0.5},
		{Path: sriracha.FieldAddressLocality, Required: false, Weight: 1.0},
		{Path: sriracha.FieldAddressPostalCode, Required: false, Weight: 1.0},
		{Path: sriracha.FieldContactEmail, Required: false, Weight: 2.0},
		{Path: sriracha.FieldContactPhone, Required: false, Weight: 1.5},
	},
	BloomParams: sriracha.DefaultBloomConfig(),
}

// DefaultFieldSet returns a deep copy of the canonical Sriracha v0.1 FieldSet
// with all 16 standard fields.
//
// Weights are unitless relative magnitudes used as the denominator in the
// weighted average computed by token.Score. They are not probabilities or
// information-content estimates — only their ratios matter, so doubling
// every weight produces identical scores. The defaults below are tuned for
// the typical PPRL trade-off (high-uniqueness identifiers > stable name
// fields > address > optional contact > low-information geo):
//
//   - 3.0 — national_id, passport: high cardinality, near-unique per person
//   - 2.5 — tax_id, family name: high cardinality, frequently disagrees
//     across institutions only via typo
//   - 2.0 — given name, date of birth, email: stable, fairly unique
//   - 1.5 — full name, phone: noisier formatting / lower cardinality
//   - 1.0 — middle name, date of death, locality, postal code
//   - 0.5 — country, admin area, registration date: low information / heavy
//     ties across populations
//
// Tune them for your population by running token.Calibrate against a labeled
// pair set; the defaults are a reasonable starting point, not a tuned
// answer.
func DefaultFieldSet() sriracha.FieldSet {
	fields := make([]sriracha.FieldSpec, len(defaultV01.Fields))
	copy(fields, defaultV01.Fields)

	bp := defaultV01.BloomParams
	bp.NgramSizes = append([]int(nil), defaultV01.BloomParams.NgramSizes...)

	return sriracha.FieldSet{
		Version:     defaultV01.Version,
		Fields:      fields,
		BloomParams: bp,
	}
}
