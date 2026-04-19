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
// with all 16 standard fields. Weights are relative; the probabilistic scoring
// formula normalizes by their sum.
func DefaultFieldSet() sriracha.FieldSet {
	fields := make([]sriracha.FieldSpec, len(defaultV01.Fields))
	copy(fields, defaultV01.Fields)

	return sriracha.FieldSet{
		Version:     defaultV01.Version,
		Fields:      fields,
		BloomParams: defaultV01.BloomParams,
	}
}
