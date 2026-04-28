package fieldset

import (
	"fmt"
	"sort"
	"sync"

	"go.sriracha.dev/sriracha"
)

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

var (
	registryMu sync.RWMutex
	registry   = map[string]sriracha.FieldSet{}
)

func init() {
	// defaultV01 is hard-coded and known valid; insert it directly so init
	// has no failure mode and Register remains free for runtime use.
	registry[defaultV01.Version] = defaultV01Copy()
}

// Register validates fs and stores a deep copy under fs.Version.
// Returns an error if fs fails validation or if fs.Version is already
// registered. Safe for concurrent use.
func Register(fs sriracha.FieldSet) error {
	if err := Validate(fs); err != nil {
		return err
	}

	registryMu.Lock()
	defer registryMu.Unlock()

	if _, exists := registry[fs.Version]; exists {
		return fmt.Errorf("fieldset: version %q already registered", fs.Version)
	}
	registry[fs.Version] = deepCopy(fs)
	return nil
}

// Lookup returns a deep copy of the FieldSet registered under version, and
// reports whether one was found. Safe for concurrent use.
func Lookup(version string) (sriracha.FieldSet, bool) {
	registryMu.RLock()
	defer registryMu.RUnlock()

	fs, ok := registry[version]
	if !ok {
		return sriracha.FieldSet{}, false
	}
	return deepCopy(fs), true
}

// Versions returns the sorted list of registered FieldSet versions.
func Versions() []string {
	registryMu.RLock()
	defer registryMu.RUnlock()

	out := make([]string, 0, len(registry))
	for v := range registry {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

// DefaultFieldSet returns a deep copy of the canonical Sriracha v0.1 FieldSet
// with all 16 standard fields. Weights are relative; the probabilistic scoring
// formula normalizes by their sum.
func DefaultFieldSet() sriracha.FieldSet {
	return defaultV01Copy()
}

// defaultV01Copy returns a deep copy of the canonical v0.1 field set, used
// once at init time to seed the registry.
func defaultV01Copy() sriracha.FieldSet {
	return deepCopy(defaultV01)
}

// deepCopy clones a FieldSet so the registry's stored copy and the value
// returned to callers cannot mutate one another.
func deepCopy(fs sriracha.FieldSet) sriracha.FieldSet {
	fields := make([]sriracha.FieldSpec, len(fs.Fields))
	copy(fields, fs.Fields)

	bp := fs.BloomParams
	bp.NgramSizes = append([]int(nil), fs.BloomParams.NgramSizes...)

	return sriracha.FieldSet{
		Version:     fs.Version,
		Fields:      fields,
		BloomParams: bp,
	}
}
