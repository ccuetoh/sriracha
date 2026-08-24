package sriracha

import (
	"fmt"
	"strings"
)

// FieldPath identifies a field within the Sriracha schema.
// Paths follow the format: <org>::<namespace>::<name>
// where <org> is "sriracha" for canonical fields or a custom organisation identifier.
type FieldPath struct {
	raw       string
	org       string
	namespace string
	localName string
}

// String returns the canonical string form of the path.
func (f FieldPath) String() string { return f.raw }

// Org returns the organisation component of the path (e.g. "sriracha").
func (f FieldPath) Org() string { return f.org }

// Namespace returns the namespace component of the path (e.g. "identifier", "name", "date").
func (f FieldPath) Namespace() string { return f.namespace }

// LocalName returns the local name component of the path (e.g. "national_id", "given").
func (f FieldPath) LocalName() string { return f.localName }

// InNamespace reports whether the field belongs to the given namespace.
func (f FieldPath) InNamespace(ns string) bool { return f.namespace == ns }

// IsCanonical reports whether the field is part of the canonical Sriracha
// schema, that is whether its org component is OrgSriracha. Paths scoped to
// another organisation are not canonical and carry no guarantee about how
// their namespace or local name is interpreted.
func (f FieldPath) IsCanonical() bool { return f.org == OrgSriracha }

// ParseFieldPath parses and validates a field path string.
// Valid paths have the form <org>::<namespace>::<name> with all three
// components non-empty. Returns an error for malformed paths.
func ParseFieldPath(s string) (FieldPath, error) {
	parts := strings.SplitN(s, "::", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return FieldPath{}, fmt.Errorf("%w %q: must be <org>::<namespace>::<name>", ErrInvalidFieldPath, s)
	}

	return FieldPath{raw: s, org: parts[0], namespace: parts[1], localName: parts[2]}, nil
}

// MustParseFieldPath parses a field path string and panics if it is invalid.
// Intended for package-level variable declarations where the path is a
// compile-time constant. Use ParseFieldPath for runtime input.
func MustParseFieldPath(s string) FieldPath {
	fp, err := ParseFieldPath(s)
	if err != nil {
		panic(err)
	}

	return fp
}

// MarshalText emits the canonical string form of f. Implementing
// TextMarshaler (rather than MarshalJSON) means FieldPath round-trips both as
// a struct field and as a RawRecord map key under encoding/json.
func (f FieldPath) MarshalText() ([]byte, error) {
	return []byte(f.raw), nil
}

// UnmarshalText parses data with ParseFieldPath. Empty input yields the zero
// FieldPath (so a struct that marshals a zero FieldPath round-trips), but any
// other malformed input is rejected.
func (f *FieldPath) UnmarshalText(data []byte) error {
	if len(data) == 0 {
		*f = FieldPath{}
		return nil
	}
	parsed, err := ParseFieldPath(string(data))
	if err != nil {
		return err
	}
	*f = parsed
	return nil
}

// OrgSriracha is the org component of every canonical field path. Paths
// carrying any other org belong to the declaring organisation.
const OrgSriracha = "sriracha"

// Canonical namespace identifiers.
const (
	NamespaceName       = "name"
	NamespaceIdentifier = "identifier"
	NamespaceDate       = "date"
	NamespaceAddress    = "address"
	NamespaceContact    = "contact"
)

// Canonical field path variables.
var (
	FieldIdentifierNationalID = MustParseFieldPath("sriracha::identifier::national_id")
	FieldIdentifierPassport   = MustParseFieldPath("sriracha::identifier::passport")
	FieldIdentifierTaxID      = MustParseFieldPath("sriracha::identifier::tax_id")
	FieldNameGiven            = MustParseFieldPath("sriracha::name::given")
	FieldNameFamily           = MustParseFieldPath("sriracha::name::family")
	FieldNameFull             = MustParseFieldPath("sriracha::name::full")
	FieldNameMiddle           = MustParseFieldPath("sriracha::name::middle")
	FieldDateBirth            = MustParseFieldPath("sriracha::date::birth")
	FieldDateDeath            = MustParseFieldPath("sriracha::date::death")
	FieldDateRegistration     = MustParseFieldPath("sriracha::date::registration")
	FieldAddressCountry       = MustParseFieldPath("sriracha::address::country")
	FieldAddressAdminArea     = MustParseFieldPath("sriracha::address::admin_area")
	FieldAddressLocality      = MustParseFieldPath("sriracha::address::locality")
	FieldAddressPostalCode    = MustParseFieldPath("sriracha::address::postal_code")
	FieldContactEmail         = MustParseFieldPath("sriracha::contact::email")
	FieldContactPhone         = MustParseFieldPath("sriracha::contact::phone")
)
