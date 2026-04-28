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

// ParseFieldPath parses and validates a field path string.
// Valid paths have the form <org>::<namespace>::<name> with all three
// components non-empty. Returns an error for malformed paths.
func ParseFieldPath(s string) (FieldPath, error) {
	parts := strings.SplitN(s, "::", 3)
	if len(parts) != 3 || parts[0] == "" || parts[1] == "" || parts[2] == "" {
		return FieldPath{}, fmt.Errorf("fieldpath: invalid path %q: must be <org>::<namespace>::<name>", s)
	}

	return FieldPath{raw: s, org: parts[0], namespace: parts[1], localName: parts[2]}, nil
}

// MustParsePath parses a field path string and panics if it is invalid.
// Intended for package-level variable declarations where the path is a
// compile-time constant. Use ParseFieldPath for runtime input.
func MustParsePath(s string) FieldPath {
	fp, err := ParseFieldPath(s)
	if err != nil {
		panic(err)
	}

	return fp
}

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
	FieldIdentifierNationalID = MustParsePath("sriracha::identifier::national_id")
	FieldIdentifierPassport   = MustParsePath("sriracha::identifier::passport")
	FieldIdentifierTaxID      = MustParsePath("sriracha::identifier::tax_id")
	FieldNameGiven            = MustParsePath("sriracha::name::given")
	FieldNameFamily           = MustParsePath("sriracha::name::family")
	FieldNameFull             = MustParsePath("sriracha::name::full")
	FieldNameMiddle           = MustParsePath("sriracha::name::middle")
	FieldDateBirth            = MustParsePath("sriracha::date::birth")
	FieldDateDeath            = MustParsePath("sriracha::date::death")
	FieldDateRegistration     = MustParsePath("sriracha::date::registration")
	FieldAddressCountry       = MustParsePath("sriracha::address::country")
	FieldAddressAdminArea     = MustParsePath("sriracha::address::admin_area")
	FieldAddressLocality      = MustParsePath("sriracha::address::locality")
	FieldAddressPostalCode    = MustParsePath("sriracha::address::postal_code")
	FieldContactEmail         = MustParsePath("sriracha::contact::email")
	FieldContactPhone         = MustParsePath("sriracha::contact::phone")
)
