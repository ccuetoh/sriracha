package sriracha

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSentinels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		input       string
		wantFound   bool
		wantNotHeld bool
	}{
		{name: "empty string", input: "", wantFound: false, wantNotHeld: false},
		{name: "arbitrary value", input: "some value", wantFound: false, wantNotHeld: false},
		{name: "NotHeld sentinel", input: string(NotHeld), wantFound: false, wantNotHeld: true},
		{name: "NotFound sentinel", input: string(NotFound), wantFound: true, wantNotHeld: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.wantFound, IsNotFound(tt.input), "IsNotFound(%q)", tt.input)
			assert.Equal(t, tt.wantNotHeld, IsNotHeld(tt.input), "IsNotHeld(%q)", tt.input)
		})
	}
}

func TestFieldPathComponents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		path          FieldPath
		wantNamespace string
		wantLocalName string
	}{
		{FieldNameGiven, "name", "given"},
		{FieldDateBirth, "date", "birth"},
		{FieldIdentifierNationalID, "identifier", "national_id"},
		{FieldAddressCountry, "address", "country"},
		{FieldContactEmail, "contact", "email"},
	}

	for _, tt := range tests {
		t.Run(tt.path.String(), func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.wantNamespace, tt.path.Namespace(), "Namespace()")
			assert.Equal(t, tt.wantLocalName, tt.path.LocalName(), "LocalName()")
		})
	}
}

func TestFieldPathInNamespace(t *testing.T) {
	t.Parallel()

	assert.True(t, FieldNameGiven.InNamespace("name"), "FieldNameGiven should be in namespace 'name'")
	assert.False(t, FieldNameGiven.InNamespace("date"), "FieldNameGiven should not be in namespace 'date'")
}

func TestMustParsePath_Panics(t *testing.T) {
	t.Parallel()

	defer func() {
		if r := recover(); r == nil {
			t.Error("MustParsePath with invalid input should panic")
		}
	}()
	MustParsePath("not-a-valid-path")
}

func TestParseFieldPath(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		input     string
		wantErr   bool
		wantOrg   string
		wantNS    string
		wantLocal string
	}{
		{"valid", "sriracha::name::given", false, "sriracha", "name", "given"},
		{"custom org", "myorg::identifier::employee_id", false, "myorg", "identifier", "employee_id"},
		{"missing third component", "sriracha::name", true, "", "", ""},
		{"empty org", "::name::given", true, "", "", ""},
		{"empty string", "", true, "", "", ""},
		{"empty namespace", "sriracha::::given", true, "", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fp, err := ParseFieldPath(tc.input)
			if tc.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.wantOrg, fp.Org(), "Org()")
			assert.Equal(t, tc.wantNS, fp.Namespace(), "Namespace()")
			assert.Equal(t, tc.wantLocal, fp.LocalName(), "LocalName()")
		})
	}
}
