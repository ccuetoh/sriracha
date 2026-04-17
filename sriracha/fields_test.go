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
		tt := tt
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
		tt := tt
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

	t.Run("valid", func(t *testing.T) {
		t.Parallel()

		fp, err := ParseFieldPath("sriracha::name::given")
		require.NoError(t, err)
		assert.Equal(t, "sriracha", fp.Org(), "Org()")
		assert.Equal(t, "name", fp.Namespace(), "Namespace()")
		assert.Equal(t, "given", fp.LocalName(), "LocalName()")
		assert.Equal(t, "sriracha::name::given", fp.String(), "String()")
	})

	t.Run("custom org", func(t *testing.T) {
		t.Parallel()

		fp, err := ParseFieldPath("myorg::identifier::employee_id")
		require.NoError(t, err)
		assert.Equal(t, "myorg", fp.Org(), "Org()")
	})

	t.Run("missing third component", func(t *testing.T) {
		t.Parallel()

		_, err := ParseFieldPath("sriracha::name")
		assert.Error(t, err, "expected error for path with only 2 components")
	})

	t.Run("empty org", func(t *testing.T) {
		t.Parallel()

		_, err := ParseFieldPath("::name::given")
		assert.Error(t, err, "expected error for empty org")
	})

	t.Run("empty string", func(t *testing.T) {
		t.Parallel()

		_, err := ParseFieldPath("")
		assert.Error(t, err, "expected error for empty string")
	})

	t.Run("empty namespace", func(t *testing.T) {
		t.Parallel()

		_, err := ParseFieldPath("sriracha::::given")
		assert.Error(t, err, "expected error for empty namespace")
	})
}
