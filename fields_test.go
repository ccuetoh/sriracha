package sriracha

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

	assert.True(t, FieldNameGiven.InNamespace(NamespaceName), "FieldNameGiven should be in namespace 'name'")
	assert.False(t, FieldNameGiven.InNamespace(NamespaceDate), "FieldNameGiven should not be in namespace 'date'")
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

func TestFieldPath_JSON(t *testing.T) {
	t.Parallel()

	t.Run("RoundTripAsStructField", func(t *testing.T) {
		t.Parallel()
		spec := FieldSpec{Path: FieldNameGiven, Required: true, Weight: 2.5}
		data, err := json.Marshal(spec)
		require.NoError(t, err)
		assert.Contains(t, string(data), `"path":"sriracha::name::given"`,
			"FieldPath must marshal to its canonical string form, not {}")

		var got FieldSpec
		require.NoError(t, json.Unmarshal(data, &got))
		assert.Equal(t, spec, got)
	})

	t.Run("RoundTripAsMapKey", func(t *testing.T) {
		t.Parallel()
		// RawRecord is map[FieldPath]string — without TextMarshaler this would
		// fail at marshal time. Confirms TextMarshaler covers map-key usage.
		rec := RawRecord{
			FieldNameGiven:  "Alice",
			FieldNameFamily: "Smith",
		}
		data, err := json.Marshal(rec)
		require.NoError(t, err)
		assert.Contains(t, string(data), `"sriracha::name::given":"Alice"`)

		var got RawRecord
		require.NoError(t, json.Unmarshal(data, &got))
		assert.Equal(t, rec, got)
	})

	t.Run("ZeroValueRoundTrips", func(t *testing.T) {
		t.Parallel()
		var fp FieldPath
		data, err := json.Marshal(fp)
		require.NoError(t, err)
		assert.Equal(t, `""`, string(data))

		var got FieldPath
		require.NoError(t, json.Unmarshal(data, &got))
		assert.Equal(t, fp, got)
	})

	t.Run("MalformedRejected", func(t *testing.T) {
		t.Parallel()
		var fp FieldPath
		err := json.Unmarshal([]byte(`"not-a-valid-path"`), &fp)
		require.Error(t, err)
	})

	t.Run("NonStringRejected", func(t *testing.T) {
		t.Parallel()
		var fp FieldPath
		err := json.Unmarshal([]byte(`42`), &fp)
		require.Error(t, err)
	})
}

// FuzzParseFieldPath verifies that ParseFieldPath never panics and that any
// successfully parsed path survives a String() → ParseFieldPath roundtrip.
func FuzzParseFieldPath(f *testing.F) {
	f.Add("sriracha::name::given")
	f.Add("myorg::identifier::employee_id")
	f.Add("")
	f.Add("::")
	f.Add(":::")
	f.Add("a::b::c")

	f.Fuzz(func(t *testing.T, s string) {
		fp, err := ParseFieldPath(s)
		// Skip inputs that legitimately fail validation; the property under
		// test is the roundtrip on successful parses.
		if err != nil {
			return
		}
		// Roundtrip: String() must re-parse to an identical FieldPath.
		fp2, err2 := ParseFieldPath(fp.String())
		if err2 != nil {
			t.Fatalf("ParseFieldPath(%q).String() = %q, failed to re-parse: %v", s, fp.String(), err2)
		}
		if fp.String() != fp2.String() {
			t.Fatalf("roundtrip mismatch: %q → %q → %q", s, fp.String(), fp2.String())
		}
		if fp.Org() != fp2.Org() || fp.Namespace() != fp2.Namespace() || fp.LocalName() != fp2.LocalName() {
			t.Fatalf("component mismatch after roundtrip for %q", s)
		}
	})
}
