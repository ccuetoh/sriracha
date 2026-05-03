package sriracha

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFingerprint_Determinism(t *testing.T) {
	t.Parallel()

	fs := FieldSet{
		Version: "1.0.0-test",
		Fields: []FieldSpec{
			{Path: FieldNameGiven, Required: true, Weight: 2.0},
			{Path: FieldNameFamily, Required: false, Weight: 2.5},
		},
		ProbabilisticParams: ProbabilisticConfig{SizeBits: 1024, NgramSizes: []int{2, 3}, HashCount: 2},
	}
	assert.Equal(t, fs.Fingerprint(), fs.Fingerprint(),
		"Fingerprint must be stable across calls on the same value")
}

func TestFingerprint_GoldenVector(t *testing.T) {
	t.Parallel()

	fs := FieldSet{
		Version: "v1",
		Fields: []FieldSpec{
			{Path: FieldNameGiven, Required: true, Weight: 1.0},
		},
		ProbabilisticParams: ProbabilisticConfig{SizeBits: 1024, NgramSizes: []int{2}, HashCount: 2},
	}
	const want = "d99c25e70d90b328d83991580082cccb64c22a07ec7df5f829d0b75bc7a06262"
	got := fs.Fingerprint()
	if got != want {
		t.Logf("If this golden vector changed intentionally, update the canonical encoding doc in fingerprint.go")
	}
	assert.Equal(t, want, got, "golden Fingerprint vector drifted — see fingerprint.go canonical encoding spec")
}

func TestFingerprint_SensitivityMatrix(t *testing.T) {
	t.Parallel()

	base := FieldSet{
		Version: "v1",
		Fields: []FieldSpec{
			{Path: FieldNameGiven, Required: true, Weight: 1.0},
			{Path: FieldNameFamily, Required: false, Weight: 2.0},
		},
		ProbabilisticParams: ProbabilisticConfig{SizeBits: 1024, NgramSizes: []int{2, 3}, HashCount: 2},
	}

	mutate := func(fn func(*FieldSet)) FieldSet {
		fs := FieldSet{
			Version:             base.Version,
			Fields:              append([]FieldSpec(nil), base.Fields...),
			ProbabilisticParams: base.ProbabilisticParams,
		}
		fs.ProbabilisticParams.NgramSizes = append([]int(nil), base.ProbabilisticParams.NgramSizes...)
		fn(&fs)
		return fs
	}

	cases := []struct {
		name string
		got  string
	}{
		{"VersionChange", mutate(func(fs *FieldSet) { fs.Version = "v2" }).Fingerprint()},
		{"FieldOrderSwap", mutate(func(fs *FieldSet) { fs.Fields[0], fs.Fields[1] = fs.Fields[1], fs.Fields[0] }).Fingerprint()},
		{"RequiredFlip", mutate(func(fs *FieldSet) { fs.Fields[0].Required = false }).Fingerprint()},
		{"WeightChange", mutate(func(fs *FieldSet) { fs.Fields[0].Weight = 1.5 }).Fingerprint()},
		{"PathChange", mutate(func(fs *FieldSet) { fs.Fields[0].Path = FieldNameFull }).Fingerprint()},
		{"BloomSizeChange", mutate(func(fs *FieldSet) { fs.ProbabilisticParams.SizeBits = 2048 }).Fingerprint()},
		{"BloomHashCountChange", mutate(func(fs *FieldSet) { fs.ProbabilisticParams.HashCount = 3 }).Fingerprint()},
		{"NgramSizesChange", mutate(func(fs *FieldSet) { fs.ProbabilisticParams.NgramSizes = []int{2} }).Fingerprint()},
	}

	baseFP := base.Fingerprint()
	seen := map[string]string{baseFP: "Base"}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.NotEqual(t, baseFP, tc.got, "expected different fingerprint after %s", tc.name)
		})
		if other, dup := seen[tc.got]; dup {
			t.Errorf("fingerprint collision: %s and %s both produced %s", tc.name, other, tc.got)
		}
		seen[tc.got] = tc.name
	}
}

func TestFingerprint_EmptyFieldSet(t *testing.T) {
	t.Parallel()

	fs := FieldSet{}
	got := fs.Fingerprint()
	assert.Len(t, got, 64, "Fingerprint must be 64 hex chars (SHA-256)")
}
