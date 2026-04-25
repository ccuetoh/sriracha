package server

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"go.sriracha.dev/sriracha"
)

func TestBuildFieldValuesInvalidPath(t *testing.T) {
	t.Parallel()
	record := sriracha.RawRecord{}
	_, notFound := buildFieldValues(record, []string{"not-a-valid-path"})
	assert.Equal(t, []string{"not-a-valid-path"}, notFound)
}

func BenchmarkBuildFieldValues(b *testing.B) {
	record := sriracha.RawRecord{
		sriracha.FieldNameGiven:  "Alice",
		sriracha.FieldNameFamily: "Smith",
	}
	paths := []string{
		sriracha.FieldNameGiven.String(),
		sriracha.FieldNameFamily.String(),
		sriracha.FieldDateBirth.String(),
		"sriracha::invalid::path",
	}
	b.ResetTimer()
	for range b.N {
		_, _ = buildFieldValues(record, paths)
	}
}

func FuzzBuildFieldValues(f *testing.F) {
	f.Add(sriracha.FieldNameGiven.String())
	f.Add("sriracha::name::family")
	f.Add("invalid-path")
	f.Add("")
	f.Fuzz(func(t *testing.T, path string) {
		// Must never panic regardless of input.
		_, _ = buildFieldValues(sriracha.RawRecord{}, []string{path})
	})
}
