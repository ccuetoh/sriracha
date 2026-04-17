package sriracha

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultBloomConfig(t *testing.T) {
	t.Parallel()

	cfg := DefaultBloomConfig()
	assert.Equal(t, uint32(1024), cfg.SizeBits, "SizeBits")
	assert.Equal(t, []int{2, 3}, cfg.NgramSizes, "NgramSizes")
	assert.Equal(t, 2, cfg.HashCount, "HashCount")
}

func TestMatchModeConstants(t *testing.T) {
	t.Parallel()

	assert.Equal(t, MatchMode(1), Deterministic, "Deterministic")
	assert.Equal(t, MatchMode(2), Probabilistic, "Probabilistic")
}
