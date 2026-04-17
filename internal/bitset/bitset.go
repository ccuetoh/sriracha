package bitset

import (
	"encoding/binary"
	"fmt"
	"math/bits"
)

// Bitset is a fixed-size bitset backed by []uint64.
type Bitset struct {
	words []uint64
	size  int
}

// New returns a new Bitset with the given number of bits (all zero).
func New(nbits int) *Bitset {
	nwords := (nbits + 63) / 64
	return &Bitset{words: make([]uint64, nwords), size: nbits}
}

// Set sets the bit at position pos. Returns an error if pos is out of range.
func (b *Bitset) Set(pos int) error {
	if pos < 0 || pos >= b.size {
		return fmt.Errorf("bitset: Set position %d out of range [0, %d)", pos, b.size)
	}
	b.words[pos/64] |= 1 << uint(pos%64)
	return nil
}

// IsSet reports whether the bit at position pos is set.
// Returns an error if pos is out of range.
func (b *Bitset) IsSet(pos int) (bool, error) {
	if pos < 0 || pos >= b.size {
		return false, fmt.Errorf("bitset: IsSet position %d out of range [0, %d)", pos, b.size)
	}
	return b.words[pos/64]&(1<<uint(pos%64)) != 0, nil
}

// And returns a new Bitset that is the bitwise AND of a and b.
// Returns an error if a and b have different sizes.
func And(a, b *Bitset) (*Bitset, error) {
	if a.size != b.size {
		return nil, fmt.Errorf("bitset: And called on bitsets of different sizes (%d vs %d)", a.size, b.size)
	}
	out := New(a.size)
	for i := range out.words {
		out.words[i] = a.words[i] & b.words[i]
	}
	return out, nil
}

// Popcount returns the number of set bits in b.
func Popcount(b *Bitset) int {
	n := 0
	for _, w := range b.words {
		n += bits.OnesCount64(w)
	}
	return n
}

// ToBytes serialises the bitset as big-endian uint64 words.
func (b *Bitset) ToBytes() []byte {
	out := make([]byte, len(b.words)*8)
	for i, w := range b.words {
		binary.BigEndian.PutUint64(out[i*8:], w)
	}
	return out
}

// FromBytes deserialises a bitset from bytes produced by ToBytes.
// The restored bitset has len(data)*8 bits (always word-aligned).
// Returns an error if len(data) is not a multiple of 8.
func FromBytes(data []byte) (*Bitset, error) {
	if len(data)%8 != 0 {
		return nil, fmt.Errorf("bitset: FromBytes requires length divisible by 8, got %d", len(data))
	}
	nwords := len(data) / 8
	b := &Bitset{words: make([]uint64, nwords), size: nwords * 64}
	for i := range nwords {
		b.words[i] = binary.BigEndian.Uint64(data[i*8:])
	}
	return b, nil
}
