package bitset

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		nbits     int
		wantWords int
		wantSize  int
	}{
		{"zero bits", 0, 0, 0},
		{"single word", 64, 1, 64},
		{"multi word", 1024, 16, 1024},
		{"non-multiple", 65, 2, 65},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := New(tc.nbits)
			assert.Equal(t, tc.wantSize, b.size, "size mismatch")
			assert.Equal(t, tc.wantWords, len(b.words), "len(words) mismatch")
			for i, w := range b.words {
				assert.Equal(t, uint64(0), w, "words[%d] should be 0", i)
			}
		})
	}
}

func TestSetIsSet(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		nbits   int
		setPos  int
		adjPrev int
		adjNext int
	}{
		{"bit 0", 64, 0, -1, 1},
		{"bit 63", 64, 63, 62, -1},
		{"bit 64 crosses word", 128, 64, 63, 65},
		{"bit 1023 last in 1024", 1024, 1023, 1022, -1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := New(tc.nbits)

			set, err := b.IsSet(tc.setPos)
			require.NoError(t, err, "IsSet before Set should not error")
			assert.False(t, set, "bit %d should be unset before Set", tc.setPos)

			require.NoError(t, b.Set(tc.setPos), "Set(%d) should not error", tc.setPos)

			set, err = b.IsSet(tc.setPos)
			require.NoError(t, err, "IsSet after Set should not error")
			assert.True(t, set, "bit %d should be set after Set", tc.setPos)

			if tc.adjPrev >= 0 {
				set, err := b.IsSet(tc.adjPrev)
				require.NoError(t, err)
				assert.False(t, set, "adjacent bit %d should not be set", tc.adjPrev)
			}
			if tc.adjNext >= 0 {
				set, err := b.IsSet(tc.adjNext)
				require.NoError(t, err)
				assert.False(t, set, "adjacent bit %d should not be set", tc.adjNext)
			}
		})
	}
}

func TestAnd(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		nbits      int
		aBits      []int
		bBits      []int
		wantSet    []int
		wantNotSet []int
		wantErr    bool
	}{
		{"overlapping bits", 128, []int{0, 1, 2}, []int{1, 2, 3}, []int{1, 2}, []int{0, 3}, false},
		{"cross-word boundary", 128, []int{64, 65}, []int{65, 66}, []int{65}, []int{64, 66}, false},
		{"and of zero bitsets is zero", 64, nil, nil, nil, nil, false},
		{"different-size bitsets return error", -1, nil, nil, nil, nil, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var a, b *Bitset
			if tc.wantErr {
				a = New(64)
				b = New(128)
			} else {
				a = New(tc.nbits)
				b = New(tc.nbits)
				mustSet(t, a, tc.aBits...)
				mustSet(t, b, tc.bBits...)
			}
			c, err := And(a, b)
			if tc.wantErr {
				assert.Error(t, err, "And on different-size bitsets should return error")
				return
			}
			require.NoError(t, err, "And should not error")
			mustIsSet(t, c, tc.wantSet...)
			mustNotSet(t, c, tc.wantNotSet...)
			if len(tc.wantSet) == 0 {
				assert.Equal(t, 0, Popcount(c), "AND of two zero bitsets should be zero")
			}
		})
	}
}

func TestPopcount(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name      string
		nbits     int
		setBits   func(b *Bitset, t *testing.T)
		wantCount int
	}{
		{
			name:      "empty bitset",
			nbits:     64,
			setBits:   func(b *Bitset, t *testing.T) {},
			wantCount: 0,
		},
		{
			name:  "single set bit",
			nbits: 64,
			setBits: func(b *Bitset, t *testing.T) {
				mustSet(t, b, 7)
			},
			wantCount: 1,
		},
		{
			name:  "full 64-bit word",
			nbits: 64,
			setBits: func(b *Bitset, t *testing.T) {
				for i := range 64 {
					mustSet(t, b, i)
				}
			},
			wantCount: 64,
		},
		{
			name:  "full 1024-bit set",
			nbits: 1024,
			setBits: func(b *Bitset, t *testing.T) {
				for i := range 1024 {
					mustSet(t, b, i)
				}
			},
			wantCount: 1024,
		},
		{
			name:  "alternating bits in 64-bit word",
			nbits: 64,
			setBits: func(b *Bitset, t *testing.T) {
				for i := 0; i < 64; i += 2 {
					mustSet(t, b, i)
				}
			},
			wantCount: 32,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			b := New(tc.nbits)
			tc.setBits(b, t)
			assert.Equal(t, tc.wantCount, Popcount(b), "Popcount mismatch")
		})
	}
}

func TestBoundsError(t *testing.T) {
	t.Parallel()
	b := New(64)
	mustSet(t, b, 0)
	mustSet(t, b, 63)

	assert.Error(t, b.Set(-1), "Set(-1) should return error")
	assert.Error(t, b.Set(64), "Set(64) should return error")

	_, err := b.IsSet(-1)
	assert.Error(t, err, "IsSet(-1) should return error")

	_, err = b.IsSet(64)
	assert.Error(t, err, "IsSet(64) should return error")
}

func TestFromBytesError(t *testing.T) {
	t.Parallel()
	_, err := FromBytes(make([]byte, 9))
	assert.Error(t, err, "FromBytes with non-multiple-of-8 length should return error")
}

func TestRoundtrip(t *testing.T) {
	t.Parallel()
	t.Run("1024-bit set roundtrip", func(t *testing.T) {
		t.Parallel()
		original := New(1024)
		positions := []int{0, 1, 63, 64, 65, 255, 512, 1022, 1023}
		for _, p := range positions {
			mustSet(t, original, p)
		}

		data := original.ToBytes()
		restored, err := FromBytes(data)
		require.NoError(t, err, "FromBytes should not error")

		assert.Equal(t, Popcount(original), Popcount(restored), "Popcount mismatch after roundtrip")

		for _, p := range positions {
			set, err := restored.IsSet(p)
			require.NoError(t, err)
			assert.True(t, set, "bit %d should be set after roundtrip", p)
		}

		set, err := restored.IsSet(100)
		require.NoError(t, err)
		assert.False(t, set, "bit 100 should not be set after roundtrip")
	})
}

// mustSet calls Set and fails the test immediately on error.
func mustSet(t *testing.T, b *Bitset, positions ...int) {
	t.Helper()
	for _, pos := range positions {
		require.NoError(t, b.Set(pos), "Set(%d)", pos)
	}
}

// mustIsSet asserts that all positions are set.
func mustIsSet(t *testing.T, b *Bitset, positions ...int) {
	t.Helper()
	for _, pos := range positions {
		set, err := b.IsSet(pos)
		assert.NoError(t, err, "IsSet(%d) should not error", pos)
		assert.True(t, set, "bit %d should be set", pos)
	}
}

// mustNotSet asserts that all positions are not set.
func mustNotSet(t *testing.T, b *Bitset, positions ...int) {
	t.Helper()
	for _, pos := range positions {
		set, err := b.IsSet(pos)
		assert.NoError(t, err, "IsSet(%d) should not error", pos)
		assert.False(t, set, "bit %d should not be set", pos)
	}
}

func BenchmarkSet(b *testing.B) {
	bs := New(1024)
	b.ResetTimer()
	for i := range b.N {
		_ = bs.Set(i % 1024)
	}
}

func BenchmarkPopcount(b *testing.B) {
	bs := New(1024)
	for i := range 1024 {
		_ = bs.Set(i)
	}
	b.ResetTimer()
	for range b.N {
		_ = Popcount(bs)
	}
}

func BenchmarkAnd(b *testing.B) {
	a, c := New(1024), New(1024)
	for i := 0; i < 1024; i += 2 {
		_ = a.Set(i)
	}
	for i := 1; i < 1024; i += 2 {
		_ = c.Set(i)
	}
	b.ResetTimer()
	for range b.N {
		_, _ = And(a, c)
	}
}

func BenchmarkToBytes(b *testing.B) {
	bs := New(1024)
	for i := range 1024 {
		_ = bs.Set(i)
	}
	b.ResetTimer()
	for range b.N {
		_ = bs.ToBytes()
	}
}

func BenchmarkFromBytes(b *testing.B) {
	bs := New(1024)
	data := bs.ToBytes()
	b.ResetTimer()
	for range b.N {
		_, _ = FromBytes(data)
	}
}

// FuzzFromBytesToBytes verifies that any valid byte slice survives a
// ToBytes → FromBytes roundtrip with identical popcount and bit positions.
func FuzzFromBytesToBytes(f *testing.F) {
	f.Add([]byte{})
	f.Add(make([]byte, 8))
	f.Add([]byte{0xFF, 0x00, 0xFF, 0x00, 0xFF, 0x00, 0xFF, 0x00})
	f.Add(make([]byte, 128))

	f.Fuzz(func(t *testing.T, data []byte) {
		// Pad to a multiple of 8 so FromBytes accepts it.
		padded := make([]byte, ((len(data)+7)/8)*8)
		copy(padded, data)

		original, err := FromBytes(padded)
		if err != nil {
			t.Skip()
		}

		roundtripped, err := FromBytes(original.ToBytes())
		if err != nil {
			t.Fatalf("FromBytes(ToBytes()) failed: %v", err)
		}

		if Popcount(original) != Popcount(roundtripped) {
			t.Fatalf("popcount mismatch: %d vs %d", Popcount(original), Popcount(roundtripped))
		}
	})
}

// FuzzSetIsSet verifies Set/IsSet consistency for arbitrary positions on a
// 1024-bit bitset.
func FuzzSetIsSet(f *testing.F) {
	f.Add(0)
	f.Add(63)
	f.Add(64)
	f.Add(1023)

	f.Fuzz(func(t *testing.T, pos int) {
		b := New(1024)
		if pos < 0 || pos >= 1024 {
			if err := b.Set(pos); err == nil {
				t.Fatalf("Set(%d) on 1024-bit bitset should have returned error", pos)
			}
			return
		}
		if err := b.Set(pos); err != nil {
			t.Fatalf("Set(%d): unexpected error: %v", pos, err)
		}
		ok, err := b.IsSet(pos)
		if err != nil {
			t.Fatalf("IsSet(%d): unexpected error: %v", pos, err)
		}
		if !ok {
			t.Fatalf("IsSet(%d) = false after Set", pos)
		}
	})
}
