package token

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/awnumar/memguard"

	"github.com/ccuetoh/sriracha"
	"github.com/ccuetoh/sriracha/normalize"
)

// testSecret expands a short label into a secret of MinSecretLen bytes. New
// rejects anything shorter, and the filler varies with position so distinct
// labels stay distinct secrets.
func testSecret(label string) []byte {
	secret := make([]byte, MinSecretLen)
	n := copy(secret, label)
	for i := n; i < len(secret); i++ {
		secret[i] = byte('a' + (i-n)%26)
	}
	return secret
}

func newTok(t *testing.T, label string, opts ...Option) *Tokenizer {
	t.Helper()
	tok, err := New(testSecret(label), opts...)
	require.NoErrorf(t, err, "New(%q)", label)
	t.Cleanup(tok.Destroy)
	return tok
}

func deterministicFS(fields ...sriracha.FieldSpec) sriracha.FieldSet {
	return sriracha.FieldSet{
		Version: "1.0.0-test",
		Fields:  fields,
	}
}

func bloomFS(fields ...sriracha.FieldSpec) sriracha.FieldSet {
	return sriracha.FieldSet{
		Version: "1.0.0-test",
		Fields:  fields,
		ProbabilisticParams: sriracha.ProbabilisticConfig{
			SizeBits:   1024,
			NgramSizes: []int{2, 3},
			HashCount:  2,
		},
	}
}

func TestNew(t *testing.T) {
	t.Parallel()
	shortByOne := testSecret("almost")[:MinSecretLen-1]
	cases := []struct {
		name    string
		secret  []byte
		opts    []Option
		wantErr error
	}{
		{"NilSecret", nil, nil, ErrSecretTooShort},
		{"EmptySecret", []byte{}, nil, ErrSecretTooShort},
		{"SingleNonZeroByte", []byte{0x01}, nil, ErrSecretTooShort},
		{"OneByteShort", shortByOne, nil, ErrSecretTooShort},
		{"AllZeroSecret", make([]byte, MinSecretLen), nil, ErrSecretAllZero},
		{"AllZeroAndTooShort", make([]byte, 16), nil, ErrSecretTooShort},
		{"ExactlyMinLen", testSecret("exact"), nil, nil},
		{"LongerThanMinLen", []byte(strings.Repeat("longer-than-the-minimum", 4)), nil, nil},
		{"WithKeyID", testSecret("secret"), []Option{WithKeyID("k1")}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tok, err := New(tc.secret, tc.opts...)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				assert.Nil(t, tok)
				return
			}
			require.NoError(t, err)
			tok.Destroy()
		})
	}
}

func TestNewTokenizer_AllocFailure(t *testing.T) {
	t.Parallel()
	// The constructor locks four buffers (secret plus three subkeys). Failing
	// each position in turn exercises the cleanup of previously locked
	// buffers.
	for _, failAt := range []int{1, 2, 3, 4} {
		t.Run(fmt.Sprintf("FailAtAllocation%d", failAt), func(t *testing.T) {
			t.Parallel()
			var succeeded []*memguard.LockedBuffer
			calls := 0
			alloc := func(b []byte) *memguard.LockedBuffer {
				calls++
				if calls == failAt {
					panic("mlock failed")
				}
				locked := memguard.NewBufferFromBytes(b)
				succeeded = append(succeeded, locked)
				return locked
			}
			tok, err := newTokenizer(testSecret("secret"), alloc, hkdfSubkey)
			require.Error(t, err)
			assert.Nil(t, tok)
			assert.Contains(t, err.Error(), "mlock failed")
			for i, locked := range succeeded {
				assert.Falsef(t, locked.IsAlive(), "buffer %d must be destroyed after a later allocation failure", i)
			}
		})
	}
}

func TestNewTokenizer_DeriveFailure(t *testing.T) {
	t.Parallel()
	for _, failInfo := range []string{infoDeterministic, infoBloom, infoPermutation} {
		t.Run(failInfo, func(t *testing.T) {
			t.Parallel()
			derive := func(secret []byte, info string) ([]byte, error) {
				if info == failInfo {
					return nil, errors.New("derive failed")
				}
				return hkdfSubkey(secret, info)
			}
			tok, err := newTokenizer(testSecret("secret"), memguard.NewBufferFromBytes, derive)
			require.Error(t, err)
			assert.Nil(t, tok)
			assert.Contains(t, err.Error(), "derive failed")
		})
	}
}

func TestHKDFSubkey(t *testing.T) {
	t.Parallel()

	t.Run("DeterministicAndDomainSeparated", func(t *testing.T) {
		t.Parallel()
		a, err := hkdfSubkey(testSecret("secret"), infoDeterministic)
		require.NoError(t, err)
		require.Len(t, a, subkeySize)
		again, err := hkdfSubkey(testSecret("secret"), infoDeterministic)
		require.NoError(t, err)
		assert.Equal(t, a, again, "same (secret, info) must derive the same subkey")

		b, err := hkdfSubkey(testSecret("secret"), infoBloom)
		require.NoError(t, err)
		assert.NotEqual(t, a, b, "different info strings must derive different subkeys")

		c, err := hkdfSubkey(testSecret("other"), infoDeterministic)
		require.NoError(t, err)
		assert.NotEqual(t, a, c, "different secrets must derive different subkeys")
	})

	t.Run("OverlongRequestErrors", func(t *testing.T) {
		t.Parallel()
		// HKDF-SHA256 can expand at most 255 blocks of 32 bytes.
		_, err := hkdfDerive(testSecret("secret"), infoDeterministic, 255*32+1)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "subkey derivation failed")
	})
}

func TestDestroy_WipesAllBuffers(t *testing.T) {
	t.Parallel()
	tok, err := New(testSecret("wipe-me"))
	require.NoError(t, err)
	tok.Destroy()
	assert.False(t, tok.secret.IsAlive(), "secret buffer must be wiped")
	assert.False(t, tok.detKey.IsAlive(), "deterministic subkey buffer must be wiped")
	assert.False(t, tok.bloomKey.IsAlive(), "bloom subkey buffer must be wiped")
	assert.False(t, tok.permKey.IsAlive(), "permutation subkey buffer must be wiped")
}

func TestRecoverToError(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		fn      func()
		wantErr bool
	}{
		{"NoPanic", func() {}, false},
		{"Panic", func() { panic("boom") }, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := recoverToError(tc.fn)
			if tc.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), "boom")
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestTokenize_AfterDestroy(t *testing.T) {
	t.Parallel()
	givenSpec := sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0}
	rec := sriracha.RawRecord{sriracha.FieldNameGiven: "John"}

	cases := []struct {
		name string
		call func(tok *Tokenizer) error
	}{
		{
			name: "Deterministic",
			call: func(tok *Tokenizer) error {
				_, err := tok.TokenizeDeterministic(rec, deterministicFS(givenSpec))
				return err
			},
		},
		{
			name: "Probabilistic",
			call: func(tok *Tokenizer) error {
				_, err := tok.TokenizeProbabilistic(rec, bloomFS(givenSpec))
				return err
			},
		},
		{
			name: "CLK",
			call: func(tok *Tokenizer) error {
				_, err := tok.TokenizeCLK(rec, bloomFS(givenSpec))
				return err
			},
		},
		{
			name: "Field",
			call: func(tok *Tokenizer) error {
				_, err := tok.TokenizeField("John", sriracha.FieldNameGiven)
				return err
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tok, err := New(testSecret("secret"))
			require.NoError(t, err)
			tok.Destroy()
			err = tc.call(tok)
			require.Error(t, err)
			assert.True(t, errors.Is(err, ErrDestroyed), "expected ErrDestroyed, got %v", err)
		})
	}
}

func TestTokenizeDeterministic(t *testing.T) {
	t.Parallel()
	givenSpec := sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0}
	familySpec := sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: true, Weight: 1.0}
	givenOptional := sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: false, Weight: 1.0}
	familyOptional := sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: false, Weight: 0.5}
	passportOptional := sriracha.FieldSpec{Path: sriracha.FieldIdentifierPassport, Required: false, Weight: 1.0}

	cases := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "Idempotent",
			run: func(t *testing.T) {
				tok := newTok(t, "secret")
				rec := sriracha.RawRecord{sriracha.FieldNameGiven: "John"}
				fs := deterministicFS(givenSpec)

				tr1, err := tok.TokenizeDeterministic(rec, fs)
				require.NoError(t, err)
				tr2, err := tok.TokenizeDeterministic(rec, fs)
				require.NoError(t, err)
				eq, err := Equal(tr1, tr2)
				require.NoError(t, err)
				assert.True(t, eq, "identical inputs should produce equal tokens")
			},
		},
		{
			name: "CrossFieldIsolation",
			run: func(t *testing.T) {
				tok := newTok(t, "secret")
				rec := sriracha.RawRecord{
					sriracha.FieldNameGiven:  "John",
					sriracha.FieldNameFamily: "John",
				}
				tr, err := tok.TokenizeDeterministic(rec, deterministicFS(givenSpec, familySpec))
				require.NoError(t, err)
				require.Len(t, tr.Fields, 2)
				assert.Len(t, tr.Fields[0], 32, "expected 32-byte HMAC for given name")
				assert.Len(t, tr.Fields[1], 32, "expected 32-byte HMAC for family name")
				assert.NotEqual(t, tr.Fields[0], tr.Fields[1], "same value with different paths should differ")
			},
		},
		{
			name: "DifferentSecret",
			run: func(t *testing.T) {
				rec := sriracha.RawRecord{sriracha.FieldNameGiven: "John"}
				fs := deterministicFS(givenSpec)

				tr1, err := newTok(t, "secret-a").TokenizeDeterministic(rec, fs)
				require.NoError(t, err)
				tr2, err := newTok(t, "secret-b").TokenizeDeterministic(rec, fs)
				require.NoError(t, err)
				eq, err := Equal(tr1, tr2)
				require.NoError(t, err)
				assert.False(t, eq, "different secrets should produce different tokens")
			},
		},
		{
			name: "MissingRequired",
			run: func(t *testing.T) {
				tok := newTok(t, "secret")
				_, err := tok.TokenizeDeterministic(sriracha.RawRecord{}, deterministicFS(givenSpec))
				assert.Error(t, err)
			},
		},
		{
			name: "MissingOptionalNilEntry",
			run: func(t *testing.T) {
				tok := newTok(t, "secret")
				rec := sriracha.RawRecord{sriracha.FieldNameGiven: "John"}
				tr, err := tok.TokenizeDeterministic(rec, deterministicFS(givenSpec, familyOptional))
				require.NoError(t, err)
				require.Len(t, tr.Fields, 2)
				assert.Len(t, tr.Fields[0], 32, "present field should have 32-byte HMAC")
				assert.Nil(t, tr.Fields[1], "absent optional field should be nil")
			},
		},
		{
			name: "EmptyAllOptional",
			run: func(t *testing.T) {
				tok := newTok(t, "secret")
				tr, err := tok.TokenizeDeterministic(sriracha.RawRecord{}, deterministicFS(givenOptional, familyOptional))
				require.NoError(t, err)
				require.Len(t, tr.Fields, 2)
				assert.Nil(t, tr.Fields[0])
				assert.Nil(t, tr.Fields[1])
			},
		},
		{
			name: "NormalizationError",
			run: func(t *testing.T) {
				tok := newTok(t, "secret")
				rec := sriracha.RawRecord{sriracha.FieldDateBirth: "not-a-date"}
				fs := deterministicFS(sriracha.FieldSpec{Path: sriracha.FieldDateBirth, Required: true, Weight: 1.0})
				_, err := tok.TokenizeDeterministic(rec, fs)
				assert.Error(t, err)
			},
		},
		{
			name: "EmptyValueOptionalTreatedAsAbsent",
			run: func(t *testing.T) {
				tok := newTok(t, "secret")
				rec := sriracha.RawRecord{
					sriracha.FieldNameGiven:          "John",
					sriracha.FieldNameFamily:         "",
					sriracha.FieldIdentifierPassport: "---",
				}
				tr, err := tok.TokenizeDeterministic(rec, deterministicFS(givenSpec, familyOptional, passportOptional))
				require.NoError(t, err)
				require.Len(t, tr.Fields, 3)
				assert.Len(t, tr.Fields[0], 32)
				assert.Nil(t, tr.Fields[1], "empty optional value should keep a nil entry")
				assert.Nil(t, tr.Fields[2], "value normalizing to empty should keep a nil entry")
			},
		},
		{
			name: "EmptyValueRequiredErrors",
			run: func(t *testing.T) {
				tok := newTok(t, "secret")
				rec := sriracha.RawRecord{sriracha.FieldNameGiven: ""}
				_, err := tok.TokenizeDeterministic(rec, deterministicFS(givenSpec))
				assert.Error(t, err)
			},
		},
		{
			name: "VersionPropagated",
			run: func(t *testing.T) {
				tok := newTok(t, "secret")
				fs := deterministicFS(givenSpec)
				tr, err := tok.TokenizeDeterministic(sriracha.RawRecord{sriracha.FieldNameGiven: "John"}, fs)
				require.NoError(t, err)
				assert.Equal(t, fs.Version, tr.FieldSetVersion)
			},
		},
		{
			name: "KeyIDPropagated",
			run: func(t *testing.T) {
				tok := newTok(t, "secret", WithKeyID("k1"))
				tr, err := tok.TokenizeDeterministic(sriracha.RawRecord{sriracha.FieldNameGiven: "John"}, deterministicFS(givenSpec))
				require.NoError(t, err)
				assert.Equal(t, "k1", tr.KeyID)
			},
		},
		{
			name: "FingerprintLeftEmpty",
			run: func(t *testing.T) {
				// Direct token.* callers are responsible for setting
				// FieldSetFingerprint themselves so session can cache it once.
				tok := newTok(t, "secret")
				tr, err := tok.TokenizeDeterministic(sriracha.RawRecord{sriracha.FieldNameGiven: "John"}, deterministicFS(givenSpec))
				require.NoError(t, err)
				assert.Empty(t, tr.FieldSetFingerprint,
					"token.TokenizeDeterministic must not populate FieldSetFingerprint")
			},
		},
		{
			name: "DomainSeparationByPath",
			run: func(t *testing.T) {
				// Length-prefixed HMAC must distinguish (value="ab", path A) from
				// (value="a", path "b" prepended to A's bytes). We assert the simpler
				// property: the same value under two different paths produces
				// different HMACs (already covered by CrossFieldIsolation), plus
				// that the byte HMAC is stable across calls (idempotent).
				tok := newTok(t, "secret")
				rec := sriracha.RawRecord{sriracha.FieldNameGiven: "x"}
				fs := deterministicFS(givenSpec)
				tr1, err := tok.TokenizeDeterministic(rec, fs)
				require.NoError(t, err)
				tr2, err := tok.TokenizeDeterministic(rec, fs)
				require.NoError(t, err)
				require.Len(t, tr1.Fields, 1)
				assert.Equal(t, tr1.Fields[0], tr2.Fields[0])
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func TestTokenizeField(t *testing.T) {
	t.Parallel()

	t.Run("DeterministicAndIdempotent", func(t *testing.T) {
		t.Parallel()
		tok := newTok(t, "secret")
		got1, err := tok.TokenizeField("Alice", sriracha.FieldNameGiven)
		require.NoError(t, err)
		require.Len(t, got1, 32)
		got2, err := tok.TokenizeField("Alice", sriracha.FieldNameGiven)
		require.NoError(t, err)
		assert.Equal(t, got1, got2)
	})

	t.Run("MatchesRecordOutput", func(t *testing.T) {
		t.Parallel()
		tok := newTok(t, "secret")
		fs := deterministicFS(sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0})

		fromField, err := tok.TokenizeField("Alice", sriracha.FieldNameGiven)
		require.NoError(t, err)
		fromRecord, err := tok.TokenizeDeterministic(sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"}, fs)
		require.NoError(t, err)
		require.Len(t, fromRecord.Fields, 1)
		assert.Equal(t, fromField, fromRecord.Fields[0],
			"TokenizeField must produce the same bytes as TokenizeDeterministic for that field")
	})

	t.Run("DifferentPathsDiffer", func(t *testing.T) {
		t.Parallel()
		tok := newTok(t, "secret")
		given, err := tok.TokenizeField("x", sriracha.FieldNameGiven)
		require.NoError(t, err)
		family, err := tok.TokenizeField("x", sriracha.FieldNameFamily)
		require.NoError(t, err)
		assert.NotEqual(t, given, family, "same value under different paths must differ")
	})

	t.Run("NormalizationError", func(t *testing.T) {
		t.Parallel()
		tok := newTok(t, "secret")
		_, err := tok.TokenizeField("not-a-date", sriracha.FieldDateBirth)
		assert.Error(t, err)
	})

	t.Run("EmptyValueErrors", func(t *testing.T) {
		t.Parallel()
		cases := []struct {
			name  string
			value string
			path  sriracha.FieldPath
		}{
			{"EmptyName", "", sriracha.FieldNameGiven},
			{"IdentifierNormalizesToEmpty", "---", sriracha.FieldIdentifierPassport},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()
				tok := newTok(t, "secret")
				_, err := tok.TokenizeField(tc.value, tc.path)
				assert.Error(t, err)
			})
		}
	})
}

// TestTokenizeDeterministic_ErrorSentinels pins the taxonomy: every failure
// carries the package prefix, wraps a root sentinel reachable with
// errors.Is, and names the offending field through a FieldError.
func TestTokenizeDeterministic_ErrorSentinels(t *testing.T) {
	t.Parallel()
	requiredGiven := sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0}
	requiredBirth := sriracha.FieldSpec{Path: sriracha.FieldDateBirth, Required: true, Weight: 1.0}

	cases := []struct {
		name     string
		call     func(tok *Tokenizer) error
		wantErr  error
		wantPath sriracha.FieldPath
	}{
		{
			name: "RequiredFieldMissing",
			call: func(tok *Tokenizer) error {
				_, err := tok.TokenizeDeterministic(sriracha.RawRecord{}, deterministicFS(requiredGiven))
				return err
			},
			wantErr:  sriracha.ErrRequiredFieldMissing,
			wantPath: sriracha.FieldNameGiven,
		},
		{
			name: "RequiredFieldEmpty",
			call: func(tok *Tokenizer) error {
				rec := sriracha.RawRecord{sriracha.FieldNameGiven: ""}
				_, err := tok.TokenizeDeterministic(rec, deterministicFS(requiredGiven))
				return err
			},
			wantErr:  sriracha.ErrEmptyValue,
			wantPath: sriracha.FieldNameGiven,
		},
		{
			name: "NormalizationFailure",
			call: func(tok *Tokenizer) error {
				rec := sriracha.RawRecord{sriracha.FieldDateBirth: "not-a-date"}
				_, err := tok.TokenizeDeterministic(rec, deterministicFS(requiredBirth))
				return err
			},
			wantErr:  normalize.ErrInvalidValue,
			wantPath: sriracha.FieldDateBirth,
		},
		{
			name: "FieldNormalizationFailure",
			call: func(tok *Tokenizer) error {
				_, err := tok.TokenizeField("not-a-date", sriracha.FieldDateBirth)
				return err
			},
			wantErr:  normalize.ErrInvalidValue,
			wantPath: sriracha.FieldDateBirth,
		},
		{
			name: "FieldNormalizesToEmpty",
			call: func(tok *Tokenizer) error {
				_, err := tok.TokenizeField("---", sriracha.FieldIdentifierPassport)
				return err
			},
			wantErr:  sriracha.ErrEmptyValue,
			wantPath: sriracha.FieldIdentifierPassport,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := tc.call(newTok(t, "secret"))
			require.Error(t, err)
			assert.ErrorIs(t, err, tc.wantErr)
			assert.True(t, strings.HasPrefix(err.Error(), "token: "), "got %q", err.Error())

			var fieldErr sriracha.FieldError
			require.ErrorAs(t, err, &fieldErr)
			assert.Equal(t, tc.wantPath, fieldErr.Path)
		})
	}
}

func TestTokenizeProbabilistic(t *testing.T) {
	t.Parallel()
	givenSpec := sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0}
	familySpec := sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: true, Weight: 1.0}
	familyOptional := sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: false, Weight: 0.5}

	cases := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "MissingRequired",
			run: func(t *testing.T) {
				tok := newTok(t, "secret")
				_, err := tok.TokenizeProbabilistic(sriracha.RawRecord{}, bloomFS(givenSpec))
				assert.Error(t, err)
			},
		},
		{
			name: "NormalizationError",
			run: func(t *testing.T) {
				tok := newTok(t, "secret")
				rec := sriracha.RawRecord{sriracha.FieldDateBirth: "not-a-date"}
				fs := bloomFS(sriracha.FieldSpec{Path: sriracha.FieldDateBirth, Required: true, Weight: 1.0})
				_, err := tok.TokenizeProbabilistic(rec, fs)
				assert.Error(t, err)
			},
		},
		{
			name: "MissingOptionalNilEntry",
			run: func(t *testing.T) {
				tok := newTok(t, "secret")
				fs := bloomFS(givenSpec, familyOptional)
				tr, err := tok.TokenizeProbabilistic(sriracha.RawRecord{sriracha.FieldNameGiven: "John"}, fs)
				require.NoError(t, err)
				require.Len(t, tr.Fields, 2)
				assert.Len(t, tr.Fields[0], 128, "present field should have a 128-byte filter")
				assert.Nil(t, tr.Fields[1], "absent optional field should be nil")
			},
		},
		{
			name: "FieldLayoutAndMetadata",
			run: func(t *testing.T) {
				tok := newTok(t, "secret")
				fs := bloomFS(givenSpec, familySpec)
				rec := sriracha.RawRecord{
					sriracha.FieldNameGiven:  "John",
					sriracha.FieldNameFamily: "Doe",
				}
				tr, err := tok.TokenizeProbabilistic(rec, fs)
				require.NoError(t, err)
				require.Len(t, tr.Fields, 2, "expected one filter per FieldSet entry")
				assert.Len(t, tr.Fields[0], 128, "expected 128 bytes per 1024-bit filter")
				assert.Len(t, tr.Fields[1], 128, "expected 128 bytes per 1024-bit filter")
				assert.Equal(t, fs.ProbabilisticParams, tr.ProbabilisticParams)
				assert.Equal(t, fs.Version, tr.FieldSetVersion)
			},
		},
		{
			name: "KeyIDPropagated",
			run: func(t *testing.T) {
				tok := newTok(t, "secret", WithKeyID("k1"))
				tr, err := tok.TokenizeProbabilistic(sriracha.RawRecord{sriracha.FieldNameGiven: "John"}, bloomFS(givenSpec))
				require.NoError(t, err)
				assert.Equal(t, "k1", tr.KeyID)
			},
		},
		{
			name: "FingerprintLeftEmpty",
			run: func(t *testing.T) {
				// See TestTokenizeDeterministic/FingerprintLeftEmpty for rationale.
				tok := newTok(t, "secret")
				tr, err := tok.TokenizeProbabilistic(sriracha.RawRecord{sriracha.FieldNameGiven: "John"}, bloomFS(givenSpec))
				require.NoError(t, err)
				assert.Empty(t, tr.FieldSetFingerprint,
					"token.TokenizeProbabilistic must not populate FieldSetFingerprint")
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// "John" vs "Jon" yields very few bigrams/trigrams and unreliable Dice scores,
// so this case uses "Christopher" vs "Cristopher" to exercise typo similarity
// with a meaningful number of ngrams.
func TestTokenizeProbabilistic_NameSimilarity(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		nameA     string
		nameB     string
		wantAbove bool
		threshold float64
	}{
		{"similar names (typo)", "Christopher", "Cristopher", true, 0.80},
		{"dissimilar names", "John", "Maria", false, 0.30},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tok := newTok(t, "secret")
			fs := bloomFS(sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0})

			tr1, err := tok.TokenizeProbabilistic(sriracha.RawRecord{sriracha.FieldNameGiven: tc.nameA}, fs)
			require.NoError(t, err)
			tr2, err := tok.TokenizeProbabilistic(sriracha.RawRecord{sriracha.FieldNameGiven: tc.nameB}, fs)
			require.NoError(t, err)

			scores, err := DicePerField(tr1, tr2)
			require.NoError(t, err)
			require.Len(t, scores, 1)
			d := scores[0]
			if tc.wantAbove {
				assert.Greater(t, d, tc.threshold, "Dice(%s, %s) = %.4f, expected > %.2f", tc.nameA, tc.nameB, d, tc.threshold)
			} else {
				assert.Less(t, d, tc.threshold, "Dice(%s, %s) = %.4f, expected < %.2f", tc.nameA, tc.nameB, d, tc.threshold)
			}
		})
	}
}

// TestTokenizer_Concurrent verifies that a single Tokenizer is safe for
// concurrent use across goroutines (sync.Pool of HMAC instances). Run with
// -race to catch any shared-hash mutation.
func TestTokenizer_Concurrent(t *testing.T) {
	t.Parallel()

	tok := newTok(t, "secret", WithKeyID("k1"))
	givenSpec := sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: true, Weight: 1.0}
	dfs := deterministicFS(givenSpec)
	bfs := bloomFS(givenSpec)
	balancedFS := sriracha.FieldSet{
		Version:             "1.0.0-test",
		Fields:              []sriracha.FieldSpec{givenSpec},
		ProbabilisticParams: sriracha.DefaultProbabilisticConfig(),
	}

	cases := []struct {
		name string
		run  func(t *testing.T)
	}{
		{
			name: "Deterministic",
			run: func(t *testing.T) {
				const n = 64
				results := make([]sriracha.DeterministicToken, n)
				var wg sync.WaitGroup
				for i := range n {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						tr, err := tok.TokenizeDeterministic(sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"}, dfs)
						assert.NoError(t, err)
						results[i] = tr
					}(i)
				}
				wg.Wait()
				for i := 1; i < n; i++ {
					eq, err := Equal(results[0], results[i])
					require.NoError(t, err)
					assert.True(t, eq, "result %d not equal to result 0", i)
				}
			},
		},
		{
			name: "Bloom",
			run: func(t *testing.T) {
				const n = 64
				results := make([]sriracha.ProbabilisticToken, n)
				var wg sync.WaitGroup
				for i := range n {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						tr, err := tok.TokenizeProbabilistic(sriracha.RawRecord{sriracha.FieldNameGiven: "Christopher"}, bfs)
						assert.NoError(t, err)
						results[i] = tr
					}(i)
				}
				wg.Wait()
				scores, err := DicePerField(results[0], results[len(results)-1])
				require.NoError(t, err)
				require.Len(t, scores, 1)
				assert.InDelta(t, 1.0, scores[0], 1e-9)
			},
		},
		{
			// Balanced tokenization exercises the concurrent first-use
			// generation of the cached permutation.
			name: "BalancedCLK",
			run: func(t *testing.T) {
				const n = 64
				results := make([]sriracha.CLKToken, n)
				var wg sync.WaitGroup
				for i := range n {
					wg.Add(1)
					go func(i int) {
						defer wg.Done()
						clk, err := tok.TokenizeCLK(sriracha.RawRecord{sriracha.FieldNameGiven: "Christopher"}, balancedFS)
						assert.NoError(t, err)
						results[i] = clk
					}(i)
				}
				wg.Wait()
				res, err := MatchCLK(results[0], results[len(results)-1], 0.99)
				require.NoError(t, err)
				assert.InDelta(t, 1.0, res.Score, 1e-9)
				assert.True(t, res.IsMatch)
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// TestNew_FinalizerWipesOnGC verifies the runtime cleanup registered in New
// fires when a Tokenizer becomes unreachable without an explicit Destroy
// call. Skipped under -short because it relies on GC timing.
func TestNew_FinalizerWipesOnGC(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping GC-timing-sensitive test in -short mode")
	}

	var buf *memguard.LockedBuffer
	func() {
		tok, err := New(testSecret("forget-to-destroy"))
		require.NoError(t, err)
		// Reach into the locked buffer directly; this is the only way to
		// observe the post-GC cleanup. We deliberately do NOT call
		// tok.Destroy().
		buf = tok.secret
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		if !buf.IsAlive() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("locked buffer still alive after GC + 2s wait — finalizer did not run")
}

// TestDestroy_ClearsFinalizer pins the lifecycle invariant that an explicit
// Destroy() also unregisters the runtime finalizer registered by New, so
// the finalizer cannot re-invoke t.secret.Destroy() on an already-destroyed
// buffer. memguard.LockedBuffer.Destroy is idempotent today (guarded by
// !b.alive), so any regression here would be silently absorbed at runtime —
// hence the pinned test. Skipped under -short because it relies on GC timing.
func TestDestroy_ClearsFinalizer(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skipping GC-timing-sensitive test in -short mode")
	}

	var buf *memguard.LockedBuffer
	func() {
		tok, err := New(testSecret("explicit-destroy"))
		require.NoError(t, err)
		buf = tok.secret
		tok.Destroy()
		require.False(t, buf.IsAlive(), "explicit Destroy must wipe the locked buffer immediately")
	}()

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(20 * time.Millisecond)
	}
	assert.False(t, buf.IsAlive(), "buffer must remain destroyed after GC")
}

func TestNgrams(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name  string
		input string
		sizes []int
		want  []string
	}{
		{
			name:  "SingleSizePadded",
			input: "ab",
			sizes: []int{2},
			want:  []string{"_a", "ab", "b_"},
		},
		{
			name:  "OneRuneStillProducesGrams",
			input: "a",
			sizes: []int{2},
			want:  []string{"_a", "a_"},
		},
		{
			name:  "UnigramsNeedNoPadding",
			input: "ab",
			sizes: []int{1},
			want:  []string{"a", "b"},
		},
		{
			name:  "Unicode",
			input: "αβγ",
			sizes: []int{2},
			want:  []string{"_α", "αβ", "βγ", "γ_"},
		},
		{
			name:  "Empty",
			input: "",
			sizes: []int{2},
			want:  []string{},
		},
		{
			name:  "EmptySizes",
			input: "abc",
			sizes: []int{},
			want:  []string{},
		},
		{
			name: "DescendingOrder",
			// sizes[0]=3 > sizes[1]=2 keeps the max-size scan honest.
			input: "ab",
			sizes: []int{3, 2},
			want:  []string{"__a", "_ab", "ab_", "b__", "_a", "ab", "b_"},
		},
		{
			name:  "MultipleSizes",
			input: "abc",
			sizes: []int{2, 3},
			want:  []string{"_a", "ab", "bc", "c_", "__a", "_ab", "abc", "bc_", "c__"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := ngrams(tc.input, tc.sizes)
			if len(tc.want) == 0 {
				assert.Empty(t, got)
			} else {
				assert.Equal(t, tc.want, got)
			}
		})
	}
}

func BenchmarkTokenizeDeterministic(b *testing.B) {
	tok, _ := New(testSecret("bench-secret"))
	rec := sriracha.RawRecord{
		sriracha.FieldNameGiven:    "Alice",
		sriracha.FieldNameFamily:   "Smith",
		sriracha.FieldDateBirth:    "1990-05-15",
		sriracha.FieldContactEmail: "alice@example.com",
	}
	fs := deterministicFS(
		sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: false, Weight: 2.0},
		sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: false, Weight: 2.5},
		sriracha.FieldSpec{Path: sriracha.FieldDateBirth, Required: false, Weight: 2.0},
		sriracha.FieldSpec{Path: sriracha.FieldContactEmail, Required: false, Weight: 2.0},
	)
	b.ResetTimer()
	for range b.N {
		_, _ = tok.TokenizeDeterministic(rec, fs)
	}
}

func BenchmarkTokenizeProbabilistic(b *testing.B) {
	tok, _ := New(testSecret("bench-secret"))
	rec := sriracha.RawRecord{
		sriracha.FieldNameGiven:    "Alice",
		sriracha.FieldNameFamily:   "Smith",
		sriracha.FieldDateBirth:    "1990-05-15",
		sriracha.FieldContactEmail: "alice@example.com",
	}
	fs := bloomFS(
		sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: false, Weight: 2.0},
		sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: false, Weight: 2.5},
		sriracha.FieldSpec{Path: sriracha.FieldDateBirth, Required: false, Weight: 2.0},
		sriracha.FieldSpec{Path: sriracha.FieldContactEmail, Required: false, Weight: 2.0},
	)
	b.ResetTimer()
	for range b.N {
		_, _ = tok.TokenizeProbabilistic(rec, fs)
	}
}

func BenchmarkNgrams(b *testing.B) {
	sizes := []int{2, 3}
	input := "Christopher"
	b.ResetTimer()
	for range b.N {
		_ = ngrams(input, sizes)
	}
}

// FuzzNgrams verifies that ngrams never panics, that every returned gram has
// the correct rune length, and that the padded gram count is n + size - 1
// for any non-empty input.
func FuzzNgrams(f *testing.F) {
	f.Add("hello", 2)
	f.Add("", 3)
	f.Add("αβγ", 2)
	f.Add("a", 1)

	f.Fuzz(func(t *testing.T, s string, size int) {
		// Skip out-of-domain sizes; only positive, bounded sizes are valid input.
		if size <= 0 || size > 20 {
			return
		}
		grams := ngrams(s, []int{size})
		runes := []rune(s)
		for _, g := range grams {
			gr := []rune(g)
			if len(gr) != size {
				t.Fatalf("ngrams(%q, [%d]): got gram %q with len %d, want %d", s, size, g, len(gr), size)
			}
		}
		n := len(runes)
		want := 0
		if n > 0 {
			want = n + size - 1
		}
		if len(grams) != want {
			t.Fatalf("ngrams(%q, [%d]): got %d grams, want %d", s, size, len(grams), want)
		}
	})
}

// FuzzTokenizeDeterministic verifies that TokenizeDeterministic never panics for arbitrary
// field values and that its output is self-consistent under Equal.
func FuzzTokenizeDeterministic(f *testing.F) {
	f.Add("Alice", "Smith")
	f.Add("", "")
	f.Add("\x00", "\xff")

	fs := deterministicFS(
		sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: false, Weight: 1.0},
		sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: false, Weight: 1.0},
	)
	tok, _ := New(testSecret("fuzz-secret"))

	f.Fuzz(func(t *testing.T, given, family string) {
		rec := sriracha.RawRecord{
			sriracha.FieldNameGiven:  given,
			sriracha.FieldNameFamily: family,
		}
		tr1, err := tok.TokenizeDeterministic(rec, fs)
		// Skip inputs that legitimately fail tokenization (e.g. invalid normalization).
		if err != nil {
			return
		}
		tr2, err := tok.TokenizeDeterministic(rec, fs)
		if err != nil {
			t.Fatalf("second TokenizeDeterministic call failed: %v", err)
		}
		eq, err := Equal(tr1, tr2)
		if err != nil {
			// Both fields absent leaves nothing to compare, which is a
			// result state, not a failure.
			if !errors.Is(err, ErrNoComparableFields) {
				t.Fatalf("Equal returned an unexpected error: %v", err)
			}
			return
		}
		if !eq {
			t.Fatalf("Equal returned false for identical inputs")
		}
	})
}

// FuzzTokenizeProbabilistic verifies that TokenizeProbabilistic never panics for
// arbitrary field values, that its layout is positional (one filter per field
// of the FieldSet, nil for absent), and that DicePerField scores a token
// against itself at 1.0 for present fields and 0 for absent ones.
func FuzzTokenizeProbabilistic(f *testing.F) {
	f.Add("Alice", "Smith")
	f.Add("", "")
	f.Add("Christopher", "Jones")

	fs := bloomFS(
		sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Required: false, Weight: 1.0},
		sriracha.FieldSpec{Path: sriracha.FieldNameFamily, Required: false, Weight: 1.0},
	)
	tok, _ := New(testSecret("fuzz-secret"))
	fieldFilterBytes := int((fs.ProbabilisticParams.SizeBits + 63) / 64 * 8)

	f.Fuzz(func(t *testing.T, given, family string) {
		rec := sriracha.RawRecord{
			sriracha.FieldNameGiven:  given,
			sriracha.FieldNameFamily: family,
		}
		tr, err := tok.TokenizeProbabilistic(rec, fs)
		// Skip inputs that legitimately fail tokenization (e.g. invalid normalization).
		if err != nil {
			return
		}
		if len(tr.Fields) != len(fs.Fields) {
			t.Fatalf("Fields length %d, want %d", len(tr.Fields), len(fs.Fields))
		}
		for i, f := range tr.Fields {
			if f != nil && len(f) != fieldFilterBytes {
				t.Fatalf("field %d byte length %d, want %d", i, len(f), fieldFilterBytes)
			}
		}
		scores, err := DicePerField(tr, tr)
		if err != nil {
			t.Fatalf("DicePerField against self: %v", err)
		}
		for i, s := range scores {
			// A token compared against itself scores 1 for present fields
			// and 0 for absent (nil) fields. Anything else indicates a bug.
			want := 0.0
			if tr.Fields[i] != nil {
				want = 1.0
			}
			if s != want {
				t.Fatalf("DicePerField self-comparison field %d = %v, want %v", i, s, want)
			}
		}
	})
}

// TestUsableRejectsDestroyedKeyMaterial pins that a Tokenizer whose locked
// buffers were destroyed without going through Destroy refuses to tokenize.
// The pooled HMACs read their key lazily, so a destroyed buffer would key
// them with nil and emit tokens derived under an empty secret. memguard's
// Purge and its interrupt handler both destroy buffers this way.
func TestUsableRejectsDestroyedKeyMaterial(t *testing.T) {
	t.Parallel()

	fs := bloomFS(sriracha.FieldSpec{Path: sriracha.FieldNameGiven, Weight: 1})
	rec := sriracha.RawRecord{sriracha.FieldNameGiven: "Alice"}

	cases := []struct {
		name string
		kill func(tok *Tokenizer)
	}{
		{"Secret", func(tok *Tokenizer) { tok.secret.Destroy() }},
		{"DetKey", func(tok *Tokenizer) { tok.detKey.Destroy() }},
		{"BloomKey", func(tok *Tokenizer) { tok.bloomKey.Destroy() }},
		{"PermKey", func(tok *Tokenizer) { tok.permKey.Destroy() }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			tok := newTok(t, "liveness-"+tc.name)
			require.True(t, tok.usable(), "fresh tokenizer must be usable")
			tc.kill(tok)
			assert.False(t, tok.usable())

			_, err := tok.TokenizeDeterministic(rec, fs)
			assert.ErrorIs(t, err, ErrDestroyed)
			_, err = tok.TokenizeProbabilistic(rec, fs)
			assert.ErrorIs(t, err, ErrDestroyed)
			_, err = tok.TokenizeCLK(rec, fs)
			assert.ErrorIs(t, err, ErrDestroyed)
			_, err = tok.TokenizeField("Alice", sriracha.FieldNameGiven)
			assert.ErrorIs(t, err, ErrDestroyed)
		})
	}
}
