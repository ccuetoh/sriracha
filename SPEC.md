# Sriracha wire format specification

Version 2 ("sriracha/det/2", "sriracha/bloom/2", "sriracha/clk/2").

This document is normative. A conforming implementation in any language
must produce byte-identical tokens for identical inputs. The golden vector
tests in `token/golden_test.go` and `normalize/golden_test.go` define
conformance; any change that alters emitted bytes must update the format
version, the golden vectors, and this document in the same commit.

All integers are big-endian unless stated otherwise. `BE32(x)` and
`BE64(x)` denote 4- and 8-byte big-endian encodings. `len(b)` is a byte
length. HMAC is HMAC-SHA256 throughout.

## 1. Versioning

Every token carries a `format` string naming the mode and format version.
Comparison functions reject tokens whose formats differ before any other
check. Byte-affecting changes (normalization, preimages, hashing, filter
construction, serialization) require a new format version. Before v1.0.0
such changes ship only in minor releases; after v1.0.0 only in major
releases.

## 2. Normalization

Input is a Unicode string per field. Pipeline, in order:

1. Replace invalid UTF-8 bytes with U+FFFD.
2. Remove every rune in Unicode category Cf (format characters).
3. Apply NFKD decomposition.
4. Lowercase with the locale-independent Unicode case mapping
   (language-und, no Turkish or Lithuanian special casing).
5. Collapse every run of Unicode whitespace to one ASCII space.
6. Trim leading and trailing whitespace.
7. Apply the field rule selected by the field path:
   - namespace `date`: value must be ISO 8601 `YYYY-MM-DD`, else error.
   - namespace `identifier`: remove `-`, `.`, and spaces, then re-apply
     NFKD.
   - namespace `name`: remove each rune in category Mn whose most recent
     preceding non-Mn rune is in the Unicode Latin script; a leading mark
     with no preceding base rune is kept. Re-apply NFKD, re-collapse
     whitespace, trim.
   - path `sriracha::contact::email`: reject whitespace, require exactly
     one `@` with non-empty parts, strip trailing dots from the domain.
   - path `sriracha::contact::phone`: keep digits and one leading `+`,
     require at least 7 digits.
   - path `sriracha::address::country`: uppercase, require exactly 2
     ASCII letters.
   - anything else: no field rule.

A value that normalizes to the empty string is treated as absent. For a
required field this is an error; for an optional field the field is
omitted from the token.

Steps 3, 4, and 7 depend on the Unicode character tables of the build
toolchain. The golden vectors pin the expected outputs; an implementation
built against different Unicode tables that fails them is not conformant.

## 3. Key derivation

The caller supplies a secret of at least 1 byte that is not all zero
bytes. Three 32-byte subkeys are derived with HKDF-SHA256, salt absent
(nil), no extract-skip, using these `info` strings:

- `sriracha/v2/deterministic`
- `sriracha/v2/bloom`
- `sriracha/v2/permutation`

## 4. Field paths

A field path serializes as its canonical string `org::namespace::name`.
Whenever a preimage includes a path it is this UTF-8 string.

## 5. Deterministic tokens (`sriracha/det/2`)

For each present field with normalized value `v` and path `p`:

    HMAC(det_subkey, BE32(len(v)) || v || BE32(len(p)) || p)

The 32-byte digest is the field entry. Absent optional fields are null.
Token JSON (Go `encoding/json`, `[]byte` as standard base64):

    {"format":"sriracha/det/2","field_set_version":...,"key_id":...,
     "field_set_fingerprint":...,"fields":[base64-or-null, ...]}

`fields` is aligned with the FieldSet's field order.

## 6. Probabilistic configuration

    {"size_bits":u32, "ngram_sizes":[int,...], "hash_count":int,
     "balanced":bool}

`size_bits` is the emitted filter size in bits. `balanced` applies to
per-field filters only; CLK filters are always balanced. When a filter is
balanced, the base filter has `size_bits/2` bits and `size_bits` must be
even. `hash_count` must be positive; `ngram_sizes` must be non-empty and
positive.

Presets: Fast {512, [2], 2}, Default {1024, [2,3], 3}, HighPrecision
{2048, [2,3], 5}, all with `balanced` false. Per-field filters default to
unbalanced because balancing compresses per-field Dice onto
value-dependent baselines and degrades weighted multi-field matching.

## 7. Q-gram extraction

For each gram size `q` in `ngram_sizes`, in declared order: pad the
normalized value with `q-1` underscore runes (U+005F) on each side, then
emit every window of `q` consecutive runes left to right. Grams are the
UTF-8 bytes of the window. Runes, not bytes, define windows.

## 8. Bloom position derivation

One HMAC per gram under the bloom subkey:

    d = HMAC(bloom_subkey, BE32(len(gram)) || gram || BE32(len(p)) || p)
    h1 = BE64(d[0..8])
    h2 = BE64(d[8..16]) with the low bit forced to 1
    pos_i = (h1 + i*h2) mod baseBits    for i in [0, hash_count)

Arithmetic is wrapping unsigned 64-bit. `baseBits` is `size_bits` when
not balanced and `size_bits/2` when balanced. Each `pos_i` sets one bit of
the base filter.

## 9. Balanced construction

Let `half = size_bits/2` and `B` be the base filter. Build the extended
filter `E` of `size_bits` bits: for j in [0, half), if `B[j]` is set then
`E[perm[j]]` is set, else `E[perm[half+j]]` is set. Exactly one of each
pair is set, so the emitted popcount is exactly `half` for every value.

`perm` is a bijection on [0, size_bits) derived once per (secret,
size_bits): start with the identity array, then run a Fisher-Yates
shuffle from index `size_bits-1` down to 1, swapping index `i` with index
`j = uniform(i+1)`. Uniform values come from an HMAC counter stream under
the permutation subkey: block `k` is
`HMAC(perm_subkey, BE32(size_bits) || BE32(k))` for k = 0, 1, ...; each
32-byte block yields four BE64 samples consumed in order. `uniform(bound)`
uses rejection sampling: with `rem = 2^64 mod bound`, draw samples `v`
until `rem == 0` or `v <= 2^64 - 1 - rem`, then return `v mod bound`.

Unbalanced filters skip this section; the base filter is emitted as is.

## 10. Filter serialization

A filter of `n` bits serializes as `ceil(n/64)` little-endian uint64
words, bit `b` stored at word `b/64`, bit position `b%64`. In JSON the
bytes are standard base64.

## 11. Per-field probabilistic tokens (`sriracha/bloom/2`)

One filter per FieldSet field, aligned with field order. Present fields
carry their serialized filter; absent optional fields are null. Token
JSON adds `"probabilistic_params"` with the configuration of section 6.

## 12. CLK tokens (`sriracha/clk/2`)

All present fields' grams are set into one shared base filter of
`size_bits/2` bits using the section 8 derivation (the path in the
preimage keeps fields separated), then the section 9 treatment applies
unconditionally. CLK filters are always balanced regardless of the
`balanced` flag, so `size_bits` must be even and the emitted popcount is
exactly `size_bits/2`. A record where no field contributes is an error.
Token JSON:

    {"format":"sriracha/clk/2","field_set_version":...,"key_id":...,
     "field_set_fingerprint":...,"probabilistic_params":{...},
     "filter":base64}

## 13. Comparison

Deterministic equality is bitwise per field with constant-time byte
comparison, false on format, field set version, key id, fingerprint (when
both set), or field count mismatch. Probabilistic and CLK similarity is
the Sorensen-Dice coefficient over set bits. Both-null fields are
excluded as absent; a null against a populated filter scores 0 at full
weight. Balanced filters concentrate unrelated pairs near 0.5 and
compress scores upward, so thresholds must be calibrated per deployment.

## 14. FieldSet fingerprint

Lowercase hex SHA-256 of the canonical encoding:

    BE32(len(Version)) || Version
    BE32(len(Fields))
    for each field, in order:
        BE32(len(Path)) || Path || u8(Required) || BE64(float64bits(Weight))
    BE32(SizeBits) || BE32(HashCount)
    BE32(len(NgramSizes)) || BE32(size) for each
    u8(Balanced)

## 15. Compatibility policy

Tokens are comparable only when produced with the same secret, the same
format version, and the same FieldSet (version, fingerprint, and
probabilistic parameters are all checked). Unicode table upgrades in the
toolchain can change normalization output; they are wire-breaking and
require a coordinated re-tokenization, a golden vector update, and a
format version bump.
