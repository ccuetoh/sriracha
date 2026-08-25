# Threat Model

This document states what Sriracha protects, what it does not protect, and what
a party holding tokens can learn. It describes the library only. Transport,
storage, access control, and audit are the caller's responsibility.

## Parties

**Linking institutions.** Two or more organizations that hold raw person
records and want to find records that refer to the same person. They agree on a
FieldSet and share one HMAC secret. Every party that holds the secret can
tokenize arbitrary values of its own choosing.

**Token recipient.** Whoever receives the tokens and runs the comparison. This
may be one of the linking institutions or a separate matching service. A
separate matching service must not hold the secret.

**Everyone else.** Anyone who observes tokens in transit or at rest without the
secret.

## Trust assumptions

- The HMAC secret is shared between the linking institutions and must never
  reach a third party. A party holding the secret can reverse tokens by
  dictionary attack, so handing it to a matching service defeats the entire
  construction.
- Token recipients are honest-but-curious. They follow the protocol and return
  correct results, but they may retain everything they receive and analyze it
  offline. Sriracha does not defend against a recipient who deviates from the
  protocol.
- The linking institutions do not defend against each other. The secret is
  symmetric, so either side can dictionary-attack the other side's tokens.
- Each institution's own raw records are protected by that institution. Sriracha
  is not involved.

## What each token form leaks

All three forms are deterministic. The same input under the same secret always
produces the same bytes. There is no per-record salt and no noise, so identical
values are always linkable by anyone holding the tokens. This is the property
that makes matching work and it is also the main leak.

### Deterministic tokens

A 32-byte HMAC-SHA256 per field over the length-prefixed (normalized value,
field path) pair. Effectively a stable pseudonym for that value in that field.

A holder without the secret learns

- exact equality across records and across institutions, for every field
  independently
- the frequency of every value, because the pseudonym is stable. The most common
  family-name token in a national corpus is the most common family name
- which fields each record carries, since absent fields are nil entries aligned
  positionally with the FieldSet

Frequency alone is often enough to recover common names and dates of birth
without the secret. Deterministic tokens are the weakest form on privacy and the
strongest on exactness.

### Per-field probabilistic tokens

One Bloom filter per present field, built from the field's q-grams. Comparison
is Sørensen-Dice per field.

A holder without the secret learns

- a similarity score between any two values of the same field, across the whole
  corpus, not just for the pairs it was asked to compare
- which fields each record carries, since absent optional fields are nil entries
- the popcount of each field filter, which tracks the number of distinct q-grams
  and therefore correlates with value length, unless `Balanced` is set

Setting `Balanced` on `ProbabilisticConfig` fixes the popcount at `SizeBits/2`
for every present field and removes that signal. It does not remove the presence
pattern or the pairwise similarity graph. Balancing also compresses per-field
Dice scores onto value-dependent baselines, so thresholds must be recalibrated.

### CLK tokens

One record-level filter. Every present field contributes its q-grams, with the
field path in each gram's preimage, to a single shared filter. CLK filters are
always balanced regardless of `Balanced`.

A holder without the secret learns

- a whole-record similarity score between any two records

It does not learn per-field structure, per-field similarity, which fields the
record carries, or anything from the popcount, which is exactly `SizeBits/2` for
every record.

CLK is the recommended form for sharing when per-field scores are not required.
It leaks strictly less than the other two forms.

## What a party holding the secret can do

Names, dates of birth, and national identifier formats are low-entropy. A party
with the secret can enumerate that universe and tokenize each candidate, then
match the results against the tokens it holds.

- Deterministic tokens fall to a direct dictionary lookup. Tokenize every
  plausible name, compare 32-byte values, done.
- Probabilistic and CLK tokens fall to the same enumeration followed by a Dice
  comparison against candidate filters. Balancing and the secret permutation do
  not help here, because the attacker reproduces both.

Dates of birth over any realistic range are a universe of a few tens of
thousands of values. Given names and family names in a single country are a few
hundred thousand. Neither resists enumeration.

Therefore Sriracha tokens are **pseudonymized personal data** under GDPR
Art. 4(5), not anonymized data. They remain regulated personal data and must be
handled as such. Treat a token store with the same controls you would apply to
the raw identifiers.

## Attacks in scope

- A recipient without the secret reading tokens at rest or in transit.
- A recipient without the secret correlating tokens across batches or across
  institutions.
- Accidental schema drift between the two sides. `FieldSetVersion` is stamped
  on every token and compared strictly. `FieldSetFingerprint` is stamped only
  by `session.Session`, which also refuses any token whose fingerprint differs
  from its own (`ErrFingerprintDrift`); `token.Tokenizer` leaves it empty and
  the comparison helpers check it only when both sides carry one, so two
  unstamped tokens are compared on version alone. A recipient that wants the
  stronger guarantee should go through a `Session`, and
  `session.WithStrictFingerprint` additionally rejects unstamped tokens.
- Accidental key rotation drift. `KeyID` is stamped on every token and
  comparison helpers refuse to compare tokens that disagree, so a post-rotation
  mismatch surfaces as an error instead of silently scoring zero.
- Casual recovery of the secret from process memory. The secret and the three
  derived subkeys live in memguard locked, non-swappable buffers, wiped by
  `Destroy` or by a runtime finalizer.
- Accidental disclosure through logs. `String()` and `Annotate` return metadata
  and presence counts only, never token bytes.

## Attacks out of scope

Sriracha does **not** defend against any of the following.

- **Frequency analysis.** Stable pseudonyms and stable filters expose the value
  distribution. This is a known and effective attack on Bloom-filter PPRL.
- **Graph matching against an auxiliary population.** An attacker with a
  population register or voter file can build the same similarity graph over
  known values and align it with the token graph to re-identify records. This is
  the standard published attack against Bloom-filter PPRL schemes and this
  library does not mitigate it.
- **Dictionary attack by a secret holder**, as described above.
- **Chosen-value probing.** Anyone who can get values of their choice tokenized
  under the secret can confirm whether a specific person is present.
- **A malicious recipient** who returns forged match results or submits crafted
  records.
- **Traffic analysis.** Record counts, batch sizes, ordering, and timing are all
  visible and are not padded.
- **Timing side channels in comparison.** `token.Equal` compares field bytes in
  constant time, but Dice scoring over Bloom filters is not constant time and is
  not intended to be.
- **Transport and storage security.** Use TLS and encrypt at rest. The library
  emits bytes and does not move them.
- **Differential privacy.** Balanced filters are not differential privacy.
  Identical values still produce identical filters.

## Secret custody

- Generate 32 random bytes from a CSPRNG (`crypto/rand`). The library rejects
  secrets shorter than 32 bytes, since HKDF-SHA256 has a 32-byte PRK width and
  anything shorter is a false sense of strength. A passphrase is not a secret.
- Inject the secret from a KMS, a secrets manager, or the process environment.
  Never commit it to source, never put it in a config file that ships with the
  code, never log it.
- Both linking institutions need the same bytes. Move them over a channel with
  authentication and confidentiality. Never send them to a matching service or
  any other third party.
- Call `Destroy` on the Tokenizer or Session when you are done with it.
- Label every deployment with `WithKeyID` so tokens carry the key generation
  they were produced under.

## Blast radius of secret compromise

Compromise is retroactive and total. Derivation is deterministic with no
per-record randomness, so an attacker who obtains the secret can dictionary
attack every token ever issued under it, including tokens issued years earlier
and tokens already sent to a counterparty. There is no forward secrecy and no
way to invalidate a token that has already left the building.

The only remedy is re-tokenization. Generate a new secret, assign it a new
`KeyID`, re-tokenize the source records, and treat every token issued under the
old secret as compromised plaintext-equivalent data. Tokens already shared with
counterparties cannot be recalled, so rotation limits future exposure only.

Rotate on a schedule anyway, so that the re-tokenization path is exercised and
known to work before you need it.

## No compliance claim

Sriracha makes **no claim** of HIPAA or GDPR compliance, and no claim of
compliance with any other regime. It is a tokenization library. Compliance is a
property of a whole system, including legal basis, data processing agreements,
retention, access control, and audit, none of which this library provides or
assesses. Consult your own counsel and your own privacy engineers.
