# testdata/corpus/ncvr

Benchmark corpus for Sriracha's Bloom-filter tokenizer, derived from the
North Carolina Statewide Voter Registration roll. Provides a real-world
English name-and-address distribution to stress the fuzzy-name + Bloom-Dice
match path that FEBRL4's strong `national_id` signal mostly bypasses.

## Provenance

**Dataset:** North Carolina Statewide Voter Registration snapshot (NCVR)
**URL:** <https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvoter_Statewide.zip>
**Snapshot date:** 2026-04-26 (Last-Modified header on the upstream object)
**Input file:** `ncvoter_Statewide.txt` (tab-delimited, ~4 GB uncompressed)
**Records in source:** ~9 million (active + inactive + removed)

## License and legal posture

NCVR is **public record** under North Carolina General Statute § 163-82.10.
No license attaches; redistribution is permitted. Attribution to the North
Carolina State Board of Elections (NCSBE) is courteous but not required.

This corpus is deliberately scoped to keep the privacy footprint bounded:

- **Dropped at source:** street address, full date of birth (NCVR carries
  birth_year only — even that is dropped), SSN, drivers-license number,
  party affiliation, gender, race, ethnicity, voter-status reason,
  registration date.
- **Internal-only:** `voter_reg_num` and `ncid` are used to seed
  `canonical_id` but do not appear in the emitted JSONL.
- **Retained:** given / middle / family / full name; city; state code;
  5-digit ZIP; constant country = `US`.

The retained fields match what OpenSanctions already exposes for sanctioned
individuals; this corpus does not change the repository's overall posture
on real-name PII.

## Preprocessing

NCVR columns are mapped to Sriracha `FieldPath` strings:

| NCVR column      | Sriracha path                       |
| ---------------- | ----------------------------------- |
| `first_name`     | `sriracha::name::given`             |
| `middle_name`    | `sriracha::name::middle`            |
| `last_name`      | `sriracha::name::family`            |
| `res_city_desc`  | `sriracha::address::locality`       |
| `state_cd`       | `sriracha::address::admin_area`     |
| `zip_code` (5d)  | `sriracha::address::postal_code`    |

A synthetic `sriracha::name::full` is composed from `given [middle] family`
when at least given and family are non-empty. `sriracha::address::country`
is pinned to `US`.

Filtering: `status_cd == "A"` (active voters only); `ncid` non-empty;
`first_name` and `last_name` non-empty. Sampling: a SHA-256 hash of `ncid`
modulo 1500 selects ~1 in 1500 active voters, landing on ~5 000 originals.

## Ground truth via synthetic corruption

NCVR is a current-state snapshot — one row per `ncid`, no native ground-truth
duplicate labels. To measure precision / recall / F1 / AUROC we fabricate
the positive class using the standard PPRL approach (Christen 2009): for
each sampled original, emit one duplicate with 1–2 controlled modifications.

Operators applied (one per edit):

- **Typo** — keyboard-adjacent character substitution.
- **Drop char** — single-character deletion at a random position.
- **Phonetic** — common digraph substitution (`ph→f`, `ck→k`, `ie→y`, etc.).
- **OCR** — visually-confusable substitution (`m↔rn`, `e↔c`, `i↔l`, `o↔0`).
- **Missing** — drop the field entirely.

Edits are weighted toward name fields (which is what real cross-source
duplicates disagree on) and away from address fields (which are typically
copy-pasted from forms in voter records). Each duplicate is seeded by
`SHA-256(canonical_id || "::dup")` so corruption is reproducible across
regenerations.

Records sharing a `canonical_id` are positive pairs; everything else is a
negative.

## Sampling

`ncvr.jsonl` contains ~8 800 records spanning ~4 400 canonical groups
(every group is exactly size 2 — one original + one synthetic duplicate).
The exact count drifts slightly when NCVR's roll size shifts; the
sample-rate constant in the script lands the count near 5 000 groups by
construction.

## Regenerating

The corpus can be reproduced byte-for-byte from the upstream NCVR release
when paired with the same snapshot:

```sh
curl -L -o ncvoter.zip "https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvoter_Statewide.zip"
unzip ncvoter.zip ncvoter_Statewide.txt
python3 testdata/corpus/ncvr/scripts/gen_ncvr.py ncvoter_Statewide.txt testdata/corpus/ncvr/ncvr.jsonl
```

The upstream URL is mutable: NCSBE re-publishes weekly, so a regeneration
against today's file will produce a different sample. The committed
`ncvr.jsonl` is frozen against the snapshot dated above, and the script
applies a deterministic hash filter so re-running it against the **same**
input file produces the **same** output bytes.
