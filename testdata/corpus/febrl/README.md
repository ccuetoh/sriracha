# testdata/corpus/febrl

Benchmark corpus for Sriracha's Bloom-filter tokenizer, derived from FEBRL4.
Measures precision, recall, and latency on synthetic person records with
controlled typo / phonetic / OCR-style noise, using paired ground truth from
the original FEBRL4 ground-truth links.

## Provenance

**Dataset:** FEBRL4 (load_febrl4 from the recordlinkage Python package)
**URL:** <https://recordlinkage.readthedocs.io/en/latest/ref-datasets.html#recordlinkage.datasets.load_febrl4>
**Upstream project:** FEBRL — Freely Extensible Biomedical Record Linkage
**Upstream URL:** <https://sourceforge.net/projects/febrl/>
**Composition:** 5 000 original records + 5 000 duplicate records (one duplicate
per original) with synthetic errors, 5 000 ground-truth match pairs.

## License

Data is published under the **Australian National University Open Source License
(ANUOS), Version 1.3**. The license text and full terms are obtainable from
<https://sourceforge.net/projects/febrl/>.

> The Original Software is: "Febrl - Freely Extensible Biomedical Record Linkage".
> The Initial Developer of the Original Software is Dr Peter Christen
> (Research School of Computer Science, The Australian National University).
> Copyright © 2002–2011 the Australian National University and others.
> All Rights Reserved.

Attribution: data sourced from FEBRL4 via the `recordlinkage` Python package.
Original FEBRL software and synthetic-data generator authored by Peter Christen
and contributors at the Australian National University.

## Preprocessing

FEBRL4 columns are mapped to Sriracha `FieldPath` strings:

| FEBRL column        | Sriracha path                          |
| ------------------- | -------------------------------------- |
| `given_name`        | `sriracha::name::given`                |
| `surname`           | `sriracha::name::family`               |
| `soc_sec_id`        | `sriracha::identifier::national_id`    |
| `date_of_birth`     | `sriracha::date::birth` (YYYY-MM-DD)   |
| `postcode`          | `sriracha::address::postal_code`       |
| `suburb`            | `sriracha::address::locality`          |
| `state`             | `sriracha::address::admin_area`        |

A synthetic `sriracha::name::full` is composed as `"{given} {family}"` when both
parts are present. `sriracha::address::country` is pinned to `AU` for every
record because FEBRL4 is wholly Australian. Street fields (`street_number`,
`address_1`, `address_2`) are dropped because they have no canonical mapping in
the v0.1 default `FieldSet` and would be ignored at tokenization time anyway.

`date_of_birth` is reformatted from FEBRL's `YYYYMMDD` to ISO 8601 `YYYY-MM-DD`.
Synthetically corrupted dates that fail the reformat (e.g. month `00`) are
emitted as-is and dropped by the harness's `Sanitize` step.

Ground truth: FEBRL record IDs follow `rec-N-org` / `rec-N-dup-0`; the numeric
component `N` is used as `canonical_id`. Records sharing a `canonical_id` are
positive pairs (one original + one duplicate); different `canonical_id` values
are negative pairs.

## Sampling

`febrl.jsonl` is the **full FEBRL4 corpus**: 10 000 records spanning 5 000
canonical groups. No sub-sampling is applied because the upstream dataset is
already small and was designed as a complete benchmark. The two FEBRL4 source
DataFrames are concatenated into a single JSONL stream tagged by `dataset` =
`febrl4_org` (originals) or `febrl4_dup` (synthetic duplicates).

## Regenerating

The corpus can be reproduced byte-for-byte from the upstream FEBRL4 release:

```sh
pip install recordlinkage
python3 testdata/corpus/febrl/scripts/gen_febrl.py testdata/corpus/febrl/febrl.jsonl
```

The script applies the field mapping documented above and writes one JSON
object per line, sorted by key for stable diffs.
