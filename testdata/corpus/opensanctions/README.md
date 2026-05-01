# testdata/corpus/opensanctions

Benchmark corpus for Sriracha's Bloom-filter tokenizer, derived from OpenSanctions.
Measures precision, recall, and latency on real-world multi-jurisdiction person records,
using natural cross-source duplicates as ground truth.

## Provenance

**Dataset:** OpenSanctions Default dataset (`all`)
**URL:** <https://www.opensanctions.org/datasets/default/>
**Snapshot date:** 2026-04-28
**Input file:** `statements.csv` (long-form FTM statements, ~10.5 GB uncompressed)

## License

Data is published under [CC BY-NC 4.0](https://creativecommons.org/licenses/by-nc/4.0/).
Non-commercial use only. For commercial use obtain a license at <https://www.opensanctions.org/licensing/>.

Attribution: data sourced from [OpenSanctions](https://www.opensanctions.org/), snapshot 2026-04-28.

## Preprocessing

Only `schema == "Person"` rows are retained. `original_value` is preferred over `value` so
Sriracha's normalizer performs cleaning rather than inheriting OpenSanctions' deduplication.
FTM properties are mapped to Sriracha `FieldPath` strings; `alias`, `weakAlias`, and
`previousName` are collected into a separate `aliases` list.

Ground truth is constructed by grouping records by `(canonical_id, entity_id)`. Records
sharing a `canonical_id` are positive pairs; different `canonical_id` values are negative pairs.

## Sampling

`open_sanctions.jsonl` is a ~2% deterministic sample (N=50) of canonical groups from the
full corpus (~1 338 711 records), keeping all source records for each person together: