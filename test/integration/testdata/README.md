# Integration test fixtures

This directory holds the frozen labelled PII corpus used by the sriracha
integration suite (`test/integration/...`, `//go:build integration`).

## Files

- `febrl_small.csv` — 60 records, 25 cross-party clusters of 2 records each
  plus 10 unmatched singletons. Every cluster's two records share a
  `cluster_id`; about half of the cluster pairs include a single-character
  perturbation in the `given_name` or `surname` field to exercise the
  probabilistic (Bloom) matcher. The columns map to canonical sriracha
  `FieldPath`s as follows:

  | CSV column      | sriracha FieldPath                  |
  | --------------- | ----------------------------------- |
  | `rec_id`        | (record ID, not a field)            |
  | `cluster_id`    | (ground-truth label, not a field)   |
  | `given_name`    | `FieldNameGiven`                    |
  | `surname`       | `FieldNameFamily`                   |
  | `date_of_birth` | `FieldDateBirth` (`YYYY-MM-DD`)     |
  | `suburb`        | `FieldAddressLocality`              |
  | `state`         | `FieldAddressAdminArea`             |
  | `postcode`      | `FieldAddressPostalCode`            |
  | `soc_sec_id`    | `FieldIdentifierNationalID`         |

  The corpus is partitioned at load time: rows are sorted by `rec_id`, then
  even-index rows go to party A and odd-index rows go to party B. With the
  current naming scheme, this places `cluster-NNN-a` in A and `cluster-NNN-b`
  in B for every cluster pair.

## Provenance

The committed `febrl_small.csv` is hand-crafted synthetic data, not derived
from any real population. It exists to bootstrap the integration suite
without requiring a Python toolchain in CI; the values are realistic enough
in shape (Australian-style place names, ISO dates, formatted SSN-like
identifiers) to drive the normalisation, tokenisation, and matching paths
end-to-end.

## Regenerating from FEBRL

Once a maintainer wants to grow the corpus to FEBRL scale (5–10k records),
use the Python regeneration tool under `../scripts/`:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r ../scripts/requirements.txt
python ../scripts/prepfixture.py --out .
```

This produces `febrl_small.csv` from the FEBRL synthetic data generator,
reformatting `date_of_birth` from `YYYYMMDD` → `YYYY-MM-DD` (required by
`normalize.normalizeDate`) and deriving `cluster_id` from FEBRL's
`rec-N-org` / `rec-N-dup-M` record identifiers. Re-running the integration
suite (`go test -tags=integration ./test/integration/...`) against the
regenerated fixture should remain green; if quality thresholds drift,
adjust the `assertMatchQuality` floors in the relevant test.

## Licensing

The committed fixture is original synthetic data released under the same
licence as the rest of the repository (see top-level `LICENSE`). FEBRL
itself is GPL-licensed; its output is not redistributed here.
