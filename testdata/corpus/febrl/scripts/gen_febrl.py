"""Generate testdata/corpus/febrl/febrl.jsonl from recordlinkage's FEBRL4 dataset.

This is a one-off provenance script — checked into the repo at scripts/ for
reproducibility, but not part of the build. The output JSONL is what tests
consume.
"""

from __future__ import annotations
import json
import sys
from pathlib import Path

import pandas as pd
from recordlinkage.datasets import load_febrl4


# Mapping from FEBRL columns to Sriracha field paths. Fields not present in
# DefaultFieldSet (street_number, address_1, address_2) are dropped because
# they would not be tokenized anyway.
FIELD_MAP = {
    "given_name": "sriracha::name::given",
    "surname": "sriracha::name::family",
    "soc_sec_id": "sriracha::identifier::national_id",
    "postcode": "sriracha::address::postal_code",
    "suburb": "sriracha::address::locality",
    "state": "sriracha::address::admin_area",
}


def canonical_id(rec_id: str) -> str:
    # rec-1234-org / rec-1234-dup-N → "1234". A shared canonical_id is the
    # ground-truth match signal Sriracha's harness consumes.
    if rec_id.startswith("rec-"):
        body = rec_id[4:]
        for suffix_marker in ("-org", "-dup-"):
            i = body.find(suffix_marker)
            if i >= 0:
                return body[:i]
    return rec_id


def to_iso_date(s: str) -> str:
    # FEBRL dates are YYYYMMDD strings; Sriracha's normalizer requires
    # YYYY-MM-DD ISO 8601. Invalid synthetic dates pass through unchanged
    # and are dropped by the harness's Sanitize step.
    if not isinstance(s, str) or len(s) != 8 or not s.isdigit():
        return s
    return f"{s[0:4]}-{s[4:6]}-{s[6:8]}"


def to_record(row: pd.Series) -> dict:
    out: dict = {}
    for col, path in FIELD_MAP.items():
        val = row.get(col)
        if pd.isna(val):
            continue
        text = str(val).strip()
        if not text:
            continue
        out[path] = text

    dob = row.get("date_of_birth")
    if pd.notna(dob):
        out["sriracha::date::birth"] = to_iso_date(str(dob).strip())

    given = out.get("sriracha::name::given", "").strip()
    family = out.get("sriracha::name::family", "").strip()
    if given and family:
        out["sriracha::name::full"] = f"{given} {family}"

    # FEBRL4 is wholly Australian; pin country so address blocks compare on
    # equal footing across the corpus.
    out["sriracha::address::country"] = "AU"
    return out


def emit(df: pd.DataFrame, dataset: str, out):
    for rec_id, row in df.iterrows():
        record = to_record(row)
        if not record:
            continue
        line = {
            "canonical_id": canonical_id(rec_id),
            "entity_id": rec_id,
            "dataset": dataset,
            "record": record,
            "aliases": [],
        }
        out.write(json.dumps(line, sort_keys=True))
        out.write("\n")


def main(path: Path) -> None:
    df_a, df_b, _links = load_febrl4(return_links=True)
    path.parent.mkdir(parents=True, exist_ok=True)
    written = 0
    with path.open("w", encoding="utf-8") as f:
        for df, name in ((df_a, "febrl4_org"), (df_b, "febrl4_dup")):
            before = written
            emit(df, name, f)
            written += len(df)
    print(f"wrote {path} ({written} records)", file=sys.stderr)


if __name__ == "__main__":
    out = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("febrl.jsonl")
    main(out)
