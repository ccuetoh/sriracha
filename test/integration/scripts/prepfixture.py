"""Regenerate test/integration/testdata/febrl_small.csv from FEBRL output.

Usage:
    python prepfixture.py --out ../testdata/

This is a maintainer-only script; CI runs the integration suite against the
frozen CSV that this script emits, so it does not need to run on every push.

The script wraps FEBRL's synthetic data generator (`ds-gen` / the `febrl`
package's `generator` module), then post-processes the output to match the
sriracha fixture conventions:

  - Reformats `date_of_birth` from `YYYYMMDD` (FEBRL's default) to ISO 8601
    `YYYY-MM-DD`. The sriracha date normaliser rejects any other format.
  - Derives `cluster_id` from FEBRL's record identifiers, which follow the
    pattern `rec-{N}-org` for the original record and `rec-{N}-dup-{M}` for
    each duplicate; both share the same N and therefore the same cluster.
  - Keeps the canonical 9-column layout consumed by `corpus_test.go`:
        rec_id, cluster_id, given_name, surname,
        date_of_birth, suburb, state, postcode, soc_sec_id

The committed `febrl_small.csv` was hand-crafted (no FEBRL dependency) so
that the integration suite runs everywhere without requiring this script.
Use this script when you want to grow the corpus past a few hundred rows or
add deliberate FEBRL-style perturbations (typos, missing fields, swapped
characters).
"""

from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path


CLUSTER_RE = re.compile(r"^rec-(\d+)(?:-(?:org|dup-\d+))?$")


def cluster_id_from_rec_id(rec_id: str) -> str:
    """Derive a stable cluster identifier from a FEBRL record id.

    >>> cluster_id_from_rec_id("rec-42-org")
    'cluster-42'
    >>> cluster_id_from_rec_id("rec-42-dup-1")
    'cluster-42'
    """
    m = CLUSTER_RE.match(rec_id)
    if not m:
        raise ValueError(f"unexpected FEBRL rec_id format: {rec_id!r}")
    return f"cluster-{int(m.group(1)):03d}"


def reformat_dob(dob: str) -> str:
    """Convert FEBRL's YYYYMMDD to ISO 8601 YYYY-MM-DD."""
    if len(dob) == 10 and dob[4] == "-" and dob[7] == "-":
        return dob  # already ISO
    if len(dob) != 8 or not dob.isdigit():
        raise ValueError(f"unexpected DOB shape: {dob!r}")
    return f"{dob[0:4]}-{dob[4:6]}-{dob[6:8]}"


def transform(in_path: Path, out_path: Path) -> int:
    """Read FEBRL CSV at in_path, emit normalised CSV at out_path. Returns row count."""
    rows_written = 0
    with in_path.open(newline="", encoding="utf-8") as fin, \
         out_path.open("w", newline="", encoding="utf-8") as fout:
        reader = csv.DictReader(fin)
        writer = csv.DictWriter(
            fout,
            fieldnames=[
                "rec_id", "cluster_id", "given_name", "surname",
                "date_of_birth", "suburb", "state", "postcode", "soc_sec_id",
            ],
        )
        writer.writeheader()
        for row in reader:
            rec_id = row["rec_id"]
            writer.writerow({
                "rec_id":        rec_id,
                "cluster_id":    cluster_id_from_rec_id(rec_id),
                "given_name":    row["given_name"],
                "surname":       row["surname"],
                "date_of_birth": reformat_dob(row["date_of_birth"]),
                "suburb":        row["suburb"],
                "state":         row["state"],
                "postcode":      row["postcode"],
                "soc_sec_id":    row["soc_sec_id"],
            })
            rows_written += 1
    return rows_written


def main(argv: list[str]) -> int:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--in", dest="in_path", type=Path, default=Path("febrl_raw.csv"),
                   help="FEBRL ds-gen output CSV (default: febrl_raw.csv)")
    p.add_argument("--out", dest="out_dir", type=Path, default=Path("../testdata"),
                   help="Directory in which to write febrl_small.csv")
    args = p.parse_args(argv)

    if not args.in_path.exists():
        print(
            f"error: FEBRL input {args.in_path} not found.\n"
            "Generate it first with FEBRL's ds-gen tool, e.g.:\n"
            "    ds-gen --num-records 5000 --num-duplicates 1 "
            "--out febrl_raw.csv",
            file=sys.stderr,
        )
        return 2

    args.out_dir.mkdir(parents=True, exist_ok=True)
    out = args.out_dir / "febrl_small.csv"
    n = transform(args.in_path, out)
    print(f"wrote {n} rows to {out}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
