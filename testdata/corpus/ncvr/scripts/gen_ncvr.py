"""Generate testdata/corpus/ncvr/ncvr.jsonl from a North Carolina Statewide
Voter snapshot.

Input format: the tab-delimited ncvoter_Statewide.txt file from
<https://s3.amazonaws.com/dl.ncsbe.gov/data/ncvoter_Statewide.zip>. Pass
the unzipped path on the command line.

Output: ~10 000 records (5 000 originals + 5 000 synthetic duplicates) in
the same JSONL shape as the OpenSanctions and FEBRL4 corpora. Records
sharing a canonical_id are positive pairs; everything else is a negative.

Why synthetic duplicates? NCVR is a current-state snapshot — one row per
ncid, no native ground-truth duplicate labels. To measure precision /
recall / F1 / AUROC we have to fabricate the positive class. We borrow
the standard PPRL synthesis approach (Christen 2009): for each sampled
original, emit one duplicate with 1-2 controlled modifications drawn
from a typo / phonetic / OCR / missing-value pipeline. The modifications
target name and address fields specifically — the goal of including NCVR
in this harness is to stress the fuzzy-name + Bloom-Dice path on
realistic English-distribution names that FEBRL4 doesn't reach.

Privacy posture: NCVR is public record under North Carolina General
Statute § 163-82.10. The script emits names and city / state / zip /
country only — street address, DOB, SSN, drivers-license ID, party
affiliation, gender, race, and ethnicity are all dropped at the source
mapping. voter_reg_num and ncid are used only as internal canonical_id
seeds; they do not appear in the output JSONL.

Reproducibility: the sample is deterministic across runs given a fixed
input snapshot. A SHA-256 hash of (ncid) decides whether a record is
included; corruption decisions key off SHA-256(ncid + salt) so each
duplicate's modifications are stable.
"""

from __future__ import annotations
import hashlib
import json
import random
import sys
from pathlib import Path


# Sample-rate controls how many of NCVR's ~9M rows survive. We aim for ~5 000
# originals after filtering for active voters with non-empty names, so the
# rate is calibrated to the active-voter share. Adjust if NCVR's roll size
# drifts substantially.
TARGET_ORIGINALS = 5_000
SAMPLE_DENOMINATOR = 1_500   # 1-in-N hash filter; tuned empirically to land near TARGET_ORIGINALS

# Columns we read from the source. Indices are recomputed at runtime against
# the actual header row so a future column reorder doesn't silently break.
COLUMNS = [
    "ncid", "voter_reg_num", "status_cd",
    "first_name", "middle_name", "last_name",
    "res_city_desc", "state_cd", "zip_code",
]


def parse_header(line: str) -> dict[str, int]:
    fields = [c.strip().strip('"') for c in line.rstrip("\n").split("\t")]
    return {name: i for i, name in enumerate(fields)}


def parse_row(line: str, idx: dict[str, int]) -> dict[str, str]:
    fields = [c.strip().strip('"') for c in line.rstrip("\n").split("\t")]
    return {name: fields[i] if i < len(fields) else "" for name, i in idx.items()}


def passes_sample(ncid: str) -> bool:
    # Deterministic: same ncid always lands on the same side of the cut.
    h = hashlib.sha256(ncid.encode("utf-8")).digest()
    return int.from_bytes(h[:8], "big") % SAMPLE_DENOMINATOR == 0


def deterministic_rng(seed_str: str) -> random.Random:
    # Per-record RNG so each duplicate's corruption is reproducible without
    # global state leaking between records.
    h = hashlib.sha256(seed_str.encode("utf-8")).digest()
    return random.Random(int.from_bytes(h, "big"))


def to_record(row: dict[str, str]) -> dict[str, str]:
    out = {}
    given = row["first_name"].strip()
    middle = row["middle_name"].strip()
    family = row["last_name"].strip()
    if given:
        out["sriracha::name::given"] = given
    if middle:
        out["sriracha::name::middle"] = middle
    if family:
        out["sriracha::name::family"] = family
    if given and family:
        full = f"{given} {middle} {family}".strip() if middle else f"{given} {family}"
        out["sriracha::name::full"] = " ".join(full.split())

    city = row["res_city_desc"].strip()
    state = row["state_cd"].strip()
    zip_code = row["zip_code"].strip()
    if city:
        out["sriracha::address::locality"] = city
    if state:
        out["sriracha::address::admin_area"] = state
    if zip_code:
        # NCVR sometimes carries 9-digit ZIP+4; truncate to 5 for canonical
        # matching across the country.
        out["sriracha::address::postal_code"] = zip_code[:5]
    out["sriracha::address::country"] = "US"
    return out


# Christen 2009-style corruption operators. Each operates on a single field
# value and returns a corrupted version. The probabilities are tuned to land
# 1-2 modifications per duplicate on average, with a bias toward name fields
# (which are what real-world cross-source duplicates disagree on).
TYPO_KEYBOARD = {
    "a": "qwsz", "b": "vghn", "c": "xdfv", "d": "serfcx", "e": "wsdr",
    "f": "drtgvc", "g": "ftyhbv", "h": "gyujnb", "i": "ujko", "j": "huikmn",
    "k": "jiolm", "l": "kop", "m": "njk", "n": "bhjm", "o": "iklp",
    "p": "ol", "q": "wa", "r": "edft", "s": "awedxz", "t": "rfgy",
    "u": "yhji", "v": "cfgb", "w": "qase", "x": "zsdc", "y": "tghu",
    "z": "asx",
}


def corrupt_typo(value: str, rng: random.Random) -> str:
    if not value:
        return value
    i = rng.randrange(len(value))
    ch = value[i].lower()
    repl = TYPO_KEYBOARD.get(ch)
    if not repl:
        return value
    new = rng.choice(repl)
    if value[i].isupper():
        new = new.upper()
    return value[:i] + new + value[i + 1:]


def corrupt_drop_char(value: str, rng: random.Random) -> str:
    if len(value) < 3:
        return value
    i = rng.randrange(len(value))
    return value[:i] + value[i + 1:]


def corrupt_phonetic(value: str, rng: random.Random) -> str:
    pairs = [("ph", "f"), ("ck", "k"), ("ie", "y"), ("oo", "u"),
             ("ee", "e"), ("th", "t"), ("kn", "n"), ("wr", "r")]
    rng.shuffle(pairs)
    lower = value.lower()
    for a, b in pairs:
        if a in lower:
            i = lower.index(a)
            return value[:i] + b + value[i + len(a):]
    return value


def corrupt_ocr(value: str, rng: random.Random) -> str:
    swaps = {"m": "rn", "rn": "m", "e": "c", "c": "e", "i": "l", "l": "i", "o": "0", "0": "o"}
    keys = list(swaps.keys())
    rng.shuffle(keys)
    for k in keys:
        idx = value.lower().find(k)
        if idx >= 0:
            return value[:idx] + swaps[k] + value[idx + len(k):]
    return value


def corrupt_missing(value: str, rng: random.Random) -> str:
    return ""


CORRUPTORS = [corrupt_typo, corrupt_drop_char, corrupt_phonetic, corrupt_ocr, corrupt_missing]


# Field-corruption weights: name fields take most edits, address fields take
# fewer (they're rarely typo'd in voter records — copy-pasted from forms).
FIELD_WEIGHTS = {
    "sriracha::name::given": 4,
    "sriracha::name::family": 4,
    "sriracha::name::middle": 3,
    "sriracha::name::full": 3,
    "sriracha::address::locality": 2,
    "sriracha::address::postal_code": 1,
    "sriracha::address::admin_area": 1,
}


def corrupt_record(rec: dict[str, str], rng: random.Random) -> dict[str, str]:
    out = dict(rec)
    n_edits = rng.choice([1, 1, 2])  # 2/3 single edit, 1/3 double edit
    for _ in range(n_edits):
        # Recompute the eligible-field list each iteration: a previous edit
        # may have dropped a field via corrupt_missing.
        fields = [(p, w) for p, w in FIELD_WEIGHTS.items() if p in out]
        if not fields:
            break
        path = rng.choices([p for p, _ in fields], weights=[w for _, w in fields], k=1)[0]
        op = rng.choice(CORRUPTORS)
        new_val = op(out[path], rng)
        if new_val == "":
            del out[path]
        else:
            out[path] = new_val
    # If we wiped given+family, the synthetic 'full' is stale; drop it too.
    if "sriracha::name::full" in out:
        if "sriracha::name::given" not in out and "sriracha::name::family" not in out:
            del out["sriracha::name::full"]
    return out


def main(input_path: Path, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    rows: list[tuple[str, dict[str, str]]] = []
    with input_path.open("r", encoding="latin-1") as f:
        header = parse_header(f.readline())
        idx = {col: header[col] for col in COLUMNS}
        for line in f:
            row = parse_row(line, idx)
            if row["status_cd"] != "A":  # active voters only
                continue
            ncid = row["ncid"].strip()
            if not ncid:
                continue
            if not passes_sample(ncid):
                continue
            if not (row["first_name"].strip() and row["last_name"].strip()):
                continue
            rec = to_record(row)
            if not rec:
                continue
            canonical = hashlib.sha256(ncid.encode("utf-8")).hexdigest()[:16]
            rows.append((canonical, rec))
            if len(rows) >= TARGET_ORIGINALS:
                break

    if not rows:
        print("bench: ncvr sample is empty — check input path / sample denominator", file=sys.stderr)
        sys.exit(1)

    # Emit originals + synthetic duplicates, sorted by canonical_id for stable
    # diffs across regenerations.
    rows.sort(key=lambda kv: kv[0])
    with output_path.open("w", encoding="utf-8") as f:
        for canonical, rec in rows:
            org = {
                "canonical_id": canonical,
                "entity_id": canonical + "-org",
                "dataset": "ncvr_org",
                "record": rec,
                "aliases": [],
            }
            f.write(json.dumps(org, sort_keys=True) + "\n")
            rng = deterministic_rng(canonical + "::dup")
            dup = corrupt_record(rec, rng)
            line = {
                "canonical_id": canonical,
                "entity_id": canonical + "-dup",
                "dataset": "ncvr_dup",
                "record": dup,
                "aliases": [],
            }
            f.write(json.dumps(line, sort_keys=True) + "\n")

    print(f"wrote {output_path} ({2 * len(rows)} records, {len(rows)} canonical groups)", file=sys.stderr)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("usage: gen_ncvr.py <ncvoter_Statewide.txt> [output.jsonl]", file=sys.stderr)
        sys.exit(2)
    inp = Path(sys.argv[1])
    out = Path(sys.argv[2]) if len(sys.argv) > 2 else Path("ncvr.jsonl")
    main(inp, out)
