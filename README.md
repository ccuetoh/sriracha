# testdata-corpus

Frozen benchmark corpus snapshots for the sriracha bench harness. This
orphan branch is data only. It is not part of the Go module: module zips
are built from tagged source trees, so nothing here ships with go get.

The bench harness (go test -tags=bench ./test/bench/...) downloads these
files on demand, verifies their SHA-256 against checksums pinned in the
source tree, and caches them locally.

Contents:

- ncvr.jsonl.gz: North Carolina voter registration derived corpus.
  Snapshot 2026-04-26. Public record under N.C.G.S. 163-82.10; note the
  statutory restriction on commercial use of voter data. Provenance and
  preprocessing are documented in testdata/corpus/ncvr/README.md on main.
- open_sanctions.jsonl.gz: OpenSanctions Default dataset derived corpus.
  Snapshot 2026-04-28. Licensed CC BY-NC 4.0 (non commercial use only),
  https://creativecommons.org/licenses/by-nc/4.0/. For commercial use see
  https://www.opensanctions.org/licensing/. Attribution: data sourced from
  OpenSanctions. Provenance is documented in
  testdata/corpus/opensanctions/README.md on main.

Neither file is covered by the repository's Apache-2.0 license grant.

Data subjects who wish to have a record removed from a corpus snapshot can
open an issue or use the contact in SECURITY.md on main; the snapshot will
be regenerated without the record and this branch force-updated.
