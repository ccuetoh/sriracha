# Governance

## Who maintains this

One person: [@ccuetoh](https://github.com/ccuetoh). That is the whole
maintainer set today.

## The self-review gap

`.github/CODEOWNERS` assigns every path to `@ccuetoh`, and the `main` ruleset
requires code owner review. With one maintainer, that means the person writing
a change is the person approving it. Branch protection is satisfied, but no
second pair of eyes ever sees the diff.

This matters most for the cryptographic code in `token/` and for the canonical
encoding in `fingerprint.go`, where a subtle mistake is invisible in tests that
were written by the same person who made the mistake. Treat every crypto change
in this repo as self-reviewed until this section says otherwise.

What partly compensates:

- Golden vector tests in `token/` and `normalize/` fail on any byte-level drift
  in derivation or normalization.
- The FieldSet fingerprint is pinned by a test with a hard-coded digest.
- CI runs `go vet`, `golangci-lint`, `govulncheck`, fuzz targets, and a
  cross-platform test matrix.
- CodeQL and OpenSSF Scorecard run on a schedule.

None of that is review. It catches regressions against decisions already made;
it does not catch a wrong decision.

Outside review of the cryptographic design is welcome and is the single most
useful contribution anyone could make to this project right now.

## How decisions get made

Anything that changes the public API, the token wire format, the normalization
pipeline, or the field schema starts as an issue before it starts as a PR.
The issue records the reasoning, so the decision is auditable later even though
only one person made it.

Everything else is a normal PR. See [`CONTRIBUTING.md`](CONTRIBUTING.md).

Design decisions that survive are recorded in the CHANGELOG entry for the
release that carries them, and pinned by a test wherever a test can pin them.
A decision that cannot be pinned by a test gets a comment at the site that
depends on it.

Disagreement is resolved in the issue thread. If it stays unresolved, the
maintainer decides and writes down why. That is not a good process, it is just
an honest description of a one-person project.

## Becoming a maintainer

There is no application. The path is sustained review-quality contribution,
roughly:

- Several merged non-trivial PRs, meaning real changes rather than typo fixes.
- Demonstrated review of other people's changes, especially in `token/`.
- Willingness to be on the receiving end of a private vulnerability report.

The maintainer invites; there is no vote to hold with a set of one. A new
maintainer is added to `CODEOWNERS` and to the repository's security advisory
access, and this file is updated to name them. Adding a second maintainer
closes the self-review gap above, and that is the point of doing it.

## Succession

If the maintainer is unreachable for **90 days** with open security reports or
unaddressed advisories, the project is unmaintained in practice and should be
treated that way by consumers. Do not wait for an announcement that may never
come. Check the commit history and the advisory list.

The intent, in order of preference:

1. Hand the repository and the security contact to a maintainer added under the
   section above.
2. Failing that, archive the repository so that its unmaintained state is
   visible on the repository page and to the module proxy, and mark the last
   release as unmaintained in a final advisory.

Anyone is free to fork. The Apache 2.0 license permits it without asking, and a
maintained fork is better for consumers than an abandoned original. A fork
carrying security fixes should change the module path so the Go tool resolves
it as a distinct module.

The GitHub organization, the Bencher project, and the Codecov project are all
tied to the same single account. There is no bus-factor mitigation for those
today, and that is a known gap alongside the review one.
