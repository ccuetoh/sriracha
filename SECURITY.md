# Security Policy

## Supported Versions

Security fixes are delivered as a new patch tag on the supported minor. Tags
are what `go get` resolves, so a fix that only lands on `main` is not a fix
that consumers receive.

| Line     | Status                                        |
|----------|-----------------------------------------------|
| `v0.2.x` | Supported. Security fixes land here.          |
| `v0.1.x` | End of life. Upgrade to `v0.2.x`.             |
| `main`   | Fast path. Fixes land here first, untagged.   |

Only the most recent minor is supported. When `v0.3.0` ships, `v0.2.x` goes
end of life and the supported line moves with it. This is a pre-1.0 project
and there are no long-term support branches.

Tracking `main` is a supported way to get fixes before they are tagged, at the
cost of an unstable API. Everyone else should pin a `v0.2.x` tag and upgrade
the patch when an advisory is published.

The `v0.2` tag is two-component and is not a valid Go module version, so the
Go tool cannot resolve it. It will be re-tagged as `v0.2.0`. See
[`RELEASING.md`](RELEASING.md).

## Secret Custody

The HMAC secret is the whole security boundary. Anyone holding it can
re-derive every token from a guessed identifier, so tokens are only as private
as the secret. Custody, rotation, and the attacks this library does and does
not defend against are described in [`THREAT_MODEL.md`](THREAT_MODEL.md).
Weaknesses already documented there are not vulnerabilities in this library.

## Reporting a Vulnerability

Please report suspected vulnerabilities **privately** using GitHub's
[private vulnerability reporting](https://github.com/ccuetoh/sriracha/security/advisories/new)
on this repository. Do **not** open a public issue, pull request, or discussion
for security-sensitive reports.

When reporting, include:

- A description of the vulnerability and its impact
- Steps to reproduce, ideally with a minimal proof of concept
- The affected tag, or the affected commit range if you are tracking `main`
- Any known mitigations or workarounds

You should expect an initial acknowledgement within **7 days**. We aim to
provide a remediation plan or fix within **30 days** of triage, depending on
severity and complexity.

There is one maintainer, so these windows are best effort. See
[`GOVERNANCE.md`](GOVERNANCE.md) for what happens if that maintainer is
unreachable.

## Disclosure

A fix lands on `main` first, then ships as a patch tag on the supported minor.
Once the tag is pushed we publish a GitHub Security Advisory describing the
issue, the affected and fixed versions, and remediation, crediting the reporter
unless anonymity is requested.

If a fix requires an API or wire-format break it cannot ship as a patch. In
that case the advisory names the mitigation available on the supported line and
the release that carries the real fix.
