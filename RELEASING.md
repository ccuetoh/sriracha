# Releasing

This is the checklist for cutting a Sriracha release. It exists because the
`v0.2` release was tagged in a way the Go tool cannot resolve, and no consumer
has been able to install it.

## Version tags

Module version tags **must** be three-component `vMAJOR.MINOR.PATCH`.

`v0.2` is not a valid Go module version. `go list -m
github.com/ccuetoh/sriracha@v0.2` fails with `no matching versions`, and
`@latest` still resolves to `v0.1.1`. A two-component tag does not publish a
release; it publishes nothing. The v0.2 work needs to be re-tagged as `v0.2.0`
before anyone can install it.

Leave the bad tag in place and add the correct one pointing at the same commit.
Deleting a published tag breaks the module proxy's immutability assumptions and
is worse than an unused tag sitting there.

Pre-release tags follow the same rule: `v0.3.0-rc.1`, not `v0.3-rc1`.

## Security releases

Every security fix gets its own tag on the supported minor. A commit on `main`
is not a release. Consumers resolve tags, so an untagged fix reaches nobody who
is not tracking `main` directly.

Order of operations:

1. Land the fix on `main`.
2. Tag the patch release on the supported minor.
3. Publish the advisory naming affected and fixed versions.

Never publish the advisory before the tag exists. See [`SECURITY.md`](SECURITY.md).

## Go version policy

Sriracha supports the **two most recent Go releases**. Older toolchains are not
tested and not supported.

The current floor is **Go 1.25.0**, set by `golang.org/x/text v0.41.0`, which is
the version carrying the GO-2026-5970 fix and which requires 1.25.0. The floor
is not raised for convenience; it moves when a dependency we need for a security
fix forces it, or when the two-release window has moved past it.

**If the `go` directive in `go.mod` moved since the last tag, the release cannot
be a patch release.** Raising the language floor breaks consumers on the older
toolchain, and a patch release is the one kind of release that must never do
that. Pre-1.0 the raise goes out as a minor bump. Post-1.0 a Go floor raise is
minor-at-minimum, and a raise that also breaks the API is a major.

Check it before tagging:

```bash
git diff $(git describe --tags --abbrev=0)..HEAD -- go.mod | grep '^[+-]go '
```

Any output means the release is not a patch.

## Release checklist

1. `task ci` is green on `main`, and CI is green on the release commit.
2. `go.mod`'s `go` directive is unchanged since the last tag, or the version
   bump is minor or larger. See above.
3. `CHANGELOG.md` has the `[Unreleased]` section renamed to the new version
   with a date, and a fresh empty `[Unreleased]` above it.
4. `README.md`'s install line names the exact tag being cut, in three-component
   form. It currently reads `@v0.2`, which does not resolve.
5. `fieldset.DefaultFieldSet()`'s `Version` matches what the release notes
   claim. It is a schema version, not the module version, and it does not move
   just because the module version moves.
6. Tag with `vMAJOR.MINOR.PATCH` and push the tag.
7. Confirm the module proxy sees it:

   ```bash
   go list -m github.com/ccuetoh/sriracha@vX.Y.Z
   go list -m github.com/ccuetoh/sriracha@latest
   ```

   Both must succeed and `@latest` must report the new version. If they do not,
   the release did not happen.
8. Create the GitHub release from the tag, pasting the CHANGELOG section.

## Wire breaks

A change to the FieldSet fingerprint or to a token format discriminator is a
wire break, not just an API break. Parties holding tokens from an earlier
release cannot compare against tokens from the new one, and there is no
migration other than re-tokenizing from source records with a coordinated
cutover.

Wire breaks need their own callout at the top of the release notes, naming what
changed and that all parties must re-tokenize together. The fingerprint and the
golden vector tests exist to make an accidental wire break fail CI; if a golden
test needed updating to land the change, the change is a wire break.

An API-only release, where the fingerprint and the golden vectors are
byte-unchanged, must say so explicitly. Consumers need to know they can upgrade
without re-tokenizing.
