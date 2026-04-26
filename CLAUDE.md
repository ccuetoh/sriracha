# Sriracha — Claude Instructions

## Module

`go.sriracha.dev`, Go 1.25+. External runtime deps should be carefully considered. Tests use
`github.com/stretchr/testify`.

## Package layout
```
sriracha/        # go.sriracha.dev/sriracha — root types, fields, errors, interfaces
normalize/       # Unicode normalization pipeline
token/           # HMAC-SHA256 deterministic + Bloom filter probabilistic tokenizers (uses bits-and-blooms/bitset)
fieldset/        # FieldSet validation, compatibility, semver-based version negotiation, canonical V0.1
indexer/         # TokenIndexer with pluggable IndexStorage (MemoryStorage or BadgerStorage)
audit/file/      # Append-only JSONL audit log with SHA-256 hash chaining
transport/       # gRPC client and server (mTLS, consent policy, replay cache)
```

## Hard rules
- **No panics** anywhere except `MustParsePath` (init-time field path declarations only).
- **All error paths must return `error`** — bounds checks, type assertions, I/O all return errors.
- **No comment separators** like `// --- Section name ---`.

## Key design decisions
- `FieldPath` is a struct with precomputed `org`, `namespace`, `localName` fields — never split the string at call time.
- `ParseFieldPath(s)` is the validated constructor (returns error); `MustParsePath(s)` panics and is only for package-level `var` declarations.
- `Sentinel` is a typed string (`type Sentinel string`); use `IsNotFound` / `IsNotHeld` helpers — never compare directly.
- `*Error` implements `errors.Is` by `ErrorCode` equality and `Unwrap` for chain traversal.
- `DefaultFieldSet()` returns a deep copy — the internal `defaultV01` is unexported.
- `TokenizeRecord` is deterministic-only; `TokenizeRecordBloom` is probabilistic-only — no internal mode dispatch.

## Testing conventions
- `t.Parallel()` as the **first statement** of every top-level test and every `t.Run` subtest.
- Use `require` for fatal checks (errors that stop the test), `assert` for non-fatal value checks.
- Table-driven tests with named subtests wherever multiple cases test the same function.
- Loop variable capture (`tc := tc`) before subtest closures.
- Target 100% coverage; the only accepted gap is the structurally unreachable `b.Set` error path in `tokenizeFieldBloom`.

## Running tests
```bash
go test ./...                         # all packages
go test -coverprofile=coverage.out ./... && go tool cover -func=coverage.out
go vet ./...
```
