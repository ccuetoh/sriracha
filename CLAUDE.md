# Sriracha — Claude Instructions

## Module

`go.sriracha.dev`, Go 1.25+. External runtime deps should be carefully considered. Tests use
`github.com/stretchr/testify`.

## Package layout
```
sriracha/        # go.sriracha.dev/sriracha — root types, fields, interfaces
normalize/       # Unicode normalization pipeline
token/           # HMAC-SHA256 deterministic + Bloom filter probabilistic tokenizers (uses bits-and-blooms/bitset)
fieldset/        # FieldSet validation and canonical V0.1 schema
session/         # high-level Session that bundles a Tokenizer with a FieldSet
test/bench/      # OpenSanctions quality + perf harness, gated by //go:build bench, ships BMF metrics to Bencher via bench.yml
```

## Hard rules
- **No panics** anywhere except `MustParsePath` (init-time field path declarations only).
- **All error paths must return `error`** — bounds checks, type assertions, I/O all return errors.
- **No comment separators** like `// --- Section name ---`.

## Key design decisions
- `FieldPath` is a struct with precomputed `org`, `namespace`, `localName` fields — never split the string at call time.
- `ParseFieldPath(s)` is the validated constructor (returns error); `MustParsePath(s)` panics and is only for package-level `var` declarations.
- `DefaultFieldSet()` returns a deep copy — the internal `defaultV01` is unexported.
- `TokenizeRecord` is deterministic-only; `TokenizeRecordBloom` is probabilistic-only — no internal mode dispatch.

## Testing conventions
- `t.Parallel()` as the **first statement** of every top-level test and every `t.Run` subtest.
- Use `require` for fatal checks (errors that stop the test), `assert` for non-fatal value checks.
- Table-driven tests with named subtests wherever multiple cases test the same function.
- Never use loop variable capture (`tc := tc`) before subtest closures. (Go 1.22+)
- Target 100% coverage.
- No ad-hoc mocks. Use the mocks in ./mock

## Running tests
```bash
go test ./...                         # all packages (skips test/bench)
go test -coverprofile=coverage.out ./... && go tool cover -func=coverage.out
go vet ./...

# Quality benchmark (gated):
go test -tags=bench -count=1 -timeout 10m ./test/bench/...
SRIRACHA_BENCH_OUT=quality.json go test -tags=bench -count=1 ./test/bench/...
```
