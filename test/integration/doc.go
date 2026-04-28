// Package integration hosts the sriracha end-to-end integration suite. The
// tests themselves live in package integration_test (black-box) and carry the
// //go:build integration tag so they are excluded from the default
// `go test ./...` run. Opt in with:
//
//	go test -tags=integration ./test/integration/...
//
// See test/integration/testdata/README.md for fixture provenance and
// regeneration instructions.
package integration
