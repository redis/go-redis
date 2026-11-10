// Package guard holds the behavioral regression guard (docs/negative-test.md).
//
// Each TestGuard_* scans the repository's Go sources with go/parser and
// compares the extracted signals with a golden file in testdata/guard_*.json.
// The package does not import go-redis, so the guard still runs when the root
// package does not compile.
//
// The tests are behind the "guard" build tag, so the regular test matrix
// skips them. They need no Redis and run once in CI (guard-gate.yml):
//
//	make guard    # go test -tags guard -count=1 ./internal/guard
//
// Goldens are changed only by the CI guard bot (guard-update label).
package guard
