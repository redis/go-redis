// Package guard holds the behavioral regression guard.
//
// The guard extracts signals from a source tree with go/parser and compares
// two trees: a change and its merge base. It never builds or runs the
// scanned code, so it is safe to run on untrusted code. It does not import
// go-redis, so it works when the root package does not compile. Standard
// library only.
//
// # Layers
//
//	01 constants     top-level consts, exported and unexported, with iota position
//	02 defaults      New*, *default*, (*XxxOptions).init: composite fields, x.F = v
//	03 errors        errors.New, fmt.Errorf, new*Err* factories, panic
//	04 logs          log/slog/zap/*logger* calls, prometheus/metrics/*meter calls
//	05 error_policy  error predicates (shouldRetry, isBadConn, ...): decisions in
//	                 source order, their call sites, and conn removal/close
//	06 panics        panic/log.Fatal/os.Exit reachable outside constructors
//	07 deps          every go.mod: go, toolchain, require, replace, ...
//	08 imports       import paths per package directory
//	09 tests         test funcs, Ginkgo nodes, skips, //nolint, CI go test lines,
//	                 golangci config
//	10 protocol      command tokens per command-building func in package redis
//	11 api           exported funcs, methods, types, fields, interface methods
//	                 and vars, with signatures (types only)
//
// Source layers skip example, doctests, e2e, fuzz, scripts and customvet
// directories and internal/guard itself. Layer 9 scans every Go file.
// Entries hold the package directory but no file name or line, so moving
// code between files of a package is not a change.
//
// # What needs approval
//
// A change is an entry added (+) or removed (-). Each layer has a tier:
//
//	gated   05, 06, 07, 08: every change.
//	named   01, 02, 10, 11: removals, and additions whose identity (const
//	        name, function and field, command function, API symbol) exists
//	        in the base, i.e. a changed value. A new identity is for
//	        information. In 10, a command function whose new token list
//	        keeps all old tokens in order (a new option) is for information.
//	paired  03, 04: removals, and additions next to a removal of the same
//	        kind in the same package (a changed message). Other additions
//	        are for information.
//	info    09: never needs approval.
//
// The digest is the first 12 hex characters of the SHA-256 of the sorted
// lines that need approval, each followed by a newline. Changes for
// information do not affect it.
//
// # Running
//
//	make guard                                   # working tree vs merge base with origin/master
//	scripts/guard-delta.sh <base-rev> [<head-rev>]
//	make guard.selftest                          # tests of the guard itself
//
// guard-delta.sh builds cmd/guard from its own checkout, copies both trees
// to a temp dir (git archive; the working tree copy holds tracked and
// untracked, not ignored files), runs "guard dump" on each and "guard
// delta" on the two dumps. Exit 0: nothing needs approval. Exit 1: something
// does. Exit 2: error.
//
// The exported API is also checked against the last release by
// scripts/apidiff.sh (make apidiff). That check type-checks the code, so
// it runs only in the unprivileged build workflow.
package guard
