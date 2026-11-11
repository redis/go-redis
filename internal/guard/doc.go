// Package guard holds the behavioral regression guard.
//
// Each TestGuard_* scans the repository's Go sources with go/parser and
// compares the extracted signals with a golden file in testdata/guard_*.json.
// The package does not import go-redis, so it runs even when the root package
// does not compile. Standard library only.
//
// # Layers
//
//	 1 Constants     top-level consts, exported and unexported, with iota position
//	 2 Defaults      New*, *default*, (*XxxOptions).init: composite fields, x.F = v
//	 3 Errors        errors.New, fmt.Errorf, new*Err* factories, panic
//	 4 Logs          log/slog/zap/*logger* calls, prometheus/metrics/*meter calls
//	 5 ErrorPolicy   error predicates (shouldRetry, isBadConn, ...): decisions in
//	                 source order, their call sites, and conn removal/close
//	 6 Panics        panic/log.Fatal/os.Exit reachable outside constructors
//	 7 Deps          every go.mod: go, toolchain, require, replace, ...
//	 8 Imports       import paths per package directory
//	 9 AntiCheat     test funcs, Ginkgo nodes, skips, //nolint, CI go test lines,
//	                 golangci config
//	10 Protocol      command tokens per command-building func in package redis
//
// The exported API is checked separately by scripts/apidiff.sh (make apidiff).
//
// # Running
//
// The tests are behind the "guard" build tag, so the regular test matrix
// skips them. They need no Redis and run once in CI (guard-gate.yml):
//
//	make guard    # go test -tags guard -count=1 ./internal/guard
//
// New tests (test funcs, plain Ginkgo nodes, SkipBefore/AfterRedisVersion)
// pass without a golden update. Everything else that changes a golden fails.
//
// # Changing goldens
//
// Only the guard bot commits to testdata/. A maintainer reviews the failing
// diff and adds the guard-update label to the PR (or runs the "Guard update"
// workflow). guard-update.yml regenerates the goldens with the base branch's
// guard code and pushes one signed bot commit. Do not commit goldens from
// make guard.update; guard-provenance rejects them.
//
// guard-gate.yml runs on pull_request_target with base-branch code:
//
//   - guard-provenance: if goldens changed, the newest commit touching them
//     must be a verified bot commit that touches only testdata/, and nothing
//     may change them after it.
//   - guard-check: the base guard against the PR's sources and goldens.
//
// A PR that changes internal/guard/*.go may change only internal/guard/. Its
// guard code is never executed by the gate; to regenerate, a maintainer
// reviews it and runs "Guard update" with trust_pr_guard_code.
//
// # Setup (repository admin)
//
//  1. Bot user with write access, a GPG key (no passphrase, registered on
//     the account, verified email) and a token: fine-grained PAT for
//     same-repo PRs, classic PAT with public_repo for fork PRs.
//  2. Variables: GUARD_BOT_LOGIN, GUARD_BOT_NAME, GUARD_BOT_EMAIL,
//     GUARD_BOT_SIGNING_KEY.
//  3. Environment guard-bot (branches master, v9, v9.*) with secrets
//     GUARD_BOT_TOKEN and GUARD_BOT_GPG_PRIVATE_KEY.
//  4. Label guard-update.
//  5. Branch protection: require guard-provenance, guard-check and code
//     owner review; block force and direct pushes.
package guard
