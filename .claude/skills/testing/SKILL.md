---
name: testing
description: Use when running go-redis tests of any kind — the full suite, a single test, or a filtered/focused subset (e.g. only cluster, sentinel, or pool specs); bringing up or tearing down the Docker test stack; the e2e maintenance-notifications suite (including the proxy-free logic tests); building or formatting; or setting the REDIS_VERSION / RE_CLUSTER / REDIS_PORT knobs that drive the test image and version gating. Covers how to focus a Ginkgo spec and how to run plain go tests.
---

# Running tests, builds, and the Docker stack

Tests run against a Redis stack started via Docker Compose. Profiles in `docker-compose.yml` control which services come up (`standalone`, `cluster`, `sentinel`, `all`, `e2e`, …).

## Make targets

```sh
make docker.start          # bring up the full test stack (profile: all)
make docker.stop
make test                  # docker.start -> test.ci -> docker.stop
make test.ci               # run tests assuming containers are already up
make test.ci.skip-vectorsets   # used when REDIS_VERSION < 8
make bench                 # go test -bench=. across all modules
make fmt                   # gofumpt + goimports (-local github.com/redis/go-redis)
make build
make go_mod_tidy           # go mod tidy across every module
```

The Makefile iterates over every `go.mod` (`GO_MOD_DIRS`) for `test.ci`, `go_mod_tidy`, etc.

## E2E (maintenance notifications)

Needs an extra `cae-resp-proxy` service:

```sh
make test.e2e              # auto-starts the e2e profile, runs ./maintnotifications/e2e/, tears down
make test.e2e.docker       # subset that runs inside docker
make test.e2e.logic        # logic-only tests, no proxy required
```

## Env knobs (passed through the Makefile)

- `REDIS_VERSION` — e.g. `8.8`. Drives both the test image tag and `main_test.go` version-gating helpers (`SkipBeforeRedisVersion` / `SkipAfterRedisVersion`).
- `CLIENT_LIBS_TEST_IMAGE` — full image ref, e.g. `redislabs/client-libs-test:8.8-m03`. (To change the default tag, use the `update-ci-image` skill — it lives in several files.)
- `RE_CLUSTER=true` — point tests at a Redis Enterprise cluster instead of the docker-compose stack. The suite then skips ring/sentinel/TLS-cluster setup.
- `RCE_DOCKER=true` — using Redis CE in docker (default for `make test`).
- `REDIS_PORT` — override the default standalone port (`6380`) used by `main_test.go`.

## Running a single test

The root suite is Ginkgo-based (`bsm/ginkgo` + `bsm/gomega` forks). `go test -run` matches the Go-level wrapper, so to focus a Ginkgo spec use the Ginkgo focus flag:

```sh
go test -run TestGinkgoSuite ./... -ginkgo.focus="ZAdd"
go test -run TestGinkgoSuite . -ginkgo.focus="cluster"
```

Plain `go test`-style tests (most files outside the Ginkgo suite, e.g. `internal/...`, `maintnotifications/...`) work the usual way:

```sh
go test -run TestPoolNew ./internal/pool/...
go test -race -run TestCircuitBreaker ./maintnotifications/...
```

## Version-gating

Gate Redis-version-specific tests with `SkipBeforeRedisVersion` / `SkipAfterRedisVersion` (driven by `REDIS_VERSION`) rather than skipping at the suite level.
