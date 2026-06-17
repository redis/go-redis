# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Repository

go-redis is the official Redis client for Go. Module path: `github.com/redis/go-redis/v9` (Go 1.24+). The repo is a multi-module workspace — every directory containing a `go.mod` is built and tested independently:

- root (`github.com/redis/go-redis/v9`) — the client library.
- `extra/redisotel`, `extra/redisotel-native`, `extra/redisprometheus`, `extra/rediscensus`, `extra/rediscmd` — instrumentation adapters with their own module paths (so they can pin large telemetry deps without forcing them on root consumers).
- `internal/customvet` — custom `go vet` analyzers (also its own module).
- `maintnotifications/e2e`, `doctests`, `fuzz`, examples under `example/` — separate modules.

The Makefile iterates over every `go.mod` (`GO_MOD_DIRS`) when running `test.ci`, `go_mod_tidy`, etc. When you add a dependency in one module, you almost never need to update the others.

## Common commands

Running tests, the Docker test stack, single-test focus, e2e, and the `REDIS_VERSION` / cluster / port knobs are documented in the `testing` skill (auto-triggers on test/docker tasks). `make fmt` (`gofumpt` + `goimports -local github.com/redis/go-redis`), `make build`, and `make go_mod_tidy` are covered there too.

CI also runs the custom vet tool (`go vet -vettool ./internal/customvet/customvet ./...`); the `setval` analyzer requires every `Cmder` with a `Result()` to have a `SetVal()` — see the `add-command` skill.

## Architecture

### Client types (root package)

All clients are in the root package and share most plumbing:

- `Client` (`redis.go`) — single-node client.
- `ClusterClient` (`osscluster.go`) — Redis Cluster aware. `osscluster_router.go` routes commands to the right shard; `internal/routing/` handles cluster-wide aggregation policies (e.g. fan-out for `KEYS`, `DBSIZE`).
- `Ring` (`ring.go`) — client-side sharding across independent Redis nodes (consistent hashing, no cluster protocol).
- Failover client (`sentinel.go`) — Sentinel-managed failover.
- `UniversalClient` (`universal.go`) — wrapper that picks one of the above based on options.

Command surface lives in topical files: `string_commands.go`, `hash_commands.go`, `stream_commands.go`, `search_commands.go`, `vectorset_commands.go`, etc. Each file defines methods on the shared `Cmdable` interface so every client type gets the same API.

### Hooks (`redis.go` `hooksMixin`)

Three hook chains run around every operation: `DialHook`, `ProcessHook`, `ProcessPipelineHook`. Hooks are registered via `client.AddHook(...)` and chain in FIFO order; each hook must call `next` to continue. When a hook wraps an error, it must call `cmd.SetErr(wrappedErr)` so the typed-error helpers (`redis.IsLoadingError`, `IsMovedError`, etc. in `error.go`) keep working through `errors.As`. The README has a longer pipeline-hook example.

### Connection pool (`internal/pool`)

Owns dialing, idle/active connection bookkeeping, conn state (`conn_state.go`), pubsub-conn lifecycle (`pubsub.go`), and the dial-retry/backoff logic that powers `DialerRetries` / `DialerRetryBackoff` (also exposed at `dial_retry_backoff.go` in the root). `OnConnect`, `MinIdleConns`, and the buffer-size options (`ReadBufferSize`/`WriteBufferSize`, default 32 KiB since v9.12) flow through here.

### Protocol (`internal/proto`)

RESP2/RESP3 reader and writer. Push notifications (RESP3 `>`-prefixed frames) are peeked here and dispatched via the `push/` package. The `push.Registry` lets callers register handlers for specific notification names; `pushnotif_handler.go`-style code is also how `maintnotifications` plugs in.

### Maintenance notifications (`maintnotifications/`)

This is a non-trivial subsystem worth understanding before touching cluster/handoff code. It listens for RESP3 push notifications about cluster maintenance (`MOVING`, `MIGRATING`, `MIGRATED`, `FAILING_OVER`, `FAILED_OVER` for standalone; `SMIGRATING`, `SMIGRATED` for cluster) and performs seamless connection handoff to new endpoints. Key pieces:

- `manager.go` — coordinates state transitions.
- `handoff_worker.go` — moves in-flight ops to new connections.
- `pool_hook.go` — integrates with `internal/pool` to mark/replace connections.
- `circuit_breaker.go` — backs off when the upstream is unhealthy.
- `state.go` — per-connection state machine.
- E2E coverage lives in `maintnotifications/e2e/` and drives a fault-injector / RESP proxy (`cae-resp-proxy`).

Configuration is via `redis.Options.MaintNotificationsConfig`; modes are `ModeAuto` (default), `ModeEnabled` (require server support), `ModeDisabled`. RESP3 (`Protocol: 3`) is required.

### Authentication (`auth/`, `internal/auth/streaming`)

Four credential sources, in priority order: streaming provider (e.g. Entra ID via `go-redis-entraid`), context-based provider, function provider, static `Username`/`Password`. The streaming provider is what enables token rotation without reconnecting — the listener in `auth/reauth_credentials_listener.go` issues `AUTH` on each refresh.

### Internal helpers

- `internal/hscan` — struct scanning for `HGETALL` results (`Scan` interface re-exported as `redis.Scanner`).
- `internal/hashtag` — extracts `{tag}` segments for cluster slot routing.
- `internal/routing` — aggregator policies and shard pickers used by `ClusterClient` for multi-shard commands.
- `internal/otel` — small OpenTelemetry shim used to keep root free of telemetry deps; full instrumentation lives in `extra/redisotel-native`.

## Architectural specs

Repo-local skills under `.claude/skills/` auto-trigger from their descriptions (no need to list them here).

Architectural reference docs (read on demand — link from a task) live under `.claude/specs/`. They cover invariants and design decisions that aren't obvious from reading the code alone. Read the relevant one **before** changing code in that subsystem:

- `pool.md` — connection pool: `wantConn` queue and FIFO discipline, `ConnState` machine, dial retry/backoff, hook integration, the re-auth/handoff coexistence contract.
- `cluster-routing.md` — slot computation, MOVED/ASK redirection, request/response policies, aggregators, replica routing, topology reload, cross-slot rules.
- `maintnotifications.md` — RESP3 push notification protocol, mode handshake, per-conn state, handoff worker pool, circuit breaker, endpoint-type resolution, cluster vs. standalone differences.

## Conventions worth knowing

- New Cmder type → also implement `SetVal` (the custom vet check enforces this; `SetErr` is on the embedded `baseCmd`).
- Wrap errors with custom error types that implement `Unwrap`, or use `fmt.Errorf("...: %w", err)`. Always call `cmd.SetErr(...)` after wrapping so typed-error checks still pass.
- `gofumpt` + `goimports -local github.com/redis/go-redis` is the formatter. CI runs both.
- Don't log directly — use `internal.Logger` (set via `redis.SetLogger`); `logging.Disable()` is called in tests.
- Version-gate Redis-version-specific tests with `SkipBeforeRedisVersion` / `SkipAfterRedisVersion` rather than skipping at the suite level.
