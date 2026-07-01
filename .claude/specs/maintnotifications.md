# Maintenance notifications (smart client handoff)

This is an architectural reference for `maintnotifications/`. Read this before changing notification handling, the handoff worker pool, the per-conn state machine, or anything that touches `MaintNotificationsConfig`.

The mental model: Redis Enterprise pushes RESP3 notifications when nodes are about to move or fail over. The client either (a) **relaxes timeouts** so in-flight commands don't false-time-out during the disruption, or (b) **hands off** an existing TCP connection to a new endpoint without dropping pending operations. The user-visible promise is "no spurious errors during planned maintenance."

---

## Why this exists

During a Redis Enterprise upgrade or scale event, individual nodes get drained, slots get migrated, and primaries fail over to replicas. Without help, a client sees this as connection drops, slow commands, and timeout errors. With maintenance notifications, the server tells the client *what's about to happen* via RESP3 push frames, and the client compensates: extends timeouts during the disruption, opens a new connection to the new endpoint, and migrates pending commands across.

This only works on Redis Enterprise (and Redis Cloud) — open-source Redis doesn't emit these notifications. It also only works on RESP3 (`Protocol: 3`), since RESP2 has no concept of out-of-band push frames.

---

## Notifications and what they mean

Two families of frames:

### Per-connection (standalone clients)

| Frame | Meaning | Client action |
|---|---|---|
| `["MOVING", seqID, timeS, endpoint]` | This connection's node is being drained; reconnect to `endpoint` within `timeS`. | Hand off the connection. |
| `["MIGRATING", seqID, ...]` | A slot migration is starting; expect latency spikes. | Apply `RelaxedTimeout`. |
| `["MIGRATED", seqID, ...]` | Migration done. | Clear relaxed timeout. |
| `["FAILING_OVER", seqID, ...]` | This primary is about to fail over. | Apply `RelaxedTimeout`. |
| `["FAILED_OVER", seqID, ...]` | Failover done. | Clear relaxed timeout. |

`MOVING` is the only frame that triggers a real connection handoff. The others only adjust timeouts on the existing connection.

### Cluster-level

| Frame | Meaning | Client action |
|---|---|---|
| `["SMIGRATING", seqID, slot/range, ...]` | Cluster slot migration starting. | Relax timeouts on connections to the source node. |
| `["SMIGRATED", seqID, [[src, dst, slots], ...]]` | Cluster slot migration done. | Clear relaxed timeouts; trigger lazy `clusterState` reload (selectively, using the slot ranges). |

ClusterClient supports only `SMIGRATING`/`SMIGRATED`. Per-connection `MOVING` is meaningless in cluster mode because cluster clients discover endpoint changes via `MOVED`/`ASK` redirection — the cluster routing path already handles node moves. Adding `MOVING` to the cluster path would duplicate (and potentially conflict with) the routing layer's redirection logic.

`SMIGRATED` deduplication is by `seqID` (`Manager.MarkSMigratedSeqIDProcessed`) — Redis can re-send the same notification on multiple connections, and we only want to reload topology once.

---

## Modes

`MaintNotificationsConfig.Mode`:

- `ModeDisabled` — feature off; no handshake sent. Choose this when running against open-source Redis.
- `ModeEnabled` — `CLIENT MAINT_NOTIFICATIONS ON` is mandatory. Hard error if the server rejects it.
- `ModeAuto` (default) — try the handshake, silently downgrade if the server doesn't understand it.

The handshake lives inside `initConn`. The order is: `HELLO 3` → `CLIENT MAINT_NOTIFICATIONS ON`. If `HELLO 3` fails (e.g., RESP2-only server), `ModeAuto` skips the second step and the connection runs without notifications. `ModeEnabled` errors out — the user explicitly asked for the feature.

Three real bugs trace to this handshake:

- **#3788** — `ModeAuto` would still attempt the maintenance handshake after `HELLO` failed, panicking on the unexpected reply shape.
- **#3707** — `MaintNotificationsConfig` could be nil (when disabled), and `initConn` dereferenced it. The fix nil-checks before every access; preserve that pattern when adding fields.
- **#3789** — Endpoint-type auto-detection was flaky over slow DNS; a 2-second resolution timeout and clearer IP-range classification (RFC1918, RFC4193, loopback, RFC6598) fixed it.

---

## Per-connection state

`maintnotifications/state.go` defines a tiny manager-level enum: `StateIdle` (no upgrade in progress) and `StateMoving` (handoff initiated). The richer per-connection state lives in `internal/pool` via the conn state machine — `StateUnusable` is what the pool uses to keep handoff-targeted connections out of `Get` while the worker is operating on them.

The flow for a `MOVING` notification on conn X:

1. Push handler reads `["MOVING", seqID, timeS, endpoint]` and calls `conn.MarkForHandoff(endpoint, seqID)` on X.
2. The next `Put` to return X to the pool sees `ShouldHandoff()` true and routes X to `MarkQueuedForHandoff` (transitioning to `StateUnusable`) and enqueues a handoff request.
3. The pool no longer hands X out via `Get` (the `OnGet` hook rejects unusable conns).
4. A handoff worker picks up the request, dials the new endpoint, runs `initConn` against the new socket, and swaps the underlying conn.
5. On success, `ClearHandoffState()` returns the conn to `StateIdle`. On terminal failure, the worker removes it.

The whole sequence is async w.r.t. the user's request that triggered the `Put`. The user does not block on handoff.

---

## Handoff worker pool (`handoff_worker.go`)

Workers are created on demand and self-terminate after 15 seconds idle. Auto-sizing (when not explicitly configured):

```
MaxWorkers       = min(PoolSize/2, max(10, PoolSize/3))
HandoffQueueSize = max(20 * MaxWorkers, PoolSize)
                   capped by min(MaxActiveConns + 1, 5 * PoolSize)
```

Examples:
- `PoolSize=100` → 33 workers, 660 queue (capped at 500).
- `PoolSize=100, MaxActiveConns=150` → 33 workers, 151 queue.

These bounds are tuned to absorb a "whole pool moves at once" event — during a node drain you can get a `MOVING` notification on every connection at roughly the same time.

### Queue-full behavior

If the handoff queue is full, the worker manager waits 100ms for space; if still full, it returns `ErrHandoffQueueFull` and the connection is removed from the pool (no retry at this layer). It will be re-dialed by the pool when needed. **This is intentional**: queueing forever would mask a fundamental misconfiguration, and dropping the conn is recoverable.

### Retries

A failed handoff retries up to `MaxHandoffRetries` (default 3) with exponential backoff (`HandoffTimeout/3` initial). Some errors are non-retryable (e.g., `initConn` failure — the new endpoint is unreachable or rejecting auth). After max retries, the conn is permanently removed.

### Post-handoff relaxed duration

After a successful handoff, the *new* conn has its read/write deadlines extended for `PostHandoffRelaxedDuration` (default `2 * RelaxedTimeout`). Why: even after the new endpoint accepts the connection, the cluster is still settling — first commands may be slow as topology propagates. The relaxed window covers the post-stabilization tail. This is applied *before* `initConn` so the auth handshake also benefits.

### Past pitfall

#3633 — the worker's logging path could call `closeConnFromRequest` with a nil error and then `err.Error()` panic-ed. Always nil-check errors before logging in this code; the worker runs on a long-lived goroutine and a panic kills future handoffs.

---

## Pool hook integration (`pool_hook.go`)

The maintnotifications subsystem plugs into the pool via `pool.PoolHook`:

- `OnGet` — reject the conn if `IsUsable()` is false or `ShouldHandoff()` is true.
- `OnPut` — if the conn needs handoff, queue it (transitioning to `StateUnusable` so the pool keeps it but won't hand it out).
- `OnRemove` / `Shutdown` — clean up.

`enableMaintNotificationsUpgrades` (called from `redis.go` after `initConn` confirms RESP3) constructs the `Manager`, registers the pool hook, and registers push handlers on `push.Registry`. On client `Close`, the manager calls `pool.RemovePoolHook` and shuts down its workers.

The "ReAuth must not interfere with handoff" invariant (#3547) lives at this layer: both subsystems use `StateUnusable`, so whichever one transitions first wins and the other observes the unusable state via `OnGet`. See [pool.md](pool.md) for the full coexistence contract.

---

## Circuit breaker (`circuit_breaker.go`)

Per-endpoint circuit breaker keyed by destination. States: `Closed` (allow), `Open` (reject), `HalfOpen` (probe). Defaults: open after 5 consecutive failures, half-open after 60s, close after 3 consecutive successes from half-open.

Workflow: before a handoff dial, check `IsOpen(endpoint)`. If open, fail fast — no point burning the retry budget on an endpoint that's known-broken. After each handoff, record the result.

A background goroutine GCs unused circuit breakers (>30 min unused) every 5 minutes so a long-lived client doesn't accumulate state for one-off endpoint failures.

---

## Endpoint type resolution (`config.go`)

`EndpointType` selects which form the client uses when reconnecting after a `MOVING` notification (Redis Enterprise emits multiple endpoint forms in topology):

- `EndpointTypeAuto` — auto-detect based on the current connection (TLS preference, IP ranges).
- `EndpointTypeInternalIP` / `EndpointTypeInternalFQDN`.
- `EndpointTypeExternalIP` / `EndpointTypeExternalFQDN`.
- `EndpointTypeNone` — reconnect with the originally configured address.

Auto-detection logic in `DetectEndpointType`:

- TLS enabled → prefer FQDN (so SNI and cert validation work).
- TLS disabled → prefer IP (avoids unnecessary DNS).
- Internal vs external is decided by IP range (RFC1918 / RFC4193 / loopback / RFC6598) for IP literals, or by resolution-and-classification for hostnames (with a 2-second DNS timeout, post #3789).

Choose explicitly if your network topology requires a specific form — e.g., a client running outside the VPC must use `EndpointTypeExternalFQDN`.

---

## Manager (`manager.go`)

Central coordinator. Owns:

- The push notification handler registrations (one per notification type).
- A `sync.Map` of in-flight `MOVING` operations keyed by conn ID, so duplicate notifications don't double-handoff.
- The `SMIGRATED` seqID dedup set.
- The cluster-state-reload callback (set by `ClusterClient` so the manager can trigger a partial reload on `SMIGRATED` without holding a back-reference to the cluster client).

Lock-free state where possible: `atomic.Int64` for active operation count, `atomic.Bool` for closed flag. Callbacks fire on the push notification handler goroutine — keep them fast and avoid blocking I/O directly inside them.

---

## E2E testing

`maintnotifications/e2e/` has its own `go.mod` and a separate test entrypoint. It drives a fault-injecting RESP proxy (`cae-resp-proxy`) that sits between the client and Redis and can synthesize maintenance notifications on demand. Scenarios are written as Go tests that:

1. Start the proxy with a `DEFAULT_INTERCEPTORS` profile (`cluster`, `hitless`).
2. Create a real client.
3. Issue commands while the proxy injects `MOVING`/`SMIGRATING`/etc.
4. Assert no spurious errors and correct handoff timing.

Run with `make test.e2e` (auto-starts the proxy via the `e2e` docker profile). `make test.e2e.logic` runs only the logic-layer tests (no proxy). `make test.e2e.docker` runs the proxy-based subset that fits in CI's docker-network constraints.

If you're changing the subsystem in ways that could affect handoff timing, run E2E locally — unit tests don't catch races between notification arrival and request lifecycle.

---

## History worth knowing

Recurring failure modes:

- **Nil `MaintNotificationsConfig`** dereferenced in `initConn` when the feature is disabled (#3707). Always nil-check before access.
- **HELLO failure not handled** before the maintenance handshake (#3788). The handshake assumes RESP3 — if `HELLO 3` failed, skip it.
- **Endpoint-type detection over slow DNS** (#3789). Use the bounded resolver, not bare `net.LookupHost`.
- **Failover client wiring gaps** (#3600). When you add fields to `MaintNotificationsConfig`, check that `FailoverClient` and `FailoverClusterClient` propagate them.
- **Pool/handoff/re-auth interaction** (#3547). Always use `StateUnusable` for exclusive ownership, never a separate flag.
- **Logging panics on nil error** (#3633) in long-lived worker goroutines. Defensive nil-checks in log paths.
- **Test flakes from notification-injection timing** (#3641). Synchronize injection with client state, don't rely on sleeps.

If you add a new notification type, the checklist is roughly:

1. Define the frame shape and document it in `README.md` / this file.
2. Add a parser in `push_notification_handler.go`.
3. Decide: per-connection action (mark-for-handoff, relax timeout) or manager-level (topology reload, dedup)?
4. If per-connection: route through the pool hook so the existing state-machine guarantees apply. Don't invent a new exclusion mechanism.
5. Add an E2E scenario in `maintnotifications/e2e/`.
6. Decide whether `ClusterClient` should also handle it — if it duplicates routing-layer behaviour (`MOVED`/`ASK`), probably no.
