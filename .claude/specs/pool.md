# Connection pool

This is an architectural reference for `internal/pool/`. Read this before changing connection lifecycle, the wantConn queue, the conn state machine, or hooks. The pool is the single most common source of bugs in this codebase (`fix(pool):` is the top commit-prefix in the history) — almost always because someone violated an invariant below without realizing it.

This doc captures **invariants and the "why"**, not a tour of the code. File names anchor each section but the line numbers will drift.

---

## What the pool actually does

`ConnPool` (`internal/pool/pool.go`) hands out `*Conn` to callers, recycles them when returned, dials new ones when needed, and replaces them when they die. It's used by every client type (`Client`, `ClusterClient`, `Ring`, sentinel) — they all share the same pool implementation. There are two thin wrappers:

- `SingleConnPool` (`pool_single.go`) — wraps exactly one `*Conn`, no concurrency. Used during `initConn` (`HELLO`, `AUTH`, `CLIENT SETNAME`) and re-auth, where we must run commands on a specific connection that is *not* yet in the regular pool.
- `StickyConnPool` (`pool_sticky.go`) — wraps a Pooler and pins one connection across many `Get`/`Put` cycles. Used by PubSub, where the subscription state lives on a specific TCP connection.

Concurrency from clients is bounded by `PoolSize` via a semaphore (`waitTurn`/`freeTurn`). `MaxActiveConns` is a hard ceiling that includes excess (non-pooled) connections; if 0, only `PoolSize` matters.

---

## The four primary invariants

### 1. Every `waitTurn` that succeeds must call `freeTurn`

The semaphore is the only thing keeping concurrency bounded. A leaked turn is permanently lost capacity. Every error path in `Get`, every retry loop, every panic-recovery must release. Audit new exit paths in `Get`/`Put` carefully — most pool bugs in history trace back to a missed `freeTurn`.

### 2. FIFO order on `wantConnQueue`

When the pool is exhausted, callers queue as `wantConn` requests in `wantConnQueue` (`want_conn.go`). They're served in arrival order. Two real bugs hinged on this:

- **#3777 — FIFO violation in `notifyWaiters`**: the wakeup loop re-read the atomic state on each iteration, so a goroutine that woke up fast could "skip the line" by re-entering `notifyWaiters` ahead of an earlier waiter. Fix: snapshot state locally inside `notifyWaiters` and only consider transitions made within this call. See the comment near `conn_state.go:300` ("atomic would let a fast goroutine's `Transition(StateIdle)` leak into our…").
- **#3680 — zombie `wantConn` elements**: cancelled or timed-out waiters stayed in the queue head until the next `discardDoneAtFront`, which delayed real waiters and caused spurious retries. Fix: scan and prune done elements proactively.

If you change `wantConnQueue`, write a test that asserts a slow first waiter still gets the connection before a faster second waiter.

### 3. The `ConnState` machine is the single source of truth for liveness

`conn_state.go` defines six states:

```
StateCreated → StateInitializing → StateIdle ⇄ StateInUse
                                       ↓
                                  StateUnusable   (handoff or re-auth)
                                       ↓
                                   StateIdle / StateClosed
```

- `StateCreated` — just allocated, no I/O yet.
- `StateInitializing` — running `initConn` (`HELLO`, `AUTH`, etc.). Reachable only via `SingleConnPool`.
- `StateIdle` — sitting in `idleConns`, eligible to be popped by `Get`.
- `StateInUse` — handed out to a caller.
- `StateUnusable` — owned by a background worker (handoff, re-auth). The pool will *not* return this conn from `Get` and will *not* close it from `Put`.
- `StateClosed` — terminal.

There used to also be an atomic `Conn.closed` bool. It was removed in #3783 because it duplicated `StateClosed` and the duplication caused missed updates. **Do not re-introduce shadow flags.** If you need a new connection-level fact, add a state or use the existing fields under the state machine's lock.

`StateUnusable` is the contract that lets handoff and re-auth coexist with normal traffic without races. `OnGet` rejects unusable conns, `OnPut` keeps them in the pool (instead of recycling/closing), and the background worker drives them back to `StateIdle` (or removes them) when its work finishes.

### 4. Dial timeout is per-attempt, not cumulative

`DialerRetries` (default 5), `DialerRetryTimeout` (default 100ms), and the optional `DialerRetryBackoff` function shape retry behaviour. `DialTimeout` is applied **per attempt**: each retry gets a fresh window (#3705 — `fix(timeout): make dial timeout per-retry`). The caller's context deadline is honored only when it's tighter than the per-attempt budget. This prevents a long backoff from eating the next attempt's I/O time.

If all attempts fail, `tryDial` keeps probing in the background once a second so the pool can recover without a foreground request having to retry the whole sequence.

---

## Get / Put semantics

`Get`:
1. `waitTurn` — acquire a semaphore token (blocks).
2. `popIdle` — pop an idle conn, retrying past `StateUnusable`/`StateClosed` ones.
3. If no idle conn, queue a `wantConn` and let `queuedNewConn` dial in a background goroutine.
4. Run `OnGet` hooks (auth, handoff guard).
5. Transition `StateIdle → StateInUse` and return.

`Put`:
1. If the conn has unread bytes, peek for a RESP3 push notification. If it's a push frame, leave the buffered data in place (the push processor will consume it on the next `Get`); otherwise, treat the conn as broken and remove it.
2. Run `OnPut` hooks. A hook may transition the conn to `StateUnusable` to schedule a background operation; if so, `Put` returns the conn to the pool **without** recycling so the worker can pick it up.
3. If the conn is past `ConnMaxIdleTime` / `ConnMaxLifetime` (with jitter), close it.
4. Otherwise transition `StateInUse → StateIdle` and prepend to `idleConns`.

The "unread data on Put → check for push frame" path is what makes RESP3 maintenance notifications work without dedicated reader goroutines. Don't simplify it.

### Errors on close are noise for stale conns

#3778 (`fix(pool): suppress pool Close() errors for stale connections`): a connection that exceeded `ConnMaxIdleTime` is half-broken by definition; reporting `EBADF`/`ECONNRESET` from its `Close` is alarm-fatigue. The pool only surfaces close errors for connections that were healthy when we decided to close them.

---

## `conn_check`: detecting dead sockets and push frames

`conn_check.go` (Unix) does a non-blocking `MSG_PEEK` on the socket to distinguish three situations on an idle connection:

- 0 bytes available, no error → healthy.
- EOF → peer closed; remove the conn.
- Bytes available → either a push notification (RESP3, leave it) or unsolicited data (broken protocol, remove it).

`conn_check_dummy.go` (non-Unix) is a no-op stub. Push notification handling on Windows is therefore best-effort; the `Put` peek-on-unread-data path remains correct cross-platform but is the only line of defense. If you change `conn_check`, check both files.

---

## Re-auth and handoff coexistence (#3547)

Both re-auth (driven by `internal/auth/streaming`) and handoff (driven by `maintnotifications`) need exclusive access to a connection while running on it. Both use `StateUnusable` to tell the pool "don't touch this." The bug fixed in #3547 was that re-auth could grab a connection that handoff had already targeted, causing the handoff worker to run against a connection whose credentials had changed.

The contract now: a conn that is `StateUnusable` *for handoff* must not be picked up by re-auth. Whichever subsystem marks the conn first wins; the other one observes the unusable state via `OnGet` and skips it. The auth pool hook lives in `internal/auth/streaming/pool_hook.go`; the handoff pool hook lives in `maintnotifications/pool_hook.go`. They share the pool's hook list — order of registration does not matter for correctness because the state-machine transitions are atomic, but be careful when adding a third subsystem that needs the same exclusion.

---

## Pool configuration

| Option | Default | Notes |
|---|---|---|
| `PoolSize` | `10 * GOMAXPROCS` | Soft cap on pooled conns. |
| `MinIdleConns` | 0 | Pre-warmed at init; replenished after `Remove`. Background goroutine fills these via `dialHook`, hence the read-lock in `dialHook` (#3225). |
| `MaxActiveConns` | 0 (unlimited) | Hard cap including excess (non-pooled) conns used during bursts. |
| `PoolTimeout` | `ReadTimeout + 1s` | How long `waitTurn` will block. |
| `ConnMaxIdleTime` | 30 min | Eviction in `Put`. |
| `ConnMaxLifetime` | 0 (none) | Absolute age limit; checked on `Put`. |
| `ConnMaxLifetimeJitter` | 0 | Random ±jitter on `ConnMaxLifetime` to avoid all conns expiring together (#3666). |
| `ReadBufferSize` / `WriteBufferSize` | 32 KiB (since v9.12) | Was 4 KiB before. Sentinel client uses 4 KiB (#3476) because its connections are short-lived. If you see odd performance regressions, flip back to 4 KiB and compare. |

`ConnMaxLifetimeJitter` matters in deployments where many clients started at the same time: without jitter, all their connections expire in the same second and reconnect simultaneously, hammering the server (thundering herd).

---

## Hook integration (`hooks.go`)

`PoolHookManager` carries `OnGet`, `OnPut`, `OnRemove`. It's stored as an `atomic.Value` on `ConnPool` so reads in the hot path don't need a mutex. Hooks are called *outside* the pool's internal lock — never call back into the pool from inside a hook in a way that requires the lock you're not holding.

`OnGet` is allowed to reject a conn (e.g. handoff hook seeing `ShouldHandoff()` true). `OnPut` is allowed to mutate state (mark unusable, schedule background work). `OnRemove` is for cleanup (per-conn registries, listener unsubscribes).

When you add a new subsystem that needs per-connection state, prefer registering a pool hook over reaching into `ConnPool` directly. The hook is the seam.

---

## History worth knowing

The recurring shape of pool bugs:

- **State duplication**: shadow flags (`closed`, old `usable`/`Inited`/`used` atomics) drift out of sync with the real state. Removed in #3559 / #3783.
- **Lost waiters / lost turns**: missing `freeTurn` on an error path, or `wantConn` left in the queue with no one to deliver to. #3680, #3777, #3626 (`putIdleConn` turn management).
- **Race between background work and `Close`**: pool tries to close a conn that handoff/re-auth still owns. Fixed by `StateUnusable` + hook contract.
- **Errors that aren't really errors**: stale-conn close errors (#3778), failed dial-tcp errors that should be treated as redirectable (#3786 — these are routing-layer, but the pool surfaces them).
- **Identity verification (HELLO/SETINFO) failures coupling into pool init**: #3295, #3294, #3788. The pattern: feature handshakes during `initConn` should fail soft when the server doesn't support them, and must not deadlock or leave the conn half-initialized.

When you write a pool fix, add a regression test that fails without your change. The pool's behaviour is too easy to verify by inspection only.
