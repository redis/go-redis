# Client-Side Caching strategy guide

go-redis ships three client-side caching (CSC) architectures behind one
option. Pick a strategy at client construction:

```go
client := redis.NewClient(&redis.Options{
    Addr:                  "localhost:6379",
    Protocol:              3, // CSC requires RESP3
    ClientSideCacheConfig: &redis.ClientSideCacheConfig{MaxEntries: 100_000},

    // Optional — the zero value is CSCStrategySharedTracking (the default).
    ClientSideCacheStrategy: redis.CSCStrategySharedTracking,
})
```

All strategies share the same `Cache` interface, the same cacheable-command
allow-list, the same DB-0 restriction, and the same RESP3 requirement.
They differ in **who receives invalidation push frames and how those frames
reach the cache**.

## Enabling and disabling CSC

CSC is turned on by **providing a cache config**: set `ClientSideCacheConfig`
(or pass an explicit `ClientSideCache`) on a `Protocol: 3` client. Leave both
nil to disable it. `ClientSideCacheStrategy` only selects the architecture
once CSC is enabled; on its own it does nothing.

## TL;DR

| | `CSCStrategyBroadcast` | `CSCStrategyPerConnection` | `CSCStrategySharedTracking` (default) |
|---|---|---|---|
| Cache | one shared, sharded | one private per pool conn | one shared, sharded |
| Tracking | sidecar conn, `CLIENT TRACKING ON BCAST` | every pool conn, `CLIENT TRACKING ON` | every pool conn, `CLIENT TRACKING ON` |
| Invalidate delivery | sidecar read loop → shared cache | per-conn push → that conn's cache | background drainer scans idle conns one-at-a-time every 5 ms |
| Cache-hit cost | in-memory lookup | in-memory lookup (+ rare conn peek) | in-memory lookup |
| Staleness bound | push latency (~RTT) | 5 ms drain-skip window per conn | 5 ms drain period |
| Conn-churn behaviour | unaffected (cache outlives conns) | per-conn cache dies with its conn (clean) | server drops tracking state on conn close; churned entries persist until overwrite+drain |
| Extra cost | one extra connection; receives invalidates for ALL DB writes | memory × conns; per-conn warm-up | background goroutine; scans idle conns every 5 ms |

## When to choose each

### `CSCStrategySharedTracking` — the default; standard CLIENT TRACKING, shared cache

The default (the zero value of `ClientSideCacheStrategy`). One shared cache; every
pool connection issues plain `CLIENT TRACKING ON` (no BCAST), and a background
drainer applies buffered invalidations to the shared cache.

Why it's the default:
- Plain `CLIENT TRACKING` works wherever RESP3 does — including managed or proxied
  environments where BCAST is restricted — and needs no extra connection.
- It matches the CSC model used by the other Redis clients (shared cache +
  per-connection tracking), so behaviour is consistent across languages.

Trade-off: heavier tail latency than Broadcast under high concurrency, because
invalidation frames arrive spread across the pool connections (and are applied by
the background drainer) instead of on one dedicated connection.

**How the background drainer works.** With per-connection tracking, an
invalidation arrives as a push message on the connection that read the key and
sits unread while that connection is idle in the pool. A small background
goroutine takes care of it:

- Every `DrainInterval` (default 5 ms) it scans the idle pooled connections and
  applies any buffered invalidations to the shared cache.
- It borrows one connection at a time, only for the microseconds needed to read
  what is already buffered, so it does not compete with your commands.
- A connection that is busy running a command drains its own invalidations, so
  the drainer only has to cover idle connections.
- An invalidation is therefore applied within about one drain pass (≈5 ms; longer
  under heavy load or very large pools). `MaxStaleness`, if set, is the hard upper
  bound on how long a stale entry can be served.

### `CSCStrategyBroadcast` — highest throughput; opt in where BCAST is available

A dedicated out-of-pool "sidecar" connection subscribes with `CLIENT TRACKING ON
BCAST` and owns all invalidation traffic; pool connections never track, so cache
hits are pure in-memory lookups.

Choose when BCAST is available and you want maximum performance:
- Best throughput and tail latency at every tested concurrency (10–500
  goroutines); cache hits never touch the connection pool.
- Robust to invalidation noise from other applications (±3 % throughput under
  2 000 irrelevant SETs/s in the noise stress test); lowest stale-read rate under
  connection churn.
- Self-heals: on sidecar disconnect the cache is flushed at teardown AND after
  re-subscribe, so an outage can't leave permanently-stale entries.

Costs to be aware of:
- One extra connection per client (outside the pool).
- The sidecar receives invalidates for **every write in the DB**, including
  keys this client never reads. The CPU cost is the sidecar parsing frames;
  measured impact on serving throughput: none at 2 000 noise-writes/s.
- BCAST mode is all-keys (no PREFIX configured today). For very write-heavy
  multi-tenant DBs the sidecar's invalidate stream scales with total DB write
  rate, not this client's working set.

### `CSCStrategyPerConnection` — few, long-lived connections

Each pool connection owns a private cache and its own tracking
subscription, the way a single-connection client does.

Choose when:
- The client runs a small pool (≲10 conns) with low churn — e.g. a
  helper process, CLI tooling, or a low-QPS service.
- You want hard isolation: an invalidate lost on one conn can never
  poison another conn's cache, and a closed conn takes exactly its own
  entries with it.

Avoid when:
- Concurrency is high: hit rate caps at roughly (1 / active conns) of the
  shared-cache equivalent because every conn must warm independently.
- Memory matters: cache memory multiplies by pool size.



## Operational notes

- **RESP2**: all strategies silently no-op (no cache, no errors).
- **DB ≠ 0**: CSC is disabled with a log warning, all strategies.
- **`Options.ClientSideCache` (explicit cache instance)**: honoured by
  Broadcast and SharedTracking. PerConnection ignores it (per-conn caches
  are built from `ClientSideCacheConfig`) and logs a warning.
