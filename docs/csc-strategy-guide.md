# Client-Side Caching strategy guide

go-redis ships three client-side caching (CSC) architectures behind one
option. Pick a strategy at client construction:

```go
client := redis.NewClient(&redis.Options{
    Addr:                  "localhost:6379",
    Protocol:              3, // CSC requires RESP3
    ClientSideCacheConfig: &redis.ClientSideCacheConfig{MaxEntries: 100_000},

    // Optional — the zero value is CSCStrategyBroadcast (recommended).
    ClientSideCacheStrategy: redis.CSCStrategyBroadcast,
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

| | `CSCStrategyBroadcast` (default) | `CSCStrategyPerConnection` | `CSCStrategySharedTracking` |
|---|---|---|---|
| Cache | one shared, sharded | one private per pool conn | one shared, sharded |
| Tracking | sidecar conn, `CLIENT TRACKING ON BCAST` | every pool conn, `CLIENT TRACKING ON` | every pool conn, `CLIENT TRACKING ON` |
| Invalidate delivery | sidecar read loop → shared cache | per-conn push → that conn's cache | background drainer walks idle conns every 5 ms |
| Cache-hit cost | in-memory lookup | in-memory lookup (+ rare conn peek) | in-memory lookup |
| Staleness bound | push latency (~RTT) | 5 ms drain-skip window per conn | 5 ms drain period |
| Conn-churn behaviour | unaffected (cache outlives conns) | per-conn cache dies with its conn (clean) | server drops tracking state on conn close; churned entries persist until overwrite+drain |
| Extra cost | one extra connection; receives invalidates for ALL DB writes | memory × conns; per-conn warm-up | background goroutine; pool walk every 5 ms |

## When to choose each

### `CSCStrategyBroadcast` — the default; use unless you have a reason not to

- Best throughput and tail latency at every tested concurrency (10–500
  goroutines).
- Cache hits never touch the connection pool.
- Robust to invalidation noise from other applications (±3 % throughput
  under 2 000 irrelevant SETs/s in the noise stress test).
- Lowest stale-read rate under connection churn.
- Self-heals: on sidecar disconnect the cache is flushed at teardown AND
  after re-subscribe, so an outage can't leave permanently-stale entries.

Costs to be aware of:
- One extra connection per client (outside the pool).
- The sidecar receives invalidates for **every write in the DB**, including
  keys this client never reads. The CPU cost is the sidecar parsing frames;
  measured impact on serving throughput: none at 2 000 noise-writes/s.
- BCAST mode is all-keys (no PREFIX configured today). For very
  write-heavy multi-tenant DBs the sidecar's invalidate stream scales with
  total DB write rate, not this client's working set.

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
  shared-cache equivalent because every conn must warm independently

- Memory matters: cache memory multiplies by pool size.

### `CSCStrategySharedTracking` — shared cache without BCAST

A shared cache with per-connection `CLIENT TRACKING ON` and a background
drainer that consumes buffered invalidation frames once per 5 ms. 
Choose when:
- Policy requires plain `CLIENT TRACKING ON` semantics (no BCAST) with a
  shared cache — e.g. proxies or managed environments where BCAST is
  restricted.



## Operational notes

- **RESP2**: all strategies silently no-op (no cache, no errors).
- **DB ≠ 0**: CSC is disabled with a log warning, all strategies.
- **`Options.ClientSideCache` (explicit cache instance)**: honoured by
  Broadcast and SharedTracking. PerConnection ignores it (per-conn caches
  are built from `ClientSideCacheConfig`) and logs a warning.
