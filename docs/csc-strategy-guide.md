# Client-Side Caching strategy guide

go-redis implements client-side caching (CSC) with a shared, sharded cache kept
fresh by standard `CLIENT TRACKING` plus a background invalidation drainer. The
architecture is selected by `Options.ClientSideCacheStrategy`, whose only
currently-implemented value is the default, `CSCStrategySharedTracking`:

```go
client := redis.NewClient(&redis.Options{
    Addr:                  "localhost:6379",
    Protocol:              3, // CSC requires RESP3
    ClientSideCacheConfig: &redis.ClientSideCacheConfig{MaxEntries: 100_000},

    // Optional — the zero value is CSCStrategySharedTracking (the default and,
    // today, the only implemented strategy).
    ClientSideCacheStrategy: redis.CSCStrategySharedTracking,
})
```

The `CSCStrategy` type is an extension point: the strategy field and the
`attachCSC` dispatch exist so an alternative invalidation architecture (e.g. a
`CLIENT TRACKING ON BCAST` sidecar) can be added later without a breaking API
change. Until such a strategy lands, any non-default value falls back to
`CSCStrategySharedTracking` with a log warning.

## Enabling and disabling CSC

CSC is turned on by **providing a cache config**: set `ClientSideCacheConfig`
(or pass an explicit `ClientSideCache`) on a `Protocol: 3` client. Leave both
nil to disable it. `ClientSideCacheStrategy` only selects the architecture
once CSC is enabled; on its own it does nothing.

## `CSCStrategySharedTracking` — standard CLIENT TRACKING, shared cache

One shared cache; every pool connection issues plain `CLIENT TRACKING ON` (no
BCAST), and a background drainer applies buffered invalidations to the shared
cache.

Why this is the model go-redis ships:
- Plain `CLIENT TRACKING` works wherever RESP3 does — including managed or proxied
  environments where BCAST is restricted — and needs no extra connection.
- It matches the CSC model used by the other Redis clients (shared cache +
  per-connection tracking), so behaviour is consistent across languages.

| | `CSCStrategySharedTracking` |
|---|---|
| Cache | one shared, sharded |
| Tracking | every pool conn, `CLIENT TRACKING ON` |
| Invalidate delivery | background drainer scans idle conns one-at-a-time every 5 ms |
| Cache-hit cost | in-memory lookup |
| Staleness bound | ~5 ms drain period (`MaxStaleness`, if set, is the hard cap) |
| Conn-churn behaviour | server drops tracking state on conn close; the owning-conn eviction hook evicts that conn's entries |
| Extra cost | background goroutine; scans idle conns every 5 ms |

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

**Connection-lifecycle staleness.** When a pool connection closes, the server
drops its tracking state, so invalidations for keys that connection read would go
nowhere. go-redis attributes each cached entry to the connection that fetched it
and evicts that connection's entries when it closes for **any** reason — pool
removal, `ConnMaxLifetime`/idle-timeout retirement, or a maintnotifications socket
swap. `MaxStaleness` (off by default) is an additional time-based backstop when
configured.

## Operational notes

- **RESP2**: CSC silently no-ops (no cache, no errors).
- **DB ≠ 0**: CSC is disabled with a log warning. `CLIENT TRACKING` is bound to a
  connection's DB and a runtime `SELECT` does not re-key the server's tracking
  table, so use one client per DB if you need caching against a non-zero database.
- **`Options.ClientSideCache` (explicit cache instance)**: honoured — takes
  precedence over `ClientSideCacheConfig`. A shared `Cache` is only safe across
  clients on the same server and DB. It **must implement `ConnOwnedCache`**
  (`FulfillOwned`/`EvictByConn`); SharedTracking needs per-connection eviction to
  drop a connection's entries when it closes. A cache without it disables CSC with
  a warning. `NewLocalCache` implements it.
- **Derived clients**: `Client.WithTimeout` shares the parent's cache. `Client.Conn`
  returns a single-connection client backed by a sticky pool, which has no
  background drainer, so CSC is **not** active on it (its reads go straight to the
  server) — use the parent client for cached reads.
- **RESP3 read buffer**: `ReadBufferSize` is clamped up to a small minimum on
  RESP3 clients so push-notification headers always fit the peek window.
