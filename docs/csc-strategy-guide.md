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
| Invalidate delivery | background drainer scans idle conns one-at-a-time every 5 ms by default; opaque transports use a throttled fallback probe |
| Cache-hit cost | in-memory lookup |
| Staleness bound | usually one drain period; opaque/non-Unix fallback probes run no more often than 100 ms (`MaxStaleness`, if set, is the hard cap) |
| Conn-churn behaviour | server drops tracking state on conn close; the owning-conn eviction hook evicts that conn's entries |
| Extra cost | background goroutine; scans idle conns every 5 ms |

**How the background drainer works.** With per-connection tracking, an
invalidation arrives as a push message on the connection that read the key and
sits unread while that connection is idle in the pool. A small background
goroutine takes care of it:

- Every `DrainInterval` (default 5 ms) it scans the idle pooled connections and
  applies any buffered invalidations to the shared cache.
- It borrows one connection at a time, only for the microseconds needed to read
  what is already buffered, and takes a normal pool turn while doing so. The
  drainer yields if no turn is immediately available; a command that arrives
  during a borrow waits at most `PoolTimeout`.
- A connection that is busy running a command drains its own invalidations, so
  the drainer only has to cover idle connections.
- On transports with a non-consuming readiness check, an invalidation is usually
  applied within one drain pass (about 5 ms by default; longer under heavy load
  or with very large pools). Opaque wrappers and platforms without that check use
  bounded periodic reads throttled to at least `max(DrainInterval, 100 ms)`, with
  the probe occurring on the next drain pass. `MaxStaleness`, if set, is the hard
  upper bound on how long a stale entry can be served.

**Connection-lifecycle staleness.** When a pool connection closes, the server
drops its tracking state, so invalidations for keys that connection read would go
nowhere. go-redis attributes each cached entry to the connection that fetched it
and evicts that connection's entries when it closes for **any** reason — pool
removal, `ConnMaxLifetime`/idle-timeout retirement, or a maintnotifications socket
swap. `MaxStaleness` (off by default) is an additional time-based backstop when
configured.

## Operational notes

- **RESP2**: CSC silently no-ops (no cache, no errors). If a client configured
  for RESP3 discovers during connection setup that the server rejects `HELLO 3`
  and falls back to RESP2, CSC is disabled for that client before it can serve
  cache hits.
- **DB ≠ 0**: CSC is disabled with a log warning. `CLIENT TRACKING` is bound to a
  connection's DB and a runtime `SELECT` does not re-key the server's tracking
  table, so use one client per DB if you need caching against a non-zero database.
- **Credential providers**: CSC is disabled when `CredentialsProvider`,
  `CredentialsProviderContext`, or `StreamingCredentialsProvider` is set.
  Provider-backed credentials can change the ACL identity while cached replies
  use a namespace fixed at client creation. Fixed credentials are supported;
  the ACL `Username` is hashed into the cache namespace, so password rotation
  preserves the same identity while different users cannot reuse each other's
  entries.
- **`Options.ClientSideCache` (explicit cache instance)**: honoured — takes
  precedence over `ClientSideCacheConfig`. A shared `Cache` is only safe across
  clients on the same server and DB; fixed authentication identities are
  automatically isolated. It **must implement `ConnOwnedCache`**
  (`FulfillOwned`/`EvictByConn`); SharedTracking needs per-connection eviction
  to drop a connection's entries when it closes. A cache without it disables
  CSC with a warning. `NewLocalCache` implements it.
- **Derived clients**: `Client.WithTimeout` shares the parent's cache. `Client.Conn`
  returns a single-connection client backed by a sticky pool, which has no
  background drainer, so CSC is **not** active on it (its reads go straight to the
  server). Claiming the sticky connection evicts any parent-cache entries owned
  by that connection before it leaves the parent's drainer coverage. Use the
  parent client for cached reads.
- **Connection-state commands**: while CSC is active, user-issued `SELECT`,
  `AUTH`, `RESET`, `CLIENT TRACKING`, and protocol-changing `HELLO` commands are
  rejected because they would invalidate the cache namespace or tracking
  assumptions for only one pooled connection. Raw `SUBSCRIBE`, `PSUBSCRIBE`,
  and `SSUBSCRIBE` are also rejected on the ordinary pool; the typed subscription
  methods remain supported because they use dedicated `PubSub` connections.
- **RESP3 read buffer**: `ReadBufferSize` is clamped up to a small minimum on
  RESP3 clients so push-notification headers always fit the peek window.
