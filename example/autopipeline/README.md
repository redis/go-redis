# Automatic pipelining

> **EXPERIMENTAL:** the autopipelining API is subject to change in a future release.

A tour of go-redis **autopipelining** plus a runnable throughput comparison.

Autopipelining batches commands from many goroutines into Redis pipelines
automatically. It comes in two faces:

| Face | Call shape | Use when |
|---|---|---|
| `AutoPipeline()` (blocking) | each call blocks until executed — drop-in for a plain client | you want a speedup without changing code; ordering per goroutine |
| `AsyncAutoPipeline()` (deferred) | calls return immediately; result accessors block | you can submit a window of commands and read results later — highest throughput |

## Run

```bash
docker run --rm -p 6379:6379 redis
go run .
```

- `REDIS_ADDR` — point at a different server (default `localhost:6379`).
- `REDIS_CLUSTER_ADDRS` — comma-separated cluster seed addresses; enables the
  cluster part of the tour (slot sharding is automatic, per-key order holds).

## What it shows

**Act 1 — usage tour**

1. Blocking face as a drop-in: goroutines `Set`+`Get` with plain-client call
   shape; the engine batches them under the hood on a handful of connections.
2. Async face: submit a window of `Get`s, read the results afterwards — the
   throughput pattern.
3. `Submit` + `AutoFuture` for raw `Cmder`s (async face only — `Submit` is
   rejected on the blocking face by design). `Wait`/`WaitContext` to collect.
4. `Do` — the escape hatch. Runs on a **normal** connection outside the
   pipeline (plain `Client.Do` semantics); use it for raw commands the typed
   surface doesn't cover, never expect it to batch. (Typed blocking commands
   — `BLPop`, `XRead` with `Block`, ... — are diverted to a normal connection
   automatically.)
5. A bounded queue: `MaxQueuedCommands` caps the commands accepted but not
   yet completed. A command over the cap is not sent; it fails at once with
   `ErrAutoPipelineQueueFull`, and the caller backs off and retries. The tour
   fires 10,000 `SET`s at a cap of 16 and retries every rejection.
6. Tuning notes: `Unordered` + `MaxConcurrentBatches: 2-4` for peak async
   throughput; leave `NumShards` at 0; the instance is cached per client
   (first call's config wins); optional dedicated pipeline pool via
   `PipelineReadBufferSize`/`PipelineWriteBufferSize`/`PipelinePoolSize`.

**Act 2 — throughput comparison** (sample, 500 goroutines, 3s, loopback —
indicative, not a spec):

```
  approach                                      ops/sec  ordering  vs normal
  1. normal blocking                              57438  ordered   1.0x
  2. autopipeline ordered, blocking read         632742  ordered   11.0x
  3. autopipeline ordered, read later           2436067  ordered   42.4x
  4. autopipeline unordered, read later         2629600  UNORDERED 45.8x
```

## Caveats worth knowing

- With the default `MaxQueuedCommands: 0` nothing bounds the accepted
  commands: a server slower than the producers grows client memory. Set a cap
  for producers that can outrun the server. A rejection is per command, so on
  the async face a `SET` can be rejected while a later `GET` of the same key
  is accepted; check the error of a command a later one depends on.
- A command's context is not honored once queued; use a plain client for
  per-command deadlines (or `AutoFuture.WaitContext` to bound a wait).
- A batch that fails on a network error is retried whole (up to `MaxRetries`),
  so non-idempotent commands may execute twice on a dropped connection.
- On `ClusterClient`, ordering across nodes is per key.
- Batched commands fire the client's *pipeline* hooks (one span per batch, not
  per command), so per-command instrumentation looks different from a plain
  client.

## Full duplex

`FullDuplex: true` streams every command over one held connection, with no
request-response wait between batches. Two examples cover it:

- [`autopipeline-fullduplex`](../autopipeline-fullduplex): the blocking face,
  and pipelines on the full-duplex connection.
- [`autopipeline-fullduplex-async`](../autopipeline-fullduplex-async): the
  async face, `Submit` and submission windows, on one connection and on
  several (`NumShards > 1`).
