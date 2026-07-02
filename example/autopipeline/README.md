# Automatic pipelining

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
   pipeline (plain `Client.Do` semantics); use it for anything stateful or
   blocking, never expect it to batch.
5. Tuning notes: `Unordered` + `MaxConcurrentBatches: 2-4` for peak async
   throughput; leave `NumShards` at 0; the instance is cached per client
   (first call's config wins); optional dedicated pipeline pool via
   `PipelineReadBufferSize`/`PipelineWriteBufferSize`/`PipelinePoolSize`.

**Act 2 — throughput comparison** (sample, 500 goroutines, 3s, loopback —
indicative, not a spec):

```
  approach                                      ops/sec  ordering  vs normal
  1. normal blocking                              56648  ordered   1.0x
  2. autopipeline ordered, blocking read         552386  ordered   9.8x
  3. autopipeline ordered, read later           2406600  ordered   42.5x
  4. autopipeline unordered, read later         2661933  UNORDERED 47.0x
```

## Caveats worth knowing

- A command's context is not honored once queued; use a plain client for
  per-command deadlines (or `AutoFuture.WaitContext` to bound a wait).
- A batch that fails on a network error is retried whole (up to `MaxRetries`),
  so non-idempotent commands may execute twice on a dropped connection.
- On `ClusterClient`, ordering across nodes is per key.
