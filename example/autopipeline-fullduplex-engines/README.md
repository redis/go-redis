# Full-duplex autopipelining with several engines

> **EXPERIMENTAL:** the autopipelining API is subject to change in a future release.

One connection has a throughput ceiling. With `FullDuplex: true` and
`NumShards: N`, one client runs **N full-duplex engines**, each holding its own
connection. The pool, the hook chain and the invalidation stream stay shared;
only the engines multiply. The price is ordering: it holds per key, not for
the whole submit stream.

See [`autopipeline-fullduplex`](../autopipeline-fullduplex) and
[`autopipeline-fullduplex-async`](../autopipeline-fullduplex-async) for one
engine.

## Run

```bash
docker run --rm -p 6379:6379 redis
go run .
```

`REDIS_ADDR` points it at a different server (default `localhost:6379`).

## What it shows

1. **Sizing.** Each engine holds one pipeline-pool connection. Construction
   fails when `PipelinePoolSize` (default 10) is smaller than `NumShards`. The
   check covers one autopipeliner: the blocking and async faces share the
   pool, so size it for every full-duplex autopipeliner the client runs.
2. **Routing by first key.** A command goes to the engine its first key
   hashes to, so commands for the same key keep their order, and unawaited
   `SET`/`GET` pairs read their writes. `Unordered` is not needed.
3. **What is not ordered.** Commands with different first keys; a multi-key
   command (`COPY`, `RENAME`, `MSET`, multi-key `DEL`, `EVAL` with several
   keys) against its other keys; keyless commands (`FLUSHDB`,
   `SCRIPT FLUSH`, ...), which round-robin. Await the first command when
   another depends on it, or keep `NumShards` at 1.
4. **Pipelines.** A batch is contiguous on one connection, so it rides an
   engine only when all its keyed commands hash to that engine. A one-key
   batch always does. Engines hash the whole key, so a `{hash tag}` does not
   group keys here. For a batch that spans engines, `FDPipelined` returns
   `ErrFDPipelineSpansEngines`, and `Pipeline()` runs it as an ordinary
   pipeline on a pooled connection, not ordered against unawaited commands
   on the engines.
5. **When more engines pay off.** A short run with few and many commands in
   flight.

Sample output (loopback; indicative only):

```
1. NumShards=4, PipelinePoolSize=2 -> redis: AutoPipelineOptions.NumShards=4 needs a pipeline pool of at least 4 connections (each full-duplex engine holds one), but PipelinePoolSize gives 2; raise Options.PipelinePoolSize or lower NumShards
   NumShards=4, PipelinePoolSize=4 -> 4 engines, full duplex true
2. 100 unawaited SET+GET pairs, each pair on its key's engine: all read their write
   connections: 4 (one per engine)
3. COPY awaited before GET dst -> "copied"
4. one-key batch rode its engine: map[name:ada visits:1]
   two-key batch on two engines: FDPipelined -> redis: FDPipelined batch has keys on different full-duplex engines
   the same batch through Pipeline() ran pooled
5. SET/s, 2s per run:
   load                           1 engine    4 engines
   4 goroutines x window 1          129781       103558  (-20%)
   512 goroutines x window 32      3068320      3523248  (+15%)
done
```

## Choosing NumShards

More engines help only when one connection is saturated. With few commands
in flight, each batch fragments across the engines and throughput drops.
With many, the engines share the load. In the measurements behind the
`NumShards` doc (10-command pipelines on a 14-core client), 8 engines were 30%
slower than 1 at 8 callers, broke even at about 128, and were 2.2x faster at
1024. There is no automatic sizing yet: `NumShards` is the count you set.

More engines also widen the latency tail: a key waits behind its own
engine's queue while another engine may be idle.
