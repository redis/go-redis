# Full-duplex autopipelining (async face, one or several engines)

> **EXPERIMENTAL:** the autopipelining API is subject to change in a future release.

The deferred face (`AsyncAutoPipeline`) with `FullDuplex: true`. Calls return
at once; the result accessors (`Val`, `Result`, `Err`) block until the reply
lands. Submit a window of commands, then read the results: the connection
stays full, and that is where full duplex is fastest.

One connection has a throughput ceiling. With `NumShards: N`, one client runs
**N full-duplex engines**, each holding its own connection. The pool, the hook
chain and the invalidation stream stay shared; only the engines multiply. The
price is ordering: it holds per key, not for the whole submit stream.

See [`autopipeline-fullduplex`](../autopipeline-fullduplex) for the blocking
face and the pipeline rules.

## Run

```bash
docker run --rm -p 6379:6379 redis
go run .
```

`REDIS_ADDR` points it at a different server (default `localhost:6379`).

## What it shows

**A. One engine**

1. **A submission window.** 1000 `SET`s are on the wire before the first
   result is read.
2. **Order.** One goroutine's commands run in the order it submitted them,
   even unawaited: a `SET` then a `GET` of the same key reads the write, and
   `INCR`s count up in order. The exception is a command the engine diverts
   off the shared connection (a blocking command, a per-command read
   timeout): it can settle after a later one, so await it before a command
   that depends on it.
3. **`Submit` and `AutoFuture`.** `Submit` takes any `Cmder`. `Wait` blocks
   until the reply lands; `WaitContext` also returns when its ctx is done.
   Giving up does not cancel the command: it is already on the connection.

**B. Several engines (`NumShards: 4`)**

4. **Sizing.** Each engine holds one pipeline-pool connection. Construction
   fails when `PipelinePoolSize` (default 10) is smaller than `NumShards`. The
   check covers one autopipeliner: the blocking and async faces share the
   pool, so size it for every full-duplex autopipeliner the client runs.
5. **Routing by first key.** A command goes to the engine its first key
   hashes to, so commands for the same key keep their order, and unawaited
   `SET`/`GET` pairs read their writes. `Unordered` is not needed.
6. **What is not ordered.** Commands with different first keys; a multi-key
   command (`COPY`, `RENAME`, `MSET`, multi-key `DEL`, `EVAL` with several
   keys) against its other keys; keyless commands (`FLUSHDB`,
   `SCRIPT FLUSH`, ...), which round-robin. Await the first command when
   another depends on it, or keep `NumShards` at 1.
7. **Pipelines.** A batch is contiguous on one connection, so it rides an
   engine only when all its keyed commands hash to that engine. A one-key
   batch always does. Engines hash the whole key, so a `{hash tag}` does not
   group keys here. For a batch that spans engines, `FDPipelined` returns
   `ErrFDPipelineSpansEngines`, and `Pipeline()` runs it as an ordinary
   pipeline on a pooled connection, not ordered against unawaited commands
   on the engines.

**C. Throughput**

8. One engine against four, with few and with many commands in flight.

Sample output (loopback; indicative only):

```
A. one engine
   full duplex engaged: true, window 65536
1. submitted 1000 SETs, then read every result
2. unawaited SET then GET -> "first"; INCRs -> 1..5 in order
3. Submit INCRBY 10 -> 15; WaitContext GET -> "first"
B. several engines
4. NumShards=4, PipelinePoolSize=2 -> redis: AutoPipelineOptions.NumShards=4 needs a pipeline pool of at least 4 connections (each full-duplex engine holds one), but PipelinePoolSize gives 2; raise Options.PipelinePoolSize or lower NumShards
   NumShards=4, PipelinePoolSize=4 -> 4 engines, full duplex true
5. 100 unawaited SET+GET pairs, each pair on its key's engine: all read their write
   connections: 4 (one per engine)
6. COPY awaited before GET dst -> "copied"
7. one-key batch rode its engine: map[name:ada visits:1]
   two-key batch on two engines: FDPipelined -> redis: FDPipelined batch has keys on different full-duplex engines
   the same batch through Pipeline() ran pooled
C. throughput
8. SET/s, 2s per run:
   load                           1 engine    4 engines
   4 goroutines x window 1          130155       104590  (-20%; connections 1 vs 4)
   512 goroutines x window 32      3029168      3491072  (+15%; connections 1 vs 4)
done
```

## Tuning

- `FullDuplexWindow` (default 65536) bounds the commands in flight on each
  connection. It must exceed round-trip time x target rate, or it throttles
  throughput. It costs memory only while commands are really in flight. With
  `NumShards > 1` every engine has its own window.
- `Unordered` and `MaxConcurrentBatches > 1` are rejected with `FullDuplex`:
  replies are matched by position on a connection, which needs one ordered
  stream per connection. Use `NumShards` for more connections.
- **Choosing `NumShards`.** More engines help only when one connection is
  saturated. With few commands in flight, each batch fragments across the
  engines and throughput drops. In the measurements behind the `NumShards`
  doc (10-command pipelines on a 14-core client), 8 engines were 30% slower
  than 1 at 8 callers, broke even at about 128, and were 2.2x faster at 1024.
  There is no automatic sizing yet. More engines also widen the latency
  tail: a key waits behind its own engine's queue while another engine may
  be idle.
- A full window blocks the submitter, and its ctx bounds the wait; nothing
  is rejected. `MaxQueuedCommands`, the half-duplex cap that rejects with
  `ErrAutoPipelineQueueFull`, limits only the commands run outside the
  pipeline under full duplex.
- The autopipeliner is cached per client and face; the first call's options
  win. Close it, or the client, to release its goroutines.
