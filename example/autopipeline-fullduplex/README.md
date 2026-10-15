# Full-duplex autopipelining (blocking face)

> **EXPERIMENTAL:** the autopipelining API is subject to change in a future release.

With `FullDuplex: true` the autopipeliner sends every goroutine's commands
over **one held connection**. The writer keeps sending while the reader
receives, and replies are matched to commands by their position on the wire,
so there is no request-response wait between batches.

This example uses the blocking face (`AutoPipeline`): each call blocks until
its reply lands, like a plain client. For the async face, and for several
connections (`NumShards > 1`), see
[`autopipeline-fullduplex-async`](../autopipeline-fullduplex-async).

## Run

```bash
docker run --rm -p 6379:6379 redis
go run .
```

`REDIS_ADDR` points it at a different server (default `localhost:6379`).

## What it shows

1. **Turning it on.** `AutoPipelineWithOptions(&redis.AutoPipelineOptions{FullDuplex: true})`.
   `Config().FullDuplex` reports whether it engaged. It needs a standalone
   `*Client` with a pipeline pool, which every client has unless
   `PipelinePoolSize < 0`.
2. **Drop-in use.** 200 goroutines run `SET`/`GET` with the plain-client call
   shape; each reads its own writes, and the client uses one connection for
   all of them, plus the main pool's.
3. **Pipelines on the same connection.** `ap.Pipeline()` and `ap.FDPipelined`
   submit the batch whole, so its commands are adjacent on the wire. `Exec`
   returns what an ordinary pipeline returns: the first error, `redis.Nil`
   included, and each command keeps its own result.
4. **What runs on a pooled connection instead.** A blocking command would
   stall every caller on the shared connection, so it is diverted. A batch
   that holds one is refused by `FDPipelined` (`ErrFDPipelineDiverts`), and
   `Pipeline()` runs it as an ordinary pipeline. `TxPipeline` always runs
   pooled, because MULTI/EXEC needs a connection to itself.

Sample output:

```
1. full duplex engaged: true
2. 200 goroutines x 50 SET+GET, read-your-writes held; connections named "fd-example": 2
3. pipeline on the FD connection: INCR=1 LRANGE=[a b c] GET missing=redis: nil (Exec err: redis: nil)
   FDPipelined: INCR, INCR -> 2, 3
4. FDPipelined refused a BLPOP batch; Pipeline() ran it pooled: [fd:list a]
   TxPipeline ran on a pooled connection (MULTI/EXEC needs one to itself)
done
```

## Caveats worth knowing

- **Hooks.** Process hooks run per command and observe it; they cannot stop
  it. The write is already queued when the hook runs, so a hook that returns
  without calling `next` does not prevent the server write. Run blocking or
  mutating hooks on a plain client.
- **Retries.** After a dropped connection, commands whose replies did not
  arrive are sent again on a new connection, each within `MaxRetries`. Plan
  for non-idempotent commands to run twice.
- **ctx.** A command's ctx is checked before it is queued. Once queued it
  runs; `ReadTimeout` bounds each reply.
- **A bounded queue.** `FullDuplexWindow` bounds the commands on the
  connection, and a full window blocks the submitter (its ctx bounds the
  wait) instead of rejecting. So `MaxQueuedCommands`, the half-duplex cap
  that rejects with `ErrAutoPipelineQueueFull`, limits only the commands run
  outside the pipeline here (blocking commands, `Do`).
- **Cluster.** On a `ClusterClient`, full duplex runs one engine per node;
  `FDPipelined` is for a standalone `*Client` only.
