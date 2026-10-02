# Full-duplex autopipelining (async face)

> **EXPERIMENTAL:** the autopipelining API is subject to change in a future release.

The deferred face (`AsyncAutoPipeline`) with `FullDuplex: true`. Calls return
at once; the result accessors (`Val`, `Result`, `Err`) block until the reply
lands. Submit a window of commands, then read the results: the connection
stays full, and that is where full duplex is fastest.

See [`autopipeline-fullduplex`](../autopipeline-fullduplex) for the blocking
face and the pipeline rules, and
[`autopipeline-fullduplex-engines`](../autopipeline-fullduplex-engines) for
several connections.

## Run

```bash
docker run --rm -p 6379:6379 redis
go run .
```

`REDIS_ADDR` points it at a different server (default `localhost:6379`).

## What it shows

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
4. **A timed run.** 64 goroutines submit windows of 256 `SET`s on the one
   connection.

Sample output (loopback; indicative only):

```
full duplex engaged: true, window 65536
1. submitted 1000 SETs, then read every result
2. unawaited SET then GET -> "first"; INCRs -> 1..5 in order
3. Submit INCRBY 10 -> 15; WaitContext GET -> "first"
4. 64 goroutines, windows of 256: 3147264 SET/s; connections named "fd-async-example": 2
done
```

## Tuning

- `FullDuplexWindow` (default 65536) bounds the commands in flight on the
  connection. It must exceed round-trip time x target rate, or it throttles
  throughput. It costs memory only while commands are really in flight.
- `Unordered` and `MaxConcurrentBatches > 1` are rejected with
  `FullDuplex`: replies are matched by position on one connection, which
  needs one ordered stream. For more than one connection, use `NumShards`
  (see the engines example).
- The autopipeliner is cached per client and face; the first call's options
  win. Close it, or the client, to release its goroutines.
