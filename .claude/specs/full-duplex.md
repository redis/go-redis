# Full-duplex autopipeline engine

Design notes for the ordered full-duplex (FD) autopipeline engine
(`autopipeline_fullduplex.go`, `autopipeline.go`). Read this before changing the
engine. It records invariants and decisions that are not obvious from the code.

FD is opt-in (`AutoPipelineConfig.FullDuplex`) and standalone-only: it is enabled
only for a single-node `*Client`. It holds ONE connection for MANY callers and
streams commands on it while a reader drains replies in FIFO order, so a caller does
not wait for a round trip before the next command is written.

## Engine model

One background goroutine (`fd.run`) owns the engine. Per session it runs:

- `attempt` — leases and initializes ONE connection (pipeline pool, spilling to the
  main pool on saturation), then runs `session`. Its deferred release Removes the conn
  on a connection error (`fdConnErr`) and Puts it on any clean end (idle / recycle /
  graceful), so a handoff-marked conn is handed off by the pool's OnPut hook.
- `session` — spawns a READER goroutine (reads replies in FIFO order, completes each
  command as its reply lands) and runs the WRITER on the engine goroutine (re-issues
  the recovered carry, then serves the queue in `MaxBatchSize`/`MaxBatchBytes` chunks).
  On a connection error it stops the reader, waits for it, and returns the unacked tail
  as the recovery set.

`run` re-issues the unacked tail (`carry`) on a fresh connection at the next attempt.
Each command carries its own attempt count; the tail is partitioned so a command that
has spent its retry budget is failed while the rest stay eligible, and a SENT
`NoRetry` command (and everything ordered after it) is never re-sent.

Carry replay runs BEFORE the serve loop, so the serve loop's `FullDuplexMaxHold` and
handoff checks are not yet reached while a large tail replays. `writeCarryChunked` polls
both between chunks (LIVE path only — a terminating Close bounds its own flush and
outranks max-hold): a `ShouldHandoff()` mark returns `errFDConnMoving`, and passing
`FullDuplexMaxHold` returns `errFDMaxHold`. Both return the UNWRITTEN suffix out-of-band
(not pushed into the in-flight deque) and route to the clean `fdRecycle` arm — the reader
drains the already-written prefix (those callers complete, never re-executed), `attempt`
Puts the conn (a handoff-marked conn hands off via OnPut; a max-held conn simply returns
to the pool so the hold ends), and `run` replays only the never-sent suffix on the next
lease. Pinned by `TestWriteCarryChunkedStopsOnHandoff` / `TestWriteCarryChunkedStopsOnMaxHold`.

## Panic boundaries

User code runs on the engine goroutine (a `Cmder`'s `Args()`/encoder, `Options.OnConnect`,
a `Limiter`, metrics callbacks). A panic there must NOT kill the sole engine goroutine
or let a live/half-initialized connection return to the pool. The boundaries:

- Reader goroutine: its own `recover` marks the session failed so `run` takes the
  connection-error path (`autopipeline_fullduplex.go`, reader `defer`).
- Reply-policy `NoRetry()`: `fdNoRetrySafe` recovers a panic AFTER the reply has landed
  and treats the command as non-retryable, so the reply is surfaced inline. Without it
  the panic reaches the reader's session-failure recover, which would replay an
  already-answered command — a mutating command run twice.
- Carry / in-session Close sizing: `fdBatchEndSafe` recovers a panicking `Args()` during
  carry replay — the session-start command (taken straight from `fd.ch`) and the
  in-session Close backlog (`flushBacklogForClose` drains `fd.ch` inline, then
  `writeCarryChunked`), which never passed the serve loop's `cmdApproxBytesSafe`
  admission. It fails+drops just the offending command and keeps the healthy connection.
- Serve-loop sizing: `cmdApproxBytesSafe` (same fail+drop behavior).
- Idle / between-sessions Close (`takeQueue` → `shutdownFlush` → `flushReqs`): this flush
  is ORDERED, so it deliberately does NOT drop-and-continue. A panicking `Args()`/
  `NoRetry()` is contained by the outer shutdown-flush recover, which fails the affected
  group and aborts the remainder — completing later ordered commands after an earlier
  failure would break the accepted-⇒-completes-in-order contract (see `flushCarryBudgeted`
  and `TestFDShutdownFlushAbortsAfterRecoveredPanic`). This is a different, intentional
  policy from the live/in-session paths above, not a missing guard.
- `attempt` acquisition/initialization: a recover retires the leased conn (Remove, never
  Put) and returns the carry as `fdLeaseErr` — the same disposition an `initPooledConn`
  ERROR gets — so `run` applies the lease-retry budget instead of crashing or poisoning
  the pool.
- `attempt` connection RELEASE: the deferred release runs LAST (LIFO — the init recover
  is registered after it and runs FIRST), so it has NO outer boundary. Its clean-end
  branch (`releaseConnToPool` drains pending pushes — a custom `PushNotificationProcessor`
  is user code — then Puts) is wrapped in a nested recover that Removes the conn on panic:
  after a panic the conn's drain/Put state is unknown, so Put would poison the pool. Pinned
  by `TestFDReleasePanicRetiresConn`.
- `session` writer path: a backstop recover runs the `fdConnErr` teardown (stop the
  reader, wait it out, recover the tail) and returns `fdConnErr` normally, so `attempt`
  Removes the desynced conn and `run` replays the eligible tail.
- `Limiter.Allow`: `fdAllow`. Metrics callbacks: `reportReplyMetrics` / `emitMetricsGuarded`.

The contract still requires deterministic, panic-free `Args()`/hooks (see `AddHook` and
a `Cmder`'s `Args`); the boundaries stop one bad command or callback from stranding the
whole accepted backlog, they do not license panicking user code.

## Close ordering (Sentinel)

`onCloseHooks.run` invokes close hooks in REVERSE registration order (LIFO). A hook
registered later is a consumer of state an earlier registration provides. The Sentinel
failover teardown registers at construction (first); an autopipeliner drain registers
lazily (later). The drain needs `MasterAddr` (a live failover client) to dial a
replacement connection for accepted-but-unsent work, so it must run BEFORE the failover
teardown — otherwise `MasterAddr` returns `pool.ErrClosed` and replayable commands fail
even though Redis and the pools are still up. See `onCloseHooks.run` and `sentinel.go`.

## Known limitations

These are documented gaps, tracked as follow-ups, not fixed in the current PR.

- **CSC on the FD fast path.** When client-side caching is active, a cacheable command
  submitted in FD mode is streamed straight to the FD writer; the initial submit does
  NOT consult CSC. The cache is involved only when a retryable reply diverts to the
  normal client path. So sparse sequential reads on the blocking face (e.g. repeated
  `GET`) neither hit nor populate the cache while FD is active, unlike the half-duplex
  single-command path, which routes cacheable commands through `process`. Routing
  cacheable FD submits through the cache-aware path (reserve/capture/fulfill) before
  streaming is a follow-up.

- **Terminal-failure duration metrics.** A command that terminates through
  `failReqs`/`failQueue` (lease failure, limiter denial, retry exhaustion, failed Close
  backlog) bypasses the reader and emits no `RecordOperationDuration` sample, so those
  terminal failures are absent from the operation-duration histogram. The ERROR callback
  is still emitted, so error telemetry is intact; only the duration sample is missing.
  Emitting it needs a submit-time start anchor carried on `fdReq` through every terminal
  path (and a choice of anchor semantics — submit vs first-write), so it is a follow-up.
