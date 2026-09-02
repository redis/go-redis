# Push-notification consumption (RESP3)

How the v9 client consumes server pushes (`>`-typed frames: CSC invalidations,
maintenance notifications), the guarantees it gives, and the rules that keep the
model closed. Read this before adding any code that reads pushes.

## The model: checkpoints, not owned readers

v9 connections have no owning reader: the goroutine that holds a conn reads it.
Pushes are therefore consumed at fixed checkpoints:

| Checkpoint | When | Mode |
|---|---|---|
| Reply read | after every command write | blocking loop: frame is a push → process, read again; else it is the reply |
| Pre-command drain | before a command uses a pooled conn | speculative (peek, bounded) |
| Cache-hit stale-guard | after a CSC cache hit | speculative |
| Pool put-path check | before a conn returns to the pool | speculative |
| Idle-conn drainer | background tick over idle pooled conns | speculative |
| FD session / pubsub | connection with an owning reader | at arrival |

Speculative checkpoints cannot know whether a complete frame is present. They
gate on two levels — `HasBufferedData` (bufio) and `MaybeHasData` (kernel
`MSG_PEEK`) — and drain under a hard read cap (`cscDrainHardReadCap`, raised to
the relaxed timeout while maintenance relaxation is active: pushDrainBudget).
Physics of the model, not bugs:

- A frame, once begun, must be completed (may block under the cap); a mid-frame
  timeout is a real desync and retires the conn.
- Kernel readability over TLS can mean zero RESP bytes (control records), so
  bare readability is handled only by the built-in processor, which treats a
  boundary timeout as benign.
- A custom `PushNotificationProcessor` implements only the blocking loop, so
  speculative checkpoints hand it work only when real bytes are buffered
  (spurious-retire risk otherwise) and skip it for kernel-only data (staleness
  cost). Both bounded; the fix is the follow-up drainer capability below.

## The freshness guarantee (user-facing)

Invalidations apply: at arrival on FD-autopipeline conns; at the next command on
active conns; at the next drainer tick on idle pooled conns. Hard cap
everywhere: the cache's MaxStaleness. This is documented on
`Options.ClientSideCacheConfig`.

## Rules (enforced in review)

1. **No new speculative drain call sites.** Each one re-buys the whole edge-case
   catalog (partial frames, TLS peeks, cap tuning, custom-processor gating). New
   features consume pushes through the existing checkpoints or not at all.
2. **A feature that needs fresher pushes than checkpoints give must own a
   reader** (the FD-session / miss-coalescer / pubsub pattern: one goroutine
   owns the conn's read side; pushes are consumed at arrival). It must not add
   checkpoints.
3. **The mid-frame rule is not negotiable:** never abandon a partially consumed
   frame; a mid-frame timeout retires the conn.

## Planned follow-up (needs maintainer sign-off)

One-mode convergence: a `NotificationDrainer` capability interface (consume
complete frames until the buffer is empty at a boundary; finish any begun frame;
never wait for an unbegun frame; boundary-empty and boundary-timeout = success).
Speculative checkpoints type-assert it — custom processors that implement it get
full treatment (kernel-only data included, no spurious retires); the blocking
method remains for the reply loop and pub/sub and as the legacy fallback. Most
users should register handlers on the built-in processor instead of replacing
the processor.
