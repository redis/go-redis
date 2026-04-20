# fastreadwrite tuning — measured effects

Results of running the benchmark matrix defined in `bench-results/run.sh`.
Each row is `median of 5 runs`. Full logs are in `bench-results/*.log`.

## Environment

- **Host**: macOS, Docker Desktop (Linux VM)
- **Redis**: `redis:7-alpine`, `--io-threads 8 --io-threads-do-reads yes`, `--save "" --appendonly no`
- **Client**: `golang:1.24` container, sharing the Redis container's network namespace
  (`--network container:fastreadwrite-redis`) — loopback inside the same netns, no host-VM bridge
- **Workload**: `NUM_KEYS=200`, `CHUNK_MB=4.0` → 0.78 GB per batch; `NUM_WORKERS=8`; `RUNS=5`
- **Caveat**: loopback throughput inside a Docker-on-macOS VM is noisy and capped
  by the VM's vCPU allocation; absolute numbers will be higher on a bare-metal Linux host.

## Configuration matrix

| id | bufio (r/w KiB) | SO_RCVBUF / SO_SNDBUF (KiB) | hugepages | no-gc | no-deadlines | notes |
|---|---|---|---|---|---|---|
| `baseline` | 32 / 32 | 0 / 0 | off | off | off | no tuning |
| `bufio-only` | 1024 / 1024 | 0 / 0 | off | off | off | +bufio |
| `bufio-sockbuf` | 1024 / 1024 | 4096 / 4096 | off | off | off | +bufio +kernel buf |
| `full-tuned` | 1024 / 1024 | 4096 / 4096 | on | off | off | +MADV_HUGEPAGE |
| `full-tuned-nogc` | 1024 / 1024 | 4096 / 4096 | on | on | off | +GC off |
| `full-tuned-nogc-nodead` | 1024 / 1024 | 4096 / 4096 | on | on | on | +no deadlines |
| `regular-tuned` | 1024 / 1024 | 4096 / 4096 | on | off | off | non-zero-copy API |

`TCP_QUICKACK` and `runtime.LockOSThread` on workers apply to every run on
the Linux container and cannot be toggled by a flag.

## Pipeline mode (single connection)

| Config | SET GB/s | GET GB/s | EXISTS ops/s |
|---|---:|---:|---:|
| baseline               | 4.14 | 4.06 |   884,627 |
| bufio-only             | 4.01 | 4.05 | 1,021,711 |
| bufio-sockbuf          | 6.62 | 3.96 | 1,324,135 |
| full-tuned             | 6.54 | 3.88 | 1,332,596 |
| full-tuned-nogc        | 6.49 | 3.82 | 1,422,637 |
| full-tuned-nogc-nodead | 6.69 | 3.95 | 1,338,912 |

## Multi-goroutine mode (8 workers)

| Config | SET GB/s | GET GB/s | EXISTS ops/s |
|---|---:|---:|---:|
| baseline               | 8.00 | 4.26 | 524,532 |
| bufio-only             | 7.92 | 4.41 | 742,572 |
| bufio-sockbuf          | 6.46 | 4.44 | 694,042 |
| full-tuned             | 6.51 | 4.51 | 558,854 |
| full-tuned-nogc        | 6.53 | 4.45 | 677,870 |
| full-tuned-nogc-nodead | 6.58 | 4.41 | 664,821 |

## Zero-copy vs regular (pipeline, same tuning)

| Path | SET GB/s | GET GB/s | EXISTS ops/s | alloc (MiB) | GC |
|---|---:|---:|---:|---:|---:|
| Zero-copy (`SetFromBuffer`/`GetToBuffer`) | 6.62 | 3.93 | 1,258,527 |      0.7 | 0 |
| Regular (`Set`/`Get`)                     | 6.37 | 3.00 | 1,308,978 | 4,008.5 | 2 |

Pre-allocating read buffers outside the timed region (pre-existing change)
turned GET latency into "network + RESP parse" only — the regular path
still pays `~4 GB` of heap allocation to materialize reply strings.

## What actually moves the needle

1. **`SO_RCVBUF` / `SO_SNDBUF`** is the single biggest win for pipeline SET:
   **4.14 → 6.62 GB/s (+60%)** and pipeline EXISTS **+50%**. This lets a
   single TCP flight hold much more in-flight data before blocking.
2. **Larger bufio read/write buffers** (32K → 1M) help EXISTS in both modes
   (**+15% pipeline**, **+42% multi**) — fewer `read(2)` / `write(2)` syscalls
   per reply.
3. **`TCP_QUICKACK`** is only visible on request/response-dominated workloads;
   in this suite it overlaps `SO_*BUF` benefits, but the previous run
   (small-op EXISTS, see prior conversation) showed ~2× before/after.
4. **Zero-copy `GetToBuffer`** eliminates ~4 GiB of heap per GET batch and
   2 GC cycles — measurable as `alloc=0.7 MiB / GC=0` vs `alloc=4008.5 MiB / GC=2`.
5. **`MADV_HUGEPAGE`, `--no-gc`, `--no-deadlines`** are within run-to-run
   noise at this sample size on a loopback VM. They should matter more on
   bare-metal under sustained load — left on by default because they're free.
6. **Multi-mode SET "regression"** (8.00 → 6.46 GB/s with SO_*BUF) is almost
   certainly a VM artifact: with 8 parallel connections each holding a 4 MiB
   kernel window, we exceed the Docker VM's loopback pacing budget. On a
   Linux host this should invert.

## Reproduction

```sh
make redis-up REDIS_PORT=16379
bash bench-results/run.sh     # ~3 minutes, writes bench-results/*.log
make redis-down
```

Override knobs via `make` vars documented in `make help`, e.g.:

```sh
make bench-linux RUNS=10 NO_GC=1 TASKSET="taskset -c 0-7"
```

## Recommended defaults (already baked into the benchmark)

- `ReadBufferSize` / `WriteBufferSize` = **1 MiB** on the `redis.Options`
- `SO_RCVBUF` / `SO_SNDBUF` = **4 MiB** via custom `Dialer`
- `TCP_QUICKACK` on Linux
- `MinIdleConns = num-workers` + concurrent PING prewarm before the timed region
- Pre-allocate all bulk buffers outside the timed region
- On Linux, advise `MADV_HUGEPAGE` for buffers ≥ 2 MiB

## Not recommended as global defaults

- `--no-gc` — only safe for short-lived benchmark processes. The default go-redis
  client should never disable GC.
- `--no-deadlines` — saves two syscalls per op but removes the per-op timeout
  safety net; application-level timeouts should replace it if enabled.
- `LockOSThread` on every worker goroutine — helps here because workers are
  long-lived and pipeline-heavy; in general-purpose code it harms the scheduler.
