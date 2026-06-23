# Autopipelining benchmarks

These benchmarks validate the goal of autopipelining: batching concurrent
commands into pipelines cuts network round-trips and raises throughput, without
callers writing pipeline code.

**Throughput is always measured on executed commands** — a command is counted
only after its result has been read (`.Result()` / `.Err()`), never when it is
merely queued. `ap.Do`/`ap.Set` return immediately (the result is deferred), so
a benchmark that counts calls without reading results measures enqueue speed,
not throughput. The throughput benchmarks here read every result before counting.

## Running

The benchmarks talk to a real Redis on `:6379`. Start one first:

```sh
make docker.start            # or: docker run --rm -p 6379:6379 redis

# the headline three-way throughput comparison:
go test -run '^$' -bench BenchmarkAutoPipelineThroughput -benchtime=1x .

# everything (latency micro-benches + tuning sweeps):
go test -run '^$' -bench Benchmark -benchmem -benchtime=1x .
```

The throughput benchmarks run for a fixed wall-clock duration and report
`ops/sec`, so `-benchtime=1x` (one iteration) is correct for them.

## The headline benchmark: BenchmarkAutoPipelineThroughput

Three ways to issue the same workload (2000 goroutines), each counting only
executed commands:

1. **Normal** — a plain client; each `Set` is a blocking round-trip. Bounded by
   Redis's non-pipelined ceiling (~100k SET/sec, like `redis-benchmark` without
   `-P`).
2. **AutoPipelineBlocking** — `client.AutoPipeline()` (the blocking face):
   `ap.Set(...)` blocks until executed, the same call shape as a normal client.
   Only one command per caller is in flight, but the flusher batches across the
   2000 callers into deep, parallel pipelines. Per-goroutine order is preserved.
3. **AutoPipelineWindowed** — `client.AsyncAutoPipeline()` (the deferred face):
   `ap.Set(...)` returns immediately; submit a window of commands, then read
   their results. Keeps each pipeline deepest; the highest-throughput usage.

## Sample results

Local run, redis:latest on `:6379`, Apple Silicon (14 logical CPUs).
Indicative, not a spec.

```
BenchmarkAutoPipelineThroughput/Normal-14                  ~100k ops/sec   1x
BenchmarkAutoPipelineThroughput/AutoPipelineBlocking-14    ~1.1M ops/sec  ~11x
BenchmarkAutoPipelineThroughput/AutoPipelineWindowed-14    ~2.4M ops/sec  ~24x
```

For reference, `redis-benchmark -t set` on the same box: ~50–80k/sec without
pipelining (`-c 50`), ~1.1M/sec with `-P 16`. The Normal client matches the
former; autopipelining reaches the latter automatically.

What the numbers say:

- **`AutoPipeline()` (blocking) clears ~1.1M executed SET/sec** even though each
  caller blocks on every command — the flusher coalesces the 2000 concurrent
  callers into deep pipelines and runs many batches in parallel (its default,
  `DefaultBlockingAutoPipelineConfig`, uses `MaxConcurrentBatches: 50`). A
  drop-in replacement for a normal client, ~14× its throughput, with
  per-goroutine ordering intact.
- **`AsyncAutoPipeline()` windowed reaches ~2.5M** by also batching within each
  caller (submit a window, then read) — the highest-throughput usage. Its
  default is ordered (`MaxConcurrentBatches: 1`), which already saturates under
  windowing while keeping a single goroutine's deferred commands in order.
- **Ordering.** A blocking caller always sees its own commands execute in order,
  regardless of `MaxConcurrentBatches`, because it waits for each result before
  issuing the next — that is why the blocking face can use parallel batches
  safely. The async face defaults to `MaxConcurrentBatches: 1` because a single
  goroutine submitting a window without reading between commands would otherwise
  have those commands reordered. Override either via the optional config arg.

## Other benchmarks

- **BenchmarkIndividualCommands / BenchmarkManualPipeline** — baselines: one
  command per round-trip, vs hand-written `Pipeline()` (the ceiling
  autopipelining approaches automatically).
- **BenchmarkAutoPipeline / BenchmarkConcurrentAutoPipeline** — per-operation
  latency of the autopipeline path; each command awaits its result, so `ns/op`
  is real round-trip latency (1 caller has nothing to batch with and is slowest;
  latency drops as more goroutines let batches fill).
- **BenchmarkAutoPipelineBatchSizes / MaxBatchSizes / MaxFlushDelays /
  MaxFlushDelay / BufferSizes** — tuning sweeps for `MaxBatchSize`,
  `MaxFlushDelay` (and `AdaptiveDelay`), and the dedicated pipeline pool buffers.
- **BenchmarkAutoPipelineSubmit / BenchmarkFutureFace** — the lower-level
  `Submit`/windowed paths; their windowed variants also reach ~2.3M ops/sec,
  consistent with the headline.

## Notes

- **Manual pipelining is still fastest when applicable** — it batches a known
  set with zero coordination overhead. Autopipelining's value is doing this
  automatically for concurrent or windowed callers that cannot batch by hand,
  not beating a hand-written pipeline.
- The flusher coalesces with a small default window when `MaxFlushDelay` is 0,
  and flushes early as soon as the batch is full.
