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
executed commands. The autopipeline variants use a parallel-batch config
(`MaxBatchSize: 300, MaxConcurrentBatches: 80, Unordered: true`):

1. **Normal** — a plain client; each `Set` is a blocking round-trip. Bounded by
   Redis's non-pipelined ceiling (~100k SET/sec, like `redis-benchmark` without
   `-P`).
2. **AutoPipelineBlocking** — `ap.Set(...).Result()` read immediately, the way a
   normal client is used (drop-in). Only one command per caller is in flight,
   but the flusher batches across the 2000 callers into deep, parallel pipelines.
3. **AutoPipelineWindowed** — submit a window of commands, then read their
   results. Keeps each pipeline deepest; the high-throughput async usage.

## Sample results

Local run, redis:latest on `:6379`, Apple Silicon (14 logical CPUs).
Indicative, not a spec.

```
BenchmarkAutoPipelineThroughput/Normal-14                  ~82k  ops/sec   1x
BenchmarkAutoPipelineThroughput/AutoPipelineBlocking-14    ~1.1M ops/sec  ~14x
BenchmarkAutoPipelineThroughput/AutoPipelineWindowed-14    ~2.5M ops/sec  ~30x
```

For reference, `redis-benchmark -t set` on the same box: ~50–80k/sec without
pipelining (`-c 50`), ~1.1M/sec with `-P 16`. The Normal client matches the
former; autopipelining reaches the latter automatically.

What the numbers say:

- **Blocking autopipelining clears ~1.1M executed SET/sec** even though each
  caller reads its result before issuing the next — the flusher coalesces the
  2000 concurrent callers into deep pipelines and runs many batches in parallel.
  This is a drop-in replacement for a normal client (`.Result()` immediately),
  ~14× its throughput.
- **Windowed autopipelining reaches ~2.5M** by also batching within each caller
  (submit a window, then read) — the highest-throughput usage.
- **Config matters.** The numbers above use parallel batches
  (`MaxConcurrentBatches: 80, Unordered: true`). The **default** config is
  ordered (`MaxConcurrentBatches: 1`): it serializes batch execution, so
  blocking usage caps near ~500k — but windowed still reaches a few million
  because one ordered batch stays deep. Choose ordered for a drop-in correctness-
  preserving speedup, parallel/unordered for maximum throughput.

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
