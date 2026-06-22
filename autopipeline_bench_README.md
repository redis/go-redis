# Autopipelining benchmarks

`autopipeline_bench_test.go` validates the goal of autopipelining: batching
concurrent commands into pipelines cuts network round-trips and raises
throughput, without callers writing pipeline code.

## Running

The benchmarks talk to a real Redis on `:6379`. Start one first:

```sh
# from the repo root, using the project's docker stack
make docker.start            # or: docker run --rm -p 6379:6379 redis

go test -run '^$' -bench 'BenchmarkAutoPipeline|BenchmarkManualPipeline|BenchmarkIndividualCommands' -benchmem .
```

Run the full suite (sweeps included) with:

```sh
go test -run '^$' -bench 'Benchmark' -benchmem -benchtime 3s .
```

## What each benchmark shows

The goal is proven by the first group; the rest are tuning sweeps.

- **BenchmarkIndividualCommands** — baseline: one command per round-trip.
- **BenchmarkManualPipeline** — hand-written `Pipeline()` batching (the
  ceiling autopipelining aims to approach automatically).
- **BenchmarkAutoPipeline** — `WithAutoPipeline()`/`AutoPipeline()` under
  concurrent load. Expected: ops/sec well above IndividualCommands and close
  to ManualPipeline, with no pipeline code at the call site.
- **BenchmarkAutoPipelineVsManual** — head-to-head of the two batched paths.
- **BenchmarkConcurrentAutoPipeline** — scaling with concurrent goroutines.

Tuning sweeps (pick config for a workload):

- **BenchmarkAutoPipelineBatchSizes / BenchmarkAutoPipelineMaxBatchSizes** —
  `MaxBatchSize` vs throughput.
- **BenchmarkAutoPipelineMaxFlushDelays / BenchmarkMaxFlushDelay** —
  latency/CPU trade-off of `MaxFlushDelay` (and `AdaptiveDelay`).
- **BenchmarkBufferSizes** — effect of the dedicated pipeline pool buffers
  (`PipelineReadBufferSize` / `PipelineWriteBufferSize`, see the pool change).
- **BenchmarkThroughput** — aggregate ops/sec under a representative mix.

## Reading results

Compare ns/op and B/op across IndividualCommands → AutoPipeline → ManualPipeline.
Autopipelining is working as intended when AutoPipeline sits much closer to
ManualPipeline than to IndividualCommands while requiring no manual batching.

## Sample results

Local run, redis:latest on `:6379`, Apple Silicon (14 logical CPUs).
Numbers are indicative, not a spec.

The headline result — `BenchmarkHighConcurrencyThroughput`, 2000 concurrent
callers issuing independent commands over a typical pool (`PoolSize: 10`):

```
BenchmarkHighConcurrencyThroughput/Individual-14        36600 ops/sec
BenchmarkHighConcurrencyThroughput/AutoPipeline-14     972000 ops/sec   (~26x)
```

That is the workload autopipelining is for: many goroutines issuing commands
that cannot be batched by hand. Two effects compound:

1. **Connection multiplexing.** A plain command holds a connection for the
   whole round-trip, so individual throughput is capped by the pool size
   (PoolSize=10 here → ~37k ops/sec, and it stays there no matter how many
   goroutines call). Autopipelining packs many commands per connection, so the
   same small pool sustains ~1M ops/sec.
2. **Concurrency.** `ap.Do` blocks until its command's batch is flushed, so the
   batch can never be larger than the number of goroutines issuing commands at
   once. The win scales with concurrent callers.

The multiple therefore depends on the baseline pool size — i.e. how starved
the non-pipelined client is:

```
individual PoolSize=10   ~37k ops/sec  -> ~26x
individual PoolSize=50   ~63k ops/sec  -> ~15x
individual PoolSize=140  ~90k ops/sec  -> ~11x   (redis single-instance ceiling)
autopipeline (any of the above pools)  ~970k-1M ops/sec
```

Autopipelining is bounded by what a pipelined single redis instance can
service (~1M SET/sec on this box); the individual client is bounded by its
pool. Past ~PoolSize 140 the individual client hits redis's own non-pipelined
ceiling and cannot improve, so the gap is structural, not a tuning artifact.

Concurrency scaling (per-op latency, `BenchmarkConcurrentAutoPipeline`):

```
1goroutine    ~1.3 ms/op     (one caller, nothing to batch with)
10goroutines  ~206 µs/op
100goroutines  ~17 µs/op     (batches fill, round-trips amortized)
```

Other notes:

- **Manual pipelining is still fastest when applicable** — it batches a known
  set with zero coordination overhead. Autopipelining's value is doing this
  automatically for concurrent, independent callers that cannot batch by hand,
  not beating a hand-written pipeline.
- Tune `MaxBatchSize` / `MaxFlushDelay` (and `AdaptiveDelay`) for the workload;
  the sweep benchmarks exist for exactly that. The flusher also coalesces with
  a small default window when `MaxFlushDelay` is 0, and flushes early as soon
  as the batch is full or the queue stops growing.
