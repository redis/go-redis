# OTel Native Instrumentation Benchmarks

This directory contains benchmarks to measure the performance overhead of the native OpenTelemetry instrumentation.

## Quick Start

### Run All Benchmarks

```bash
# From the repository root
cd extra/redisotel-native
go test -bench=BenchmarkOTelOverhead -benchmem -benchtime=10s -count=5
```

### Run Specific Benchmarks

```bash
# OTel Disabled (measures dormant code overhead)
go test -bench=BenchmarkOTelOverhead/OTel_Disabled -benchmem -benchtime=10s -count=5

# OTel Enabled (measures active metrics overhead)
go test -bench=BenchmarkOTelOverhead/OTel_Enabled -benchmem -benchtime=10s -count=5

# Specific operation
go test -bench=BenchmarkOTelOverhead/OTel_Enabled/Ping -benchmem -benchtime=10s -count=5

# Connection pool stress test
go test -bench=BenchmarkOTelOverhead_ConnectionPool -benchmem -benchtime=10s -count=5
```

## Automated Comparison

For a comprehensive 3-way comparison (Baseline vs Disabled vs Enabled), use the automated script from the repository root:

```bash
# From the repository root
./compare_perf.sh
```

This script will:
1. Run benchmarks on the current branch (OTel Enabled + Disabled)
2. Checkout upstream/master and run baseline benchmarks
3. Generate comparison reports using benchstat
4. Save all results to a timestamped directory

## Understanding Results

### Expected Overhead

#### OTel Disabled vs Baseline
- **Target**: ~0% overhead (within statistical noise)
- **Acceptable**: <1% overhead
- **Indicates**: Cost of dormant code (should be negligible due to no-op pattern)

#### OTel Enabled vs Disabled
- **Target**: <5% overhead for simple operations
- **Acceptable**: <10% overhead for complex operations
- **Indicates**: Cost of active metrics collection

### Sample Output

```
name                                old time/op    new time/op    delta
OTelOverhead/OTel_Disabled/Ping-8     30.0µs ± 2%    30.1µs ± 3%   ~     (p=0.234 n=5+5)
OTelOverhead/OTel_Enabled/Ping-8      30.1µs ± 3%    31.5µs ± 2%  +4.65%  (p=0.008 n=5+5)

name                                old alloc/op   new alloc/op   delta
OTelOverhead/OTel_Disabled/Ping-8      172B ± 0%      172B ± 0%   ~     (all equal)
OTelOverhead/OTel_Enabled/Ping-8       172B ± 0%     5738B ± 0%  +3236%  (p=0.000 n=5+5)
```

**Interpretation**:
- Disabled mode: No measurable latency overhead (✅)
- Enabled mode: ~4.65% latency overhead (✅ acceptable)
- Memory: Enabled mode allocates more for metrics collection (expected)

## Benchmark Coverage

### Operations
- **Ping** - Simplest operation, baseline overhead
- **Set** - Write operations
- **Get** - Read operations
- **SetGet_Mixed** - Realistic workload (70% reads, 30% writes)
- **Pipeline** - Batch operations

### Scenarios
- **OTel_Enabled** - Full metrics collection
- **OTel_Disabled** - Code present but disabled
- **Baseline** - No OTel code (run on upstream/master)

## Prerequisites

1. **Redis server running**:
   ```bash
   docker run -d -p 6379:6379 redis:latest
   ```

2. **benchstat installed** (for comparisons):
   ```bash
   go install golang.org/x/perf/cmd/benchstat@latest
   ```

## Manual Comparison

```bash
# Run benchmarks on current branch
cd extra/redisotel-native
go test -bench=BenchmarkOTelOverhead -benchmem -count=5 > current.txt

# Checkout master and run baseline
cd ../..
git stash
git checkout upstream/master
go test -bench=BenchmarkOTelOverhead_Baseline -benchmem -count=5 > baseline.txt

# Compare
benchstat baseline.txt current.txt

# Return to your branch
git checkout -
git stash pop
```

## Profiling

### CPU Profile

```bash
go test -bench=BenchmarkOTelOverhead/OTel_Enabled/Ping \
    -cpuprofile=cpu.prof -benchtime=30s

go tool pprof cpu.prof
```

### Memory Profile

```bash
go test -bench=BenchmarkOTelOverhead/OTel_Enabled/Ping \
    -memprofile=mem.prof -benchtime=30s

go tool pprof mem.prof
```

### Trace

```bash
go test -bench=BenchmarkOTelOverhead/OTel_Enabled/Ping \
    -trace=trace.out -benchtime=10s

go tool trace trace.out
```

## Tips for Accurate Benchmarks

1. **Close unnecessary applications** to reduce system noise
2. **Run multiple iterations** (at least 5) for statistical significance
3. **Use longer benchmark times** (10s+) for stable results
4. **Check p-values** - only trust differences with p < 0.05
5. **Run on a quiet system** - avoid background tasks

## Troubleshooting

### High Variance
If you see high variance (±10% or more):
- Increase benchmark time: `-benchtime=30s`
- Increase iteration count: `-count=10`
- Close other applications
- Disable CPU frequency scaling (if possible)

### Redis Connection Errors
```bash
# Check if Redis is running
redis-cli ping

# Start Redis if needed
docker run -d -p 6379:6379 redis:latest
```

### Benchmark Timeout
Increase the timeout:
```bash
go test -bench=... -timeout=60m
```

## See Also

- [../../BENCHMARK_OVERHEAD.md](../../BENCHMARK_OVERHEAD.md) - Comprehensive benchmarking guide
- [../../compare_perf.sh](../../compare_perf.sh) - Automated comparison script
- [Go Benchmarking Guide](https://pkg.go.dev/testing#hdr-Benchmarks)
- [benchstat Documentation](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat)

