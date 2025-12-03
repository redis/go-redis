# OTel Observability Performance Overhead Benchmarking

This directory contains a comprehensive benchmarking suite to measure the performance overhead of the OpenTelemetry (OTel) observability implementation in go-redis.

## 📋 Overview

The benchmarking suite performs a **3-way comparison** to measure:

1. **Baseline Performance** - Upstream master without any OTel code
2. **Dormant Code Overhead** - Current branch with OTel code present but disabled
3. **Active Metrics Overhead** - Current branch with OTel metrics enabled

## 🎯 Goals

### Primary Goals
- **Prove zero overhead when disabled**: The no-op pattern should add negligible overhead (<1%) when metrics are disabled
- **Measure active overhead**: Quantify the performance cost when metrics are actively collected
- **Validate production readiness**: Ensure overhead is acceptable for production use

### Success Criteria
- **Disabled vs Master**: ~0% overhead (within statistical noise)
- **Enabled vs Disabled**: <5-10% overhead for most operations
- **Memory allocations**: Minimal increase in allocations per operation

## 📁 Files

### Core Files
- **`benchmark_overhead_test.go`** - Go benchmark suite with table-driven tests
- **`compare_perf.sh`** - Automated comparison script
- **`BENCHMARK_OVERHEAD.md`** - This documentation

### Generated Files (after running benchmarks)
- **`benchmark_results_*/`** - Results directory with timestamp
  - `current_branch.txt` - Raw results from current branch
  - `upstream_master.txt` - Raw results from upstream/master
  - `otel_enabled.txt` - Extracted enabled results
  - `otel_disabled.txt` - Extracted disabled results
  - `comparison_*.txt` - benchstat comparison reports
  - `README.md` - Summary of the benchmark run

## 🚀 Quick Start

### Prerequisites

1. **Redis server running**:
   ```bash
   docker run -d -p 6379:6379 redis:latest
   ```

2. **benchstat installed** (script will auto-install if missing):
   ```bash
   go install golang.org/x/perf/cmd/benchstat@latest
   ```

### Running the Full Comparison

```bash
# Run with default settings (5 iterations, 10s per benchmark)
./compare_perf.sh

# Run with custom settings
BENCHMARK_COUNT=10 BENCHMARK_TIME=30s ./compare_perf.sh

# Run specific benchmarks only
BENCHMARK_FILTER="BenchmarkOTelOverhead/.*Ping" ./compare_perf.sh
```

### Running Individual Benchmarks

```bash
# Run all OTel overhead benchmarks
go test -bench=BenchmarkOTelOverhead -benchmem -benchtime=10s -count=5

# Run specific scenario
go test -bench=BenchmarkOTelOverhead/OTel_Enabled -benchmem -benchtime=10s -count=5

# Run specific operation
go test -bench=BenchmarkOTelOverhead/OTel_Enabled/Ping -benchmem -benchtime=10s -count=5

# Run connection pool benchmarks
go test -bench=BenchmarkOTelOverhead_ConnectionPool -benchmem -benchtime=10s -count=5
```

## 📊 Understanding the Results

### benchstat Output Format

```
name                    old time/op    new time/op    delta
Ping-8                    156µs ± 2%     158µs ± 3%   +1.28%  (p=0.008 n=5+5)

name                    old alloc/op   new alloc/op   delta
Ping-8                     112B ± 0%      112B ± 0%     ~     (all equal)

name                    old allocs/op  new allocs/op  delta
Ping-8                     4.00 ± 0%      4.00 ± 0%     ~     (all equal)
```

### Interpreting Results

- **`~`** - No statistically significant difference (excellent for disabled mode!)
- **`+X%`** - Slower by X% (overhead)
- **`-X%`** - Faster by X% (unlikely, usually measurement variance)
- **`p-value`** - Statistical significance (p < 0.05 means difference is real)
- **`n=X+Y`** - Number of samples used for comparison

### What to Look For

#### Comparison 1: Master vs Disabled
```
✅ GOOD: ~0% difference, p > 0.05 (no significant difference)
❌ BAD:  >1% overhead with p < 0.05 (dormant code has measurable cost)
```

#### Comparison 2: Disabled vs Enabled
```
✅ GOOD: <5% overhead for simple operations (Ping, Get, Set)
✅ ACCEPTABLE: <10% overhead for complex operations (Pipeline)
⚠️  REVIEW: >10% overhead (may need optimization)
```

#### Comparison 3: Master vs Enabled
```
✅ GOOD: Total overhead <10% for production workloads
⚠️  REVIEW: >15% overhead (consider if metrics value justifies cost)
```

## 🔬 Benchmark Coverage

### Operations Tested

1. **Ping** - Simplest operation, measures baseline overhead
2. **Set** - Write operation with key generation
3. **Get** - Read operation with cache hits
4. **SetGet_Mixed** - Realistic workload (70% reads, 30% writes)
5. **Pipeline** - Batch operations (10 commands per pipeline)

### Scenarios Tested

1. **OTel_Enabled** - Full metrics collection
2. **OTel_Disabled** - Code present but disabled
3. **No_OTel** - Baseline from upstream/master

### Concurrency

All benchmarks use `b.RunParallel()` to simulate real-world concurrent access patterns.

## 🛠️ Customization

### Environment Variables

```bash
# Number of benchmark iterations (default: 5)
BENCHMARK_COUNT=10 ./compare_perf.sh

# Time per benchmark (default: 10s)
BENCHMARK_TIME=30s ./compare_perf.sh

# Filter benchmarks by name (default: BenchmarkOTelOverhead)
BENCHMARK_FILTER="BenchmarkOTelOverhead/.*Ping" ./compare_perf.sh

# Upstream remote name (default: upstream)
UPSTREAM_REMOTE=origin ./compare_perf.sh

# Upstream branch name (default: master)
UPSTREAM_BRANCH=main ./compare_perf.sh
```

### Combining Options

```bash
# Run 10 iterations of 30s each, only Ping benchmarks
BENCHMARK_COUNT=10 \
BENCHMARK_TIME=30s \
BENCHMARK_FILTER="BenchmarkOTelOverhead/.*Ping" \
./compare_perf.sh
```

## 📈 Example Results

### Expected Results (Hypothetical)

#### Comparison 1: Master vs Disabled (Dormant Code Overhead)
```
name                                old time/op    new time/op    delta
OTelOverhead/OTel_Disabled/Ping-8     156µs ± 2%     157µs ± 3%   ~     (p=0.234 n=5+5)
OTelOverhead/OTel_Disabled/Set-8      189µs ± 1%     190µs ± 2%   ~     (p=0.421 n=5+5)
OTelOverhead/OTel_Disabled/Get-8      145µs ± 2%     146µs ± 1%   ~     (p=0.548 n=5+5)

name                                old alloc/op   new alloc/op   delta
OTelOverhead/OTel_Disabled/Ping-8      112B ± 0%      112B ± 0%   ~     (all equal)
```
**✅ Result: No measurable overhead when disabled**

#### Comparison 2: Disabled vs Enabled (Active Metrics Overhead)
```
name                                old time/op    new time/op    delta
OTelOverhead/OTel_Enabled/Ping-8      157µs ± 3%     164µs ± 2%  +4.46%  (p=0.008 n=5+5)
OTelOverhead/OTel_Enabled/Set-8       190µs ± 2%     199µs ± 3%  +4.74%  (p=0.016 n=5+5)
OTelOverhead/OTel_Enabled/Get-8       146µs ± 1%     153µs ± 2%  +4.79%  (p=0.008 n=5+5)

name                                old alloc/op   new alloc/op   delta
OTelOverhead/OTel_Enabled/Ping-8       112B ± 0%      128B ± 0%  +14.29%  (p=0.000 n=5+5)
```
**✅ Result: ~5% latency overhead, acceptable for production**

## 🔍 Troubleshooting

### Redis Not Running
```
❌ Redis is not running on localhost:6379
💡 Start Redis with: docker run -d -p 6379:6379 redis:latest
```

### benchstat Not Found
The script will auto-install benchstat. If it fails:
```bash
go install golang.org/x/perf/cmd/benchstat@latest
export PATH=$PATH:$(go env GOPATH)/bin
```

### Benchmark Timeout
Increase the timeout:
```bash
# In compare_perf.sh, modify the timeout flag:
go test -bench=... -timeout=60m ...
```

### High Variance in Results
- Ensure system is not under load
- Increase `BENCHMARK_COUNT` for more samples
- Increase `BENCHMARK_TIME` for longer runs
- Close other applications

## 📝 Best Practices

### Before Running Benchmarks

1. **Close unnecessary applications** to reduce system noise
2. **Ensure stable system load** (no background tasks)
3. **Use consistent Redis configuration** (same version, same settings)
4. **Run multiple iterations** (at least 5) for statistical significance

### Interpreting Results

1. **Focus on p-values** - Only trust differences with p < 0.05
2. **Look at trends** - Consistent overhead across operations is more meaningful
3. **Consider absolute values** - 10% of 1µs is less concerning than 10% of 1ms
4. **Check allocations** - Memory overhead can be as important as latency

### Reporting Results

When sharing benchmark results:
1. Include system information (CPU, RAM, OS)
2. Include Redis version and configuration
3. Include full benchstat output
4. Note any anomalies or special conditions
5. Include multiple runs to show consistency

## 🎓 Advanced Usage

### Profiling

```bash
# CPU profile
go test -bench=BenchmarkOTelOverhead/OTel_Enabled/Ping \
    -cpuprofile=cpu.prof -benchtime=30s

# Memory profile
go test -bench=BenchmarkOTelOverhead/OTel_Enabled/Ping \
    -memprofile=mem.prof -benchtime=30s

# Analyze profiles
go tool pprof cpu.prof
go tool pprof mem.prof
```

### Comparing Specific Commits

```bash
# Benchmark commit A
git checkout commit-a
go test -bench=BenchmarkOTelOverhead -benchmem -count=5 > commit-a.txt

# Benchmark commit B
git checkout commit-b
go test -bench=BenchmarkOTelOverhead -benchmem -count=5 > commit-b.txt

# Compare
benchstat commit-a.txt commit-b.txt
```

## 📚 References

- [Go Benchmarking Guide](https://pkg.go.dev/testing#hdr-Benchmarks)
- [benchstat Documentation](https://pkg.go.dev/golang.org/x/perf/cmd/benchstat)
- [OpenTelemetry Semantic Conventions](https://opentelemetry.io/docs/specs/semconv/)
- [go-redis Documentation](https://redis.uptrace.dev/)

## 🤝 Contributing

When adding new benchmarks:
1. Follow the existing naming convention
2. Use `b.RunParallel()` for realistic concurrency
3. Use `b.ReportAllocs()` to track memory
4. Add documentation to this file
5. Update the comparison script if needed

## 📄 License

Same as go-redis (BSD 2-Clause License)

