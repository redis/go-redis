# Redis Throughput Test

A simple command-line tool to test Redis client throughput with configurable parameters.

## Features

- Support for both single-node and cluster Redis deployments
- Configurable read/write ratio
- Concurrent workers for parallel load generation
- Optional autopipelining support
- Real-time progress reporting
- Detailed statistics

## Building

```bash
cd examples/throughput
go build
```

## Usage

### Basic Usage

Test a local Redis instance:
```bash
./throughput
```

Test a Redis cluster:
```bash
./throughput -mode cluster -host redis-cluster.example.com -port 6379
```

### Command-Line Options

| Flag | Default | Description |
|------|---------|-------------|
| `-host` | `localhost` | Redis host |
| `-port` | `6379` | Redis port |
| `-mode` | `single` | Redis mode: `single` or `cluster` |
| `-password` | `""` | Redis password |
| `-db` | `0` | Redis database (single mode only) |
| `-workers` | `10` | Number of concurrent workers |
| `-duration` | `10s` | Test duration |
| `-key-size` | `16` | Size of keys in bytes |
| `-value-size` | `64` | Size of values in bytes |
| `-read-ratio` | `0.5` | Ratio of read operations (0.0-1.0) |
| `-autopipeline` | `false` | Enable autopipelining |
| `-pipeline-size` | `100` | Autopipeline batch size |
| `-pipeline-delay` | `100µs` | Autopipeline flush delay |
| `-report-interval` | `1s` | Interval for progress reports |

### Examples

#### High-throughput test with autopipelining
```bash
./throughput -workers 50 -duration 30s -autopipeline -pipeline-size 200
```

#### Write-heavy workload
```bash
./throughput -read-ratio 0.2 -workers 20 -duration 60s
```

#### Read-only workload
```bash
./throughput -read-ratio 1.0 -workers 100 -duration 30s
```

#### Large values test
```bash
./throughput -value-size 1024 -workers 10 -autopipeline
```

#### Cluster mode with authentication
```bash
./throughput -mode cluster -host redis-cluster.example.com -password mypassword -workers 50
```

## Output

The tool provides real-time progress updates and a final summary:

```
=== Redis Throughput Test ===
Mode:            single
Endpoint:        localhost:6379
Workers:         10
Duration:        10s
Key size:        16 bytes
Value size:      64 bytes
Read ratio:      0.50
Autopipeline:    true
Pipeline size:   100
Pipeline delay:  100µs

✓ Connected to Redis
✓ Autopipelining enabled

Starting test...
[14:23:01] Ops: 125430 | Throughput: 125430 ops/sec | Errors: 0
[14:23:02] Ops: 251234 | Throughput: 125804 ops/sec | Errors: 0
...

=== Test Results ===
Duration:        10.002s
Total ops:       1254321
  GET ops:       627160 (50.0%)
  SET ops:       627161 (50.0%)
Errors:          0 (0.00%)
Throughput:      125407 ops/sec
Avg latency:     79.74 µs/op
```

## Finding the Sweet Spot

Use the included script to automatically find the optimal configuration for your setup:

```bash
./find-sweet-spot.sh [host] [port]
```

This will test various combinations of workers, batch sizes, and delays to find the configuration that gives you maximum throughput.

Example output:
```
=== Best Configuration ===
Throughput: 964066 ops/sec
Config: workers=300, batch=500, delay=0
```

## Tips for Best Performance

1. **Use autopipelining** for maximum throughput: `-autopipeline`
2. **Find your sweet spot** using the `find-sweet-spot.sh` script
3. **Optimal configuration** (for local Redis on M4 Max):
   - Workers: 300
   - Batch size: 500
   - Delay: 0
   - Result: **~964k ops/sec** with 1.04 µs/op latency
4. **Tune pipeline parameters** based on your network latency:
   - Lower latency: smaller batch size, shorter delay (or 0)
   - Higher latency: larger batch size, longer delay
5. **Adjust read/write ratio** to match your workload: `-read-ratio 0.8`
6. **Run on the same network** as Redis for accurate results

## Performance Comparison

### Autopipeline vs No Autopipeline

Based on testing with local Redis on Apple M4 Max:

| Workers | Without Autopipeline | With Autopipeline (optimized) | Improvement |
|---------|---------------------|-------------------------------|-------------|
| 10      | 100k ops/sec (10.04 µs) | 110k ops/sec (9.10 µs) | **1.1x** |
| 50      | 132k ops/sec (7.57 µs) | 405k ops/sec (2.47 µs) | **3.1x** |
| 100     | 137k ops/sec (7.32 µs) | 642k ops/sec (1.56 µs) | **4.7x** |
| 300     | 135k ops/sec (7.41 µs) | **964k ops/sec (1.04 µs)** | **7.1x** |

**Key Findings:**
- Autopipelining provides **minimal benefit** with low concurrency (10 workers)
- Autopipelining provides **massive benefits** with high concurrency (300 workers)
- **7.1x throughput improvement** at optimal configuration
- **7.1x latency reduction** at optimal configuration
- Without autopipelining, throughput **plateaus around 135k ops/sec** regardless of worker count
- With autopipelining, throughput **scales linearly** with worker count up to the sweet spot

## Graceful Shutdown

Press `Ctrl+C` to stop the test early. The tool will gracefully shut down and display results for the completed portion of the test.

