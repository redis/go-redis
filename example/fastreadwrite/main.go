package main

import (
	"bytes"
	"context"
	"flag"
	"fmt"
	"log"
	"math"
	"net"
	"runtime"
	"runtime/debug"
	"sort"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Benchmark for go-redis zero-copy operations (SetFromBuffer / GetToBuffer).
//
// Measures raw throughput of batch SET, GET, and EXISTS using pipelining
// and multi-goroutine concurrency. Designed to mirror the LMCache Python
// benchmark (benchmark_resp_client.py) for direct comparison.
//
// Prerequisites:
//   - A running Redis server
//
// Run:
//
//	go run . --host 127.0.0.1 --port 6379 --chunk-mb 4.0 --num-keys 500

func main() {
	host := flag.String("host", "127.0.0.1", "Redis server host")
	port := flag.Int("port", 6379, "Redis server port")
	chunkMB := flag.Float64("chunk-mb", 4.0, "Chunk size in MB")
	numWorkers := flag.Int("num-workers", 8, "Number of goroutines for multi-worker mode")
	numKeys := flag.Int("num-keys", 500, "Number of keys to benchmark")
	username := flag.String("username", "", "Redis username for authentication")
	password := flag.String("password", "", "Redis password for authentication")
	mode := flag.String("mode", "both", "Benchmark mode: pipeline, multi, or both")
	poolSize := flag.Int("pool-size", 0, "Connection pool size (0 = default)")
	regular := flag.Bool("regular", false, "Also benchmark regular (non-zero-copy) Set/Get for comparison")
	readBufKB := flag.Int("read-buf-kb", 1024, "go-redis per-connection bufio read buffer size in KiB")
	writeBufKB := flag.Int("write-buf-kb", 1024, "go-redis per-connection bufio write buffer size in KiB")
	sockRcvKB := flag.Int("sock-rcvbuf-kb", 4096, "TCP SO_RCVBUF in KiB (0 = OS default)")
	sockSndKB := flag.Int("sock-sndbuf-kb", 4096, "TCP SO_SNDBUF in KiB (0 = OS default)")
	noDeadlines := flag.Bool("no-deadlines", false, "Skip per-op SetReadDeadline/SetWriteDeadline syscalls")
	runs := flag.Int("runs", 5, "Number of timed repetitions per operation; stats are aggregated across runs")
	noGC := flag.Bool("no-gc", false, "Disable GC during timed region (debug.SetGCPercent(-1))")
	minIdle := flag.Int("min-idle", 0, "Pool MinIdleConns to pre-dial (0 = auto: max(num-workers,1))")
	hugePages := flag.Bool("huge-pages", true, "madvise(MADV_HUGEPAGE) on the bulk buffers (Linux-only hint)")
	flag.Parse()

	chunkBytes := int((*chunkMB) * 1024 * 1024)
	totalBytes := int64(*numKeys) * int64(chunkBytes)
	totalGB := float64(totalBytes) / (1024 * 1024 * 1024)

	ctx := context.Background()

	opts := &redis.Options{
		Network:         "tcp",
		Addr:            fmt.Sprintf("%s:%d", *host, *port),
		Username:        *username,
		Password:        *password,
		MaxRetries:      0,
		ReadTimeout:     5 * time.Minute,
		WriteTimeout:    5 * time.Minute,
		ReadBufferSize:  *readBufKB * 1024,
		WriteBufferSize: *writeBufKB * 1024,
	}
	if *noDeadlines {
		// -2 disables SetReadDeadline/SetWriteDeadline syscalls entirely.
		opts.ReadTimeout = -2
		opts.WriteTimeout = -2
	}
	if *poolSize > 0 {
		opts.PoolSize = *poolSize
	}
	// Pre-dial pool connections so that dial + HELLO/AUTH happens before the
	// timed region. Defaults to at least numWorkers so multi-goroutine mode
	// never dials inside the hot loop.
	mic := *minIdle
	if mic <= 0 {
		mic = *numWorkers
	}
	if mic < 1 {
		mic = 1
	}
	opts.MinIdleConns = mic

	// Custom dialer that raises kernel socket buffers and applies
	// platform-specific TCP tuning (e.g. TCP_QUICKACK on Linux).
	opts.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
		d := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 5 * time.Minute}
		c, err := d.DialContext(ctx, network, addr)
		if err != nil {
			return nil, err
		}
		if tc, ok := c.(*net.TCPConn); ok {
			if *sockRcvKB > 0 {
				_ = tc.SetReadBuffer(*sockRcvKB * 1024)
			}
			if *sockSndKB > 0 {
				_ = tc.SetWriteBuffer(*sockSndKB * 1024)
			}
			tuneTCPConn(tc)
		}
		return c, nil
	}

	client := redis.NewClient(opts)
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}

	fmt.Println("Redis go-redis Zero-Copy Benchmark")
	fmt.Printf("Server: %s, Workers: %d, Runs: %d\n", opts.Addr, *numWorkers, *runs)
	fmt.Printf("Chunk size: %dKB, Keys: %d\n", chunkBytes/1024, *numKeys)
	fmt.Printf("Total data: %.2f GB, Mode: %s\n", totalGB, *mode)
	fmt.Printf("bufio: r=%dKiB w=%dKiB, SO_RCVBUF=%dKiB SO_SNDBUF=%dKiB, deadlines=%v\n",
		*readBufKB, *writeBufKB, *sockRcvKB, *sockSndKB, !*noDeadlines)
	fmt.Printf("MinIdleConns=%d, no-gc=%v, huge-pages=%v, GOMAXPROCS=%d\n",
		opts.MinIdleConns, *noGC, *hugePages, runtime.GOMAXPROCS(0))
	fmt.Println(strings(60, '-'))

	// Prepare test data
	fmt.Println("starting buffer initialization (this might take a while)")
	keys, buffers := prepareData(*numKeys, chunkBytes)
	// Pre-allocate read buffers outside the timed region so Batch GET measures
	// only network + parse throughput, not make() + page-fault cost.
	readBufs := prepareReadBufs(*numKeys, chunkBytes)
	if *hugePages {
		// Best-effort huge-page hint for the big bulk buffers. No-op on non-Linux.
		for i := range buffers {
			madviseHugepage(buffers[i])
			madviseHugepage(readBufs[i])
		}
	}
	fmt.Println("buffer initialization complete")

	// Warm up pool: force all MinIdleConns through dial + HELLO/AUTH so none of
	// that shows up inside the timed region.
	prewarmPool(ctx, client, opts.MinIdleConns)
	// Warm up protocol / TCP slow-start with a handful of real ops.
	warmup(ctx, client, chunkBytes)

	// Optionally disable GC around the timed region. All buffers are pre-
	// allocated so no steady-state heap growth; we just want to silence GC
	// jitter from the measurement.
	if *noGC {
		prev := debug.SetGCPercent(-1)
		debug.SetMemoryLimit(math.MaxInt64)
		defer debug.SetGCPercent(prev)
	}

	fmt.Println("starting throughput benchmarks")

	// Pipeline mode
	if *mode == "pipeline" || *mode == "both" {
		fmt.Printf("\n=== Pipeline Mode (single connection, %d runs) ===\n", *runs)
		runBenchmarkSuite(ctx, client, keys, buffers, readBufs, *numKeys, totalGB, *runs,
			benchPipelineSet, benchPipelineGet, benchPipelineExists)
	}

	// Multi-goroutine mode
	if *mode == "multi" || *mode == "both" {
		fmt.Printf("\n=== Multi-Goroutine Mode (%d workers, %d runs) ===\n", *numWorkers, *runs)
		nw := *numWorkers
		runBenchmarkSuite(ctx, client, keys, buffers, readBufs, *numKeys, totalGB, *runs,
			func(ctx context.Context, c *redis.Client, k []string, b [][]byte) (time.Duration, error) {
				return benchMultiSet(ctx, c, k, b, nw)
			},
			func(ctx context.Context, c *redis.Client, k []string, rb [][]byte) (time.Duration, error) {
				return benchMultiGet(ctx, c, k, rb, nw)
			},
			func(ctx context.Context, c *redis.Client, k []string) (time.Duration, int, error) {
				return benchMultiExists(ctx, c, k, nw)
			},
		)
	}

	// Regular (non-zero-copy) comparison
	if *regular {
		fmt.Printf("\n=== Regular Mode (non-zero-copy, pipeline, %d runs) ===\n", *runs)
		runBenchmarkSuite(ctx, client, keys, buffers, readBufs, *numKeys, totalGB, *runs,
			benchRegularSet, benchRegularGet, benchPipelineExists)
	}

	// Cleanup
	cleanupKeys(ctx, client, keys)

	fmt.Println(strings(60, '-'))
	fmt.Println("All tests passed")
}

func strings(n int, ch byte) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = ch
	}
	return string(b)
}

// prepareReadBufs allocates numKeys buffers of chunkBytes each in parallel.
// make() zero-fills the memory, which forces page faults up front so the
// pages are already resident before the timed GET path runs.
func prepareReadBufs(numKeys, chunkBytes int) [][]byte {
	bufs := make([][]byte, numKeys)

	workers := runtime.GOMAXPROCS(0)
	if workers > numKeys {
		workers = numKeys
	}
	if workers < 1 {
		workers = 1
	}

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(id int) {
			defer wg.Done()
			for i := id; i < numKeys; i += workers {
				bufs[i] = make([]byte, chunkBytes)
			}
		}(w)
	}
	wg.Wait()
	return bufs
}

func prepareData(numKeys, chunkBytes int) ([]string, [][]byte) {
	keys := make([]string, numKeys)
	buffers := make([][]byte, numKeys)

	workers := runtime.GOMAXPROCS(0)
	if workers > numKeys {
		workers = numKeys
	}
	if workers < 1 {
		workers = 1
	}

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(id int) {
			defer wg.Done()
			for i := id; i < numKeys; i += workers {
				keys[i] = fmt.Sprintf("bench:key:%d", i)
				buf := make([]byte, chunkBytes)
				for j := 0; j < chunkBytes; j++ {
					buf[j] = byte((i + j) % 256)
				}
				buffers[i] = buf
			}
		}(w)
	}
	wg.Wait()
	return keys, buffers
}

func warmup(ctx context.Context, client *redis.Client, chunkBytes int) {
	buf := make([]byte, chunkBytes)
	for i := range buf {
		buf[i] = byte(i % 256)
	}
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("bench:warmup:%d", i)
		pipe := client.Pipeline()
		pipe.SetFromBuffer(ctx, key, buf)
		readBuf := make([]byte, chunkBytes)
		pipe.GetToBuffer(ctx, key, readBuf)
		pipe.Del(ctx, key)
		pipe.Exec(ctx)
	}
}

// prewarmPool forces n pooled connections through dial + HELLO/AUTH before
// the timed region. It issues n concurrent PINGs held on a release gate so
// they cannot share the same pool connection.
func prewarmPool(ctx context.Context, client *redis.Client, n int) {
	if n <= 0 {
		return
	}
	var done sync.WaitGroup
	done.Add(n)
	release := make(chan struct{})
	for i := 0; i < n; i++ {
		go func() {
			defer done.Done()
			<-release
			if err := client.Ping(ctx).Err(); err != nil {
				log.Printf("prewarm ping: %v", err)
			}
		}()
	}
	close(release)
	done.Wait()
}

// cpuTime holds user and sys CPU time in nanoseconds.
// Implemented per-platform in tune_linux.go / tune_other.go.
type cpuTime struct {
	user int64
	sys  int64
}

// --- stats helpers ---

func summarize(samples []float64) (minV, median, mean, stddev float64) {
	if len(samples) == 0 {
		return 0, 0, 0, 0
	}
	sorted := append([]float64(nil), samples...)
	sort.Float64s(sorted)
	minV = sorted[0]
	median = sorted[len(sorted)/2]
	if len(sorted)%2 == 0 {
		median = 0.5 * (sorted[len(sorted)/2-1] + sorted[len(sorted)/2])
	}
	var sum float64
	for _, v := range sorted {
		sum += v
	}
	mean = sum / float64(len(sorted))
	if len(sorted) > 1 {
		var sq float64
		for _, v := range sorted {
			d := v - mean
			sq += d * d
		}
		stddev = math.Sqrt(sq / float64(len(sorted)-1))
	}
	return
}

func printStats(label, unit string, samples []float64) {
	if len(samples) == 1 {
		fmt.Printf("%s %8.2f %s\n", label, samples[0], unit)
		return
	}
	minV, median, mean, stddev := summarize(samples)
	fmt.Printf("%s min=%.2f median=%.2f mean=%.2f stddev=%.2f %s (n=%d)\n",
		label, minV, median, mean, stddev, unit, len(samples))
}

type setFunc func(ctx context.Context, client *redis.Client, keys []string, buffers [][]byte) (time.Duration, error)
type getFunc func(ctx context.Context, client *redis.Client, keys []string, readBufs [][]byte) (time.Duration, error)
type existsFunc func(ctx context.Context, client *redis.Client, keys []string) (time.Duration, int, error)

func runBenchmarkSuite(
	ctx context.Context,
	client *redis.Client,
	keys []string,
	buffers [][]byte,
	readBufs [][]byte,
	numKeys int,
	totalGB float64,
	runs int,
	setFn setFunc,
	getFn getFunc,
	existsFn existsFunc,
) {
	if runs < 1 {
		runs = 1
	}
	setThroughputs := make([]float64, 0, runs)
	getThroughputs := make([]float64, 0, runs)
	existsOps := make([]float64, 0, runs)

	// Snapshot memstats + CPU time around the whole suite for one-shot
	// reporting (GC pauses, CPU utilization).
	var memBefore, memAfter runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	cpuBefore := readCPUTime()
	wallStart := time.Now()

	for r := 0; r < runs; r++ {
		// Batch SET
		elapsed, err := setFn(ctx, client, keys, buffers)
		if err != nil {
			log.Fatalf("Batch SET failed (run %d): %v", r+1, err)
		}
		setThroughputs = append(setThroughputs, totalGB/elapsed.Seconds())

		// Batch GET
		elapsed, err = getFn(ctx, client, keys, readBufs)
		if err != nil {
			log.Fatalf("Batch GET failed (run %d): %v", r+1, err)
		}
		getThroughputs = append(getThroughputs, totalGB/elapsed.Seconds())

		// Verify only on the first run — data is deterministic.
		if r == 0 {
			if err := verifyData(readBufs, buffers); err != nil {
				log.Fatalf("Data verification failed: %v", err)
			}
		}

		// Batch EXISTS
		elapsed, _, err = existsFn(ctx, client, keys)
		if err != nil {
			log.Fatalf("Batch EXISTS failed (run %d): %v", r+1, err)
		}
		existsOps = append(existsOps, float64(numKeys)/elapsed.Seconds())
	}

	wallElapsed := time.Since(wallStart)
	cpuAfter := readCPUTime()
	runtime.ReadMemStats(&memAfter)

	printStats("Batch SET:   ", "GB/s ", setThroughputs)
	printStats("Batch GET:   ", "GB/s ", getThroughputs)
	printStats("Batch EXISTS:", "ops/s", existsOps)

	// Suite-level resource usage
	gcCount := memAfter.NumGC - memBefore.NumGC
	gcPauseMs := float64(memAfter.PauseTotalNs-memBefore.PauseTotalNs) / 1e6
	allocMB := float64(memAfter.TotalAlloc-memBefore.TotalAlloc) / (1024 * 1024)
	userMs := float64(cpuAfter.user-cpuBefore.user) / 1e6
	sysMs := float64(cpuAfter.sys-cpuBefore.sys) / 1e6
	cpuPct := 100.0 * (userMs + sysMs) / float64(wallElapsed.Milliseconds())
	fmt.Printf("Resources: wall=%.2fs cpu=user+sys=%.2fs (%.0f%%), GC=%d pauses=%.2fms, alloc=%.1f MiB\n",
		wallElapsed.Seconds(), (userMs+sysMs)/1000, cpuPct, gcCount, gcPauseMs, allocMB)
}

// --- Pipeline mode ---

func benchPipelineSet(ctx context.Context, client *redis.Client, keys []string, buffers [][]byte) (time.Duration, error) {
	start := time.Now()
	pipe := client.Pipeline()
	for i, key := range keys {
		pipe.SetFromBuffer(ctx, key, buffers[i])
	}
	_, err := pipe.Exec(ctx)
	return time.Since(start), err
}

func benchPipelineGet(ctx context.Context, client *redis.Client, keys []string, readBufs [][]byte) (time.Duration, error) {
	cmds := make([]*redis.ZeroCopyStringCmd, len(keys))
	// Reset slice lengths to full capacity so GetToBuffer has space to write.
	for i := range readBufs {
		readBufs[i] = readBufs[i][:cap(readBufs[i])]
	}

	start := time.Now()
	pipe := client.Pipeline()
	for i, key := range keys {
		cmds[i] = pipe.GetToBuffer(ctx, key, readBufs[i])
	}
	_, err := pipe.Exec(ctx)
	elapsed := time.Since(start)

	if err != nil {
		return elapsed, err
	}

	// Trim buffers to actual size
	for i, cmd := range cmds {
		n := cmd.Val()
		readBufs[i] = readBufs[i][:n]
	}

	return elapsed, nil
}

func benchPipelineExists(ctx context.Context, client *redis.Client, keys []string) (time.Duration, int, error) {
	start := time.Now()
	pipe := client.Pipeline()
	cmds := make([]*redis.IntCmd, len(keys))
	for i, key := range keys {
		cmds[i] = pipe.Exists(ctx, key)
	}
	_, err := pipe.Exec(ctx)
	elapsed := time.Since(start)

	if err != nil {
		return elapsed, 0, err
	}

	hits := 0
	for _, cmd := range cmds {
		hits += int(cmd.Val())
	}
	return elapsed, hits, nil
}

// --- Multi-goroutine mode ---

func shardKeys(total, numWorkers int) [][2]int {
	shards := make([][2]int, numWorkers)
	perWorker := total / numWorkers
	remainder := total % numWorkers
	offset := 0
	for i := 0; i < numWorkers; i++ {
		size := perWorker
		if i < remainder {
			size++
		}
		shards[i] = [2]int{offset, offset + size}
		offset += size
	}
	return shards
}

func benchMultiSet(ctx context.Context, client *redis.Client, keys []string, buffers [][]byte, numWorkers int) (time.Duration, error) {
	shards := shardKeys(len(keys), numWorkers)
	errs := make([]error, numWorkers)
	var wg sync.WaitGroup

	start := time.Now()
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(id int) {
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()
			defer wg.Done()
			lo, hi := shards[id][0], shards[id][1]
			pipe := client.Pipeline()
			for i := lo; i < hi; i++ {
				pipe.SetFromBuffer(ctx, keys[i], buffers[i])
			}
			_, errs[id] = pipe.Exec(ctx)
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	for _, e := range errs {
		if e != nil {
			return elapsed, e
		}
	}
	return elapsed, nil
}

func benchMultiGet(ctx context.Context, client *redis.Client, keys []string, readBufs [][]byte, numWorkers int) (time.Duration, error) {
	shards := shardKeys(len(keys), numWorkers)
	cmdSlices := make([][]*redis.ZeroCopyStringCmd, numWorkers)
	errs := make([]error, numWorkers)
	var wg sync.WaitGroup

	// Reset slice lengths to full capacity so GetToBuffer has space to write.
	for i := range readBufs {
		readBufs[i] = readBufs[i][:cap(readBufs[i])]
	}

	start := time.Now()
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(id int) {
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()
			defer wg.Done()
			lo, hi := shards[id][0], shards[id][1]
			pipe := client.Pipeline()
			cmds := make([]*redis.ZeroCopyStringCmd, hi-lo)
			for i := lo; i < hi; i++ {
				cmds[i-lo] = pipe.GetToBuffer(ctx, keys[i], readBufs[i])
			}
			_, errs[id] = pipe.Exec(ctx)
			cmdSlices[id] = cmds
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	for _, e := range errs {
		if e != nil {
			return elapsed, e
		}
	}

	// Trim buffers to actual size
	for w := 0; w < numWorkers; w++ {
		lo := shards[w][0]
		for j, cmd := range cmdSlices[w] {
			n := cmd.Val()
			readBufs[lo+j] = readBufs[lo+j][:n]
		}
	}

	return elapsed, nil
}

func benchMultiExists(ctx context.Context, client *redis.Client, keys []string, numWorkers int) (time.Duration, int, error) {
	shards := shardKeys(len(keys), numWorkers)
	hitCounts := make([]int, numWorkers)
	errs := make([]error, numWorkers)
	var wg sync.WaitGroup

	start := time.Now()
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(id int) {
			runtime.LockOSThread()
			defer runtime.UnlockOSThread()
			defer wg.Done()
			lo, hi := shards[id][0], shards[id][1]
			pipe := client.Pipeline()
			cmds := make([]*redis.IntCmd, hi-lo)
			for i := lo; i < hi; i++ {
				cmds[i-lo] = pipe.Exists(ctx, keys[i])
			}
			_, errs[id] = pipe.Exec(ctx)
			for _, cmd := range cmds {
				hitCounts[id] += int(cmd.Val())
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	for _, e := range errs {
		if e != nil {
			return elapsed, 0, e
		}
	}

	total := 0
	for _, h := range hitCounts {
		total += h
	}
	return elapsed, total, nil
}

// --- Regular (non-zero-copy) mode ---

func benchRegularSet(ctx context.Context, client *redis.Client, keys []string, buffers [][]byte) (time.Duration, error) {
	start := time.Now()
	pipe := client.Pipeline()
	for i, key := range keys {
		pipe.Set(ctx, key, buffers[i], 0)
	}
	_, err := pipe.Exec(ctx)
	return time.Since(start), err
}

// benchRegularGet cannot reuse the caller-provided readBufs because the
// regular Get API allocates its own []byte per response; we overwrite each
// slot with cmd.Bytes() so verifyData still sees the returned payloads.
func benchRegularGet(ctx context.Context, client *redis.Client, keys []string, readBufs [][]byte) (time.Duration, error) {
	start := time.Now()
	pipe := client.Pipeline()
	cmds := make([]*redis.StringCmd, len(keys))
	for i, key := range keys {
		cmds[i] = pipe.Get(ctx, key)
	}
	_, err := pipe.Exec(ctx)
	elapsed := time.Since(start)

	if err != nil {
		return elapsed, err
	}

	for i, cmd := range cmds {
		data, err := cmd.Bytes()
		if err != nil {
			return elapsed, fmt.Errorf("GET key %d: %w", i, err)
		}
		readBufs[i] = data
	}

	return elapsed, nil
}

// --- Helpers ---

func verifyData(readBufs, writeBufs [][]byte) error {
	for i := range readBufs {
		if !bytes.Equal(readBufs[i], writeBufs[i]) {
			for j := range writeBufs[i] {
				if j >= len(readBufs[i]) || readBufs[i][j] != writeBufs[i][j] {
					return fmt.Errorf("data mismatch: key %d, byte %d: expected %d, got %d",
						i, j, writeBufs[i][j], readBufs[i][j])
				}
			}
		}
	}
	return nil
}

func cleanupKeys(ctx context.Context, client *redis.Client, keys []string) {
	const batchSize = 1000
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		pipe := client.Pipeline()
		for _, key := range keys[i:end] {
			pipe.Del(ctx, key)
		}
		pipe.Exec(ctx)
	}
}
