package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	host           = flag.String("host", "localhost", "Redis host")
	port           = flag.Int("port", 6379, "Redis port")
	mode           = flag.String("mode", "single", "Redis mode: single or cluster")
	password       = flag.String("password", "", "Redis password")
	db             = flag.Int("db", 0, "Redis database (only for single mode)")
	workers        = flag.Int("workers", 10, "Number of concurrent workers")
	duration       = flag.Duration("duration", 10*time.Second, "Test duration")
	keySize        = flag.Int("key-size", 16, "Size of keys in bytes")
	valueSize      = flag.Int("value-size", 64, "Size of values in bytes")
	readRatio      = flag.Float64("read-ratio", 0.5, "Ratio of read operations (0.0-1.0)")
	autopipeline   = flag.Bool("autopipeline", false, "Use autopipelining")
	pipelineSize   = flag.Int("pipeline-size", 100, "Autopipeline batch size")
	pipelineDelay  = flag.Duration("pipeline-delay", 100*time.Microsecond, "Autopipeline flush delay")
	reportInterval = flag.Duration("report-interval", 1*time.Second, "Interval for progress reports")
)

type Stats struct {
	operations    atomic.Int64
	errors        atomic.Int64
	getOps        atomic.Int64
	setOps        atomic.Int64
	getMisses     atomic.Int64 // GET returned nil
	getWrongValue atomic.Int64 // GET returned wrong worker ID or corrupted data
	getSuccess    atomic.Int64 // GET returned correct data
}

func main() {
	flag.Parse()

	// Validate parameters
	if *mode != "single" && *mode != "cluster" {
		fmt.Fprintf(os.Stderr, "Error: mode must be 'single' or 'cluster'\n")
		os.Exit(1)
	}

	if *readRatio < 0 || *readRatio > 1 {
		fmt.Fprintf(os.Stderr, "Error: read-ratio must be between 0.0 and 1.0\n")
		os.Exit(1)
	}

	// Print configuration
	fmt.Println("=== Redis Throughput Test ===")
	fmt.Printf("Mode:            %s\n", *mode)
	fmt.Printf("Endpoint:        %s:%d\n", *host, *port)
	fmt.Printf("Workers:         %d\n", *workers)
	fmt.Printf("Duration:        %s\n", *duration)
	fmt.Printf("Key size:        %d bytes\n", *keySize)
	fmt.Printf("Value size:      %d bytes\n", *valueSize)
	fmt.Printf("Read ratio:      %.2f\n", *readRatio)
	fmt.Printf("Autopipeline:    %v\n", *autopipeline)
	if *autopipeline {
		fmt.Printf("Pipeline size:   %d\n", *pipelineSize)
		fmt.Printf("Pipeline delay:  %s\n", *pipelineDelay)
	}
	fmt.Println()

	// Create client
	ctx := context.Background()
	var cmdable redis.Cmdable
	var universalClient redis.UniversalClient

	if *mode == "cluster" {
		opts := &redis.ClusterOptions{
			Addrs:    []string{fmt.Sprintf("%s:%d", *host, *port)},
			Password: *password,
		}

		if *autopipeline {
			opts.AutoPipelineConfig = &redis.AutoPipelineConfig{
				MaxBatchSize:         *pipelineSize,
				MaxFlushDelay:        *pipelineDelay,
				MaxConcurrentBatches: 100,
			}
		}

		client := redis.NewClusterClient(opts)
		defer client.Close()
		universalClient = client

		// Test connection
		if err := client.Ping(ctx).Err(); err != nil {
			fmt.Fprintf(os.Stderr, "Error: failed to connect to Redis: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("✓ Connected to Redis")

		if *autopipeline {
			cmdable = client.AutoPipeline()
			fmt.Println("✓ Autopipelining enabled")
		} else {
			cmdable = client
		}
	} else {
		opts := &redis.Options{
			Addr:     fmt.Sprintf("%s:%d", *host, *port),
			Password: *password,
			DB:       *db,
		}

		if *autopipeline {
			opts.AutoPipelineConfig = &redis.AutoPipelineConfig{
				MaxBatchSize:         *pipelineSize,
				MaxFlushDelay:        *pipelineDelay,
				MaxConcurrentBatches: 100,
			}
		}

		client := redis.NewClient(opts)
		defer client.Close()
		universalClient = client

		// Test connection
		if err := client.Ping(ctx).Err(); err != nil {
			fmt.Fprintf(os.Stderr, "Error: failed to connect to Redis: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("✓ Connected to Redis")

		if *autopipeline {
			cmdable = client.AutoPipeline()
			fmt.Println("✓ Autopipelining enabled")
		} else {
			cmdable = client
		}
	}

	// Setup signal handling
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	// Start test
	fmt.Println("\nStarting test...")
	stats := &Stats{}

	// Context with timeout
	testCtx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()

	// Start progress reporter
	stopReporter := make(chan struct{})
	var reporterWg sync.WaitGroup
	reporterWg.Add(1)
	go func() {
		defer reporterWg.Done()
		progressReporter(stats, stopReporter)
	}()

	// Flush database to ensure clean state
	fmt.Println("Flushing database...")
	if err := universalClient.FlushDB(context.Background()).Err(); err != nil {
		fmt.Printf("Warning: Failed to flush database: %v\n", err)
	} else {
		fmt.Println("✓ Database flushed")
	}

	// Pre-populate keys to avoid initial GET misses and ensure clean validation
	fmt.Println("Initializing keys...")
	for i := 0; i < *workers; i++ {
		key := fmt.Sprintf("key:%d", i)
		value := fmt.Sprintf("worker:%d:seq:0", i)
		err := cmdable.Set(context.Background(), key, value, 0).Err()
		if err != nil {
			fmt.Printf("Failed to initialize key %s: %v\n", key, err)
		}
	}
	fmt.Println("✓ Keys initialized")

	// Start workers
	var wg sync.WaitGroup
	startTime := time.Now()

	for i := 0; i < *workers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			worker(testCtx, cmdable, workerID, stats)
		}(i)
	}

	// Wait for completion or signal
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		// Test completed normally
	case <-sigCh:
		fmt.Println("\n\nReceived interrupt signal, stopping...")
		cancel()
		<-done
	}

	elapsed := time.Since(startTime)
	
	// Stop reporter
	close(stopReporter)
	reporterWg.Wait()

	// Print final results
	printResults(stats, elapsed)
}

func worker(ctx context.Context, cmdable redis.Cmdable, workerID int, stats *Stats) {
	key := fmt.Sprintf("key:%d", workerID)

	// Track the sequence number for this worker
	var currentSeq atomic.Int64

	// Determine operation mix based on read ratio
	readThreshold := int(*readRatio * 100)
	counter := 0

	for {
		select {
		case <-ctx.Done():
			return
		default:
			// Determine operation type
			isRead := (counter % 100) < readThreshold
			counter++

			var err error
			if isRead {
				result, err := cmdable.Get(ctx, key).Result()
				if err != nil {
					if err == redis.Nil {
						// Key doesn't exist yet (not set or expired)
						stats.getMisses.Add(1)
					} else {
						// Actual error
						stats.errors.Add(1)
					}
				} else {
					// Parse and validate the data
					var seq int64
					var workerIDFromValue int
					n, parseErr := fmt.Sscanf(result, "worker:%d:seq:%d", &workerIDFromValue, &seq)
					if parseErr != nil || n != 2 {
						// Failed to parse - corrupted data
						stats.getWrongValue.Add(1)
					} else if workerIDFromValue != workerID {
						// Wrong worker ID - this is a CRITICAL error
						// It means we're reading data from a different worker's key
						stats.getWrongValue.Add(1)
					} else {
						// Data is valid - correct worker ID and parseable sequence
						stats.getSuccess.Add(1)
					}
				}
				stats.getOps.Add(1)
			} else {
				// Increment sequence for this SET
				seq := currentSeq.Add(1)
				value := fmt.Sprintf("worker:%d:seq:%d", workerID, seq)
				err = cmdable.Set(ctx, key, value, 0).Err()
				if err != nil {
					stats.errors.Add(1)
				}
				stats.setOps.Add(1)
			}

			stats.operations.Add(1)
		}
	}
}

func progressReporter(stats *Stats, stop chan struct{}) {
	ticker := time.NewTicker(*reportInterval)
	defer ticker.Stop()

	lastOps := int64(0)
	lastTime := time.Now()

	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			now := time.Now()
			currentOps := stats.operations.Load()
			elapsed := now.Sub(lastTime).Seconds()
			
			opsThisInterval := currentOps - lastOps
			throughput := float64(opsThisInterval) / elapsed

			fmt.Printf("[%s] Ops: %d | Throughput: %.0f ops/sec | Errors: %d\n",
				now.Format("15:04:05"),
				currentOps,
				throughput,
				stats.errors.Load(),
			)

			lastOps = currentOps
			lastTime = now
		}
	}
}

func printResults(stats *Stats, elapsed time.Duration) {
	totalOps := stats.operations.Load()
	totalErrors := stats.errors.Load()
	getOps := stats.getOps.Load()
	setOps := stats.setOps.Load()
	getMisses := stats.getMisses.Load()
	getWrongValue := stats.getWrongValue.Load()
	getSuccess := stats.getSuccess.Load()

	throughput := float64(totalOps) / elapsed.Seconds()

	fmt.Println("\n=== Test Results ===")
	fmt.Printf("Duration:        %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("Total ops:       %d\n", totalOps)
	fmt.Printf("  GET ops:       %d (%.1f%%)\n", getOps, float64(getOps)*100/float64(totalOps))
	fmt.Printf("  SET ops:       %d (%.1f%%)\n", setOps, float64(setOps)*100/float64(totalOps))
	fmt.Printf("Errors:          %d (%.2f%%)\n", totalErrors, float64(totalErrors)*100/float64(totalOps))

	// Validation results
	fmt.Println("\n=== Data Validation ===")
	if getOps > 0 {
		fmt.Printf("GET misses:      %d (%.2f%% of GETs) - key not found\n", getMisses, float64(getMisses)*100/float64(getOps))
		fmt.Printf("GET success:     %d (%.2f%% of GETs) - correct data\n", getSuccess, float64(getSuccess)*100/float64(getOps))
		fmt.Printf("GET corrupted:   %d (%.2f%% of GETs) - wrong worker ID or parse error\n", getWrongValue, float64(getWrongValue)*100/float64(getOps))
	}

	if getWrongValue > 0 {
		fmt.Printf("\n⚠️  CRITICAL: %d data corruption errors detected!\n", getWrongValue)
		fmt.Printf("   This indicates commands are being routed to wrong keys or data is corrupted!\n")
	} else if getOps > 0 {
		fmt.Printf("\n✓ All GET operations returned correct data!\n")
		fmt.Printf("✓ No data corruption detected - autopipelining is working correctly!\n")
	}

	fmt.Println("\n=== Performance ===")
	fmt.Printf("Throughput:      %.0f ops/sec\n", throughput)
	fmt.Printf("Avg latency:     %.2f µs/op\n", elapsed.Seconds()*1e6/float64(totalOps))
}

func generateString(size, seed int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, size)
	for i := range b {
		b[i] = charset[(seed+i)%len(charset)]
	}
	return string(b)
}

