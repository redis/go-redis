package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	fmt.Println("=== Redis AutoPipeline Examples ===")
	fmt.Println()

	// Example 1: Basic autopipelining
	example1BasicAutoPipeline()

	// Example 2: Concurrent autopipelining
	example2ConcurrentAutoPipeline()

	// Example 3: Custom configuration
	example3CustomConfig()

	// Example 4: Performance comparison
	example4PerformanceComparison()

	// Example 5: Realistic streaming workload
	example5StreamingWorkload()

	// Example 6: AutoPipeline at its best - concurrent web server simulation
	example6WebServerSimulation()

	// Example 7: Tuned AutoPipeline matching manual pipeline
	example7TunedAutoPipeline()
}

// Example 1: Basic autopipelining usage
func example1BasicAutoPipeline() {
	fmt.Println("Example 1: Basic AutoPipeline Usage")
	fmt.Println("------------------------------------")

	ctx := context.Background()

	// Create client with autopipelining enabled
	client := redis.NewClient(&redis.Options{
		Addr:               "localhost:6379",
		AutoPipelineConfig: redis.DefaultAutoPipelineConfig(),
	})
	defer client.Close()

	// Create autopipeliner
	ap := client.AutoPipeline()
	defer ap.Close()

	// Queue commands concurrently - they will be automatically batched and executed
	// Note: Do() blocks until the command is executed, so use goroutines for batching
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("user:%d", idx)
			ap.Do(ctx, "SET", key, fmt.Sprintf("User %d", idx))
		}(i)
	}

	// Wait for all commands to complete
	wg.Wait()

	// Verify some values
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("user:%d", i)
		val, err := client.Get(ctx, key).Result()
		if err != nil {
			fmt.Printf("Error getting %s: %v\n", key, err)
			continue
		}
		fmt.Printf("  %s = %s\n", key, val)
	}

	fmt.Println("✓ Successfully set 100 keys using autopipelining")
	fmt.Println()
}

// Example 2: Concurrent autopipelining from multiple goroutines
func example2ConcurrentAutoPipeline() {
	fmt.Println("Example 2: Concurrent AutoPipeline")
	fmt.Println("-----------------------------------")

	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxBatchSize:         100,
			MaxConcurrentBatches: 10,
		},
	})
	defer client.Close()

	ap := client.AutoPipeline()
	defer ap.Close()

	// Launch multiple goroutines that all use the same autopipeliner
	const numGoroutines = 10
	const commandsPerGoroutine = 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines)

	start := time.Now()

	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < commandsPerGoroutine; i++ {
				key := fmt.Sprintf("concurrent:g%d:item%d", goroutineID, i)
				ap.Do(ctx, "SET", key, i)
			}
		}(g)
	}

	wg.Wait()

	// Wait for final flush
	time.Sleep(100 * time.Millisecond)

	elapsed := time.Since(start)

	totalCommands := numGoroutines * commandsPerGoroutine
	fmt.Printf("✓ Executed %d commands from %d goroutines in %v\n", totalCommands, numGoroutines, elapsed)
	fmt.Printf("  Throughput: %.0f ops/sec\n\n", float64(totalCommands)/elapsed.Seconds())
}

// Example 3: Custom autopipelining configuration
func example3CustomConfig() {
	fmt.Println("Example 3: Custom Configuration")
	fmt.Println("--------------------------------")

	ctx := context.Background()

	// Create client with custom autopipelining settings
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			// Flush when we have 50 commands
			MaxBatchSize: 50,
			// Allow up to 5 concurrent pipeline executions
			MaxConcurrentBatches: 5,
		},
	})
	defer client.Close()

	ap := client.AutoPipeline()
	defer ap.Close()

	// Queue commands concurrently
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("config:item%d", idx)
			ap.Do(ctx, "SET", key, idx)
		}(i)
	}

	// Wait for all commands to complete
	wg.Wait()

	fmt.Printf("✓ Configured with MaxBatchSize=50, FlushInterval=5ms\n")
	fmt.Printf("  Queue length: %d (should be 0 after flush)\n\n", ap.Len())
}

// Example 4: Performance comparison
func example4PerformanceComparison() {
	fmt.Println("Example 4: Performance Comparison")
	fmt.Println("----------------------------------")

	ctx := context.Background()
	const numCommands = 10000

	// Test 1: Individual commands
	client1 := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client1.Close()

	start := time.Now()
	for i := 0; i < numCommands; i++ {
		key := fmt.Sprintf("perf:individual:%d", i)
		if err := client1.Set(ctx, key, i, 0).Err(); err != nil {
			fmt.Printf("Error: %v\n", err)
			return
		}
	}
	individualTime := time.Since(start)

	// Test 2: Manual pipeline
	client2 := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client2.Close()

	start = time.Now()
	pipe := client2.Pipeline()
	for i := 0; i < numCommands; i++ {
		key := fmt.Sprintf("perf:manual:%d", i)
		pipe.Set(ctx, key, i, 0)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	manualTime := time.Since(start)

	// Test 3: AutoPipeline (with worker pool - OPTIMIZED)
	client3 := redis.NewClient(&redis.Options{
		Addr:             "localhost:6379",
		PipelinePoolSize: 30,
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxConcurrentBatches: 300,
			MaxBatchSize:         200,
		},
	})
	defer client3.Close()

	ap := client3.AutoPipeline()
	defer ap.Close()

	start = time.Now()

	// Use worker pool instead of spawning 10k goroutines
	const numWorkers = 100
	workCh := make(chan int, numWorkers)
	var wg3 sync.WaitGroup

	// Start workers
	for w := 0; w < numWorkers; w++ {
		wg3.Add(1)
		go func() {
			defer wg3.Done()
			for idx := range workCh {
				key := fmt.Sprintf("perf:auto:%d", idx)
				ap.Do(ctx, "SET", key, idx)
			}
		}()
	}

	// Send work to workers
	for i := 0; i < numCommands; i++ {
		workCh <- i
	}
	close(workCh)
	wg3.Wait()
	autoTime := time.Since(start)

	// Test 4: AutoPipeline (UNOPTIMIZED - spawning 10k goroutines)
	client4 := redis.NewClient(&redis.Options{
		Addr:             "localhost:6379",
		PipelinePoolSize: 30,
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxConcurrentBatches: 300,
			MaxBatchSize:         200,
		},
	})
	defer client4.Close()

	ap4 := client4.AutoPipeline()
	defer ap4.Close()

	start = time.Now()
	var wg4 sync.WaitGroup
	for i := 0; i < numCommands; i++ {
		wg4.Add(1)
		go func(idx int) {
			defer wg4.Done()
			key := fmt.Sprintf("perf:auto-unopt:%d", idx)
			ap4.Do(ctx, "SET", key, idx)
		}(i)
	}
	wg4.Wait()
	autoTimeUnopt := time.Since(start)

	// Results
	fmt.Printf("Executing %d SET commands:\n", numCommands)
	fmt.Printf("  Individual commands:        %v (%.0f ops/sec)\n", individualTime, float64(numCommands)/individualTime.Seconds())
	fmt.Printf("  Manual pipeline:            %v (%.0f ops/sec) - %.1fx faster\n", manualTime, float64(numCommands)/manualTime.Seconds(), float64(individualTime)/float64(manualTime))
	fmt.Printf("  AutoPipeline (optimized):   %v (%.0f ops/sec) - %.1fx faster\n", autoTime, float64(numCommands)/autoTime.Seconds(), float64(individualTime)/float64(autoTime))
	fmt.Printf("  AutoPipeline (unoptimized): %v (%.0f ops/sec) - %.1fx faster\n", autoTimeUnopt, float64(numCommands)/autoTimeUnopt.Seconds(), float64(individualTime)/float64(autoTimeUnopt))
	fmt.Println()
	fmt.Println("📊 Analysis:")
	fmt.Println("  • Manual pipeline: Single batch of 10k commands (lowest latency)")
	fmt.Println("  • AutoPipeline: Multiple batches with coordination overhead")
	fmt.Println("  • Worker pool: Reduces goroutine creation but adds channel overhead")
	fmt.Println()
	fmt.Println("💡 When to use AutoPipeline:")
	fmt.Println("  ✓ Commands arrive from multiple sources/goroutines over time")
	fmt.Println("  ✓ You want automatic batching without manual pipeline management")
	fmt.Println("  ✓ Concurrent workloads where commands trickle in continuously")
	fmt.Println()
	fmt.Println("💡 When to use Manual Pipeline:")
	fmt.Println("  ✓ You have all commands ready upfront (like this benchmark)")
	fmt.Println("  ✓ You want absolute maximum throughput for batch operations")
	fmt.Println("  ✓ Single-threaded batch processing")
	fmt.Println()

	// Cleanup
	client1.Del(ctx, "perf:*")
}

// Example 5: Realistic streaming workload - commands arriving over time
func example5StreamingWorkload() {
	fmt.Println("Example 5: Streaming Workload (Realistic Use Case)")
	fmt.Println("---------------------------------------------------")

	ctx := context.Background()
	const totalCommands = 10000
	const arrivalRateMicros = 10 // Commands arrive every 10 microseconds

	// Test 1: Individual commands with streaming arrival
	client1 := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client1.Close()

	start := time.Now()
	ticker := time.NewTicker(time.Duration(arrivalRateMicros) * time.Microsecond)
	defer ticker.Stop()

	count := 0
	for range ticker.C {
		if count >= totalCommands {
			break
		}
		key := fmt.Sprintf("stream:individual:%d", count)
		client1.Set(ctx, key, count, 0)
		count++
	}
	individualStreamTime := time.Since(start)

	// Test 2: AutoPipeline with streaming arrival
	client2 := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxBatchSize:         100,
			MaxConcurrentBatches: 50,
			MaxFlushDelay:        0, // Flush immediately when batch is ready
		},
	})
	defer client2.Close()

	ap := client2.AutoPipeline()
	defer ap.Close()

	start = time.Now()
	ticker2 := time.NewTicker(time.Duration(arrivalRateMicros) * time.Microsecond)
	defer ticker2.Stop()

	var wg sync.WaitGroup
	count = 0
	for range ticker2.C {
		if count >= totalCommands {
			break
		}
		wg.Add(1)
		idx := count
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("stream:auto:%d", idx)
			ap.Do(ctx, "SET", key, idx)
		}()
		count++
	}
	wg.Wait()
	autoStreamTime := time.Since(start)

	// Results
	fmt.Printf("Streaming %d commands (1 every %dµs):\n", totalCommands, arrivalRateMicros)
	fmt.Printf("  Individual commands: %v (%.0f ops/sec)\n", individualStreamTime, float64(totalCommands)/individualStreamTime.Seconds())
	fmt.Printf("  AutoPipeline:        %v (%.0f ops/sec) - %.1fx faster\n", autoStreamTime, float64(totalCommands)/autoStreamTime.Seconds(), float64(individualStreamTime)/float64(autoStreamTime))
	fmt.Println()
	fmt.Println("💡 In streaming workloads, AutoPipeline automatically batches commands")
	fmt.Println("   as they arrive, providing better throughput without manual batching.")
	fmt.Println()
}

// Example 6: Web server simulation - AutoPipeline's ideal use case
func example6WebServerSimulation() {
	fmt.Println("Example 6: Web Server Simulation (AutoPipeline's Sweet Spot)")
	fmt.Println("-------------------------------------------------------------")

	ctx := context.Background()
	const numRequests = 10000
	const numConcurrentUsers = 100

	// Simulate web server handling concurrent requests
	// Each request needs to increment a counter and set user data

	// Test 1: Individual commands (typical naive approach)
	client1 := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client1.Close()

	start := time.Now()
	var wg1 sync.WaitGroup
	for i := 0; i < numRequests; i++ {
		wg1.Add(1)
		go func(reqID int) {
			defer wg1.Done()
			// Simulate web request: increment counter + set session data
			client1.Incr(ctx, "requests:total")
			client1.Set(ctx, fmt.Sprintf("session:%d", reqID), fmt.Sprintf("data-%d", reqID), 0)
		}(i)
	}
	wg1.Wait()
	individualWebTime := time.Since(start)

	// Test 2: AutoPipeline (automatic batching)
	client2 := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxBatchSize:         200,
			MaxConcurrentBatches: 100,
		},
	})
	defer client2.Close()

	ap := client2.AutoPipeline()
	defer ap.Close()

	start = time.Now()
	var wg2 sync.WaitGroup
	for i := 0; i < numRequests; i++ {
		wg2.Add(1)
		go func(reqID int) {
			defer wg2.Done()
			// Same operations, but automatically batched
			ap.Incr(ctx, "requests:total:auto")
			ap.Set(ctx, fmt.Sprintf("session:auto:%d", reqID), fmt.Sprintf("data-%d", reqID), 0)
		}(i)
	}
	wg2.Wait()
	autoWebTime := time.Since(start)

	// Results
	totalOps := numRequests * 2 // 2 operations per request
	fmt.Printf("Simulating %d concurrent web requests (%d total operations):\n", numRequests, totalOps)
	fmt.Printf("  Individual commands: %v (%.0f ops/sec)\n", individualWebTime, float64(totalOps)/individualWebTime.Seconds())
	fmt.Printf("  AutoPipeline:        %v (%.0f ops/sec) - %.1fx faster\n", autoWebTime, float64(totalOps)/autoWebTime.Seconds(), float64(individualWebTime)/float64(autoWebTime))
	fmt.Println()
	fmt.Println("🎯 This is AutoPipeline's ideal scenario:")
	fmt.Println("   • Multiple concurrent goroutines (simulating web requests)")
	fmt.Println("   • Commands arriving continuously over time")
	fmt.Println("   • No manual pipeline management needed")
	fmt.Println("   • Automatic batching provides massive speedup")
	fmt.Println()
}

// Example 7: Tuned AutoPipeline to match manual pipeline performance
func example7TunedAutoPipeline() {
	fmt.Println("Example 7: Tuned AutoPipeline Configuration")
	fmt.Println("--------------------------------------------")

	ctx := context.Background()
	const numCommands = 10000

	// Test 1: Manual pipeline (baseline)
	client1 := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client1.Close()

	start := time.Now()
	pipe := client1.Pipeline()
	for i := 0; i < numCommands; i++ {
		key := fmt.Sprintf("tuned:manual:%d", i)
		pipe.Set(ctx, key, i, 0)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	manualTime := time.Since(start)

	// Test 2: AutoPipeline with default config
	client2 := redis.NewClient(&redis.Options{
		Addr:               "localhost:6379",
		AutoPipelineConfig: redis.DefaultAutoPipelineConfig(),
	})
	defer client2.Close()

	ap2 := client2.AutoPipeline()
	defer ap2.Close()

	start = time.Now()
	var wg2 sync.WaitGroup
	for i := 0; i < numCommands; i++ {
		wg2.Add(1)
		go func(idx int) {
			defer wg2.Done()
			key := fmt.Sprintf("tuned:auto-default:%d", idx)
			ap2.Do(ctx, "SET", key, idx)
		}(i)
	}
	wg2.Wait()
	autoDefaultTime := time.Since(start)

	// Test 3: AutoPipeline with tuned config (large batches)
	client3 := redis.NewClient(&redis.Options{
		Addr:             "localhost:6379",
		PipelinePoolSize: 50,
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxBatchSize:         2000, // Much larger batches
			MaxConcurrentBatches: 50,   // More parallelism
			MaxFlushDelay:        0,    // No delay
		},
	})
	defer client3.Close()

	ap3 := client3.AutoPipeline()
	defer ap3.Close()

	start = time.Now()
	var wg3 sync.WaitGroup
	for i := 0; i < numCommands; i++ {
		wg3.Add(1)
		go func(idx int) {
			defer wg3.Done()
			key := fmt.Sprintf("tuned:auto-tuned:%d", idx)
			ap3.Do(ctx, "SET", key, idx)
		}(i)
	}
	wg3.Wait()
	autoTunedTime := time.Since(start)

	// Test 4: AutoPipeline with extreme tuning (single batch)
	client4 := redis.NewClient(&redis.Options{
		Addr:                    "localhost:6379",
		PipelinePoolSize:        10,
		PipelineReadBufferSize:  1024 * 1024, // 1 MiB
		PipelineWriteBufferSize: 1024 * 1024, // 1 MiB
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxBatchSize:         10000, // Single batch like manual pipeline
			MaxConcurrentBatches: 10,
			MaxFlushDelay:        0,
		},
	})
	defer client4.Close()

	ap4 := client4.AutoPipeline()
	defer ap4.Close()

	start = time.Now()
	var wg4 sync.WaitGroup
	for i := 0; i < numCommands; i++ {
		wg4.Add(1)
		go func(idx int) {
			defer wg4.Done()
			key := fmt.Sprintf("tuned:auto-extreme:%d", idx)
			ap4.Do(ctx, "SET", key, idx)
		}(i)
	}
	wg4.Wait()
	autoExtremeTime := time.Since(start)

	// Results
	fmt.Printf("Executing %d SET commands:\n", numCommands)
	fmt.Printf("  Manual pipeline:              %v (%.0f ops/sec) [baseline]\n", manualTime, float64(numCommands)/manualTime.Seconds())
	fmt.Printf("  AutoPipeline (default):       %v (%.0f ops/sec) - %.1fx slower\n", autoDefaultTime, float64(numCommands)/autoDefaultTime.Seconds(), float64(autoDefaultTime)/float64(manualTime))
	fmt.Printf("  AutoPipeline (tuned):         %v (%.0f ops/sec) - %.1fx slower\n", autoTunedTime, float64(numCommands)/autoTunedTime.Seconds(), float64(autoTunedTime)/float64(manualTime))
	fmt.Printf("  AutoPipeline (extreme):       %v (%.0f ops/sec) - %.1fx slower\n", autoExtremeTime, float64(numCommands)/autoExtremeTime.Seconds(), float64(autoExtremeTime)/float64(manualTime))
	fmt.Println()
	fmt.Println("📊 Configuration Impact:")
	fmt.Printf("  Default config:  MaxBatchSize=50,   MaxConcurrentBatches=10\n")
	fmt.Printf("  Tuned config:    MaxBatchSize=2000, MaxConcurrentBatches=50\n")
	fmt.Printf("  Extreme config:  MaxBatchSize=10000 (single batch), 1MiB buffers\n")
	fmt.Println()
	fmt.Println("💡 Key Insights:")
	fmt.Println("  • Larger batches reduce the number of round-trips")
	fmt.Println("  • More concurrent batches improve parallelism")
	fmt.Println("  • Larger buffers reduce memory allocations")
	fmt.Println("  • Even with extreme tuning, coordination overhead remains")
	fmt.Println("  • For pure batch workloads, manual pipeline is still optimal")
	fmt.Println()
}
