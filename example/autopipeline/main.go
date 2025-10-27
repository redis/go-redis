package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	fmt.Println("=== Redis AutoPipeline Examples ===\n")

	// Example 1: Basic autopipelining
	example1BasicAutoPipeline()

	// Example 2: Concurrent autopipelining
	example2ConcurrentAutoPipeline()

	// Example 3: Custom configuration
	example3CustomConfig()

	// Example 4: Performance comparison
	example4PerformanceComparison()
}

// Example 1: Basic autopipelining usage
func example1BasicAutoPipeline() {
	fmt.Println("Example 1: Basic AutoPipeline Usage")
	fmt.Println("------------------------------------")

	ctx := context.Background()

	// Create client with autopipelining enabled
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
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

	fmt.Println("✓ Successfully set 100 keys using autopipelining\n")
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
			FlushInterval:        10 * time.Millisecond,
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
			// Or flush every 5ms, whichever comes first
			FlushInterval: 5 * time.Millisecond,
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
	const numCommands = 1000

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

	// Test 3: AutoPipeline
	client3 := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		AutoPipelineConfig: redis.DefaultAutoPipelineConfig(),
	})
	defer client3.Close()

	ap := client3.AutoPipeline()
	defer ap.Close()

	start = time.Now()
	var wg3 sync.WaitGroup
	for i := 0; i < numCommands; i++ {
		wg3.Add(1)
		go func(idx int) {
			defer wg3.Done()
			key := fmt.Sprintf("perf:auto:%d", idx)
			ap.Do(ctx, "SET", key, idx)
		}(i)
	}
	wg3.Wait()
	autoTime := time.Since(start)

	// Results
	fmt.Printf("Executing %d SET commands:\n", numCommands)
	fmt.Printf("  Individual commands: %v (%.0f ops/sec)\n", individualTime, float64(numCommands)/individualTime.Seconds())
	fmt.Printf("  Manual pipeline:     %v (%.0f ops/sec) - %.1fx faster\n", manualTime, float64(numCommands)/manualTime.Seconds(), float64(individualTime)/float64(manualTime))
	fmt.Printf("  AutoPipeline:        %v (%.0f ops/sec) - %.1fx faster\n", autoTime, float64(numCommands)/autoTime.Seconds(), float64(individualTime)/float64(autoTime))
	fmt.Println()

	// Cleanup
	client1.Del(ctx, "perf:*")
}

