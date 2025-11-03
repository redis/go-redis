package redis_test

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// ExampleAutoPipeliner_adaptiveDelay demonstrates using adaptive delay
// to automatically adjust batching behavior based on load.
func ExampleAutoPipeliner_adaptiveDelay() {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxBatchSize:         100,
			MaxConcurrentBatches: 10,
			MaxFlushDelay:        100 * time.Microsecond,
			AdaptiveDelay:        true, // Enable adaptive delay
		},
	})
	defer client.Close()

	ctx := context.Background()

	// The autopipeliner will automatically adjust delays:
	// - When queue is ≥75% full: No delay (flush immediately)
	// - When queue is ≥50% full: 25μs delay
	// - When queue is ≥25% full: 50μs delay
	// - When queue is <25% full: 100μs delay (maximize batching)

	// Low load scenario - commands will batch with longer delays
	for i := 0; i < 10; i++ {
		_ = client.Set(ctx, fmt.Sprintf("key:%d", i), i, 0)
		time.Sleep(10 * time.Millisecond) // Slow rate
	}

	// High load scenario - commands will flush immediately when queue fills
	for i := 0; i < 200; i++ {
		_ = client.Set(ctx, fmt.Sprintf("key:%d", i), i, 0)
		// No sleep - high rate, queue fills up quickly
	}

	fmt.Println("Adaptive delay automatically adjusted to load patterns")
	// Output: Adaptive delay automatically adjusted to load patterns
}

// ExampleAutoPipeliner_fixedDelay demonstrates using a fixed delay
// for predictable batching behavior.
func ExampleAutoPipeliner_fixedDelay() {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxBatchSize:         100,
			MaxConcurrentBatches: 10,
			MaxFlushDelay:        100 * time.Microsecond,
			AdaptiveDelay:        false, // Use fixed delay
		},
	})
	defer client.Close()

	ctx := context.Background()

	// With fixed delay, the autopipeliner always waits 100μs
	// between flushes, regardless of queue fill level
	for i := 0; i < 100; i++ {
		_ = client.Set(ctx, fmt.Sprintf("key:%d", i), i, 0)
	}

	fmt.Println("Fixed delay provides predictable batching")
	// Output: Fixed delay provides predictable batching
}

// ExampleAutoPipeliner_noDelay demonstrates zero-delay configuration
// for lowest latency at the cost of higher CPU usage.
func ExampleAutoPipeliner_noDelay() {
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxBatchSize:         100,
			MaxConcurrentBatches: 10,
			MaxFlushDelay:        0, // No delay
			AdaptiveDelay:        false,
		},
	})
	defer client.Close()

	ctx := context.Background()

	// With zero delay, the autopipeliner flushes as fast as possible
	// This provides lowest latency but higher CPU usage
	for i := 0; i < 100; i++ {
		_ = client.Set(ctx, fmt.Sprintf("key:%d", i), i, 0)
	}

	fmt.Println("Zero delay provides lowest latency")
	// Output: Zero delay provides lowest latency
}

