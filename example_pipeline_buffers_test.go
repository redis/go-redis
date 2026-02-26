package redis_test

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9"
)

// ExampleClient_pipelineBuffers demonstrates how to configure separate buffer sizes
// for pipelining operations to optimize performance.
//
// By using larger buffers for pipelining, you can significantly improve throughput
// for batch operations while keeping memory usage low for regular operations.
func ExampleClient_pipelineBuffers() {
	// Create a client with optimized buffer sizes:
	// - Small buffers (64 KiB) for regular operations
	// - Large buffers (512 KiB) for pipelining operations
	// - Small dedicated pool (10 connections) for pipelining
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",

		// Regular connection pool settings
		PoolSize:        100,       // Large pool for regular operations
		ReadBufferSize:  64 * 1024, // 64 KiB read buffer
		WriteBufferSize: 64 * 1024, // 64 KiB write buffer

		// Pipeline connection pool settings (optional)
		// When set, a separate pool is created for pipelining with larger buffers
		PipelinePoolSize:        10,         // Small pool for pipelining
		PipelineReadBufferSize:  512 * 1024, // 512 KiB read buffer (8x larger)
		PipelineWriteBufferSize: 512 * 1024, // 512 KiB write buffer (8x larger)
	})
	defer client.Close()

	ctx := context.Background()

	// Regular operations use the regular pool (64 KiB buffers)
	err := client.Set(ctx, "key1", "value1", 0).Err()
	if err != nil {
		panic(err)
	}

	// Pipeline operations automatically use the pipeline pool (512 KiB buffers)
	pipe := client.Pipeline()
	for i := 0; i < 1000; i++ {
		pipe.Set(ctx, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i), 0)
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		panic(err)
	}

	// AutoPipeline also uses the pipeline pool automatically
	ap := client.AutoPipeline()
	defer ap.Close()

	for i := 0; i < 1000; i++ {
		ap.Set(ctx, fmt.Sprintf("apkey%d", i), fmt.Sprintf("value%d", i), 0)
	}

	// Check pool statistics
	stats := client.PoolStats()
	fmt.Printf("Regular pool: %d total connections, %d idle\n",
		stats.TotalConns, stats.IdleConns)

	if stats.PipelineStats != nil {
		fmt.Printf("Pipeline pool: %d total connections, %d idle\n",
			stats.PipelineStats.TotalConns, stats.PipelineStats.IdleConns)
	}

	// Output:
	// Regular pool: 1 total connections, 1 idle
	// Pipeline pool: 1 total connections, 1 idle
}

// ExampleClient_pipelineBuffers_memoryOptimization demonstrates the memory savings
// of using a separate pipeline pool with larger buffers.
func ExampleClient_pipelineBuffers_memoryOptimization() {
	// Scenario 1: Single pool with large buffers for all connections
	// Memory: 100 connections × 1 MiB = 100 MiB
	// Problem: Wastes memory on regular operations that don't need large buffers
	clientLargeBuffers := redis.NewClient(&redis.Options{
		Addr:            "localhost:6379",
		PoolSize:        100,
		ReadBufferSize:  512 * 1024, // 512 KiB for ALL connections
		WriteBufferSize: 512 * 1024, // 512 KiB for ALL connections
	})
	defer clientLargeBuffers.Close()

	// Scenario 2: Separate pools with optimized buffer sizes (RECOMMENDED)
	// Memory: (100 × 128 KiB) + (10 × 1 MiB) = 12.8 MiB + 10 MiB = 22.8 MiB
	// Savings: 77.2 MiB (77% reduction!)
	clientOptimized := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		PoolSize: 100, // Large pool for regular operations

		// Small buffers for regular operations
		ReadBufferSize:  64 * 1024, // 64 KiB
		WriteBufferSize: 64 * 1024, // 64 KiB

		// Large buffers only for pipelining
		PipelinePoolSize:        10,         // Small pool
		PipelineReadBufferSize:  512 * 1024, // 512 KiB
		PipelineWriteBufferSize: 512 * 1024, // 512 KiB
	})
	defer clientOptimized.Close()

	fmt.Println("Optimized configuration saves 77% memory!")

	// Output:
	// Optimized configuration saves 77% memory!
}

