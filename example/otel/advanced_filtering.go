package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/extra/redisotel/v9"
	"github.com/redis/go-redis/v9"
)

func runAdvancedFilteringExamples() {
	ctx := context.Background()

	// Example 1: High-performance O(1) command exclusion
	fmt.Println("=== Example 1: O(1) Command Exclusion ===")
	rdb1 := redis.NewClient(&redis.Options{Addr: ":6379"})

	if err := redisotel.InstrumentTracing(rdb1,
		redisotel.WithExcludedCommands("PING", "INFO", "SELECT")); err != nil {
		panic(err)
	}

	// These commands won't be traced
	rdb1.Ping(ctx)
	rdb1.Info(ctx)

	// This command will be traced
	rdb1.Set(ctx, "key1", "value1", 0)
	fmt.Println("✓ O(1) exclusion configured")

	// Example 2: Custom filtering logic
	fmt.Println("\n=== Example 2: Custom Process Filter ===")
	rdb2 := redis.NewClient(&redis.Options{Addr: ":6379"})

	if err := redisotel.InstrumentTracing(rdb2,
		redisotel.WithProcessFilter(func(cmd redis.Cmder) bool {
			// Exclude commands that start with "DEBUG_" or "INTERNAL_"
			name := strings.ToUpper(cmd.Name())
			return strings.HasPrefix(name, "DEBUG_") ||
				strings.HasPrefix(name, "INTERNAL_")
		})); err != nil {
		panic(err)
	}
	fmt.Println("✓ Custom process filter configured")

	// Example 3: Pipeline filtering
	fmt.Println("\n=== Example 3: Pipeline Filter ===")
	rdb3 := redis.NewClient(&redis.Options{Addr: ":6379"})

	if err := redisotel.InstrumentTracing(rdb3,
		redisotel.WithProcessPipelineFilter(func(cmds []redis.Cmder) bool {
			// Exclude large pipelines (>10 commands) from tracing
			return len(cmds) > 10
		})); err != nil {
		panic(err)
	}

	// Small pipeline - will be traced
	pipe := rdb3.Pipeline()
	pipe.Set(ctx, "key1", "value1", 0)
	pipe.Set(ctx, "key2", "value2", 0)
	pipe.Exec(ctx)
	fmt.Println("✓ Pipeline filter configured")

	// Example 4: Dial filtering
	fmt.Println("\n=== Example 4: Dial Filter ===")
	rdb4 := redis.NewClient(&redis.Options{Addr: ":6379"})

	if err := redisotel.InstrumentTracing(rdb4,
		redisotel.WithDialFilterFunc(func(network, addr string) bool {
			// Don't trace connections to localhost for development
			return strings.Contains(addr, "localhost") ||
				strings.Contains(addr, "127.0.0.1")
		})); err != nil {
		panic(err)
	}
	fmt.Println("✓ Dial filter configured")

	// Example 5: Combined approach for optimal performance
	fmt.Println("\n=== Example 5: Combined Filtering Approach ===")
	rdb5 := redis.NewClient(&redis.Options{Addr: ":6379"})

	if err := redisotel.InstrumentTracing(rdb5,
		// Fast O(1) exclusion for common commands
		redisotel.WithExcludedCommands("PING", "INFO", "SELECT"),
		// Custom logic for additional filtering
		redisotel.WithProcessFilter(func(cmd redis.Cmder) bool {
			return strings.HasPrefix(strings.ToUpper(cmd.Name()), "DEBUG_")
		})); err != nil {
		panic(err)
	}

	// Test the combined approach
	rdb5.Ping(ctx)                   // Excluded by O(1) set
	rdb5.Set(ctx, "key", "value", 0) // Will be traced
	fmt.Println("✓ Combined filtering approach configured")

	// Example 6: Backward compatibility with legacy APIs
	fmt.Println("\n=== Example 6: Legacy API Compatibility ===")
	rdb6 := redis.NewClient(&redis.Options{Addr: ":6379"})

	if err := redisotel.InstrumentTracing(rdb6,
		// Legacy command filter still works
		redisotel.WithCommandFilter(func(cmd redis.Cmder) bool {
			return strings.ToUpper(cmd.Name()) == "AUTH"
		})); err != nil {
		panic(err)
	}
	fmt.Println("✓ Legacy API compatibility maintained")

	fmt.Println("\n=== All filtering examples completed successfully! ===")
}
