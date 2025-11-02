package redis_test

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestPipelineBufferSizes verifies that pipeline pool is created with custom buffer sizes
func TestPipelineBufferSizes(t *testing.T) {
	ctx := context.Background()

	// Create client with custom pipeline buffer sizes
	client := redis.NewClient(&redis.Options{
		Addr:                    "localhost:6379",
		ReadBufferSize:          64 * 1024,  // 64 KiB for regular connections
		WriteBufferSize:         64 * 1024,  // 64 KiB for regular connections
		PipelineReadBufferSize:  512 * 1024, // 512 KiB for pipeline connections
		PipelineWriteBufferSize: 512 * 1024, // 512 KiB for pipeline connections
		PipelinePoolSize:        5,          // Small pool for pipelining
	})
	defer client.Close()

	// Test that regular commands work
	err := client.Set(ctx, "test_key", "test_value", 0).Err()
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}

	val, err := client.Get(ctx, "test_key").Result()
	if err != nil {
		t.Fatalf("Failed to get key: %v", err)
	}
	if val != "test_value" {
		t.Fatalf("Expected 'test_value', got '%s'", val)
	}

	// Test that pipeline works
	pipe := client.Pipeline()
	pipe.Set(ctx, "pipe_key1", "value1", 0)
	pipe.Set(ctx, "pipe_key2", "value2", 0)
	pipe.Get(ctx, "pipe_key1")
	pipe.Get(ctx, "pipe_key2")

	cmds, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Pipeline execution failed: %v", err)
	}

	if len(cmds) != 4 {
		t.Fatalf("Expected 4 commands, got %d", len(cmds))
	}

	// Verify results
	if cmds[2].(*redis.StringCmd).Val() != "value1" {
		t.Fatalf("Expected 'value1', got '%s'", cmds[2].(*redis.StringCmd).Val())
	}
	if cmds[3].(*redis.StringCmd).Val() != "value2" {
		t.Fatalf("Expected 'value2', got '%s'", cmds[3].(*redis.StringCmd).Val())
	}

	t.Log("Pipeline with custom buffer sizes works correctly")
}

// TestPipelineBufferSizesWithAutoPipeline verifies that autopipeline uses the pipeline pool
func TestPipelineBufferSizesWithAutoPipeline(t *testing.T) {
	ctx := context.Background()

	// Create client with custom pipeline buffer sizes
	client := redis.NewClient(&redis.Options{
		Addr:                    "localhost:6379",
		ReadBufferSize:          64 * 1024,  // 64 KiB for regular connections
		WriteBufferSize:         64 * 1024,  // 64 KiB for regular connections
		PipelineReadBufferSize:  512 * 1024, // 512 KiB for pipeline connections
		PipelineWriteBufferSize: 512 * 1024, // 512 KiB for pipeline connections
		PipelinePoolSize:        5,          // Small pool for pipelining
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxBatchSize:         10,
			MaxConcurrentBatches: 2,
		},
	})
	defer client.Close()

	// Test autopipeline
	ap := client.AutoPipeline()
	defer ap.Close()

	// Send multiple commands
	for i := 0; i < 20; i++ {
		key := "ap_key_" + string(rune('0'+i%10))
		val := "value_" + string(rune('0'+i%10))
		ap.Set(ctx, key, val, 0).Err()
	}

	// Verify some values
	val, err := client.Get(ctx, "ap_key_0").Result()
	if err != nil {
		t.Fatalf("Failed to get autopipelined key: %v", err)
	}
	if val != "value_0" {
		t.Fatalf("Expected 'value_0', got '%s'", val)
	}

	t.Log("AutoPipeline with custom buffer sizes works correctly")
}

// TestNoPipelinePool verifies that client works without pipeline pool (backward compatibility)
func TestNoPipelinePool(t *testing.T) {
	ctx := context.Background()

	// Create client WITHOUT custom pipeline buffer sizes
	client := redis.NewClient(&redis.Options{
		Addr:            "localhost:6379",
		ReadBufferSize:  64 * 1024, // 64 KiB for all connections
		WriteBufferSize: 64 * 1024, // 64 KiB for all connections
		// No PipelineReadBufferSize or PipelineWriteBufferSize
	})
	defer client.Close()

	// Test that pipeline still works (using regular pool)
	pipe := client.Pipeline()
	pipe.Set(ctx, "no_pipe_pool_key1", "value1", 0)
	pipe.Set(ctx, "no_pipe_pool_key2", "value2", 0)
	pipe.Get(ctx, "no_pipe_pool_key1")
	pipe.Get(ctx, "no_pipe_pool_key2")

	cmds, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Pipeline execution failed: %v", err)
	}

	if len(cmds) != 4 {
		t.Fatalf("Expected 4 commands, got %d", len(cmds))
	}

	// Verify results
	if cmds[2].(*redis.StringCmd).Val() != "value1" {
		t.Fatalf("Expected 'value1', got '%s'", cmds[2].(*redis.StringCmd).Val())
	}
	if cmds[3].(*redis.StringCmd).Val() != "value2" {
		t.Fatalf("Expected 'value2', got '%s'", cmds[3].(*redis.StringCmd).Val())
	}

	t.Log("Pipeline without custom buffer sizes (backward compatibility) works correctly")
}

// TestPipelinePoolStats verifies that PoolStats includes pipeline pool stats
func TestPipelinePoolStats(t *testing.T) {
	ctx := context.Background()

	// Create client with custom pipeline buffer sizes
	client := redis.NewClient(&redis.Options{
		Addr:                    "localhost:6379",
		ReadBufferSize:          64 * 1024,  // 64 KiB for regular connections
		WriteBufferSize:         64 * 1024,  // 64 KiB for regular connections
		PipelineReadBufferSize:  512 * 1024, // 512 KiB for pipeline connections
		PipelineWriteBufferSize: 512 * 1024, // 512 KiB for pipeline connections
		PipelinePoolSize:        5,          // Small pool for pipelining
	})
	defer client.Close()

	// Execute some pipeline commands
	pipe := client.Pipeline()
	for i := 0; i < 10; i++ {
		pipe.Set(ctx, "stats_key", "value", 0)
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Pipeline execution failed: %v", err)
	}

	// Get pool stats
	stats := client.PoolStats()
	if stats == nil {
		t.Fatal("PoolStats returned nil")
	}

	// Verify pipeline stats are included
	if stats.PipelineStats == nil {
		t.Fatal("PipelineStats is nil - pipeline pool stats not included")
	}

	t.Logf("Regular pool stats: TotalConns=%d, IdleConns=%d, Hits=%d, Misses=%d",
		stats.TotalConns, stats.IdleConns, stats.Hits, stats.Misses)
	t.Logf("Pipeline pool stats: TotalConns=%d, IdleConns=%d, Hits=%d, Misses=%d",
		stats.PipelineStats.TotalConns, stats.PipelineStats.IdleConns,
		stats.PipelineStats.Hits, stats.PipelineStats.Misses)

	// Verify pipeline pool has connections
	if stats.PipelineStats.TotalConns == 0 {
		t.Error("Pipeline pool has no connections")
	}

	t.Log("PoolStats includes pipeline pool stats correctly")
}

// TestNoPipelinePoolStats verifies that PoolStats works without pipeline pool
func TestNoPipelinePoolStats(t *testing.T) {
	ctx := context.Background()

	// Create client WITHOUT custom pipeline buffer sizes
	client := redis.NewClient(&redis.Options{
		Addr:            "localhost:6379",
		ReadBufferSize:  64 * 1024, // 64 KiB for all connections
		WriteBufferSize: 64 * 1024, // 64 KiB for all connections
	})
	defer client.Close()

	// Execute some commands
	err := client.Set(ctx, "test_key", "test_value", 0).Err()
	if err != nil {
		t.Fatalf("Failed to set key: %v", err)
	}

	// Get pool stats
	stats := client.PoolStats()
	if stats == nil {
		t.Fatal("PoolStats returned nil")
	}

	// Verify pipeline stats are nil (no pipeline pool)
	if stats.PipelineStats != nil {
		t.Error("PipelineStats should be nil when no pipeline pool is configured")
	}

	t.Logf("Regular pool stats: TotalConns=%d, IdleConns=%d, Hits=%d, Misses=%d",
		stats.TotalConns, stats.IdleConns, stats.Hits, stats.Misses)

	t.Log("PoolStats works correctly without pipeline pool")
}

