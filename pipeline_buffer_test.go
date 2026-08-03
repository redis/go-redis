package redis_test

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

// skipWithoutRedis skips the test when no server answers at apTestAddr(), so a
// unit-only run doesn't turn missing integration infrastructure into a
// failure. It probes with a DEFAULT-options client, not the test's configured
// one: probing with the client under test would convert a regression in the
// very options being exercised (buffer sizes, pipeline pool) into a silent
// skip while the server is up.
func skipWithoutRedis(t *testing.T, ctx context.Context, _ *redis.Client) {
	t.Helper()
	probe := redis.NewClient(&redis.Options{Addr: apTestAddr()})
	defer probe.Close()
	if err := probe.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis at %s: %v", apTestAddr(), err)
	}
}

// TestPipelineBufferSizes verifies that enabling the pipeline buffer options keeps
// pipelining working end-to-end. It does not assert the actual socket buffer sizes
// (not observable here) - only that the configured client still runs pipelines.
func TestPipelineBufferSizes(t *testing.T) {
	ctx := context.Background()

	// Create client with custom pipeline buffer sizes
	client := redis.NewClient(&redis.Options{
		Addr:                    apTestAddr(),
		ReadBufferSize:          64 * 1024,  // 64 KiB for regular connections
		WriteBufferSize:         64 * 1024,  // 64 KiB for regular connections
		PipelineReadBufferSize:  512 * 1024, // 512 KiB for pipeline connections
		PipelineWriteBufferSize: 512 * 1024, // 512 KiB for pipeline connections
		PipelinePoolSize:        5,          // Small pool for pipelining
	})
	defer client.Close()
	skipWithoutRedis(t, ctx, client)
	cleanupBufferTestKeys(t, client)

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

// TestNoPipelinePool verifies that client works without pipeline pool (backward compatibility)
func TestNoPipelinePool(t *testing.T) {
	ctx := context.Background()

	// Create client WITHOUT custom pipeline buffer sizes
	client := redis.NewClient(&redis.Options{
		Addr:            apTestAddr(),
		ReadBufferSize:  64 * 1024, // 64 KiB for all connections
		WriteBufferSize: 64 * 1024, // 64 KiB for all connections
		// No PipelineReadBufferSize or PipelineWriteBufferSize
	})
	defer client.Close()
	skipWithoutRedis(t, ctx, client)
	cleanupBufferTestKeys(t, client)

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
		Addr:                    apTestAddr(),
		ReadBufferSize:          64 * 1024,  // 64 KiB for regular connections
		WriteBufferSize:         64 * 1024,  // 64 KiB for regular connections
		PipelineReadBufferSize:  512 * 1024, // 512 KiB for pipeline connections
		PipelineWriteBufferSize: 512 * 1024, // 512 KiB for pipeline connections
		PipelinePoolSize:        5,          // Small pool for pipelining
	})
	defer client.Close()
	skipWithoutRedis(t, ctx, client)
	cleanupBufferTestKeys(t, client)

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
		Addr:            apTestAddr(),
		ReadBufferSize:  64 * 1024, // 64 KiB for all connections
		WriteBufferSize: 64 * 1024, // 64 KiB for all connections
	})
	defer client.Close()
	skipWithoutRedis(t, ctx, client)
	cleanupBufferTestKeys(t, client)

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

// cleanupBufferTestKeys removes this file's fixed test keys before and after a
// test. These tests share a Redis instance with the rest of the package (and
// "test_key" is also used by autopipeline_test.go), so leaving state behind
// makes results order-dependent — the same class of leak that produced a real
// CI flake in the search suite (Copilot review on #3942).
//
// Call it AFTER skipWithoutRedis: it talks to the server, so running it first
// turns a no-Redis environment into a hard failure instead of the intended
// skip (also Copilot, on the first cut of this cleanup).
func cleanupBufferTestKeys(t *testing.T, client *redis.Client) {
	t.Helper()
	keys := []string{
		"test_key", "stats_key",
		"pipe_key1", "pipe_key2",
		"no_pipe_pool_key1", "no_pipe_pool_key2",
	}
	ctx := context.Background()
	if err := client.Del(ctx, keys...).Err(); err != nil {
		t.Fatalf("cleanup keys: %v", err)
	}
	// The post-test sweep gets its OWN client: t.Cleanup runs after the test
	// function's defers, and every caller here defers client.Close(), so a
	// cleanup issued on the passed-in client would land on a closed one and
	// silently do nothing — leaving exactly the keys this helper exists to
	// remove (Copilot review on #3942).
	t.Cleanup(func() {
		c := redis.NewClient(&redis.Options{Addr: apTestAddr()})
		defer c.Close()
		if err := c.Del(context.Background(), keys...).Err(); err != nil {
			t.Errorf("post-test key cleanup failed, later tests may see stale keys: %v", err)
		}
	})
}
