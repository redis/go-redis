package redis_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestAutoPipelineSequential(t *testing.T) {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr: ":6379",
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxBatchSize:         10,
			FlushInterval:        50 * time.Millisecond,
			MaxConcurrentBatches: 5,
		},
	})
	defer client.Close()

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	ap := client.AutoPipeline()
	defer ap.Close()

	// Sequential usage - no goroutines needed!
	// Commands will be queued and batched automatically
	cmds := make([]redis.Cmder, 20)
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key%d", i)
		cmds[i] = ap.Do(ctx, "SET", key, i)
	}

	// Now access results - this will block until commands execute
	for i, cmd := range cmds {
		if err := cmd.Err(); err != nil {
			t.Fatalf("Command %d failed: %v", i, err)
		}
	}

	// Verify all keys were set
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key%d", i)
		val, err := client.Get(ctx, key).Int()
		if err != nil {
			t.Fatalf("Failed to get %s: %v", key, err)
		}
		if val != i {
			t.Fatalf("Expected %d, got %d", i, val)
		}
	}
}

func TestAutoPipelineSequentialSmallBatches(t *testing.T) {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr: ":6379",
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxBatchSize:         1000, // Large batch size
			FlushInterval:        20 * time.Millisecond, // Rely on timer
			MaxConcurrentBatches: 5,
		},
	})
	defer client.Close()

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	ap := client.AutoPipeline()
	defer ap.Close()

	// Queue commands sequentially with small delays
	// They should be flushed by timer, not batch size
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		cmd := ap.Do(ctx, "SET", key, i)
		
		// Access result immediately - should block until flush
		if err := cmd.Err(); err != nil {
			t.Fatalf("Command %d failed: %v", i, err)
		}
		
		time.Sleep(5 * time.Millisecond)
	}

	// Verify all keys were set
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		val, err := client.Get(ctx, key).Int()
		if err != nil {
			t.Fatalf("Failed to get %s: %v", key, err)
		}
		if val != i {
			t.Fatalf("Expected %d, got %d", i, val)
		}
	}
}

func TestAutoPipelineSequentialMixed(t *testing.T) {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr: ":6379",
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxBatchSize:         5,
			FlushInterval:        50 * time.Millisecond,
			MaxConcurrentBatches: 5,
		},
	})
	defer client.Close()

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	ap := client.AutoPipeline()
	defer ap.Close()

	// Queue some commands
	cmd1 := ap.Do(ctx, "SET", "key1", "value1")
	cmd2 := ap.Do(ctx, "SET", "key2", "value2")
	cmd3 := ap.Do(ctx, "SET", "key3", "value3")

	// Access first result - should trigger batch flush
	if err := cmd1.Err(); err != nil {
		t.Fatalf("cmd1 failed: %v", err)
	}

	// Other commands in same batch should also be done
	if err := cmd2.Err(); err != nil {
		t.Fatalf("cmd2 failed: %v", err)
	}
	if err := cmd3.Err(); err != nil {
		t.Fatalf("cmd3 failed: %v", err)
	}

	// Verify
	val, err := client.Get(ctx, "key1").Result()
	if err != nil || val != "value1" {
		t.Fatalf("key1: expected value1, got %v, err %v", val, err)
	}
}

