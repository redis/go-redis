package redis_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestClusterAutoPipelineBasic(t *testing.T) {
	ctx := context.Background()

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{":7000", ":7001", ":7002"},
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

	// Queue commands concurrently across different keys (different slots)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", idx)
			cmd := ap.Do(ctx, "SET", key, idx)
			// Wait for command to complete
			_ = cmd.Err()
		}(i)
	}

	// Wait for all commands to complete
	wg.Wait()

	// Verify all keys were set correctly
	for i := 0; i < 100; i++ {
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

func TestClusterAutoPipelineConcurrency(t *testing.T) {
	ctx := context.Background()

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{":7000", ":7001", ":7002"},
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxBatchSize:         50,
			FlushInterval:        10 * time.Millisecond,
			MaxConcurrentBatches: 10,
		},
	})
	defer client.Close()

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	ap := client.AutoPipeline()
	defer ap.Close()

	const numGoroutines = 10
	const commandsPerGoroutine = 100

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < commandsPerGoroutine; i++ {
				key := fmt.Sprintf("g%d:key%d", goroutineID, i)
				cmd := ap.Do(ctx, "SET", key, i)
				// Wait for command to complete
				_ = cmd.Err()
			}
		}(g)
	}

	// Wait for all commands to complete
	wg.Wait()

	// Verify all commands were executed
	for g := 0; g < numGoroutines; g++ {
		for i := 0; i < commandsPerGoroutine; i++ {
			key := fmt.Sprintf("g%d:key%d", g, i)
			val, err := client.Get(ctx, key).Int()
			if err != nil {
				t.Fatalf("Failed to get %s: %v", key, err)
			}
			if val != i {
				t.Fatalf("Expected %d, got %d for key %s", i, val, key)
			}
		}
	}
}

func TestClusterAutoPipelineCrossSlot(t *testing.T) {
	ctx := context.Background()

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{":7000", ":7001", ":7002"},
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxBatchSize:         20,
			FlushInterval:        10 * time.Millisecond,
			MaxConcurrentBatches: 5,
		},
	})
	defer client.Close()

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	ap := client.AutoPipeline()
	defer ap.Close()

	// Use keys that will hash to different slots
	keys := []string{
		"user:1000",
		"user:2000",
		"user:3000",
		"session:abc",
		"session:def",
		"cache:foo",
		"cache:bar",
	}

	var wg sync.WaitGroup
	for i, key := range keys {
		wg.Add(1)
		go func(k string, val int) {
			defer wg.Done()
			cmd := ap.Do(ctx, "SET", k, val)
			// Wait for command to complete
			_ = cmd.Err()
		}(key, i)
	}

	wg.Wait()

	// Verify all keys were set
	for i, key := range keys {
		val, err := client.Get(ctx, key).Int()
		if err != nil {
			t.Fatalf("Failed to get %s: %v", key, err)
		}
		if val != i {
			t.Fatalf("Expected %d, got %d for key %s", i, val, key)
		}
	}
}

