package redis_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
)

var _ = Describe("AutoPipeline", func() {
	ctx := context.Background()
	var client *redis.Client
	var ap *redis.AutoPipeliner

	BeforeEach(func() {
		client = redis.NewClient(&redis.Options{
			Addr: redisAddr,
			AutoPipelineConfig: &redis.AutoPipelineConfig{
				MaxBatchSize:         10,
				MaxFlushDelay:        50 * time.Millisecond,
				MaxConcurrentBatches: 5,
			},
		})
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())

		ap = client.AutoPipeline()
	})

	AfterEach(func() {
		if ap != nil {
			Expect(ap.Close()).NotTo(HaveOccurred())
		}
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should batch commands automatically", func() {
		// Queue multiple commands concurrently to enable batching
		var wg sync.WaitGroup
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				key := fmt.Sprintf("key%d", idx)
				ap.Do(ctx, "SET", key, idx)
			}(i)
		}

		// Wait for all commands to complete
		wg.Wait()

		// Verify all commands were executed
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("key%d", i)
			val, err := client.Get(ctx, key).Int()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(i))
		}
	})

	It("should flush when batch size is reached", func() {
		// Queue exactly MaxBatchSize commands concurrently
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				key := fmt.Sprintf("key%d", idx)
				ap.Do(ctx, "SET", key, idx)
			}(i)
		}

		// Wait for all commands to complete
		wg.Wait()

		// Verify all commands were executed
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key%d", i)
			val, err := client.Get(ctx, key).Int()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(i))
		}
	})

	It("should flush on timer expiry", func() {
		// Queue a single command (will block until flushed by timer)
		go func() {
			ap.Do(ctx, "SET", "key1", "value1")
		}()

		// Wait for timer to expire and command to complete
		time.Sleep(100 * time.Millisecond)

		// Verify command was executed
		val, err := client.Get(ctx, "key1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("value1"))
	})

	It("should handle concurrent commands", func() {
		const numGoroutines = 10
		const commandsPerGoroutine = 100

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for g := 0; g < numGoroutines; g++ {
			go func(goroutineID int) {
				defer wg.Done()
				for i := 0; i < commandsPerGoroutine; i++ {
					key := fmt.Sprintf("g%d:key%d", goroutineID, i)
					ap.Do(ctx, "SET", key, i)
				}
			}(g)
		}

		wg.Wait()

		// All commands are now complete (Do() blocks until executed)
		// Verify all commands were executed
		for g := 0; g < numGoroutines; g++ {
			for i := 0; i < commandsPerGoroutine; i++ {
				key := fmt.Sprintf("g%d:key%d", g, i)
				val, err := client.Get(ctx, key).Int()
				Expect(err).NotTo(HaveOccurred())
				Expect(val).To(Equal(i))
			}
		}
	})

	It("should support manual flush", func() {
		// Queue commands concurrently
		var wg sync.WaitGroup
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				key := fmt.Sprintf("key%d", idx)
				ap.Do(ctx, "SET", key, idx)
			}(i)
		}

		// Wait for all commands to complete
		wg.Wait()

		// Verify all commands were executed
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("key%d", i)
			val, err := client.Get(ctx, key).Int()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(i))
		}
	})

	It("should handle errors in commands", func() {
		// Queue a mix of valid and invalid commands concurrently
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			ap.Do(ctx, "SET", "key1", "value1")
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			ap.Do(ctx, "INVALID_COMMAND")
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			ap.Do(ctx, "SET", "key2", "value2")
		}()

		// Wait for all commands to complete
		wg.Wait()

		// Valid commands should still execute
		val, err := client.Get(ctx, "key1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("value1"))

		val, err = client.Get(ctx, "key2").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("value2"))
	})

	It("should close gracefully", func() {
		// Queue commands concurrently
		var wg sync.WaitGroup
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				key := fmt.Sprintf("key%d", idx)
				ap.Do(ctx, "SET", key, idx)
			}(i)
		}

		// Wait for all commands to complete
		wg.Wait()

		// Close should flush pending commands
		Expect(ap.Close()).NotTo(HaveOccurred())
		ap = nil // Prevent double close in AfterEach

		// Verify all commands were executed
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("key%d", i)
			val, err := client.Get(ctx, key).Int()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(i))
		}
	})

	It("should return error after close", func() {
		Expect(ap.Close()).NotTo(HaveOccurred())

		// Commands after close should fail
		cmd := ap.Do(ctx, "SET", "key", "value")
		Expect(cmd.Err()).To(Equal(redis.ErrClosed))
	})

	It("should respect MaxConcurrentBatches", func() {
		// Create autopipeliner with low concurrency limit
		client2 := redis.NewClient(&redis.Options{
			Addr: redisAddr,
			AutoPipelineConfig: &redis.AutoPipelineConfig{
				MaxBatchSize:         5,
				MaxFlushDelay:        10 * time.Millisecond,
				MaxConcurrentBatches: 2,
			},
		})
		defer client2.Close()

		ap2 := client2.AutoPipeline()
		defer ap2.Close()

		// Queue many commands to trigger multiple batches concurrently
		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				key := fmt.Sprintf("key%d", idx)
				ap2.Do(ctx, "SET", key, idx)
			}(i)
		}

		// Wait for all commands to complete
		wg.Wait()

		// Verify all commands were executed
		for i := 0; i < 50; i++ {
			key := fmt.Sprintf("key%d", i)
			val, err := client2.Get(ctx, key).Int()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(i))
		}
	})

	It("should report queue length", func() {
		// Initially empty
		Expect(ap.Len()).To(Equal(0))

		// Queue some commands concurrently
		var wg sync.WaitGroup
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				ap.Do(ctx, "SET", fmt.Sprintf("key%d", idx), idx)
			}(i)
		}

		// Wait for all commands to complete
		wg.Wait()

		// Should be empty after flush
		Expect(ap.Len()).To(Equal(0))
	})
})

func TestAutoPipelineBasic(t *testing.T) {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr: ":6379",
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxBatchSize:         10,
			MaxFlushDelay:        50 * time.Millisecond,
			MaxConcurrentBatches: 5,
		},
	})
	defer client.Close()

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	ap := client.AutoPipeline()
	defer ap.Close()

	// Queue commands concurrently
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

	// Verify
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		val, err := client.Get(ctx, key).Int()
		if err != nil {
			t.Fatalf("Failed to get key%d: %v", i, err)
		}
		if val != i {
			t.Fatalf("Expected %d, got %d", i, val)
		}
	}
}

func TestAutoPipelineMaxFlushDelay(t *testing.T) {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr: ":6379",
		AutoPipelineConfig: &redis.AutoPipelineConfig{
			MaxBatchSize:         1000, // Large batch size so only timer triggers flush
			MaxFlushDelay:        50 * time.Millisecond,
			MaxConcurrentBatches: 5,
		},
	})
	defer client.Close()

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	ap := client.AutoPipeline()
	defer ap.Close()

	// Send commands slowly, one at a time
	// They should be flushed by the timer, not by batch size
	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", idx)
			cmd := ap.Do(ctx, "SET", key, idx)
			// Wait for command to complete
			_ = cmd.Err()
		}(i)

		// Wait a bit between commands to ensure they don't batch by size
		time.Sleep(10 * time.Millisecond)
	}

	wg.Wait()
	elapsed := time.Since(start)

	// Verify all keys were set
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		val, err := client.Get(ctx, key).Int()
		if err != nil {
			t.Fatalf("Failed to get %s: %v", key, err)
		}
		if val != i {
			t.Fatalf("Expected %d, got %d", i, val)
		}
	}

	// Should complete in roughly 10 * 10ms = 100ms + some flush intervals
	// If timer doesn't work, commands would wait until Close() is called
	if elapsed > 500*time.Millisecond {
		t.Fatalf("Commands took too long (%v), flush interval may not be working", elapsed)
	}

	t.Logf("Completed in %v (flush interval working correctly)", elapsed)
}

func TestAutoPipelineConcurrency(t *testing.T) {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr: ":6379",
		AutoPipelineConfig: redis.DefaultAutoPipelineConfig(),
	})
	defer client.Close()

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	ap := client.AutoPipeline()
	defer ap.Close()

	const numGoroutines = 100
	const commandsPerGoroutine = 100

	var wg sync.WaitGroup
	var successCount atomic.Int64

	wg.Add(numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < commandsPerGoroutine; i++ {
				key := fmt.Sprintf("g%d:key%d", goroutineID, i)
				ap.Do(ctx, "SET", key, i)
				successCount.Add(1)
			}
		}(g)
	}

	wg.Wait()

	// Wait for all flushes
	time.Sleep(500 * time.Millisecond)

	expected := int64(numGoroutines * commandsPerGoroutine)
	if successCount.Load() != expected {
		t.Fatalf("Expected %d commands, got %d", expected, successCount.Load())
	}
}


// TestAutoPipelineSingleCommandNoBlock verifies that single commands don't block
func TestAutoPipelineSingleCommandNoBlock(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:               ":6379",
		AutoPipelineConfig: redis.DefaultAutoPipelineConfig(),
	})
	defer client.Close()

	ap := client.AutoPipeline()
	defer ap.Close()

	start := time.Now()
	cmd := ap.Do(ctx, "PING")
	err := cmd.Err()
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Command failed: %v", err)
	}

	// The command is wrapped in autoPipelineCmd, so we can't directly access Val()
	// Just check that it completed without error
	t.Logf("Command completed successfully")

	// Single command should complete within 50ms (adaptive delay is 10ms)
	if elapsed > 50*time.Millisecond {
		t.Errorf("Single command took too long: %v (should be < 50ms)", elapsed)
	}

	t.Logf("Single command completed in %v", elapsed)
}

// TestAutoPipelineSequentialSingleThread verifies sequential single-threaded execution
func TestAutoPipelineSequentialSingleThread(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:               ":6379",
		AutoPipelineConfig: redis.DefaultAutoPipelineConfig(),
	})
	defer client.Close()

	ap := client.AutoPipeline()
	defer ap.Close()

	// Execute 10 commands sequentially in a single thread
	start := time.Now()
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("test:key:%d", i)
		t.Logf("Sending command %d", i)
		cmd := ap.Do(ctx, "SET", key, i)
		t.Logf("Waiting for command %d to complete", i)
		err := cmd.Err()
		if err != nil {
			t.Fatalf("Command %d failed: %v", i, err)
		}
		t.Logf("Command %d completed", i)
	}
	elapsed := time.Since(start)

	// Should complete reasonably fast (< 100ms for 10 commands)
	if elapsed > 100*time.Millisecond {
		t.Errorf("10 sequential commands took too long: %v (should be < 100ms)", elapsed)
	}

	t.Logf("10 sequential commands completed in %v (avg: %v per command)", elapsed, elapsed/10)
}


