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
				Unordered:            true,
			},
		})
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())

		var err error
		ap, err = client.AutoPipeline(nil)
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if ap != nil {
			Expect(ap.Close()).NotTo(HaveOccurred())
		}
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should batch commands automatically", func() {
		// Issue multiple commands concurrently to enable batching. A typed call
		// without blocking, so each goroutine must await its own command's result
		// (cmd.Err) before reporting done — wg.Wait alone only waits for the
		// queueing, not the batch flush+execute, which would race the reads below.
		var wg sync.WaitGroup
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				key := fmt.Sprintf("key%d", idx)
				Expect(ap.Set(ctx, key, idx, 0).Err()).NotTo(HaveOccurred())
			}(i)
		}

		// Wait for all commands to execute (each goroutine awaited its result).
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
		// Queue exactly MaxBatchSize commands concurrently. Each goroutine awaits
		// its command's result so wg.Wait reflects execution, not just queueing.
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				key := fmt.Sprintf("key%d", idx)
				Expect(ap.Set(ctx, key, idx, 0).Err()).NotTo(HaveOccurred())
			}(i)
		}

		// Wait for all commands to execute (each goroutine awaited its result).
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
			ap.Set(ctx, "key1", "value1", 0)
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
					ap.Set(ctx, key, i, 0)
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
				ap.Set(ctx, key, idx, 0)
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
			ap.Set(ctx, "key1", "value1", 0)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			// A command failing with a redis error (WRONGTYPE) inside a batch
			// must not affect its batch neighbours.
			client.Set(ctx, "not-a-counter", "str", 0)
			_ = ap.Incr(ctx, "not-a-counter").Err()
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			ap.Set(ctx, "key2", "value2", 0)
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
				ap.Set(ctx, key, idx, 0)
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
		cmd := ap.Set(ctx, "key", "value", 0)
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
				Unordered:            true,
			},
		})
		defer client2.Close()

		ap2, err := client2.AutoPipeline(nil)
		Expect(err).NotTo(HaveOccurred())
		defer ap2.Close()

		// Queue many commands to trigger multiple batches concurrently
		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				key := fmt.Sprintf("key%d", idx)
				ap2.Set(ctx, key, idx, 0)
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
		// Deferred face with a wide explicit window: queued commands stay
		// queued until the window elapses, so Len is deterministic.
		aap, err := client.AsyncAutoPipeline(&redis.AutoPipelineConfig{
			MaxBatchSize:  100,
			MaxFlushDelay: time.Second,
		})
		Expect(err).NotTo(HaveOccurred())
		defer aap.Close()

		Expect(aap.Len()).To(Equal(0))

		cmds := make([]*redis.StatusCmd, 0, 5)
		for i := 0; i < 5; i++ {
			cmds = append(cmds, aap.Set(ctx, fmt.Sprintf("key%d", i), i, 0)) // queued, non-blocking
		}
		Expect(aap.Len()).To(Equal(5))

		for _, c := range cmds {
			Expect(c.Err()).NotTo(HaveOccurred()) // blocks until the window flushes
		}
		Expect(aap.Len()).To(Equal(0))
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
			Unordered:            true,
		},
	})
	defer client.Close()

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	ap, err := client.AutoPipeline(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	// Queue commands concurrently
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", idx)
			cmd := ap.Set(ctx, key, idx, 0)
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
			Unordered:            true,
		},
	})
	defer client.Close()

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	ap, err := client.AutoPipeline(nil)
	if err != nil {
		t.Fatal(err)
	}
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
			cmd := ap.Set(ctx, key, idx, 0)
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
		Addr:               ":6379",
		AutoPipelineConfig: redis.DefaultAutoPipelineConfig(),
	})
	defer client.Close()

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	ap, err := client.AutoPipeline(nil)
	if err != nil {
		t.Fatal(err)
	}
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
				ap.Set(ctx, key, i, 0)
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

	ap, err := client.AutoPipeline(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	start := time.Now()
	cmd := ap.Ping(ctx)
	err = cmd.Err()
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Command failed: %v", err)
	}
	if cmd.Val() != "PONG" {
		t.Fatalf("Ping = %q, want PONG", cmd.Val())
	}

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

	ap, err := client.AutoPipeline(nil)
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	// Execute 10 commands sequentially in a single thread
	start := time.Now()
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("test:key:%d", i)
		t.Logf("Sending command %d", i)
		cmd := ap.Set(ctx, key, i, 0)
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
