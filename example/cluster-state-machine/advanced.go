package main

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// AdvancedMetrics extends basic metrics with more detailed tracking
type AdvancedMetrics struct {
	Metrics

	// Latency buckets (in microseconds)
	latency0_1ms   atomic.Int64 // 0-1ms
	latency1_5ms   atomic.Int64 // 1-5ms
	latency5_10ms  atomic.Int64 // 5-10ms
	latency10_50ms atomic.Int64 // 10-50ms
	latency50ms    atomic.Int64 // >50ms
}

func (am *AdvancedMetrics) recordSuccess(latency time.Duration) {
	am.Metrics.recordSuccess(latency)

	// Record latency bucket
	micros := latency.Microseconds()
	switch {
	case micros < 1000:
		am.latency0_1ms.Add(1)
	case micros < 5000:
		am.latency1_5ms.Add(1)
	case micros < 10000:
		am.latency5_10ms.Add(1)
	case micros < 50000:
		am.latency10_50ms.Add(1)
	default:
		am.latency50ms.Add(1)
	}
}

func (am *AdvancedMetrics) printDetailed() {
	am.Metrics.print()

	total := am.successOps.Load()
	if total > 0 {
		fmt.Printf("\n=== Latency Distribution ===\n")
		fmt.Printf("0-1ms:    %d (%.2f%%)\n", am.latency0_1ms.Load(), float64(am.latency0_1ms.Load())/float64(total)*100)
		fmt.Printf("1-5ms:    %d (%.2f%%)\n", am.latency1_5ms.Load(), float64(am.latency1_5ms.Load())/float64(total)*100)
		fmt.Printf("5-10ms:   %d (%.2f%%)\n", am.latency5_10ms.Load(), float64(am.latency5_10ms.Load())/float64(total)*100)
		fmt.Printf("10-50ms:  %d (%.2f%%)\n", am.latency10_50ms.Load(), float64(am.latency10_50ms.Load())/float64(total)*100)
		fmt.Printf(">50ms:    %d (%.2f%%)\n", am.latency50ms.Load(), float64(am.latency50ms.Load())/float64(total)*100)
	}

}

// runAdvancedExample demonstrates advanced monitoring and potential issues
func runAdvancedExample() {
	ctx := context.Background()

	fmt.Println("\n=== Advanced State Machine Monitoring ===\n")
	fmt.Println("This example includes detailed state machine monitoring")
	fmt.Println("to help identify potential concurrency issues.\n")

	// Test 1: Extreme concurrency with tiny pool
	testExtremeContention(ctx)

	// Test 2: Rapid acquire/release cycles
	testRapidCycles(ctx)

	// Test 3: Long-running operations
	testLongRunningOps(ctx)

	// Test 4: Concurrent pipelines
	testConcurrentPipelines(ctx)

	// Test 5: PubSub + Get/Set pool exhaustion
	testPubSubWithGetSet()
}

func testExtremeContention(ctx context.Context) {
	fmt.Println("Test 1: Extreme Contention")
	fmt.Println("---------------------------")
	fmt.Println("Pool size: 2, Goroutines: 200")
	fmt.Println("This tests the state machine under extreme contention.\n")

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: getRedisAddrs(),
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
		PoolSize:    2, // Extremely small
		PoolTimeout: 1 * time.Second,
	})
	defer client.Close()

	metrics := &AdvancedMetrics{}

	const numGoroutines = 200
	const opsPerGoroutine = 10

	start := time.Now()
	var wg sync.WaitGroup

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("extreme:%d:%d", goroutineID, i)
				value := fmt.Sprintf("v%d", i)

				opStart := time.Now()
				err := client.Set(ctx, key, value, 0).Err()
				latency := time.Since(opStart)

				if err != nil {
					if isPoolTimeout(err) {
						metrics.recordPoolTimeout()
					}
					metrics.recordFailure()
				} else {
					metrics.recordSuccess(latency)
				}
			}
		}(g)
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("✓ Completed in %v\n", elapsed)
	fmt.Printf("  Throughput: %.0f ops/sec\n", float64(numGoroutines*opsPerGoroutine)/elapsed.Seconds())
	metrics.printDetailed()
	printPoolStats(client.PoolStats())
	fmt.Println()
}

func printPoolStats(stats *redis.PoolStats) {
	fmt.Println("===== Pool Stats: =====")
	fmt.Printf("  Hits: %d\n", stats.Hits)
	fmt.Printf("  Misses: %d\n", stats.Misses)
	fmt.Printf("  Timeouts: %d\n", stats.Timeouts)
	fmt.Printf("  TotalConns: %d\n", stats.TotalConns)
	fmt.Printf("  IdleConns: %d\n", stats.IdleConns)
	fmt.Printf("  StaleConns: %d\n", stats.StaleConns)
	fmt.Printf("  WaitCount: %d\n", stats.WaitCount)
	fmt.Printf("  WaitDurationNs: %d\n", stats.WaitDurationNs)
	fmt.Printf("  Unusable: %d\n", stats.Unusable)
	fmt.Printf("  PubSubStats: %+v\n", stats.PubSubStats)
	fmt.Println("===== End Pool Stats: =====")
}

func testRapidCycles(ctx context.Context) {
	fmt.Println("Test 2: Rapid Acquire/Release Cycles")
	fmt.Println("-------------------------------------")
	fmt.Println("Testing rapid connection state transitions.\n")

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: getRedisAddrs(),
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
		PoolSize:     5,
		MaxIdleConns: 1,
		PoolTimeout:  2 * time.Second,
	})
	defer client.Close()

	metrics := &AdvancedMetrics{}

	const numGoroutines = 50
	const opsPerGoroutine = 100

	start := time.Now()
	var wg sync.WaitGroup

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("rapid:%d:%d", goroutineID, i)
				value := "x"

				opStart := time.Now()
				err := client.Set(ctx, key, value, 0).Err()
				latency := time.Since(opStart)

				if err != nil {
					if isPoolTimeout(err) {
						metrics.recordPoolTimeout()
					}
					metrics.recordFailure()
				} else {
					metrics.recordSuccess(latency)
				}

				// No delay - rapid fire
			}
		}(g)
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("✓ Completed in %v\n", elapsed)
	fmt.Printf("  Throughput: %.0f ops/sec\n", float64(numGoroutines*opsPerGoroutine)/elapsed.Seconds())
	metrics.printDetailed()
	printPoolStats(client.PoolStats())
	fmt.Println()
}

func testLongRunningOps(ctx context.Context) {
	fmt.Println("Test 3: Long-Running Operations")
	fmt.Println("--------------------------------")
	fmt.Println("Testing connection holding with slow operations.\n")

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: getRedisAddrs(),
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
		PoolSize:       2,
		MaxIdleConns:   1,
		MaxActiveConns: 5,
		PoolTimeout:    3 * time.Second,
	})
	defer client.Close()

	metrics := &AdvancedMetrics{}

	const numGoroutines = 100
	const opsPerGoroutine = 200

	start := time.Now()
	var wg sync.WaitGroup

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("slow:%d:%d", goroutineID, i)
				value := fmt.Sprintf("data-%d", i)

				opStart := time.Now()

				// Simulate slow operation by doing multiple commands
				pipe := client.Pipeline()
				pipe.Set(ctx, key, value, 0)
				pipe.Get(ctx, key)
				pipe.Incr(ctx, fmt.Sprintf("counter:%d", goroutineID))
				_, err := pipe.Exec(ctx)

				latency := time.Since(opStart)

				if err != nil {
					if isPoolTimeout(err) {
						metrics.recordPoolTimeout()
					}
					metrics.recordFailure()
				} else {
					metrics.recordSuccess(latency)
				}

				// Simulate processing time
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(20)))
			}
		}(g)
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("✓ Completed in %v\n", elapsed)
	fmt.Printf("  Throughput: %.0f ops/sec\n", float64(numGoroutines*opsPerGoroutine)/elapsed.Seconds())
	metrics.printDetailed()
	printPoolStats(client.PoolStats())
	fmt.Println()
}

// testConcurrentPipelines tests pipeline operations under concurrency
func testConcurrentPipelines(ctx context.Context) {
	fmt.Println("Test 4: Concurrent Pipelines")
	fmt.Println("-----------------------------")
	fmt.Println("Testing pipeline operations with connection state machine.\n")

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: getRedisAddrs(),
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
		PoolSize:     10,
		MaxIdleConns: 5,
		MinIdleConns: 5,
		PoolTimeout:  5 * time.Second,
	})
	defer client.Close()

	metrics := &AdvancedMetrics{}

	const numGoroutines = 64
	const pipelinesPerGoroutine = 100
	const commandsPerPipeline = 100

	start := time.Now()
	var wg sync.WaitGroup

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < pipelinesPerGoroutine; i++ {
				opStart := time.Now()

				pipe := client.Pipeline()
				for j := 0; j < commandsPerPipeline; j++ {
					key := fmt.Sprintf("pipe:%d:%d:%d", goroutineID, i, j)
					pipe.Set(ctx, key, j, 0)
				}
				_, err := pipe.Exec(ctx)

				latency := time.Since(opStart)

				if err != nil {
					if isPoolTimeout(err) {
						metrics.recordPoolTimeout()
					}
					metrics.recordFailure()
				} else {
					metrics.recordSuccess(latency)
				}
			}
		}(g)
	}

	wg.Wait()
	elapsed := time.Since(start)

	totalCommands := numGoroutines * pipelinesPerGoroutine * commandsPerPipeline
	fmt.Printf("✓ Completed %d commands in %d pipelines in %v\n", totalCommands, numGoroutines*pipelinesPerGoroutine, elapsed)
	fmt.Printf("  Throughput: %.0f ops/sec\n", float64(totalCommands)/elapsed.Seconds())
	metrics.printDetailed()
	printPoolStats(client.PoolStats())
	fmt.Println()
}

// testPubSubWithGetSet tests pool exhaustion with concurrent pub/sub and get/set operations
func testPubSubWithGetSet() {
	fmt.Println("=== Test 5: PubSub + Get/Set Pool Exhaustion ===")
	fmt.Println("Testing pool with 100 publishers, 10 subscribers (10 channels), and 100 get/set goroutines")
	fmt.Println("Pool size: 100 connections")
	fmt.Println()

	ctx := context.Background()

	// Create client with pool size 100
	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: getRedisAddrs(),
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
		PoolSize:    100,
		PoolTimeout: 5 * time.Second,
	})
	defer client.Close()

	metrics := &AdvancedMetrics{}
	const testDuration = 10 * time.Second
	const numChannels = 10
	const numPublishers = 100
	const numSubscribers = 10
	const numGetSetWorkers = 100

	// Channel names
	channels := make([]string, numChannels)
	for i := 0; i < numChannels; i++ {
		channels[i] = fmt.Sprintf("test-channel-%d", i)
	}

	start := time.Now()
	var wg sync.WaitGroup
	stopSignal := make(chan struct{})

	// Track pub/sub specific metrics
	var publishCount atomic.Int64
	var receiveCount atomic.Int64
	var subscribeErrors atomic.Int64

	// Start subscribers (10 goroutines, each subscribing to all 10 channels)
	for s := 0; s < numSubscribers; s++ {
		wg.Add(1)
		go func(subscriberID int) {
			defer wg.Done()

			// Create a dedicated pubsub connection
			pubsub := client.Subscribe(ctx, channels...)
			defer pubsub.Close()

			// Wait for subscription confirmation
			_, err := pubsub.Receive(ctx)
			if err != nil {
				subscribeErrors.Add(1)
				fmt.Printf("Subscriber %d: failed to subscribe: %v\n", subscriberID, err)
				return
			}

			// Receive messages until stop signal
			ch := pubsub.Channel()
			for {
				select {
				case <-stopSignal:
					return
				case msg := <-ch:
					if msg != nil {
						receiveCount.Add(1)
					}
				case <-time.After(100 * time.Millisecond):
					// Timeout to check stop signal periodically
				}
			}
		}(s)
	}

	// Give subscribers time to connect
	time.Sleep(500 * time.Millisecond)

	// Start publishers (100 goroutines)
	for p := 0; p < numPublishers; p++ {
		wg.Add(1)
		go func(publisherID int) {
			defer wg.Done()

			for {
				select {
				case <-stopSignal:
					return
				default:
					opStart := time.Now()

					// Publish to a random channel
					channelIdx := rand.Intn(numChannels)
					message := fmt.Sprintf("msg-%d-%d", publisherID, time.Now().UnixNano())
					err := client.Publish(ctx, channels[channelIdx], message).Err()

					latency := time.Since(opStart)

					if err != nil {
						if isPoolTimeout(err) {
							metrics.recordPoolTimeout()
						}
						metrics.recordFailure()
					} else {
						metrics.recordSuccess(latency)
						publishCount.Add(1)
					}

					// Small delay to avoid overwhelming the system
					time.Sleep(10 * time.Millisecond)
				}
			}
		}(p)
	}

	// Start get/set workers (100 goroutines)
	for w := 0; w < numGetSetWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for {
				select {
				case <-stopSignal:
					return
				default:
					opStart := time.Now()

					// Alternate between SET and GET
					key := fmt.Sprintf("worker:%d:key", workerID)
					var err error
					if rand.Intn(2) == 0 {
						err = client.Set(ctx, key, workerID, 0).Err()
					} else {
						err = client.Get(ctx, key).Err()
						// Ignore key not found errors
						if err == redis.Nil {
							err = nil
						}
					}

					latency := time.Since(opStart)

					if err != nil {
						if isPoolTimeout(err) {
							metrics.recordPoolTimeout()
						}
						metrics.recordFailure()
					} else {
						metrics.recordSuccess(latency)
					}

					// Small delay to avoid overwhelming the system
					time.Sleep(5 * time.Millisecond)
				}
			}
		}(w)
	}

	// Run for specified duration
	time.Sleep(testDuration)
	close(stopSignal)
	wg.Wait()

	elapsed := time.Since(start)

	fmt.Printf("✓ Test completed in %v\n", elapsed)
	fmt.Printf("  Published: %d messages\n", publishCount.Load())
	fmt.Printf("  Received: %d messages\n", receiveCount.Load())
	fmt.Printf("  Subscribe errors: %d\n", subscribeErrors.Load())
	fmt.Printf("  Get/Set operations: %d\n", metrics.successOps.Load())
	fmt.Printf("  Total throughput: %.0f ops/sec\n", float64(metrics.successOps.Load())/elapsed.Seconds())
	metrics.printDetailed()
	printPoolStats(client.PoolStats())
	fmt.Println()
}
