package main

import (
	"context"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// getRedisAddrs parses the comma-separated addresses
func getRedisAddrs() []string {
	addrs := strings.Split(*redisAddrs, ",")
	for i := range addrs {
		addrs[i] = strings.TrimSpace(addrs[i])
	}
	return addrs
}

// isPoolTimeout checks if an error is a pool timeout error
// Note: This is defined in multiple files to avoid import cycles
func isPoolTimeout(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(err.Error(), "pool timeout")
}

// Metrics tracks operation statistics
type Metrics struct {
	totalOps       atomic.Int64
	successOps     atomic.Int64
	failedOps      atomic.Int64
	timeoutOps     atomic.Int64
	poolTimeouts   atomic.Int64
	totalLatencyNs atomic.Int64
}

func (m *Metrics) recordSuccess(latency time.Duration) {
	m.totalOps.Add(1)
	m.successOps.Add(1)
	m.totalLatencyNs.Add(latency.Nanoseconds())
}

func (m *Metrics) recordFailure() {
	m.totalOps.Add(1)
	m.failedOps.Add(1)
}

func (m *Metrics) recordTimeout() {
	m.totalOps.Add(1)
	m.timeoutOps.Add(1)
}

func (m *Metrics) recordPoolTimeout() {
	m.poolTimeouts.Add(1)
}

func (m *Metrics) print() {
	total := m.totalOps.Load()
	success := m.successOps.Load()
	failed := m.failedOps.Load()
	timeouts := m.timeoutOps.Load()
	poolTimeouts := m.poolTimeouts.Load()
	avgLatency := time.Duration(0)
	if success > 0 {
		avgLatency = time.Duration(m.totalLatencyNs.Load() / success)
	}

	fmt.Printf("\n=== Metrics ===\n")
	fmt.Printf("Total Operations:    %d\n", total)
	fmt.Printf("Successful:          %d (%.2f%%)\n", success, float64(success)/float64(total)*100)
	fmt.Printf("Failed:              %d (%.2f%%)\n", failed, float64(failed)/float64(total)*100)
	fmt.Printf("Timeouts:            %d (%.2f%%)\n", timeouts, float64(timeouts)/float64(total)*100)
	fmt.Printf("Pool Timeouts:       %d\n", poolTimeouts)
	fmt.Printf("Avg Latency:         %v\n", avgLatency)
}

var (
	redisAddrs = flag.String("addrs", "localhost:6379", "Comma-separated Redis addresses (e.g., localhost:7000,localhost:7001,localhost:7002)")
	mode       = flag.String("mode", "basic", "Test mode: basic, advanced, detect, all")
)

func main() {
	// Parse command line flags
	flag.Parse()

	ctx := context.Background()

	fmt.Println("=== Redis Cluster State Machine Example ===\n")
	fmt.Println("This example demonstrates the connection state machine")
	fmt.Println("under high concurrency with the cluster client.\n")
	fmt.Printf("Redis addresses: %s\n\n", *redisAddrs)

	switch *mode {
	case "basic":
		runBasicExamples(ctx)
	case "advanced":
		runAdvancedExample()
	case "detect":
		runIssueDetection()
	case "all":
		runBasicExamples(ctx)
		runAdvancedExample()
		runIssueDetection()
	default:
		fmt.Printf("Unknown mode: %s\n", *mode)
		fmt.Println("Available modes: basic, advanced, detect, all")
		os.Exit(1)
	}

	fmt.Println("\n=== All tests completed ===")
}

func runBasicExamples(ctx context.Context) {
	fmt.Println("=== Basic Examples ===\n")

	// Example 1: Basic concurrent operations
	example1(ctx)

	// Example 2: High concurrency stress test
	example2(ctx)

	// Example 3: Connection pool behavior under load
	example3(ctx)

	// Example 4: Mixed read/write workload
	example4(ctx)
}

func example1(ctx context.Context) {
	fmt.Println("Example 1: Basic Concurrent Operations")
	fmt.Println("---------------------------------------")

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: getRedisAddrs(),
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	defer client.Close()

	metrics := &Metrics{}
	const numGoroutines = 100
	const opsPerGoroutine = 5000

	start := time.Now()
	var wg sync.WaitGroup

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("user:%d:%d", goroutineID, i)
				value := fmt.Sprintf("data-%d-%d", goroutineID, i)

				opStart := time.Now()
				err := client.Set(ctx, key, value, 0).Err()
				latency := time.Since(opStart)

				if err != nil {
					if isPoolTimeout(err) {
						metrics.recordPoolTimeout()
					}
					metrics.recordFailure()
					fmt.Printf("Error in goroutine %d: %v\n", goroutineID, err)
				} else {
					metrics.recordSuccess(latency)
				}
			}
		}(g)
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("✓ Completed %d operations from %d goroutines in %v\n",
		numGoroutines*opsPerGoroutine, numGoroutines, elapsed)
	fmt.Printf("  Throughput: %.0f ops/sec\n", float64(numGoroutines*opsPerGoroutine)/elapsed.Seconds())
	metrics.print()
	fmt.Println()
}

func example2(ctx context.Context) {
	fmt.Println("Example 2: High Concurrency Stress Test")
	fmt.Println("----------------------------------------")
	fmt.Println("Testing with limited pool size and many concurrent goroutines")
	fmt.Println("to stress the connection state machine and pool management.\n")

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: getRedisAddrs(),
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
		PoolSize:    5, // Intentionally small to create contention
		PoolTimeout: 2 * time.Second,
	})
	defer client.Close()

	metrics := &Metrics{}
	const numGoroutines = 250
	const opsPerGoroutine = 250

	start := time.Now()
	var wg sync.WaitGroup

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("stress:%d:%d", goroutineID, i)
				value := fmt.Sprintf("value-%d", i)

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

				// Small random delay to simulate real workload
				time.Sleep(time.Microsecond * time.Duration(rand.Intn(100)))
			}
		}(g)
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("✓ Completed stress test in %v\n", elapsed)
	fmt.Printf("  Throughput: %.0f ops/sec\n", float64(numGoroutines*opsPerGoroutine)/elapsed.Seconds())
	metrics.print()
	fmt.Println()
}

func example3(ctx context.Context) {
	fmt.Println("Example 3: Connection Pool Behavior Under Load")
	fmt.Println("-----------------------------------------------")
	fmt.Println("Monitoring connection reuse and state transitions.\n")

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: getRedisAddrs(),
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
		PoolSize:     8,
		MinIdleConns: 2,
		PoolTimeout:  3 * time.Second,
	})
	defer client.Close()

	metrics := &Metrics{}
	const duration = 5 * time.Second
	const numWorkers = 100

	start := time.Now()
	stopChan := make(chan struct{})
	var wg sync.WaitGroup

	// Start workers
	for w := 0; w < numWorkers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			counter := 0
			for {
				select {
				case <-stopChan:
					return
				default:
					key := fmt.Sprintf("worker:%d:counter", workerID)
					counter++

					opStart := time.Now()
					err := client.Set(ctx, key, counter, 0).Err()
					latency := time.Since(opStart)

					if err != nil {
						if isPoolTimeout(err) {
							metrics.recordPoolTimeout()
						}
						metrics.recordFailure()
					} else {
						metrics.recordSuccess(latency)
					}

					// Variable workload
					time.Sleep(time.Millisecond * time.Duration(rand.Intn(50)))
				}
			}
		}(w)
	}

	// Let it run for the specified duration
	time.Sleep(duration)
	close(stopChan)
	wg.Wait()

	elapsed := time.Since(start)

	fmt.Printf("✓ Ran %d workers for %v\n", numWorkers, duration)
	fmt.Printf("  Throughput: %.0f ops/sec\n", float64(metrics.totalOps.Load())/elapsed.Seconds())
	metrics.print()
	fmt.Println()
}

func example4(ctx context.Context) {
	fmt.Println("Example 4: Mixed Read/Write Workload")
	fmt.Println("-------------------------------------")
	fmt.Println("Testing connection state machine with mixed operations.\n")

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: getRedisAddrs(),
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
		PoolSize:    10,
		PoolTimeout: 5 * time.Second,
	})
	defer client.Close()

	metrics := &Metrics{}
	const numGoroutines = 300
	const opsPerGoroutine = 1000

	// Pre-populate some data
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("data:%d", i)
		client.Set(ctx, key, fmt.Sprintf("value-%d", i), 0)
	}

	start := time.Now()
	var wg sync.WaitGroup

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("data:%d", rand.Intn(100))

				opStart := time.Now()
				var err error

				// 60% reads, 40% writes
				if rand.Float32() < 0.6 {
					_, err = client.Get(ctx, key).Result()
					if err == redis.Nil {
						err = nil // Key not found is not an error
					}
				} else {
					value := fmt.Sprintf("updated-%d-%d", goroutineID, i)
					err = client.Set(ctx, key, value, 0).Err()
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
			}
		}(g)
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("✓ Completed mixed workload in %v\n", elapsed)
	fmt.Printf("  Throughput: %.0f ops/sec\n", float64(numGoroutines*opsPerGoroutine)/elapsed.Seconds())
	metrics.print()
	fmt.Println()
}
