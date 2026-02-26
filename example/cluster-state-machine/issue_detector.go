package main

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// IssueDetector helps identify potential concurrency issues
type IssueDetector struct {
	// Timing anomalies
	slowOps           atomic.Int64 // Operations taking >100ms
	verySlowOps       atomic.Int64 // Operations taking >1s
	
	// Error patterns
	consecutiveErrors atomic.Int64
	errorBursts       atomic.Int64 // Multiple errors in short time
	
	// Pool issues
	poolExhaustion    atomic.Int64
	longWaits         atomic.Int64 // Waits >500ms for connection
	
	// State machine issues
	stateConflicts    atomic.Int64 // Potential state transition conflicts
	
	lastErrorTime     atomic.Int64 // Unix nano
	errorCount        atomic.Int64
}

func (id *IssueDetector) recordOp(latency time.Duration, err error) {
	if err != nil {
		id.errorCount.Add(1)
		now := time.Now().UnixNano()
		lastErr := id.lastErrorTime.Swap(now)
		
		// Check for error burst (multiple errors within 100ms)
		if lastErr > 0 && (now-lastErr) < 100*1000*1000 {
			id.errorBursts.Add(1)
		}

		if isPoolTimeout(err) {
			id.poolExhaustion.Add(1)
		}

		return
	}
	
	// Reset error tracking on success
	id.errorCount.Store(0)
	
	// Track slow operations
	if latency > 100*time.Millisecond {
		id.slowOps.Add(1)
	}
	if latency > 1*time.Second {
		id.verySlowOps.Add(1)
	}
	if latency > 500*time.Millisecond {
		id.longWaits.Add(1)
	}
}

func (id *IssueDetector) print() {
	fmt.Printf("\n=== Issue Detector ===\n")
	
	hasIssues := false
	
	if id.verySlowOps.Load() > 0 {
		fmt.Printf("⚠️  Very slow operations (>1s): %d\n", id.verySlowOps.Load())
		hasIssues = true
	}
	
	if id.slowOps.Load() > 0 {
		fmt.Printf("⚠️  Slow operations (>100ms): %d\n", id.slowOps.Load())
		hasIssues = true
	}
	
	if id.errorBursts.Load() > 0 {
		fmt.Printf("⚠️  Error bursts detected: %d\n", id.errorBursts.Load())
		hasIssues = true
	}
	
	if id.poolExhaustion.Load() > 0 {
		fmt.Printf("⚠️  Pool exhaustion events: %d\n", id.poolExhaustion.Load())
		hasIssues = true
	}
	
	if id.longWaits.Load() > 0 {
		fmt.Printf("⚠️  Long waits (>500ms): %d\n", id.longWaits.Load())
		hasIssues = true
	}
	
	if id.stateConflicts.Load() > 0 {
		fmt.Printf("⚠️  Potential state conflicts: %d\n", id.stateConflicts.Load())
		hasIssues = true
	}
	
	if !hasIssues {
		fmt.Printf("✓ No issues detected\n")
	}
}

// runIssueDetection runs tests specifically designed to detect concurrency issues
func runIssueDetection() {
	ctx := context.Background()

	fmt.Println("\n=== Issue Detection Tests ===\n")
	fmt.Println("Running tests designed to expose potential concurrency issues")
	fmt.Println("in the connection state machine.\n")

	// Test 1: Thundering herd
	testThunderingHerd(ctx)

	// Test 2: Bursty traffic
	testBurstyTraffic(ctx)

	// Test 3: Connection churn
	testConnectionChurn(ctx)
}

func testThunderingHerd(ctx context.Context) {
	fmt.Println("Test 1: Thundering Herd")
	fmt.Println("-----------------------")
	fmt.Println("All goroutines start simultaneously, competing for connections.\n")

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:       getRedisAddrs(),
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
		PoolSize:    5,
		PoolTimeout: 2 * time.Second,
	})
	defer client.Close()

	detector := &IssueDetector{}
	const numGoroutines = 100

	var wg sync.WaitGroup
	startGate := make(chan struct{})

	// Prepare all goroutines
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			
			// Wait for start signal
			<-startGate
			
			key := fmt.Sprintf("herd:%d", goroutineID)
			value := "data"

			opStart := time.Now()
			err := client.Set(ctx, key, value, 0).Err()
			latency := time.Since(opStart)

			detector.recordOp(latency, err)
		}(g)
	}

	// Release the herd!
	start := time.Now()
	close(startGate)
	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("✓ Completed in %v\n", elapsed)
	detector.print()
	fmt.Println()
}

func testBurstyTraffic(ctx context.Context) {
	fmt.Println("Test 2: Bursty Traffic")
	fmt.Println("----------------------")
	fmt.Println("Alternating between high and low load.\n")

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:       getRedisAddrs(),
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
		PoolSize:    8,
		PoolTimeout: 3 * time.Second,
	})
	defer client.Close()

	detector := &IssueDetector{}
	const numBursts = 5
	const goroutinesPerBurst = 50

	start := time.Now()

	for burst := 0; burst < numBursts; burst++ {
		var wg sync.WaitGroup
		
		// High load burst
		for g := 0; g < goroutinesPerBurst; g++ {
			wg.Add(1)
			go func(burstID, goroutineID int) {
				defer wg.Done()
				
				key := fmt.Sprintf("burst:%d:%d", burstID, goroutineID)
				value := "data"

				opStart := time.Now()
				err := client.Set(ctx, key, value, 0).Err()
				latency := time.Since(opStart)

				detector.recordOp(latency, err)
			}(burst, g)
		}
		
		wg.Wait()
		
		// Quiet period
		time.Sleep(100 * time.Millisecond)
	}

	elapsed := time.Since(start)

	fmt.Printf("✓ Completed %d bursts in %v\n", numBursts, elapsed)
	detector.print()
	fmt.Println()
}

func testConnectionChurn(ctx context.Context) {
	fmt.Println("Test 3: Connection Churn")
	fmt.Println("------------------------")
	fmt.Println("Rapidly creating and closing connections.\n")

	detector := &IssueDetector{}
	const numIterations = 10
	const goroutinesPerIteration = 20

	start := time.Now()

	for iter := 0; iter < numIterations; iter++ {
		// Create new client
		client := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:       getRedisAddrs(),
			MaintNotificationsConfig: &maintnotifications.Config{
				Mode: maintnotifications.ModeDisabled,
			},
			PoolSize:    5,
			PoolTimeout: 2 * time.Second,
		})

		var wg sync.WaitGroup
		for g := 0; g < goroutinesPerIteration; g++ {
			wg.Add(1)
			go func(iterID, goroutineID int) {
				defer wg.Done()
				
				key := fmt.Sprintf("churn:%d:%d", iterID, goroutineID)
				value := "data"

				opStart := time.Now()
				err := client.Set(ctx, key, value, 0).Err()
				latency := time.Since(opStart)

				detector.recordOp(latency, err)
			}(iter, g)
		}
		
		wg.Wait()
		
		// Close client
		client.Close()
		
		// Small delay before next iteration
		time.Sleep(50 * time.Millisecond)
	}

	elapsed := time.Since(start)

	fmt.Printf("✓ Completed %d iterations in %v\n", numIterations, elapsed)
	detector.print()
	fmt.Println()
}

// testRaceConditions attempts to expose race conditions in state transitions
func testRaceConditions(ctx context.Context) {
	fmt.Println("Test 4: Race Condition Detection")
	fmt.Println("---------------------------------")
	fmt.Println("Attempting to trigger race conditions in state machine.\n")

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:       getRedisAddrs(),
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
		PoolSize:    3, // Very small to increase contention
		PoolTimeout: 1 * time.Second,
	})
	defer client.Close()

	detector := &IssueDetector{}
	const numGoroutines = 100
	const opsPerGoroutine = 20

	start := time.Now()
	var wg sync.WaitGroup

	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("race:%d:%d", goroutineID, i)
				value := "x"

				opStart := time.Now()
				
				// Mix of operations to stress state machine
				var err error
				switch i % 3 {
				case 0:
					err = client.Set(ctx, key, value, 0).Err()
				case 1:
					_, err = client.Get(ctx, key).Result()
					if err == redis.Nil {
						err = nil
					}
				case 2:
					pipe := client.Pipeline()
					pipe.Set(ctx, key, value, 0)
					pipe.Get(ctx, key)
					_, err = pipe.Exec(ctx)
				}
				
				latency := time.Since(opStart)
				detector.recordOp(latency, err)
			}
		}(g)
	}

	wg.Wait()
	elapsed := time.Since(start)

	fmt.Printf("✓ Completed in %v\n", elapsed)
	fmt.Printf("  Total operations: %d\n", numGoroutines*opsPerGoroutine)
	detector.print()
	fmt.Println()
}

