package pool_test

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

// TestRaceConditionFreeTurn tests the race condition where:
// 1. Dial completes and tryDeliver succeeds
// 2. Waiter's context times out before receiving from result channel
// 3. Waiter's defer retrieves connection via cancel() and delivers to another waiter
// 4. Turn must be freed by the defer, not by dial goroutine or new waiter
func TestRaceConditionFreeTurn(t *testing.T) {
	// Create a pool with PoolSize=2 to make the race easier to trigger
	opt := &pool.Options{
		Dialer: func(ctx context.Context) (net.Conn, error) {
			// Slow dial to allow context timeout to race with delivery
			time.Sleep(50 * time.Millisecond)
			return dummyDialer(ctx)
		},
		PoolSize:           2,
		MaxConcurrentDials: 2,
		DialTimeout:        1 * time.Second,
		PoolTimeout:        1 * time.Second,
	}

	connPool := pool.NewConnPool(opt)
	defer connPool.Close()

	// Run multiple iterations to increase chance of hitting the race
	for iteration := 0; iteration < 10; iteration++ {
		// Request 1: Will timeout quickly
		ctx1, cancel1 := context.WithTimeout(context.Background(), 30*time.Millisecond)
		defer cancel1()

		var wg sync.WaitGroup
		wg.Add(2)

		// Goroutine 1: Request with short timeout
		go func() {
			defer wg.Done()
			cn, err := connPool.Get(ctx1)
			if err == nil {
				// If we got a connection, put it back
				connPool.Put(ctx1, cn)
			}
			// Expected: context deadline exceeded
		}()

		// Goroutine 2: Request with longer timeout, should receive the orphaned connection
		go func() {
			defer wg.Done()
			time.Sleep(20 * time.Millisecond) // Start slightly after first request
			ctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel2()

			cn, err := connPool.Get(ctx2)
			if err != nil {
				t.Logf("Request 2 error: %v", err)
				return
			}
			// Got connection, put it back
			connPool.Put(ctx2, cn)
		}()

		wg.Wait()

		// Give some time for all operations to complete
		time.Sleep(100 * time.Millisecond)

		// Check QueueLen - should be 0 (all turns freed)
		queueLen := connPool.QueueLen()
		if queueLen != 0 {
			t.Errorf("Iteration %d: QueueLen = %d, expected 0 (turn leak detected!)", iteration, queueLen)
		}
	}
}

// TestRaceConditionFreeTurnStress is a stress test for the race condition
func TestRaceConditionFreeTurnStress(t *testing.T) {
	var dialIndex atomic.Int32
	opt := &pool.Options{
		Dialer: func(ctx context.Context) (net.Conn, error) {
			// Variable dial time to create more race opportunities
			// Use atomic increment to avoid data race
			idx := dialIndex.Add(1)
			time.Sleep(time.Duration(10+idx%40) * time.Millisecond)
			return dummyDialer(ctx)
		},
		PoolSize:           10,
		MaxConcurrentDials: 10,
		DialTimeout:        1 * time.Second,
		PoolTimeout:        500 * time.Millisecond,
	}

	connPool := pool.NewConnPool(opt)
	defer connPool.Close()

	const numRequests = 50

	var wg sync.WaitGroup
	wg.Add(numRequests)

	// Launch many concurrent requests with varying timeouts
	for i := 0; i < numRequests; i++ {
		go func(i int) {
			defer wg.Done()

			// Varying timeouts to create race conditions
			timeout := time.Duration(20+i%80) * time.Millisecond
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			cn, err := connPool.Get(ctx)
			if err == nil {
				// Simulate some work
				time.Sleep(time.Duration(i%20) * time.Millisecond)
				connPool.Put(ctx, cn)
			}
		}(i)
	}

	wg.Wait()

	// Give time for all cleanup to complete
	time.Sleep(200 * time.Millisecond)

	// Check for turn leaks
	queueLen := connPool.QueueLen()
	if queueLen != 0 {
		t.Errorf("QueueLen = %d, expected 0 (turn leak detected!)", queueLen)
		t.Errorf("This indicates that some turns were never freed")
	}

	// Also check pool stats
	stats := connPool.Stats()
	t.Logf("Pool stats: Hits=%d, Misses=%d, Timeouts=%d, TotalConns=%d, IdleConns=%d",
		stats.Hits, stats.Misses, stats.Timeouts, stats.TotalConns, stats.IdleConns)
}

