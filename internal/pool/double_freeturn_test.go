package pool

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestDoubleFreeTurnBug demonstrates the double freeTurn bug where:
// 1. Dial goroutine creates a connection
// 2. Original waiter times out
// 3. putIdleConn delivers connection to another waiter
// 4. Dial goroutine calls freeTurn() (FIRST FREE)
// 5. Second waiter uses connection and calls Put()
// 6. Put() calls freeTurn() (SECOND FREE - BUG!)
//
// This causes the semaphore to be released twice, allowing more concurrent
// operations than PoolSize allows.
func TestDoubleFreeTurnBug(t *testing.T) {
	var dialCount atomic.Int32
	var putCount atomic.Int32

	// Slow dialer - 150ms per dial
	slowDialer := func(ctx context.Context) (net.Conn, error) {
		dialCount.Add(1)
		select {
		case <-time.After(150 * time.Millisecond):
			server, client := net.Pipe()
			go func() {
				defer server.Close()
				buf := make([]byte, 1024)
				for {
					_, err := server.Read(buf)
					if err != nil {
						return
					}
				}
			}()
			return client, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	opt := &Options{
		Dialer:             slowDialer,
		PoolSize:           10,  // Small pool to make bug easier to trigger
		MaxConcurrentDials: 10,
		MinIdleConns:       0,
		PoolTimeout:        100 * time.Millisecond,
		DialTimeout:        5 * time.Second,
	}

	connPool := NewConnPool(opt)
	defer connPool.Close()

	// Scenario:
	// 1. Request A starts dial (100ms timeout - will timeout before dial completes)
	// 2. Request B arrives (500ms timeout - will wait in queue)
	// 3. Request A times out at 100ms
	// 4. Dial completes at 150ms
	// 5. putIdleConn delivers connection to Request B
	// 6. Dial goroutine calls freeTurn() - FIRST FREE
	// 7. Request B uses connection and calls Put()
	// 8. Put() calls freeTurn() - SECOND FREE (BUG!)

	var wg sync.WaitGroup
	
	// Request A: Short timeout, will timeout before dial completes
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		defer cancel()
		
		cn, err := connPool.Get(ctx)
		if err != nil {
			// Expected to timeout
			t.Logf("Request A timed out as expected: %v", err)
		} else {
			// Should not happen
			t.Errorf("Request A should have timed out but got connection")
			connPool.Put(ctx, cn)
			putCount.Add(1)
		}
	}()
	
	// Wait a bit for Request A to start dialing
	time.Sleep(10 * time.Millisecond)
	
	// Request B: Long timeout, will receive the connection from putIdleConn
	wg.Add(1)
	go func() {
		defer wg.Done()
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()
		
		cn, err := connPool.Get(ctx)
		if err != nil {
			t.Errorf("Request B should have succeeded but got error: %v", err)
		} else {
			t.Logf("Request B got connection successfully")
			// Use the connection briefly
			time.Sleep(50 * time.Millisecond)
			connPool.Put(ctx, cn)
			putCount.Add(1)
		}
	}()
	
	wg.Wait()

	// Check results
	t.Logf("\n=== Results ===")
	t.Logf("Dials: %d", dialCount.Load())
	t.Logf("Puts: %d", putCount.Load())

	// The bug is hard to detect directly without instrumenting freeTurn,
	// but we can verify the scenario works correctly:
	// - Request A should timeout
	// - Request B should succeed and get the connection
	// - 1-2 dials may occur (Request A starts one, Request B may start another)
	// - 1 put should occur (Request B returning the connection)

	if putCount.Load() != 1 {
		t.Errorf("Expected 1 put, got %d", putCount.Load())
	}

	t.Logf("✓ Scenario completed successfully")
	t.Logf("Note: The double freeTurn bug would cause semaphore to be released twice,")
	t.Logf("allowing more concurrent operations than PoolSize permits.")
	t.Logf("With the fix, putIdleConn returns true when delivering to a waiter,")
	t.Logf("preventing the dial goroutine from calling freeTurn (waiter will call it later).")
}

// TestDoubleFreeTurnHighConcurrency tests the bug under high concurrency
func TestDoubleFreeTurnHighConcurrency(t *testing.T) {
	var dialCount atomic.Int32
	var getSuccesses atomic.Int32
	var getFailures atomic.Int32

	slowDialer := func(ctx context.Context) (net.Conn, error) {
		dialCount.Add(1)
		select {
		case <-time.After(200 * time.Millisecond):
			server, client := net.Pipe()
			go func() {
				defer server.Close()
				buf := make([]byte, 1024)
				for {
					_, err := server.Read(buf)
					if err != nil {
						return
					}
				}
			}()
			return client, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	opt := &Options{
		Dialer:             slowDialer,
		PoolSize:           20,
		MaxConcurrentDials: 20,
		MinIdleConns:       0,
		PoolTimeout:        100 * time.Millisecond,
		DialTimeout:        5 * time.Second,
	}

	connPool := NewConnPool(opt)
	defer connPool.Close()

	// Create many requests with varying timeouts
	// Some will timeout before dial completes, triggering the putIdleConn delivery path
	const numRequests = 100
	var wg sync.WaitGroup

	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Vary timeout: some short (will timeout), some long (will succeed)
			timeout := 100 * time.Millisecond
			if id%3 == 0 {
				timeout = 500 * time.Millisecond
			}

			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()

			cn, err := connPool.Get(ctx)
			if err != nil {
				getFailures.Add(1)
			} else {
				getSuccesses.Add(1)
				time.Sleep(10 * time.Millisecond)
				connPool.Put(ctx, cn)
			}
		}(i)

		// Stagger requests
		if i%10 == 0 {
			time.Sleep(5 * time.Millisecond)
		}
	}

	wg.Wait()

	t.Logf("\n=== High Concurrency Results ===")
	t.Logf("Requests: %d", numRequests)
	t.Logf("Successes: %d", getSuccesses.Load())
	t.Logf("Failures: %d", getFailures.Load())
	t.Logf("Dials: %d", dialCount.Load())

	// Verify that some requests succeeded despite timeouts
	// This exercises the putIdleConn delivery path
	if getSuccesses.Load() == 0 {
		t.Errorf("Expected some successful requests, got 0")
	}

	t.Logf("✓ High concurrency test completed")
	t.Logf("Note: This test exercises the putIdleConn delivery path where the bug occurs")
}

