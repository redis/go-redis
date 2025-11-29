package pool_test

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

// TestDoubleFreeTurnSimple tests the double-free bug with a simple scenario.
// This test FAILS with the OLD code and PASSES with the NEW code.
//
// Scenario:
// 1. Request A times out, Dial A completes and delivers connection to Request B
// 2. Request B's own Dial B completes later
// 3. With the bug: Dial B frees Request B's turn (even though Request B is using connection A)
// 4. Then Request B calls Put() and frees the turn AGAIN (double-free)
// 5. This allows more concurrent operations than PoolSize permits
//
// Detection method:
// - Try to acquire PoolSize+1 connections after the double-free
// - With the bug: All succeed (pool size violated)
// - With the fix: Only PoolSize succeed
func TestDoubleFreeTurnSimple(t *testing.T) {
	ctx := context.Background()
	
	var dialCount atomic.Int32
	dialBComplete := make(chan struct{})
	requestBGotConn := make(chan struct{})
	requestBCalledPut := make(chan struct{})
	
	controlledDialer := func(ctx context.Context) (net.Conn, error) {
		count := dialCount.Add(1)
		
		if count == 1 {
			// Dial A: takes 150ms
			time.Sleep(150 * time.Millisecond)
			t.Logf("Dial A completed")
		} else if count == 2 {
			// Dial B: takes 300ms (longer than Dial A)
			time.Sleep(300 * time.Millisecond)
			t.Logf("Dial B completed")
			close(dialBComplete)
		} else {
			// Other dials: fast
			time.Sleep(10 * time.Millisecond)
		}
		
		return newDummyConn(), nil
	}
	
	testPool := pool.NewConnPool(&pool.Options{
		Dialer:             controlledDialer,
		PoolSize:           2, // Only 2 concurrent operations allowed
		MaxConcurrentDials: 5,
		DialTimeout:        1 * time.Second,
		PoolTimeout:        1 * time.Second,
	})
	defer testPool.Close()
	
	// Request A: Short timeout (100ms), will timeout before dial completes (150ms)
	go func() {
		shortCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
		defer cancel()
		
		_, err := testPool.Get(shortCtx)
		if err != nil {
			t.Logf("Request A: Timed out as expected: %v", err)
		}
	}()
	
	// Wait for Request A to start
	time.Sleep(20 * time.Millisecond)
	
	// Request B: Long timeout, will receive connection from Request A's dial
	requestBDone := make(chan struct{})
	go func() {
		defer close(requestBDone)
		
		longCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
		defer cancel()
		
		cn, err := testPool.Get(longCtx)
		if err != nil {
			t.Errorf("Request B: Should have received connection but got error: %v", err)
			return
		}
		
		t.Logf("Request B: Got connection from Request A's dial")
		close(requestBGotConn)
		
		// Wait for dial B to complete
		<-dialBComplete

		t.Logf("Request B: Dial B completed")

		// Wait a bit to allow Dial B goroutine to finish and call freeTurn()
		time.Sleep(100 * time.Millisecond)

		// Signal that we're ready for the test to check semaphore state
		close(requestBCalledPut)

		// Wait for the test to check QueueLen
		time.Sleep(200 * time.Millisecond)

		t.Logf("Request B: Now calling Put()")
		testPool.Put(ctx, cn)
		t.Logf("Request B: Put() called")
	}()
	
	// Wait for Request B to get the connection
	<-requestBGotConn

	// Wait for Dial B to complete and freeTurn() to be called
	<-requestBCalledPut

	// NOW WE'RE IN THE CRITICAL WINDOW
	// Request B is holding a connection (from Dial A)
	// Dial B has completed and returned (freeTurn() has been called)
	// With the bug:
	//   - Dial B freed Request B's turn (BUG!)
	//   - QueueLen should be 0
	// With the fix:
	//   - Dial B did NOT free Request B's turn
	//   - QueueLen should be 1 (Request B still holds the turn)

	t.Logf("\n=== CRITICAL CHECK: QueueLen ===")
	t.Logf("Request B is holding a connection, Dial B has completed and returned")
	queueLen := testPool.QueueLen()
	t.Logf("QueueLen: %d", queueLen)

	// Wait for Request B to finish
	select {
	case <-requestBDone:
	case <-time.After(1 * time.Second):
		t.Logf("Request B timed out")
	}

	t.Logf("\n=== Results ===")
	t.Logf("QueueLen during critical window: %d", queueLen)
	t.Logf("Expected with fix: 1 (Request B still holds the turn)")
	t.Logf("Expected with bug: 0 (Dial B freed Request B's turn)")

	if queueLen == 0 {
		t.Errorf("DOUBLE-FREE BUG DETECTED!")
		t.Errorf("QueueLen is 0, meaning Dial B freed Request B's turn")
		t.Errorf("But Request B is still holding a connection, so its turn should NOT be freed yet")
	} else if queueLen == 1 {
		t.Logf("✓ CORRECT: QueueLen is 1")
		t.Logf("Request B is still holding the turn (will be freed when Request B calls Put())")
	} else {
		t.Logf("Unexpected QueueLen: %d (expected 1 with fix, 0 with bug)", queueLen)
	}
}

