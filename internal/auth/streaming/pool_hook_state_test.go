package streaming

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

// TestReAuthOnlyWhenIdle verifies that re-authentication only happens when
// a connection is in IDLE state, not when it's IN_USE.
func TestReAuthOnlyWhenIdle(t *testing.T) {
	// Create a connection
	cn := pool.NewConn(nil)

	// Initialize to IDLE state
	cn.GetStateMachine().Transition(pool.StateInitializing)
	cn.GetStateMachine().Transition(pool.StateIdle)

	// Simulate connection being acquired (IDLE → IN_USE)
	if !cn.CompareAndSwapUsed(false, true) {
		t.Fatal("Failed to acquire connection")
	}

	// Verify state is IN_USE
	if state := cn.GetStateMachine().GetState(); state != pool.StateInUse {
		t.Errorf("Expected state IN_USE, got %s", state)
	}

	// Try to transition to UNUSABLE (for reauth) - should fail
	_, err := cn.GetStateMachine().TryTransition([]pool.ConnState{pool.StateIdle}, pool.StateUnusable)
	if err == nil {
		t.Error("Expected error when trying to transition IN_USE → UNUSABLE, but got none")
	}

	// Verify state is still IN_USE
	if state := cn.GetStateMachine().GetState(); state != pool.StateInUse {
		t.Errorf("Expected state to remain IN_USE, got %s", state)
	}

	// Release connection (IN_USE → IDLE)
	if !cn.CompareAndSwapUsed(true, false) {
		t.Fatal("Failed to release connection")
	}

	// Verify state is IDLE
	if state := cn.GetStateMachine().GetState(); state != pool.StateIdle {
		t.Errorf("Expected state IDLE, got %s", state)
	}

	// Now try to transition to UNUSABLE - should succeed
	_, err = cn.GetStateMachine().TryTransition([]pool.ConnState{pool.StateIdle}, pool.StateUnusable)
	if err != nil {
		t.Errorf("Failed to transition IDLE → UNUSABLE: %v", err)
	}

	// Verify state is UNUSABLE
	if state := cn.GetStateMachine().GetState(); state != pool.StateUnusable {
		t.Errorf("Expected state UNUSABLE, got %s", state)
	}
}

// TestReAuthWaitsForConnectionToBeIdle verifies that the re-auth worker
// waits for a connection to become IDLE before performing re-authentication.
func TestReAuthWaitsForConnectionToBeIdle(t *testing.T) {
	// Create a connection
	cn := pool.NewConn(nil)

	// Initialize to IDLE state
	cn.GetStateMachine().Transition(pool.StateInitializing)
	cn.GetStateMachine().Transition(pool.StateIdle)

	// Simulate connection being acquired (IDLE → IN_USE)
	if !cn.CompareAndSwapUsed(false, true) {
		t.Fatal("Failed to acquire connection")
	}

	// Track re-auth attempts
	var reAuthAttempts atomic.Int32
	var reAuthSucceeded atomic.Bool

	// Start a goroutine that tries to acquire the connection for re-auth
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		// Try to acquire for re-auth with timeout
		timeout := time.After(2 * time.Second)
		acquired := false

		for !acquired {
			select {
			case <-timeout:
				t.Error("Timeout waiting to acquire connection for re-auth")
				return
			default:
				reAuthAttempts.Add(1)
				// Try to atomically transition from IDLE to UNUSABLE
				_, err := cn.GetStateMachine().TryTransition([]pool.ConnState{pool.StateIdle}, pool.StateUnusable)
				if err == nil {
					// Successfully acquired
					acquired = true
					reAuthSucceeded.Store(true)
				} else {
					// Connection is still IN_USE, wait a bit
					time.Sleep(10 * time.Millisecond)
				}
			}
		}

		// Release the connection
		cn.GetStateMachine().Transition(pool.StateIdle)
	}()

	// Keep connection IN_USE for 500ms
	time.Sleep(500 * time.Millisecond)

	// Verify re-auth hasn't succeeded yet (connection is still IN_USE)
	if reAuthSucceeded.Load() {
		t.Error("Re-auth succeeded while connection was IN_USE")
	}

	// Verify there were multiple attempts
	attempts := reAuthAttempts.Load()
	if attempts < 2 {
		t.Errorf("Expected multiple re-auth attempts, got %d", attempts)
	}

	// Release connection (IN_USE → IDLE)
	if !cn.CompareAndSwapUsed(true, false) {
		t.Fatal("Failed to release connection")
	}

	// Wait for re-auth to complete
	wg.Wait()

	// Verify re-auth succeeded after connection became IDLE
	if !reAuthSucceeded.Load() {
		t.Error("Re-auth did not succeed after connection became IDLE")
	}

	// Verify final state is IDLE
	if state := cn.GetStateMachine().GetState(); state != pool.StateIdle {
		t.Errorf("Expected final state IDLE, got %s", state)
	}
}

// TestConcurrentReAuthAndUsage verifies that re-auth and normal usage
// don't interfere with each other.
func TestConcurrentReAuthAndUsage(t *testing.T) {
	// Create a connection
	cn := pool.NewConn(nil)

	// Initialize to IDLE state
	cn.GetStateMachine().Transition(pool.StateInitializing)
	cn.GetStateMachine().Transition(pool.StateIdle)

	var wg sync.WaitGroup
	var usageCount atomic.Int32
	var reAuthCount atomic.Int32

	// Goroutine 1: Simulate normal usage (acquire/release)
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 100; i++ {
			// Try to acquire
			if cn.CompareAndSwapUsed(false, true) {
				usageCount.Add(1)
				// Simulate work
				time.Sleep(1 * time.Millisecond)
				// Release
				cn.CompareAndSwapUsed(true, false)
			}
			time.Sleep(1 * time.Millisecond)
		}
	}()

	// Goroutine 2: Simulate re-auth attempts
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			// Try to acquire for re-auth
			_, err := cn.GetStateMachine().TryTransition([]pool.ConnState{pool.StateIdle}, pool.StateUnusable)
			if err == nil {
				reAuthCount.Add(1)
				// Simulate re-auth work
				time.Sleep(2 * time.Millisecond)
				// Release
				cn.GetStateMachine().Transition(pool.StateIdle)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()

	wg.Wait()

	// Verify both operations happened
	if usageCount.Load() == 0 {
		t.Error("No successful usage operations")
	}
	if reAuthCount.Load() == 0 {
		t.Error("No successful re-auth operations")
	}

	t.Logf("Usage operations: %d, Re-auth operations: %d", usageCount.Load(), reAuthCount.Load())

	// Verify final state is IDLE
	if state := cn.GetStateMachine().GetState(); state != pool.StateIdle {
		t.Errorf("Expected final state IDLE, got %s", state)
	}
}

// TestReAuthRespectsClosed verifies that re-auth doesn't happen on closed connections.
func TestReAuthRespectsClosed(t *testing.T) {
	// Create a connection
	cn := pool.NewConn(nil)

	// Initialize to IDLE state
	cn.GetStateMachine().Transition(pool.StateInitializing)
	cn.GetStateMachine().Transition(pool.StateIdle)

	// Close the connection
	cn.GetStateMachine().Transition(pool.StateClosed)

	// Try to transition to UNUSABLE - should fail
	_, err := cn.GetStateMachine().TryTransition([]pool.ConnState{pool.StateIdle}, pool.StateUnusable)
	if err == nil {
		t.Error("Expected error when trying to transition CLOSED → UNUSABLE, but got none")
	}

	// Verify state is still CLOSED
	if state := cn.GetStateMachine().GetState(); state != pool.StateClosed {
		t.Errorf("Expected state to remain CLOSED, got %s", state)
	}
}
