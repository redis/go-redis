package pool

import (
	"net"
	"sync"
	"testing"
	"time"
)

// TestConcurrentRelaxedTimeoutClearing tests the race condition fix in ClearRelaxedTimeout
func TestConcurrentRelaxedTimeoutClearing(t *testing.T) {
	// Create a dummy connection for testing
	netConn := &net.TCPConn{}
	cn := NewConn(netConn)
	defer cn.Close()

	// Set relaxed timeout multiple times to increase counter
	cn.SetRelaxedTimeout(time.Second, time.Second)
	cn.SetRelaxedTimeout(time.Second, time.Second)
	cn.SetRelaxedTimeout(time.Second, time.Second)

	// Verify counter is 3
	if count := cn.relaxedCounter.Load(); count != 3 {
		t.Errorf("Expected relaxed counter to be 3, got %d", count)
	}

	// Clear timeouts concurrently to test race condition fix
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cn.ClearRelaxedTimeout()
		}()
	}
	wg.Wait()

	// Verify counter is 0 and timeouts are cleared
	if count := cn.relaxedCounter.Load(); count != 0 {
		t.Errorf("Expected relaxed counter to be 0 after clearing, got %d", count)
	}
	if timeout := cn.relaxedReadTimeoutNs.Load(); timeout != 0 {
		t.Errorf("Expected relaxed read timeout to be 0, got %d", timeout)
	}
	if timeout := cn.relaxedWriteTimeoutNs.Load(); timeout != 0 {
		t.Errorf("Expected relaxed write timeout to be 0, got %d", timeout)
	}
}

// TestRelaxedTimeoutCounterRaceCondition tests the specific race condition scenario
// TestRelaxedTimeoutExpiryConcurrentNoNegative reproduces the full-duplex race:
// a reader, a writer, and the drain backstop all call Effective*Timeout on one
// connection whose relaxed deadline has passed. The old expiry path decremented
// the counter per call, so a single expired deadline was decremented many times
// and left the counter negative — after which a later SetRelaxedTimeout could not
// restore relaxation. The deadline-triggered CAS reset must fire exactly once.
func TestRelaxedTimeoutExpiryConcurrentNoNegative(t *testing.T) {
	netConn := &net.TCPConn{}
	cn := NewConn(netConn)
	defer cn.Close()

	// One relax holder whose deadline is already in the past.
	cn.SetRelaxedTimeoutWithDeadline(time.Second, time.Second, time.Now().Add(-time.Millisecond))

	var wg sync.WaitGroup
	for i := 0; i < 64; i++ {
		wg.Add(3)
		go func() { defer wg.Done(); cn.EffectiveReadTimeout(time.Second) }()  // reader
		go func() { defer wg.Done(); cn.EffectiveWriteTimeout(time.Second) }() // writer
		go func() { defer wg.Done(); cn.EffectiveReadTimeout(time.Second) }()  // drain backstop
	}
	wg.Wait()

	// Reset fired once: counter is 0, never negative.
	if count := cn.relaxedCounter.Load(); count != 0 {
		t.Fatalf("relaxed counter = %d after concurrent expiry, want 0 (never negative)", count)
	}
	// A fresh relaxation must take effect — a wedged negative counter would make
	// HasRelaxedTimeout return false and the effective timeout stay normal.
	cn.SetRelaxedTimeout(2*time.Second, 2*time.Second)
	if !cn.HasRelaxedTimeout() {
		t.Fatal("relaxation not restored after an expiry storm — counter was wedged")
	}
	if got := cn.EffectiveReadTimeout(time.Millisecond); got != 2*time.Second {
		t.Fatalf("effective read timeout = %v, want restored relaxed 2s", got)
	}
}

func TestRelaxedTimeoutCounterRaceCondition(t *testing.T) {
	netConn := &net.TCPConn{}
	cn := NewConn(netConn)
	defer cn.Close()

	// Set relaxed timeout once
	cn.SetRelaxedTimeout(time.Second, time.Second)

	// Verify counter is 1
	if count := cn.relaxedCounter.Load(); count != 1 {
		t.Errorf("Expected relaxed counter to be 1, got %d", count)
	}

	// Test concurrent clearing with race condition scenario
	var wg sync.WaitGroup

	// Multiple goroutines try to clear simultaneously
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cn.ClearRelaxedTimeout()
		}()
	}
	wg.Wait()

	// Verify final state is consistent
	if count := cn.relaxedCounter.Load(); count != 0 {
		t.Errorf("Expected relaxed counter to be 0 after concurrent clearing, got %d", count)
	}

	// Verify timeouts are actually cleared
	if timeout := cn.relaxedReadTimeoutNs.Load(); timeout != 0 {
		t.Errorf("Expected relaxed read timeout to be cleared, got %d", timeout)
	}
	if timeout := cn.relaxedWriteTimeoutNs.Load(); timeout != 0 {
		t.Errorf("Expected relaxed write timeout to be cleared, got %d", timeout)
	}
	if deadline := cn.relaxedDeadlineNs.Load(); deadline != 0 {
		t.Errorf("Expected relaxed deadline to be cleared, got %d", deadline)
	}
}
