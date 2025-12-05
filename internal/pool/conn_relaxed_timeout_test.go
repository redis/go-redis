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
