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

	// One relax holder whose deadline is already in the past. Use a far-past
	// deadline (getEffective* reads a cached clock with up to 50ms staleness, so a
	// near-past deadline could read as unexpired and flake).
	cn.SetRelaxedTimeoutWithDeadline(time.Second, time.Second, time.Now().Add(-time.Minute))

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

// TestRelaxedTimeoutExpiryKeepsNotificationWindow reproduces the notification-vs-
// handoff clobber (conn.go:566): a notification relaxation is outstanding on the
// SAME conn as an expired handoff deadline. The old expiry path full-cleared the
// window, wiping the notification's relaxation. The fixed path retires only the
// deadline holder, so the notification window survives. Deterministic: no
// goroutines, and a far-past deadline that the cached clock always reads expired.
func TestRelaxedTimeoutExpiryKeepsNotificationWindow(t *testing.T) {
	netConn := &net.TCPConn{}
	cn := NewConn(netConn)
	defer cn.Close()

	// Handoff relaxation with an already-expired deadline: counter=1, deadline past.
	cn.SetRelaxedTimeoutWithDeadline(time.Second, time.Second, time.Now().Add(-time.Minute))
	// Notification relaxation arrives on the same conn (no deadline): counter=2,
	// timeouts overwritten to 2s, deadline still the expired handoff one.
	cn.SetRelaxedTimeout(2*time.Second, 2*time.Second)
	if count := cn.relaxedCounter.Load(); count != 2 {
		t.Fatalf("relaxed counter = %d after handoff+notification, want 2", count)
	}

	// First Effective* observes the expired deadline and triggers expiry. Per the
	// method contract it returns the normal timeout for THIS call.
	if got := cn.EffectiveReadTimeout(time.Millisecond); got != time.Millisecond {
		t.Fatalf("first effective read = %v, want normal 1ms (deadline just expired)", got)
	}

	// Expiry retired only the handoff holder: the notification window must remain.
	if count := cn.relaxedCounter.Load(); count != 1 {
		t.Fatalf("relaxed counter = %d after expiry, want 1 (notification holder kept)", count)
	}
	if !cn.HasRelaxedTimeout() {
		t.Fatal("notification relaxation was clobbered by the handoff deadline expiry")
	}
	if got := cn.EffectiveReadTimeout(time.Millisecond); got != 2*time.Second {
		t.Fatalf("effective read after expiry = %v, want notification relaxed 2s", got)
	}
	if got := cn.EffectiveWriteTimeout(time.Millisecond); got != 2*time.Second {
		t.Fatalf("effective write after expiry = %v, want notification relaxed 2s", got)
	}

	// The surviving holder still clears cleanly on its explicit unrelax.
	cn.ClearRelaxedTimeout()
	if count := cn.relaxedCounter.Load(); count != 0 {
		t.Fatalf("relaxed counter = %d after clearing the notification holder, want 0", count)
	}
	if cn.HasRelaxedTimeout() {
		t.Fatal("relaxation still active after the last holder cleared")
	}
}

// TestRelaxedTimeoutOverlappingHandoffsClear guards the other direction: two
// overlapping deadline-scoped handoffs on one conn must NOT leave it permanently
// relaxed. The holder guard means the re-arm replaces the deadline in place
// without stacking a holder, so the surviving deadline's expiry clears the window.
func TestRelaxedTimeoutOverlappingHandoffsClear(t *testing.T) {
	netConn := &net.TCPConn{}
	cn := NewConn(netConn)
	defer cn.Close()

	// Handoff A, then handoff B re-arms before A's deadline. One holder, deadline B.
	cn.SetRelaxedTimeoutWithDeadline(time.Second, time.Second, time.Now().Add(time.Hour))
	cn.SetRelaxedTimeoutWithDeadline(2*time.Second, 2*time.Second, time.Now().Add(-time.Minute))
	if count := cn.relaxedCounter.Load(); count != 1 {
		t.Fatalf("relaxed counter = %d after overlapping handoffs, want 1 (re-arm reuses holder)", count)
	}

	// B's deadline is expired: the next Effective* retires the only holder and the
	// window clears — the conn does not stay relaxed for the rest of its life.
	if got := cn.EffectiveReadTimeout(time.Millisecond); got != time.Millisecond {
		t.Fatalf("effective read = %v, want normal 1ms after both handoffs expired", got)
	}
	if count := cn.relaxedCounter.Load(); count != 0 {
		t.Fatalf("relaxed counter = %d after expiry, want 0 (no leaked holder)", count)
	}
	if cn.HasRelaxedTimeout() {
		t.Fatal("conn left permanently relaxed after overlapping handoffs expired")
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
