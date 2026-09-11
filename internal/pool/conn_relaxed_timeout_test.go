package pool

import (
	"net"
	"sync"
	"testing"
	"time"
)

// Snapshot accessors for tests: the relaxed-timeout window is published as one
// atomic pointer (nil == no relaxation), so read its fields through the snapshot.
func (cn *Conn) relaxedHolderCount() int32 {
	if s := cn.relaxed.Load(); s != nil {
		return s.count
	}
	return 0
}

func (cn *Conn) relaxedReadNs() int64 {
	if s := cn.relaxed.Load(); s != nil {
		return s.readNs
	}
	return 0
}

func (cn *Conn) relaxedWriteNs() int64 {
	if s := cn.relaxed.Load(); s != nil {
		return s.writeNs
	}
	return 0
}

func (cn *Conn) relaxedDeadline() int64 {
	if s := cn.relaxed.Load(); s != nil {
		return s.deadlineNs
	}
	return 0
}

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
	if count := cn.relaxedHolderCount(); count != 3 {
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
	if count := cn.relaxedHolderCount(); count != 0 {
		t.Errorf("Expected relaxed counter to be 0 after clearing, got %d", count)
	}
	if timeout := cn.relaxedReadNs(); timeout != 0 {
		t.Errorf("Expected relaxed read timeout to be 0, got %d", timeout)
	}
	if timeout := cn.relaxedWriteNs(); timeout != 0 {
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
	if count := cn.relaxedHolderCount(); count != 0 {
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
// handoff clobber: a notification relaxation is outstanding on the SAME conn as an
// expired handoff deadline. The old expiry path full-cleared the window, wiping
// the notification's relaxation. The fixed path retires only the deadline holder,
// so the notification window survives — and, because the whole window is one
// snapshot, the very call that triggers the expiry re-reads it and returns the
// surviving relaxed timeout rather than falling back to normal. Deterministic: no
// goroutines, and a far-past deadline the cached clock always reads expired.
func TestRelaxedTimeoutExpiryKeepsNotificationWindow(t *testing.T) {
	netConn := &net.TCPConn{}
	cn := NewConn(netConn)
	defer cn.Close()

	// Handoff relaxation with an already-expired deadline: counter=1, deadline past.
	cn.SetRelaxedTimeoutWithDeadline(time.Second, time.Second, time.Now().Add(-time.Minute))
	// Notification relaxation arrives on the same conn (no deadline): counter=2,
	// timeouts overwritten to 2s, deadline still the expired handoff one.
	cn.SetRelaxedTimeout(2*time.Second, 2*time.Second)
	if count := cn.relaxedHolderCount(); count != 2 {
		t.Fatalf("relaxed counter = %d after handoff+notification, want 2", count)
	}

	// The first Effective* observes the expired deadline, retires the handoff
	// holder, then re-reads the surviving notification window: it returns the
	// notification's 2s, NOT normal (a relaxed window is still active).
	if got := cn.EffectiveReadTimeout(time.Millisecond); got != 2*time.Second {
		t.Fatalf("first effective read = %v, want surviving notification relaxed 2s", got)
	}

	// Expiry retired only the handoff holder: the notification window must remain.
	if count := cn.relaxedHolderCount(); count != 1 {
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
	if count := cn.relaxedHolderCount(); count != 0 {
		t.Fatalf("relaxed counter = %d after clearing the notification holder, want 0", count)
	}
	if cn.HasRelaxedTimeout() {
		t.Fatal("relaxation still active after the last holder cleared")
	}
}

// TestHasRelaxedTimeoutExpiresDeadlineAndReadsSurvivor pins r3934148728: called on a
// conn whose handoff DEADLINE holder has expired while a notification holder survives,
// HasRelaxedTimeout must retire the deadline holder and re-read, reporting true — not
// return false on the stale snapshot just because no Effective* call has triggered the
// expiry yet. (TestRelaxedTimeoutExpiryKeepsNotificationWindow only reaches
// HasRelaxedTimeout AFTER an Effective* already expired the deadline.)
func TestHasRelaxedTimeoutExpiresDeadlineAndReadsSurvivor(t *testing.T) {
	netConn := &net.TCPConn{}
	cn := NewConn(netConn)
	defer cn.Close()

	// Expired handoff deadline holder + a surviving notification holder (no deadline).
	cn.SetRelaxedTimeoutWithDeadline(time.Second, time.Second, time.Now().Add(-time.Minute))
	cn.SetRelaxedTimeout(2*time.Second, 2*time.Second)
	if count := cn.relaxedHolderCount(); count != 2 {
		t.Fatalf("relaxed counter = %d, want 2", count)
	}

	// Call HasRelaxedTimeout FIRST (before any Effective*): it must expire the deadline
	// holder and re-read, seeing the surviving notification holder -> true.
	if !cn.HasRelaxedTimeout() {
		t.Fatal("HasRelaxedTimeout returned false while a notification holder survives the expired deadline (r3934148728)")
	}
	// The expiry retired only the deadline holder.
	if count := cn.relaxedHolderCount(); count != 1 {
		t.Fatalf("relaxed counter = %d after HasRelaxedTimeout expiry, want 1 (notification holder kept)", count)
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
	if count := cn.relaxedHolderCount(); count != 1 {
		t.Fatalf("relaxed counter = %d after overlapping handoffs, want 1 (re-arm reuses holder)", count)
	}

	// B's deadline is expired: the next Effective* retires the only holder and the
	// window clears — the conn does not stay relaxed for the rest of its life.
	if got := cn.EffectiveReadTimeout(time.Millisecond); got != time.Millisecond {
		t.Fatalf("effective read = %v, want normal 1ms after both handoffs expired", got)
	}
	if count := cn.relaxedHolderCount(); count != 0 {
		t.Fatalf("relaxed counter = %d after expiry, want 0 (no leaked holder)", count)
	}
	if cn.HasRelaxedTimeout() {
		t.Fatal("conn left permanently relaxed after overlapping handoffs expired")
	}
}

// TestRelaxedTimeoutConcurrentSetAndRead schedules the race the snapshot design
// must handle. One setter re-installs a deadline window many times, with
// alternating past and future deadlines. At the same time 64 readers call
// Effective*, which can observe an expiry and call expireRelaxedTimeout. Under
// -race this must stay clean, because the reads and the compare-and-swaps operate
// on one atomic pointer. The count must never go negative and must not drift above
// one holder. A relaxation set after the storm must still take effect.
func TestRelaxedTimeoutConcurrentSetAndRead(t *testing.T) {
	netConn := &net.TCPConn{}
	cn := NewConn(netConn)
	defer cn.Close()

	const iters = 500
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < iters; i++ {
			if i%2 == 0 {
				cn.SetRelaxedTimeoutWithDeadline(time.Second, time.Second, time.Now().Add(-time.Minute))
			} else {
				cn.SetRelaxedTimeoutWithDeadline(time.Second, time.Second, time.Now().Add(time.Hour))
			}
		}
	}()
	for r := 0; r < 64; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				cn.EffectiveReadTimeout(time.Millisecond)
				cn.EffectiveWriteTimeout(time.Millisecond)
				if c := cn.relaxedHolderCount(); c < 0 {
					t.Errorf("relaxed counter went negative (%d) under concurrent set/read", c)
					return
				}
			}
		}()
	}
	wg.Wait()

	// One setter never stacks holders (the deadline slot is reused), so the counter
	// is always 0 or 1 — never negative, never drifting up.
	if c := cn.relaxedHolderCount(); c < 0 || c > 1 {
		t.Fatalf("relaxed counter = %d after storm, want 0 or 1", c)
	}
	// Drain any residual holder, then prove a fresh relaxation still takes effect.
	for cn.relaxedHolderCount() > 0 {
		cn.ClearRelaxedTimeout()
	}
	cn.SetRelaxedTimeout(2*time.Second, 2*time.Second)
	if !cn.HasRelaxedTimeout() {
		t.Fatal("relaxation not restored after concurrent set/read storm — state wedged")
	}
	if got := cn.EffectiveReadTimeout(time.Millisecond); got != 2*time.Second {
		t.Fatalf("effective read = %v, want restored relaxed 2s", got)
	}
}

func TestRelaxedTimeoutCounterRaceCondition(t *testing.T) {
	netConn := &net.TCPConn{}
	cn := NewConn(netConn)
	defer cn.Close()

	// Set relaxed timeout once
	cn.SetRelaxedTimeout(time.Second, time.Second)

	// Verify counter is 1
	if count := cn.relaxedHolderCount(); count != 1 {
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
	if count := cn.relaxedHolderCount(); count != 0 {
		t.Errorf("Expected relaxed counter to be 0 after concurrent clearing, got %d", count)
	}

	// Verify timeouts are actually cleared
	if timeout := cn.relaxedReadNs(); timeout != 0 {
		t.Errorf("Expected relaxed read timeout to be cleared, got %d", timeout)
	}
	if timeout := cn.relaxedWriteNs(); timeout != 0 {
		t.Errorf("Expected relaxed write timeout to be cleared, got %d", timeout)
	}
	if deadline := cn.relaxedDeadline(); deadline != 0 {
		t.Errorf("Expected relaxed deadline to be cleared, got %d", deadline)
	}
}
