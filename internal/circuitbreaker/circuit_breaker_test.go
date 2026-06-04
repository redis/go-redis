package circuitbreaker

import (
	"sync"
	"testing"
	"time"
)

func TestCircuitBreaker_InitialState(t *testing.T) {
	cb := New(DefaultConfig())

	if cb.State() != StateClosed {
		t.Errorf("expected initial state to be Closed, got %v", cb.State())
	}
}

func TestCircuitBreaker_OpenAfterFailures(t *testing.T) {
	config := Config{
		FailureThreshold: 3,
		SuccessThreshold: 2,
		OpenTimeout:      100 * time.Millisecond,
	}
	cb := New(config)

	// Record failures
	for i := 0; i < 3; i++ {
		cb.RecordFailure()
	}

	if cb.State() != StateOpen {
		t.Errorf("expected state to be Open after %d failures, got %v", 3, cb.State())
	}
}

func TestCircuitBreaker_TransitionToHalfOpen(t *testing.T) {
	config := Config{
		FailureThreshold: 2,
		SuccessThreshold: 1,
		OpenTimeout:      50 * time.Millisecond,
	}
	cb := New(config)

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	if cb.State() != StateOpen {
		t.Fatalf("expected state to be Open, got %v", cb.State())
	}

	// Wait for timeout
	time.Sleep(60 * time.Millisecond)

	// CheckState should transition to half-open
	state := cb.CheckState()
	if state != StateHalfOpen {
		t.Errorf("expected state to be HalfOpen after timeout, got %v", state)
	}
}

func TestCircuitBreaker_CloseAfterSuccesses(t *testing.T) {
	config := Config{
		FailureThreshold: 2,
		SuccessThreshold: 2,
		OpenTimeout:      50 * time.Millisecond,
	}
	cb := New(config)

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	// Wait for timeout and transition to half-open
	time.Sleep(60 * time.Millisecond)
	cb.CheckState()

	// Record successes
	cb.RecordSuccess()
	cb.RecordSuccess()

	if cb.State() != StateClosed {
		t.Errorf("expected state to be Closed after successes, got %v", cb.State())
	}
}

func TestCircuitBreaker_ReopenOnFailureInHalfOpen(t *testing.T) {
	config := Config{
		FailureThreshold: 2,
		SuccessThreshold: 2,
		OpenTimeout:      50 * time.Millisecond,
	}
	cb := New(config)

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	// Wait for timeout and transition to half-open
	time.Sleep(60 * time.Millisecond)
	cb.CheckState()

	if cb.State() != StateHalfOpen {
		t.Fatalf("expected state to be HalfOpen, got %v", cb.State())
	}

	// Record a failure - should reopen
	cb.RecordFailure()

	if cb.State() != StateOpen {
		t.Errorf("expected state to be Open after failure in half-open, got %v", cb.State())
	}
}

func TestCircuitBreaker_IsAllowed(t *testing.T) {
	config := Config{
		FailureThreshold: 2,
		SuccessThreshold: 2,
		OpenTimeout:      50 * time.Millisecond,
	}
	cb := New(config)

	// Should be allowed when closed
	if !cb.IsAllowed() {
		t.Error("expected IsAllowed to return true when closed")
	}

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	// Should not be allowed when open
	if cb.IsAllowed() {
		t.Error("expected IsAllowed to return false when open")
	}
}

func TestCircuitBreaker_MaxHalfOpenRequests(t *testing.T) {
	config := Config{
		FailureThreshold:    2,
		SuccessThreshold:    3,
		MaxHalfOpenRequests: 2,
		OpenTimeout:         50 * time.Millisecond,
	}
	cb := New(config)

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	// Wait for timeout
	time.Sleep(60 * time.Millisecond)

	// First two requests should be allowed
	if !cb.IsAllowed() {
		t.Error("first request should be allowed in half-open")
	}
	if !cb.IsAllowed() {
		t.Error("second request should be allowed in half-open")
	}

	// Third request should be rejected
	if cb.IsAllowed() {
		t.Error("third request should be rejected (max half-open requests)")
	}
}

func TestCircuitBreaker_ReleaseHalfOpen(t *testing.T) {
	config := Config{
		FailureThreshold:    2,
		SuccessThreshold:    3,
		MaxHalfOpenRequests: 2,
		OpenTimeout:         50 * time.Millisecond,
	}
	cb := New(config)

	// Open the circuit, then wait for the half-open window.
	cb.RecordFailure()
	cb.RecordFailure()
	time.Sleep(60 * time.Millisecond)

	// Reserve both half-open slots.
	if !cb.IsAllowed() {
		t.Fatal("first request should be allowed in half-open")
	}
	if !cb.IsAllowed() {
		t.Fatal("second request should be allowed in half-open")
	}
	if cb.IsAllowed() {
		t.Fatal("third request should be rejected before release")
	}

	// Releasing a reserved slot should let a subsequent probe through.
	cb.ReleaseHalfOpen()
	if !cb.IsAllowed() {
		t.Error("request should be allowed after ReleaseHalfOpen")
	}

	// Release must not drive the counter negative or admit extra probes.
	cb.ReleaseHalfOpen()
	cb.ReleaseHalfOpen()
	if cb.requests.Load() < 0 {
		t.Errorf("requests counter must not go negative, got %d", cb.requests.Load())
	}

	// ReleaseHalfOpen is a no-op outside the half-open state.
	cb.Reset()
	cb.ReleaseHalfOpen()
	if cb.requests.Load() != 0 {
		t.Errorf("expected requests to remain 0 when closed, got %d", cb.requests.Load())
	}
}

func TestCircuitBreaker_OnStateChange(t *testing.T) {
	config := Config{
		FailureThreshold: 2,
		SuccessThreshold: 1,
		OpenTimeout:      50 * time.Millisecond,
	}
	cb := New(config)

	var transitions []struct{ old, new State }
	cb.OnStateChange(func(oldState, newState State) {
		transitions = append(transitions, struct{ old, new State }{oldState, newState})
	})

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	// Wait and transition to half-open
	time.Sleep(60 * time.Millisecond)
	cb.CheckState()

	// Close the circuit
	cb.RecordSuccess()

	if len(transitions) != 3 {
		t.Fatalf("expected 3 transitions, got %d", len(transitions))
	}

	// Closed -> Open
	if transitions[0].old != StateClosed || transitions[0].new != StateOpen {
		t.Errorf("expected Closed->Open, got %v->%v", transitions[0].old, transitions[0].new)
	}

	// Open -> HalfOpen
	if transitions[1].old != StateOpen || transitions[1].new != StateHalfOpen {
		t.Errorf("expected Open->HalfOpen, got %v->%v", transitions[1].old, transitions[1].new)
	}

	// HalfOpen -> Closed
	if transitions[2].old != StateHalfOpen || transitions[2].new != StateClosed {
		t.Errorf("expected HalfOpen->Closed, got %v->%v", transitions[2].old, transitions[2].new)
	}
}

func TestCircuitBreaker_CallbackObservesSuccessCountOnClose(t *testing.T) {
	config := Config{
		FailureThreshold: 2,
		SuccessThreshold: 2,
		OpenTimeout:      50 * time.Millisecond,
	}
	cb := New(config)

	var closeSuccesses int32 = -1
	cb.OnStateChange(func(oldState, newState State) {
		if oldState == StateHalfOpen && newState == StateClosed {
			closeSuccesses = cb.Stats().Successes
		}
	})

	// Open the circuit, wait for the timeout, then transition to half-open.
	cb.RecordFailure()
	cb.RecordFailure()
	time.Sleep(60 * time.Millisecond)
	cb.CheckState()

	// Record enough successes to close the circuit.
	cb.RecordSuccess()
	cb.RecordSuccess()

	if cb.State() != StateClosed {
		t.Fatalf("expected state to be Closed, got %v", cb.State())
	}
	// The callback must see the success count that triggered the close, not the
	// post-reset value of 0.
	if closeSuccesses != int32(config.SuccessThreshold) {
		t.Errorf("expected callback to observe %d successes, got %d",
			config.SuccessThreshold, closeSuccesses)
	}
}

func TestCircuitBreaker_Reset(t *testing.T) {
	config := Config{
		FailureThreshold: 2,
		SuccessThreshold: 1,
		OpenTimeout:      1 * time.Hour, // Long timeout
	}
	cb := New(config)

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	if cb.State() != StateOpen {
		t.Fatalf("expected state to be Open, got %v", cb.State())
	}

	// Reset
	cb.Reset()

	if cb.State() != StateClosed {
		t.Errorf("expected state to be Closed after reset, got %v", cb.State())
	}

	stats := cb.Stats()
	if stats.Failures != 0 || stats.Successes != 0 {
		t.Errorf("expected counters to be reset, got failures=%d, successes=%d",
			stats.Failures, stats.Successes)
	}
}

func TestCircuitBreaker_ResetNotifiesCallbacks(t *testing.T) {
	config := Config{
		FailureThreshold: 2,
		SuccessThreshold: 1,
		OpenTimeout:      1 * time.Hour,
	}
	cb := New(config)

	var transitions []struct{ old, new State }
	cb.OnStateChange(func(oldState, newState State) {
		transitions = append(transitions, struct{ old, new State }{oldState, newState})
	})

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	if len(transitions) != 1 {
		t.Fatalf("expected 1 transition (Closed->Open), got %d", len(transitions))
	}

	// Reset should notify callback
	cb.Reset()

	if len(transitions) != 2 {
		t.Fatalf("expected 2 transitions after reset, got %d", len(transitions))
	}

	// Verify Open -> Closed transition
	if transitions[1].old != StateOpen || transitions[1].new != StateClosed {
		t.Errorf("expected Open->Closed, got %v->%v", transitions[1].old, transitions[1].new)
	}

	// Reset when already closed should NOT notify
	cb.Reset()
	if len(transitions) != 2 {
		t.Errorf("expected no callback when resetting already-closed circuit, got %d transitions", len(transitions))
	}
}

func TestCircuitBreaker_ConcurrentAccess(t *testing.T) {
	config := Config{
		FailureThreshold: 100,
		SuccessThreshold: 50,
		OpenTimeout:      100 * time.Millisecond,
	}
	cb := New(config)

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			cb.RecordFailure()
		}()
		go func() {
			defer wg.Done()
			cb.RecordSuccess()
		}()
	}
	wg.Wait()

	// Should not panic and state should be valid
	state := cb.State()
	if state != StateClosed && state != StateOpen && state != StateHalfOpen {
		t.Errorf("invalid state: %v", state)
	}
}

func TestCircuitBreaker_Stats(t *testing.T) {
	cb := New(DefaultConfig())

	cb.RecordFailure()
	cb.RecordFailure()

	stats := cb.Stats()
	if stats.Failures != 2 {
		t.Errorf("expected 2 failures, got %d", stats.Failures)
	}
	if stats.LastFailureTime.IsZero() {
		t.Error("expected LastFailureTime to be set")
	}
}

// TestCircuitBreaker_HalfOpenCountersAreCleanOnReentry asserts the invariant
// that successes and requests start at 0 every time the breaker enters the
// HalfOpen state, regardless of what activity preceded it. The transitions
// out of HalfOpen and out of Open each zero those counters; this test guards
// against a future change that lets them carry over from a previous cycle.
func TestCircuitBreaker_HalfOpenCountersAreCleanOnReentry(t *testing.T) {
	config := Config{
		FailureThreshold: 2,
		SuccessThreshold: 2,
		OpenTimeout:      50 * time.Millisecond,
	}
	cb := New(config)

	// Cycle 1: drive the breaker through Open -> HalfOpen -> Closed so the
	// counters have non-trivial values before the next failure burst.
	cb.RecordFailure()
	cb.RecordFailure()
	time.Sleep(60 * time.Millisecond)
	cb.CheckState() // -> HalfOpen
	cb.RecordSuccess()
	cb.RecordSuccess() // -> Closed (zeroes failures, successes, requests)

	if cb.State() != StateClosed {
		t.Fatalf("setup: expected Closed after first cycle, got %v", cb.State())
	}

	// Run plenty of successful traffic in Closed; this must not leak into
	// the successes counter (which is only meaningful in HalfOpen).
	for i := 0; i < 50; i++ {
		cb.RecordSuccess()
	}
	if s := cb.Stats().Successes; s != 0 {
		t.Errorf("successes must remain 0 in Closed, got %d", s)
	}

	// Cycle 2: drive Closed -> Open. After the transition both half-open
	// counters must be 0 so the next HalfOpen cycle starts clean.
	cb.RecordFailure()
	cb.RecordFailure() // -> Open
	if cb.State() != StateOpen {
		t.Fatalf("expected Open after threshold, got %v", cb.State())
	}
	stats := cb.Stats()
	if stats.Successes != 0 {
		t.Errorf("successes must be 0 on entry to Open, got %d", stats.Successes)
	}
	if stats.Requests != 0 {
		t.Errorf("requests must be 0 on entry to Open, got %d", stats.Requests)
	}

	// And the first success after Open -> HalfOpen must count as 1, not as
	// "1 + whatever leaked from before".
	time.Sleep(60 * time.Millisecond)
	cb.CheckState() // -> HalfOpen
	cb.RecordSuccess()
	if s := cb.Stats().Successes; s != 1 {
		t.Errorf("first success in HalfOpen must be 1, got %d", s)
	}
}

