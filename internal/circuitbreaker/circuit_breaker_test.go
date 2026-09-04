package circuitbreaker

import (
	"sync"
	"sync/atomic"
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

// transitionRecorder captures state-change callbacks (delivered asynchronously)
// in a race-safe way and lets a test wait for a given count.
type transitionRecorder struct {
	mu    sync.Mutex
	items []recordedTransition
}

type recordedTransition struct {
	oldState, newState State
	stats              Stats
}

func (r *transitionRecorder) record(oldState, newState State, stats Stats) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.items = append(r.items, recordedTransition{oldState, newState, stats})
}

func (r *transitionRecorder) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.items)
}

func (r *transitionRecorder) at(i int) recordedTransition {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.items[i]
}

func (r *transitionRecorder) waitFor(t *testing.T, n int) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if r.len() >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d transitions, got %d", n, r.len())
}

func TestCircuitBreaker_OnStateChange(t *testing.T) {
	config := Config{
		FailureThreshold: 2,
		SuccessThreshold: 1,
		OpenTimeout:      50 * time.Millisecond,
	}
	cb := New(config)

	var rec transitionRecorder
	cb.OnStateChange(rec.record)

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()

	// Wait and transition to half-open
	time.Sleep(60 * time.Millisecond)
	cb.CheckState()

	// Close the circuit
	cb.RecordSuccess()

	// Callbacks are delivered asynchronously in transition order.
	rec.waitFor(t, 3)

	if got := rec.at(0); got.oldState != StateClosed || got.newState != StateOpen {
		t.Errorf("expected Closed->Open, got %v->%v", got.oldState, got.newState)
	}
	if got := rec.at(1); got.oldState != StateOpen || got.newState != StateHalfOpen {
		t.Errorf("expected Open->HalfOpen, got %v->%v", got.oldState, got.newState)
	}
	if got := rec.at(2); got.oldState != StateHalfOpen || got.newState != StateClosed {
		t.Errorf("expected HalfOpen->Closed, got %v->%v", got.oldState, got.newState)
	}
}

// TestCircuitBreaker_PanickingCallbackDoesNotStarveOthers pins that one
// OnStateChange callback panicking must not stop callbacks registered after
// it from being notified — neither for that transition nor for later ones,
// since the panicking callback is never removed and panics again on every
// subsequent delivery.
func TestCircuitBreaker_PanickingCallbackDoesNotStarveOthers(t *testing.T) {
	config := Config{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		OpenTimeout:      time.Hour,
	}
	cb := New(config)

	var rec transitionRecorder
	// Registered first and panics on every delivery.
	cb.OnStateChange(func(oldState, newState State, stats Stats) {
		panic("boom")
	})
	// Registered second: must still see every transition despite the earlier
	// callback's panic.
	cb.OnStateChange(rec.record)

	cb.RecordFailure() // Closed -> Open
	rec.waitFor(t, 1)
	if got := rec.at(0); got.oldState != StateClosed || got.newState != StateOpen {
		t.Errorf("expected Closed->Open, got %v->%v", got.oldState, got.newState)
	}

	cb.Reset() // Open -> Closed
	rec.waitFor(t, 2)
	if got := rec.at(1); got.oldState != StateOpen || got.newState != StateClosed {
		t.Errorf("expected Open->Closed, got %v->%v", got.oldState, got.newState)
	}
}

func TestCircuitBreaker_CallbackObservesSuccessCountOnClose(t *testing.T) {
	config := Config{
		FailureThreshold: 2,
		SuccessThreshold: 2,
		OpenTimeout:      50 * time.Millisecond,
	}
	cb := New(config)

	var rec transitionRecorder
	cb.OnStateChange(rec.record)

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

	// Find the HalfOpen->Closed transition; its carried snapshot must show the
	// success count that triggered the close, not the post-reset value of 0 —
	// the snapshot is taken at the transition, so async delivery cannot lose it.
	rec.waitFor(t, 3) // Closed->Open, Open->HalfOpen, HalfOpen->Closed
	var closeSuccesses int32 = -1
	for i := 0; i < rec.len(); i++ {
		if tr := rec.at(i); tr.oldState == StateHalfOpen && tr.newState == StateClosed {
			closeSuccesses = tr.stats.Successes
		}
	}
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

	var rec transitionRecorder
	cb.OnStateChange(rec.record)

	// Open the circuit
	cb.RecordFailure()
	cb.RecordFailure()
	rec.waitFor(t, 1) // Closed->Open

	// Reset should notify callback
	cb.Reset()
	rec.waitFor(t, 2)

	// Verify Open -> Closed transition
	if got := rec.at(1); got.oldState != StateOpen || got.newState != StateClosed {
		t.Errorf("expected Open->Closed, got %v->%v", got.oldState, got.newState)
	}

	// Reset when already closed should NOT notify. Give any stray delivery a
	// beat to land, then confirm the count did not grow.
	cb.Reset()
	time.Sleep(20 * time.Millisecond)
	if got := rec.len(); got != 2 {
		t.Errorf("expected no callback when resetting already-closed circuit, got %d transitions", got)
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

func TestCircuitBreaker_FailureCounterClearedBeforeCloseIsPublished(t *testing.T) {
	config := Config{
		FailureThreshold: 2,
		SuccessThreshold: 1,
		OpenTimeout:      50 * time.Millisecond,
	}
	cb := New(config)

	// A failure that lands the instant Closed becomes visible (here: from
	// the HalfOpen -> Closed callback, which runs while the state is already
	// Closed) must count against a fresh failure counter. If the counter
	// still holds the count that opened the circuit, this single failure
	// re-opens it immediately.
	fired := make(chan struct{})
	cb.OnStateChange(func(oldState, newState State, _ Stats) {
		// A callback re-entering the breaker is supported: it runs on the notify
		// goroutine, so RecordFailure here does not deadlock.
		if oldState == StateHalfOpen && newState == StateClosed {
			cb.RecordFailure()
			close(fired)
		}
	})

	cb.RecordFailure()
	cb.RecordFailure() // -> Open, failures == FailureThreshold
	time.Sleep(60 * time.Millisecond)
	cb.CheckState()    // -> HalfOpen
	cb.RecordSuccess() // -> Closed; callback records one failure (async)

	// Wait for the re-entrant failure to land before asserting.
	select {
	case <-fired:
	case <-time.After(time.Second):
		t.Fatal("HalfOpen->Closed callback never fired")
	}

	if got := cb.State(); got != StateClosed {
		t.Errorf("one failure right after recovery re-opened the circuit: state = %v", got)
	}
}

// Under concurrency, callbacks must be delivered in transition order. Winning
// CAS transitions chain by construction (each new state is the next
// transition's old state), so the delivered (old,new) pairs must form a
// contiguous chain. A broken chain means two concurrent transitions reported
// out of order — the bug this fix addresses.
func TestCircuitBreaker_CallbackOrderUnderConcurrency(t *testing.T) {
	config := Config{
		FailureThreshold:    1,
		SuccessThreshold:    1,
		MaxHalfOpenRequests: 100,
		OpenTimeout:         time.Nanosecond, // half-open is always reachable
	}
	cb := New(config)

	var rec transitionRecorder
	cb.OnStateChange(rec.record)

	// Hammer the half-open edge: a closing success races an opening failure on
	// the same transition, round after round.
	const rounds = 3000
	for r := 0; r < rounds; r++ {
		cb.CheckState() // Open (1ns elapsed) -> HalfOpen
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); cb.RecordSuccess() }()
		go func() { defer wg.Done(); cb.RecordFailure() }()
		wg.Wait()
	}

	// Wait for the notify queue to drain (delivered count stops growing).
	deadline := time.Now().Add(3 * time.Second)
	last := -1
	for time.Now().Before(deadline) {
		if n := rec.len(); n == last {
			break
		} else {
			last = n
		}
		time.Sleep(10 * time.Millisecond)
	}

	if rec.len() == 0 {
		t.Fatal("no transitions recorded")
	}
	for i := 1; i < rec.len(); i++ {
		prev, cur := rec.at(i-1), rec.at(i)
		if cur.oldState != prev.newState {
			t.Fatalf("out-of-order delivery at %d: ...->%v then %v->%v (chain broken)",
				i, prev.newState, cur.oldState, cur.newState)
		}
	}
}

func TestCircuitBreaker_HalfOpenAdmissionNotErasedByTransition(t *testing.T) {
	config := Config{
		FailureThreshold:    1,
		SuccessThreshold:    2,
		MaxHalfOpenRequests: 1,
		OpenTimeout:         time.Nanosecond,
	}
	cb := New(config)

	// Reservations taken the moment HalfOpen becomes visible must not be
	// erased by the transition's own counter maintenance: that would admit
	// more than MaxHalfOpenRequests concurrent probes. Hammer the
	// open -> half-open edge and watch the concurrent-admission gauge.
	const rounds = 5000
	workers := 8
	var overrun atomic.Bool

	for r := 0; r < rounds && !overrun.Load(); r++ {
		cb.RecordFailure() // (re-)open; OpenTimeout of 1ns has already elapsed

		var inFlight atomic.Int32
		start := make(chan struct{})
		var wg sync.WaitGroup
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				if cb.IsAllowed() {
					if inFlight.Add(1) > int32(config.MaxHalfOpenRequests) {
						overrun.Store(true)
					}
					inFlight.Add(-1)
					cb.ReleaseHalfOpen()
				}
			}()
		}
		close(start)
		wg.Wait()
	}

	if overrun.Load() {
		t.Error("more concurrent requests admitted than MaxHalfOpenRequests")
	}
}

func TestCircuitBreaker_ResetNotLostDuringHalfOpenTransition(t *testing.T) {
	config := Config{
		FailureThreshold: 1,
		SuccessThreshold: 1,
		OpenTimeout:      time.Nanosecond,
	}

	// Reset racing the open -> half-open transition must never be
	// overwritten: whatever the interleaving, the breaker must not end up
	// half-open after a Reset (either the Reset lands last, or the
	// transition saw Closed and did nothing).
	for i := 0; i < 5000; i++ {
		cb := New(config)
		cb.RecordFailure() // -> Open; the 1ns timeout has already elapsed

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; cb.CheckState() }()
		go func() { defer wg.Done(); <-start; cb.Reset() }()
		close(start)
		wg.Wait()

		if s := cb.State(); s == StateHalfOpen {
			t.Fatalf("round %d: Reset lost — breaker half-open after Reset", i)
		}
	}
}

func TestCircuitBreaker_ExternalSuccessDoesNotReleaseCommandSlots(t *testing.T) {
	config := Config{
		FailureThreshold:    1,
		SuccessThreshold:    3,
		MaxHalfOpenRequests: 1,
		OpenTimeout:         time.Nanosecond,
	}
	cb := New(config)
	cb.RecordFailure()
	time.Sleep(time.Millisecond) // let the 1ns OpenTimeout provably elapse
	cb.CheckState()              // -> HalfOpen

	if !cb.IsAllowed() {
		t.Fatal("setup: expected to reserve the only half-open slot")
	}

	// An out-of-band success (e.g. a background health check) never held an
	// admission slot, so it must not release the one a real command probe is
	// still using — that would let more than MaxHalfOpenRequests hit a
	// recovering database.
	cb.RecordExternalSuccess()
	if cb.IsAllowed() {
		t.Error("external success released a command probe's half-open slot")
	}

	// It still counts toward closing the circuit.
	cb.RecordExternalSuccess()
	cb.RecordExternalSuccess() // successes reach SuccessThreshold
	if got := cb.State(); got != StateClosed {
		t.Errorf("state = %v after SuccessThreshold external successes, want Closed", got)
	}
}

func TestCircuitBreaker_ResetClearsCountersBeforePublishingClosed(t *testing.T) {
	config := Config{
		FailureThreshold: 2,
		SuccessThreshold: 1,
		OpenTimeout:      time.Hour,
	}

	// A failure racing Reset must count against a fresh counter: if Closed
	// becomes visible while the counter still holds the count that opened
	// the circuit, one failure re-opens it — and Reset then zeroes
	// lastFailure, wedging the breaker open past its timeout guard.
	const threshold = 20
	config.FailureThreshold = threshold
	for i := 0; i < 5000; i++ {
		cb := New(config)
		for f := 0; f < threshold; f++ {
			cb.RecordFailure() // -> Open, failures == FailureThreshold
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; cb.Reset() }()
		go func() {
			defer wg.Done()
			<-start
			// Far fewer failures than the threshold: only the stale
			// pre-reset count can push the breaker over the edge.
			for f := 0; f < threshold/2; f++ {
				cb.RecordFailure()
			}
		}()
		close(start)
		wg.Wait()

		if s := cb.State(); s != StateClosed {
			t.Fatalf("round %d: %d post-Reset failures (threshold %d) re-opened the circuit (state %v)",
				i, threshold/2, threshold, s)
		}
	}
}

func TestCircuitBreaker_NoReservationSurvivesHalfOpenReopen(t *testing.T) {
	config := Config{
		FailureThreshold:    1,
		SuccessThreshold:    2,
		MaxHalfOpenRequests: 2,
		OpenTimeout:         time.Nanosecond,
	}

	// A reservation racing the half-open -> open transition must not stick:
	// the transition zeroes the counter, and a late Add would both admit a
	// request to the endpoint that just failed its probe and pollute the
	// counter for the next half-open epoch.
	for i := 0; i < 5000; i++ {
		cb := New(config)
		cb.RecordFailure()
		cb.CheckState() // -> HalfOpen

		start := make(chan struct{})
		var wg sync.WaitGroup
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				cb.IsAllowed()
			}()
		}
		wg.Add(1)
		go func() { defer wg.Done(); <-start; cb.RecordFailure() }() // re-opens
		close(start)
		wg.Wait()

		if State(cb.state.Load()) == StateOpen {
			if r := cb.Stats().Requests; r > 0 {
				t.Fatalf("round %d: reservation survived the half-open -> open transition (requests = %d)", i, r)
			}
		}
	}
}

// TestAllowReportsReservation pins the Allow contract: a closed-state
// admission reserves nothing, a half-open admission reserves one bounded
// probe slot, and a denied request reserves nothing — callers use the
// reserved flag to decide whether a later ReleaseHalfOpen is theirs to call,
// so a closed-state admission that outlives a later open -> half-open
// transition cannot free a slot a real recovery probe is holding.
func TestAllowReportsReservation(t *testing.T) {
	cb := New(Config{
		FailureThreshold:    1,
		SuccessThreshold:    1,
		MaxHalfOpenRequests: 1,
		OpenTimeout:         30 * time.Millisecond,
	})

	if allowed, reserved := cb.Allow(); !allowed || reserved {
		t.Fatalf("closed: Allow() = (%v, %v), want (true, false)", allowed, reserved)
	}

	cb.RecordFailure()
	if allowed, reserved := cb.Allow(); allowed || reserved {
		t.Fatalf("open: Allow() = (%v, %v), want (false, false)", allowed, reserved)
	}

	time.Sleep(50 * time.Millisecond)
	if allowed, reserved := cb.Allow(); !allowed || !reserved {
		t.Fatalf("half-open: Allow() = (%v, %v), want (true, true)", allowed, reserved)
	}
	if allowed, reserved := cb.Allow(); allowed || reserved {
		t.Fatalf("half-open budget exhausted: Allow() = (%v, %v), want (false, false)", allowed, reserved)
	}
}

// TestExecuteClosedAdmissionDoesNotFreeProbeSlot pins Execute's slot
// accounting: work admitted while the breaker is CLOSED reserves no
// half-open slot, so when it finishes after other failures moved the
// breaker to half-open, its success must count toward closing WITHOUT
// releasing the slot a real recovery probe is holding.
func TestExecuteClosedAdmissionDoesNotFreeProbeSlot(t *testing.T) {
	cb := New(Config{
		FailureThreshold:    1,
		SuccessThreshold:    2, // one success does not close the circuit
		MaxHalfOpenRequests: 1,
		OpenTimeout:         30 * time.Millisecond,
	})

	err := cb.Execute(func() error {
		// While the closed-admitted work runs: a failure opens the circuit,
		// the grace period elapses, and a recovery probe reserves the only
		// half-open slot.
		cb.RecordFailure()
		time.Sleep(50 * time.Millisecond)
		if allowed, reserved := cb.Allow(); !allowed || !reserved {
			t.Fatal("setup: expected to reserve the half-open probe slot")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	if cb.IsAllowed() {
		t.Error("a second half-open admission succeeded — Execute released a slot it never reserved")
	}
}

// TestHalfOpenFailureRacingClosureIsNotDropped hammers a half-open breaker
// with a concurrent closing success and a probe failure. Whatever the
// interleaving, the failure must not vanish: either it re-opens the
// half-open circuit directly, or — when the success's CAS to Closed wins —
// it must count against the fresh closed window (threshold 1 => re-open).
// A final Closed state means the failure was dropped entirely and normal
// traffic flows to an endpoint whose recovery probe just failed.
func TestHalfOpenFailureRacingClosureIsNotDropped(t *testing.T) {
	for round := 0; round < 20000; round++ {
		cb := New(Config{
			FailureThreshold:    1,
			SuccessThreshold:    1,
			MaxHalfOpenRequests: 1,
			OpenTimeout:         time.Nanosecond,
		})
		cb.RecordFailure()
		time.Sleep(time.Microsecond) // let the 1ns grace elapse past clock granularity
		if cb.CheckState() != StateHalfOpen {
			t.Fatalf("round %d: setup: expected half-open", round)
		}

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; cb.RecordSuccess() }()
		go func() { defer wg.Done(); <-start; cb.RecordFailure() }()
		close(start)
		wg.Wait()

		if got := State(cb.state.Load()); got != StateOpen {
			t.Fatalf("round %d: state = %v, want Open — the probe failure was dropped", round, got)
		}
	}
}

// TestRecordFailureResetRaceKeepsTimestamp hammers RecordFailure against
// Reset: when Reset fully completes between RecordFailure's timestamp store
// and its CAS into Open, the circuit must not end up Open with a zero
// lastFailure — CheckState's zero-timestamp guard would then never allow the
// open -> half-open transition, wedging the breaker open for callers with no
// out-of-band probe traffic.
func TestRecordFailureResetRaceKeepsTimestamp(t *testing.T) {
	for round := 0; round < 5000; round++ {
		cb := New(Config{
			FailureThreshold:    1,
			SuccessThreshold:    1,
			MaxHalfOpenRequests: 1,
			OpenTimeout:         time.Minute,
		})
		done := make(chan struct{})
		go func() {
			defer close(done)
			for i := 0; i < 200; i++ {
				cb.Reset()
			}
		}()
		for i := 0; i < 200; i++ {
			cb.RecordFailure()
		}
		<-done

		// Quiescent: whatever interleaving happened, an Open circuit must
		// carry a non-zero timestamp or it can never leave Open.
		if State(cb.state.Load()) == StateOpen && cb.lastFailure.Load() == 0 {
			t.Fatalf("round %d: circuit open with lastFailure == 0 — wedged past the zero-timestamp guard", round)
		}
	}
}

// TestCircuitBreaker_ReservationIgnoredAfterEpochChange pins the generation
// guard on AllowReserve reservations: a reservation taken in one half-open
// episode must not settle against a later one after the circuit has cycled
// through Open and back to half-open. Without it, a stale success would count
// toward closing the new episode and would free a slot it never held there.
func TestCircuitBreaker_ReservationIgnoredAfterEpochChange(t *testing.T) {
	config := Config{
		FailureThreshold:    1,
		SuccessThreshold:    5, // high so one success cannot close half-open
		MaxHalfOpenRequests: 5,
		OpenTimeout:         50 * time.Millisecond,
	}
	cb := New(config)

	// Episode A: open -> half-open, take a reservation.
	cb.RecordFailure()
	time.Sleep(60 * time.Millisecond)
	allowed, rA := cb.AllowReserve()
	if !allowed || !rA.held {
		t.Fatalf("AllowReserve in half-open: allowed=%v held=%v", allowed, rA.held)
	}
	genA := cb.generation.Load()

	// Cycle to episode B: a failure re-opens, then a probe re-enters half-open.
	cb.RecordFailure()
	time.Sleep(60 * time.Millisecond)
	cb.CheckState()
	if State(cb.state.Load()) != StateHalfOpen {
		t.Fatalf("expected half-open episode B, got %v", cb.State())
	}
	if cb.generation.Load() == genA {
		t.Fatalf("generation did not advance across episodes (still %d)", genA)
	}

	beforeSucc := cb.successes.Load()
	beforeReq := cb.requests.Load()
	// The stale episode-A reservation must be ignored.
	cb.RecordSuccessFor(rA)
	if got := cb.successes.Load(); got != beforeSucc {
		t.Errorf("stale reservation bumped successes: before=%d after=%d", beforeSucc, got)
	}
	if got := cb.requests.Load(); got != beforeReq {
		t.Errorf("stale reservation changed requests: before=%d after=%d", beforeReq, got)
	}
}

// TestCircuitBreaker_ReservationSettlesOnce pins the once-only guard: a single
// reservation shared by many successful outcomes (a pipeline batch) settles the
// half-open slot exactly once — one success counted, one slot released — instead
// of once per outcome.
func TestCircuitBreaker_ReservationSettlesOnce(t *testing.T) {
	config := Config{
		FailureThreshold:    1,
		SuccessThreshold:    5, // high so repeated settles would accumulate visibly
		MaxHalfOpenRequests: 3,
		OpenTimeout:         50 * time.Millisecond,
	}
	cb := New(config)

	cb.RecordFailure()
	time.Sleep(60 * time.Millisecond)
	allowed, r := cb.AllowReserve()
	if !allowed || !r.held {
		t.Fatalf("AllowReserve in half-open: allowed=%v held=%v", allowed, r.held)
	}
	if got := cb.requests.Load(); got != 1 {
		t.Fatalf("requests after one reservation = %d, want 1", got)
	}

	for i := 0; i < 5; i++ {
		cb.RecordSuccessFor(r) // as N successful commands sharing one admission would
	}
	if got := cb.successes.Load(); got != 1 {
		t.Errorf("successes = %d, want 1 (reservation settled once)", got)
	}
	if got := cb.requests.Load(); got != 0 {
		t.Errorf("requests = %d, want 0 (one reservation, released once)", got)
	}
}

// TestCircuitBreaker_ForceOpen pins that ForceOpen transitions straight to Open
// regardless of the failure count, blocks requests, notifies once, and is a
// no-op when already open.
// TestCircuitBreaker_ClosedAdmissionDoesNotFeedHalfOpen pins the closed-state
// reservation semantics: a slot-less admission clears the failure count while
// the circuit is still closed, but its successes never count toward closing a
// LATER half-open episode — they predate the failures that opened the circuit.
// Out-of-band evidence (RecordExternalSuccess) still counts.
func TestCircuitBreaker_ClosedAdmissionDoesNotFeedHalfOpen(t *testing.T) {
	cb := New(Config{
		FailureThreshold:    3,
		SuccessThreshold:    2,
		MaxHalfOpenRequests: 1,
		OpenTimeout:         20 * time.Millisecond,
	})

	// Closed admission: no slot, no generation.
	ok, closedRes := cb.AllowReserve()
	if !ok || closedRes.held {
		t.Fatalf("closed AllowReserve = (%v, held=%v), want admitted without a slot", ok, closedRes.held)
	}

	// While still closed, its success clears the failure count.
	cb.RecordFailure()
	cb.RecordFailure()
	cb.RecordSuccessFor(closedRes)
	if got := cb.Stats().Failures; got != 0 {
		t.Fatalf("failures after closed-admission success = %d, want 0", got)
	}
	cb.RecordFailure()
	cb.RecordFailure()
	if st := cb.State(); st != StateClosed {
		t.Fatalf("state after 2 failures post-reset = %v, want closed (the reset must have applied)", st)
	}

	// Open the circuit and let it reach half-open.
	cb.RecordFailure() // third consecutive failure -> open
	if st := cb.State(); st != StateOpen {
		t.Fatalf("state = %v, want open", st)
	}
	time.Sleep(30 * time.Millisecond)
	if st := cb.CheckState(); st != StateHalfOpen {
		t.Fatalf("state after grace = %v, want half-open", st)
	}

	// The stale closed admission reports N successes (a batch recorded late):
	// none may count toward the recovery it never probed.
	for i := 0; i < 5; i++ {
		cb.RecordSuccessFor(closedRes)
	}
	if st := cb.State(); st != StateHalfOpen {
		t.Fatalf("state after stale closed-admission successes = %v, want half-open", st)
	}
	if got := cb.Stats().Successes; got != 0 {
		t.Fatalf("half-open successes credited to a closed admission = %d, want 0", got)
	}

	// Out-of-band evidence still closes the circuit.
	cb.RecordExternalSuccess()
	cb.RecordExternalSuccess()
	if st := cb.State(); st != StateClosed {
		t.Fatalf("state after 2 external successes = %v, want closed", st)
	}
}

// TestCircuitBreaker_ClosedAdmissionFailureDoesNotReopenHalfOpen pins the
// failure-path symmetry of the closed-admission rule: a slot-less admission's
// failure counts toward opening while the circuit is still closed, but once the
// circuit has moved to half-open it must not re-open it — that failure was
// admitted in the prior closed state and never joined the recovery episode.
// TestCircuitBreaker_ClosedReservationInvalidatedByReset pins that a closed-
// state admission (slot-less, generation-stamped) whose member is reselected via
// Reset before the command settles does not record its stale failure against the
// freshly reset breaker — which, at FailureThreshold 1, would immediately undo
// the operator's reselection.
func TestCircuitBreaker_ClosedReservationInvalidatedByReset(t *testing.T) {
	cb := New(Config{FailureThreshold: 1, SuccessThreshold: 1, OpenTimeout: time.Hour})

	ok, res := cb.AllowReserve() // admitted while closed: slot-less, gen-stamped
	if !ok || res.held {
		t.Fatalf("closed AllowReserve = (%v, held=%v), want admitted without a slot", ok, res.held)
	}

	cb.Reset() // operator reselect: new closed episode, bumped generation
	if st := cb.State(); st != StateClosed {
		t.Fatalf("state after Reset = %v, want closed", st)
	}

	cb.RecordFailureFor(res) // the in-flight command fails; reservation predates the Reset
	if st := cb.State(); st != StateClosed {
		t.Fatalf("stale closed-admission failure re-opened after Reset: state=%v, want closed", st)
	}

	_, fresh := cb.AllowReserve() // a fresh closed admission still records normally
	cb.RecordFailureFor(fresh)
	if st := cb.State(); st != StateOpen {
		t.Fatalf("state after fresh closed-admission failure = %v, want open", st)
	}
}

func TestCircuitBreaker_ClosedAdmissionFailureDoesNotReopenHalfOpen(t *testing.T) {
	cfg := Config{FailureThreshold: 2, SuccessThreshold: 1, MaxHalfOpenRequests: 1, OpenTimeout: 20 * time.Millisecond}

	// Phase 1: while closed, a closed-admission failure counts toward opening.
	cb := New(cfg)
	_, closedRes := cb.AllowReserve()
	if closedRes.held {
		t.Fatal("closed AllowReserve returned a held reservation")
	}
	cb.RecordFailureFor(closedRes)
	if st := cb.State(); st != StateClosed {
		t.Fatalf("state=%v after 1 closed-admission failure, want still closed", st)
	}
	cb.RecordFailureFor(closedRes)
	if st := cb.State(); st != StateOpen {
		t.Fatalf("state=%v after threshold closed-admission failures, want open", st)
	}

	// Phase 2: a closed admission that outlives an open->half-open cycle must
	// not re-open the new episode when it finally settles as a failure.
	cb2 := New(cfg)
	_, stale := cb2.AllowReserve() // admitted while closed -> unheld
	if stale.held {
		t.Fatal("closed AllowReserve returned a held reservation")
	}
	cb2.RecordFailure() // open via other traffic, not the stale admission
	cb2.RecordFailure()
	if st := cb2.State(); st != StateOpen {
		t.Fatalf("cb2 state=%v, want open", st)
	}
	time.Sleep(30 * time.Millisecond)
	if st := cb2.CheckState(); st != StateHalfOpen {
		t.Fatalf("cb2 state=%v after grace, want half-open", st)
	}
	cb2.RecordFailureFor(stale)
	if st := cb2.State(); st != StateHalfOpen {
		t.Fatalf("stale closed-admission failure re-opened the half-open episode: state=%v, want half-open", st)
	}
}

// TestCircuitBreaker_ResetInvalidatesInFlightReservation pins that Reset ends
// the current half-open episode: a reservation handed out before the Reset is
// stale afterwards, so settling its failure must not re-open the freshly closed
// circuit (which would immediately undo an operator SetActiveDatabase).
func TestCircuitBreaker_ResetInvalidatesInFlightReservation(t *testing.T) {
	cb := New(Config{FailureThreshold: 1, SuccessThreshold: 1, MaxHalfOpenRequests: 1, OpenTimeout: 20 * time.Millisecond})
	cb.RecordFailure() // open (threshold 1)
	time.Sleep(30 * time.Millisecond)
	if st := cb.CheckState(); st != StateHalfOpen {
		t.Fatalf("state=%v after grace, want half-open", st)
	}
	ok, res := cb.AllowReserve()
	if !ok || !res.held {
		t.Fatalf("half-open AllowReserve = (%v, held=%v), want a held slot", ok, res.held)
	}

	cb.Reset() // operator re-selection ends the episode
	if st := cb.State(); st != StateClosed {
		t.Fatalf("state after Reset=%v, want closed", st)
	}

	cb.RecordFailureFor(res) // stale reservation from the ended episode
	if st := cb.State(); st != StateClosed {
		t.Fatalf("stale reservation failure re-opened after Reset: state=%v, want closed", st)
	}
	cb.RecordFailure() // a fresh failure still opens normally
	if st := cb.State(); st != StateOpen {
		t.Fatalf("state=%v after fresh failure post-reset, want open", st)
	}
}

// TestCircuitBreaker_ResetRaceKeepsThresholdConsistent exercises RecordFailure
// racing Reset under -race. The Add-then-lock window that the count re-check
// closes cannot be forced deterministically, so this is a stress/invariant
// check rather than strict discrimination: the circuit must never end up Open
// with a sub-threshold failure count (which is what opening off a count a
// concurrent Reset already zeroed would produce).
func TestCircuitBreaker_ResetRaceKeepsThresholdConsistent(t *testing.T) {
	for iter := 0; iter < 500; iter++ {
		cb := New(Config{FailureThreshold: 1, SuccessThreshold: 1, OpenTimeout: time.Hour})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); cb.RecordFailure() }()
		go func() { defer wg.Done(); cb.Reset() }()
		wg.Wait()
		if cb.State() == StateOpen && cb.Stats().Failures < 1 {
			t.Fatalf("iter %d: circuit Open with failures=%d (<threshold) — opened off a Reset-zeroed count", iter, cb.Stats().Failures)
		}
	}
}

// TestCircuitBreaker_ForceOpenRaceKeepsTimestamp exercises ForceOpen racing
// Reset under -race. Like the Reset/RecordFailure race the exact interleaving
// can't be forced, so this is a stress/invariant check: whenever ForceOpen
// leaves the circuit Open, lastFailure must be non-zero — otherwise CheckState
// refuses to advance it to half-open and the breaker wedges open forever.
func TestCircuitBreaker_ForceOpenRaceKeepsTimestamp(t *testing.T) {
	for iter := 0; iter < 500; iter++ {
		cb := New(Config{FailureThreshold: 1, SuccessThreshold: 1, OpenTimeout: time.Hour})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); cb.ForceOpen() }()
		go func() { defer wg.Done(); cb.Reset() }()
		wg.Wait()
		if cb.State() == StateOpen && cb.Stats().LastFailureTime.IsZero() {
			t.Fatalf("iter %d: circuit Open with a zero lastFailure — CheckState cannot advance it to half-open (wedged open)", iter)
		}
	}
}

func TestCircuitBreaker_ForceOpen(t *testing.T) {
	config := Config{
		FailureThreshold: 100, // large: ForceOpen must not depend on synthesizing failures
		SuccessThreshold: 2,
		OpenTimeout:      time.Hour,
	}
	cb := New(config)
	var rec transitionRecorder
	cb.OnStateChange(rec.record)

	cb.ForceOpen()
	if cb.State() != StateOpen {
		t.Fatalf("state after ForceOpen = %v, want open", cb.State())
	}
	if cb.IsAllowed() {
		t.Error("IsAllowed should be false right after ForceOpen (open, before timeout)")
	}
	cb.ForceOpen() // redundant: already open, must not fire another transition

	rec.waitFor(t, 1)
	if got := rec.at(0); got.oldState != StateClosed || got.newState != StateOpen {
		t.Errorf("expected Closed->Open, got %v->%v", got.oldState, got.newState)
	}
	if n := rec.len(); n != 1 {
		t.Errorf("transition count = %d, want 1 (ForceOpen idempotent when already open)", n)
	}
}

// TestCircuitBreaker_StaleReservationFailureIgnored pins the failure-side
// generation guard: a failure recorded through a reservation taken in a
// previous half-open episode must NOT re-open the current episode (which would
// abort its recovery). Symmetric to TestCircuitBreaker_ReservationIgnoredAfterEpochChange.
func TestCircuitBreaker_StaleReservationFailureIgnored(t *testing.T) {
	config := Config{
		FailureThreshold:    1,
		SuccessThreshold:    5, // high so the fresh episode stays half-open
		MaxHalfOpenRequests: 5,
		OpenTimeout:         50 * time.Millisecond,
	}
	cb := New(config)

	// Episode A: open -> half-open, take a reservation.
	cb.RecordFailure()
	time.Sleep(60 * time.Millisecond)
	allowed, rA := cb.AllowReserve()
	if !allowed || !rA.held {
		t.Fatalf("AllowReserve in half-open: allowed=%v held=%v", allowed, rA.held)
	}

	// Cycle to episode B: a failure re-opens, then a probe re-enters half-open.
	cb.RecordFailure()
	time.Sleep(60 * time.Millisecond)
	cb.CheckState()
	if State(cb.state.Load()) != StateHalfOpen {
		t.Fatalf("expected half-open episode B, got %v", cb.State())
	}

	// The stale episode-A reservation's failure must be ignored, leaving B open
	// for recovery.
	cb.RecordFailureFor(rA)
	if got := State(cb.state.Load()); got != StateHalfOpen {
		t.Errorf("stale reservation failure re-opened the circuit: state=%v, want half-open", got)
	}
}

// TestCircuitBreaker_HalfOpenSuccessFailureRaceNeverDropsOutcome pins that a
// success closing the circuit and a failure re-opening it, arriving at the
// same instant in half-open, can never race each other's CAS: both
// recordSuccessHalfOpenLocked and recordFailureHalfOpenLocked run fully under
// transitionMu, so exactly one of them completes first, and the other's own
// state check (not a raw CompareAndSwap outside the lock) then classifies
// correctly instead of silently discarding a completed outcome.
func TestCircuitBreaker_HalfOpenSuccessFailureRaceNeverDropsOutcome(t *testing.T) {
	config := Config{
		FailureThreshold:    1,
		SuccessThreshold:    1, // one success is enough to close from half-open
		MaxHalfOpenRequests: 2,
		OpenTimeout:         time.Nanosecond,
	}
	for i := 0; i < 2000; i++ {
		cb := New(config)
		cb.RecordFailure()
		cb.CheckState() // -> HalfOpen

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); <-start; cb.RecordSuccess() }() // may close
		go func() { defer wg.Done(); <-start; cb.RecordFailure() }() // may re-open
		close(start)
		wg.Wait()

		// With FailureThreshold 1, EVERY correct serialization ends Open, so
		// this is the assertion that actually detects a dropped failure.
		// Failure first: half-open re-opens. Success first: the circuit closes,
		// and the failure is then reclassified as a fresh closed-state failure
		// (recordFailureHalfOpenLocked's StateClosed branch) which re-opens at
		// the threshold of one. Accepting Closed here would pass exactly when
		// the failure was discarded, which is the regression under test.
		state := State(cb.state.Load())
		if state != StateOpen {
			t.Fatalf("round %d: state=%v after a concurrent half-open success/failure, want Open (the failure must survive either serialization)", i, state)
		}
		// The failure must also be accounted for, not merely reflected in the
		// state: a success that closed the circuit zeroes the failure count, so
		// a dropped failure leaves this at zero.
		if got := cb.Stats().Failures; got < 1 {
			t.Fatalf("round %d: failures=%d after settling to Open, want at least 1 (the failure outcome was dropped)", i, got)
		}
		// Both transitions into Open clear the half-open counters, so a stale
		// non-zero count would mean a botched transition.
		if got := cb.Stats().Successes; got != 0 {
			t.Fatalf("round %d: successes=%d after settling to %v, want 0", i, got, state)
		}
		if got := cb.Stats().Requests; got != 0 {
			t.Fatalf("round %d: requests=%d after settling to %v, want 0", i, got, state)
		}
	}
}

// TestCircuitBreaker_FailureAfterSettlingSuccessIsReclassified pins the exact
// interleaving the concurrent test above can only hit by luck: a failure reads
// half-open, is descheduled, a success takes transitionMu first and closes the
// circuit, and only then does the failure acquire the lock. The failure must
// then be reclassified as a fresh closed-state failure and re-open the circuit,
// not be discarded because the state it was recorded against is gone. Driving
// the locked helpers directly makes the order deterministic instead of timing
// dependent.
func TestCircuitBreaker_FailureAfterSettlingSuccessIsReclassified(t *testing.T) {
	cb := New(Config{
		FailureThreshold:    1,
		SuccessThreshold:    1,
		MaxHalfOpenRequests: 2,
		OpenTimeout:         time.Nanosecond,
	})
	cb.RecordFailure()
	cb.CheckState() // -> HalfOpen
	if got := State(cb.state.Load()); got != StateHalfOpen {
		t.Fatalf("setup: state=%v, want half-open", got)
	}

	// The success wins transitionMu and closes the circuit.
	cb.transitionMu.Lock()
	cb.recordSuccessHalfOpenLocked(false)
	cb.transitionMu.Unlock()
	if got := State(cb.state.Load()); got != StateClosed {
		t.Fatalf("after the settling success: state=%v, want closed", got)
	}

	// The failure that had already observed half-open now records. It must not
	// be dropped just because the episode closed underneath it.
	cb.transitionMu.Lock()
	cb.recordFailureHalfOpenLocked()
	cb.transitionMu.Unlock()

	if got := State(cb.state.Load()); got != StateOpen {
		t.Fatalf("state=%v, want open: the failure was discarded instead of reclassified as a closed-state failure", got)
	}
	if got := cb.Stats().Failures; got < 1 {
		t.Fatalf("failures=%d, want at least 1: the failure outcome was not counted", got)
	}
}

// The open-timeout clock is the breaker's own monotonic clock, not the wall
// clock: a wall-clock step (NTP, VM restore) must not stretch or shrink the
// grace period. Driven through the injectable clock: advancing it past
// OpenTimeout, with no real time passing, is what moves Open to HalfOpen.
func TestCircuitBreaker_OpenTimeoutUsesMonotonicClock(t *testing.T) {
	var clock atomic.Int64
	clock.Store(1)
	prev := nowNano
	nowNano = clock.Load
	t.Cleanup(func() { nowNano = prev })

	cb := New(Config{FailureThreshold: 1, SuccessThreshold: 1, OpenTimeout: time.Hour})
	cb.RecordFailure()
	if got := cb.CheckState(); got != StateOpen {
		t.Fatalf("state=%v right after opening, want open", got)
	}
	// One hour on the breaker's clock, no wall-clock time at all.
	clock.Add(int64(time.Hour) + 1)
	if got := cb.CheckState(); got != StateHalfOpen {
		t.Fatalf("state=%v after the grace period on the breaker's clock, want half-open: the grace period is not keyed off nowNano", got)
	}
	if cb.Stats().LastFailureTime.IsZero() {
		t.Fatal("LastFailureTime is zero after a recorded failure")
	}
}

// Allow admits half-open requests under the transition lock, like
// AllowReserve: no transition can land between the reservation and the
// decision. The lock-free form reserved, re-checked the state and rejected on
// any change, which also rejected a request when a concurrent success had
// just closed the circuit. The interleaving itself cannot be forced without a
// deschedule hook, so this pins the mechanism: while a transition holds the
// lock, a half-open Allow waits for it instead of deciding on its own.
func TestCircuitBreaker_AllowSerializesWithTransitions(t *testing.T) {
	cb := New(Config{FailureThreshold: 1, SuccessThreshold: 1, OpenTimeout: time.Millisecond, MaxHalfOpenRequests: 1})
	cb.RecordFailure()
	time.Sleep(5 * time.Millisecond)
	if got := cb.CheckState(); got != StateHalfOpen {
		t.Fatalf("state=%v, want half-open", got)
	}

	cb.transitionMu.Lock()
	type result struct{ allowed, reserved bool }
	done := make(chan result, 1)
	go func() {
		a, r := cb.Allow()
		done <- result{a, r}
	}()
	select {
	case r := <-done:
		t.Fatalf("Allow decided (%v, %v) while a transition held the lock: admission is not serialized with transitions", r.allowed, r.reserved)
	case <-time.After(50 * time.Millisecond):
	}
	cb.transitionMu.Unlock()
	select {
	case r := <-done:
		if !r.allowed || !r.reserved {
			t.Fatalf("Allow = (%v, %v) after the lock was released, want a half-open admission", r.allowed, r.reserved)
		}
	case <-time.After(time.Second):
		t.Fatal("Allow never returned after the lock was released")
	}
}

// A health-probe success on a CLOSED breaker must not clear the consecutive
// command-failure count: a member that answers PING but fails commands, at a
// rate below FailureThreshold per probe interval, could otherwise never open.
// In half-open the probe success still counts toward closing.
func TestCircuitBreaker_ProbeSuccessKeepsClosedFailureStreak(t *testing.T) {
	cb := New(Config{FailureThreshold: 5, SuccessThreshold: 1, OpenTimeout: time.Millisecond})
	for i := 0; i < 3; i++ {
		cb.RecordFailure()
	}
	// A scheduled probe answers between the command failures.
	cb.RecordExternalSuccessForReset(cb.ResetGeneration())
	for i := 0; i < 2; i++ {
		cb.RecordFailure()
	}
	if got := cb.State(); got != StateOpen {
		t.Fatalf("state=%v after 5 consecutive command failures around a probe success, want open: the probe cleared the failure count", got)
	}

	// Half-open: the probe success is recovery evidence and closes the circuit.
	time.Sleep(5 * time.Millisecond)
	if got := cb.CheckState(); got != StateHalfOpen {
		t.Fatalf("state=%v, want half-open", got)
	}
	cb.RecordExternalSuccessForReset(cb.ResetGeneration())
	if got := cb.State(); got != StateClosed {
		t.Fatalf("state=%v after a half-open probe success, want closed", got)
	}
}
