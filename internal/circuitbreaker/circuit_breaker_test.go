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
