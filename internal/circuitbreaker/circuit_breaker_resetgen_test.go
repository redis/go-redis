package circuitbreaker

import "testing"

// TestRecordFailureForReset pins the reset-generation gate used by out-of-band
// health probes: a failure sampled before an operator Reset (reselect) must not
// re-open the freshly selected member, while a failure with an unchanged
// generation records normally.
func TestRecordFailureForReset(t *testing.T) {
	newCB := func() *CircuitBreaker {
		return New(Config{FailureThreshold: 1, SuccessThreshold: 1})
	}

	t.Run("records when generation unchanged", func(t *testing.T) {
		cb := newCB()
		gen := cb.ResetGeneration()
		cb.RecordFailureForReset(gen)
		if cb.State() != StateOpen {
			t.Fatalf("state = %v, want Open: a fresh probe failure must open the circuit", cb.State())
		}
	})

	t.Run("drops a failure sampled before a Reset", func(t *testing.T) {
		cb := newCB()
		gen := cb.ResetGeneration()
		cb.Reset() // operator reselect bumps the reset generation
		cb.RecordFailureForReset(gen)
		if cb.State() != StateClosed {
			t.Fatalf("state = %v, want Closed: a probe failure sampled before the Reset must not re-open", cb.State())
		}
	})
}

// TestRecordExternalSuccessForReset pins the symmetric success gate: a probe
// success sampled before a Reset must not count toward closing the new episode.
func TestRecordExternalSuccessForReset(t *testing.T) {
	cb := New(Config{FailureThreshold: 1, SuccessThreshold: 1})
	// Open then transition to half-open so a success would otherwise close it.
	cb.RecordFailure()
	if cb.State() != StateOpen {
		t.Fatalf("setup: state = %v, want Open", cb.State())
	}
	gen := cb.ResetGeneration()
	cb.Reset() // reselect: back to closed, resetGen bumped
	// Re-open so we can observe whether the stale success closes it.
	cb.RecordFailure()
	if cb.State() != StateOpen {
		t.Fatalf("setup: state = %v, want Open after re-failure", cb.State())
	}
	cb.CheckState() // may move to half-open once grace elapses; here it stays Open
	cb.RecordExternalSuccessForReset(gen)
	// The stale success (gen from before the Reset) must be dropped, so the
	// circuit is not closed by it.
	if cb.State() == StateClosed {
		t.Fatalf("state = Closed: a probe success sampled before the Reset must not close the new episode")
	}
}

// TestCurrentClosedReservation pins the late-outcome reservation: none while
// the circuit is not closed, and one taken before a recovery cycle records
// nothing into the next closed episode.
func TestCurrentClosedReservation(t *testing.T) {
	cb := New(Config{FailureThreshold: 1, SuccessThreshold: 1})
	stale, ok := cb.CurrentClosedReservation()
	if !ok {
		t.Fatal("no reservation on a closed circuit")
	}
	cb.ForceOpen()
	if _, ok := cb.CurrentClosedReservation(); ok {
		t.Fatal("reservation handed out on an open circuit")
	}
	cb.Reset() // back to closed: a new episode
	cb.RecordFailureFor(stale)
	if cb.State() != StateClosed {
		t.Fatalf("state = %v, want Closed: a reservation from before the cycle must not open the new episode", cb.State())
	}
	cur, ok := cb.CurrentClosedReservation()
	if !ok {
		t.Fatal("no reservation after Reset")
	}
	cb.RecordFailureFor(cur)
	if cb.State() != StateOpen {
		t.Fatalf("state = %v, want Open: a current reservation must record", cb.State())
	}
}
