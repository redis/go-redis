package pool

import "testing"

// A handoff worker finishes re-initialization with the connection back in
// circulation (IDLE), so a Get can acquire it before the worker runs
// ClearHandoffState. Clearing the handoff state must not release that Get's
// connection: forcing it to IDLE lets a second Get acquire the same socket,
// and two owners interleave their commands and read each other's replies.
func TestClearHandoffStateKeepsAcquiredConn(t *testing.T) {
	cn := NewConn(nil)
	if err := cn.MarkForHandoff("new-endpoint:6379", 1); err != nil {
		t.Fatalf("MarkForHandoff: %v", err)
	}
	cn.GetStateMachine().Transition(StateIdle) // re-init done
	if !cn.TryAcquire() {
		t.Fatal("first Get could not acquire the idle connection")
	}

	cn.ClearHandoffState()

	if got := cn.GetStateMachine().GetState(); got != StateInUse {
		t.Fatalf("state after ClearHandoffState = %v, want %v (the acquired connection was released)", got, StateInUse)
	}
	if cn.TryAcquire() {
		t.Fatal("second Get acquired a connection that is in use")
	}
	if cn.ShouldHandoff() {
		t.Fatal("handoff state not cleared")
	}
}

// While the handoff still owns the connection (queued: UNUSABLE, or being
// re-initialized: INITIALIZING), ClearHandoffState makes it usable again.
func TestClearHandoffStateReleasesHandoffOwnedConn(t *testing.T) {
	for _, from := range []ConnState{StateUnusable, StateInitializing} {
		cn := NewConn(nil)
		cn.GetStateMachine().Transition(from)
		cn.ClearHandoffState()
		if got := cn.GetStateMachine().GetState(); got != StateIdle {
			t.Errorf("from %v: state after ClearHandoffState = %v, want %v", from, got, StateIdle)
		}
	}
}

// The MOVING handler queues a handoff for a connection it saw IDLE in the
// pool. A Get can acquire the connection between that check and the claim.
// The claim must then fail and leave the holder's connection alone; OnPut
// queues the handoff when the holder returns it.
func TestClaimIdleForHandoffSkipsAcquiredConn(t *testing.T) {
	cn := NewConn(nil)
	if err := cn.MarkForHandoff("new-endpoint:6379", 1); err != nil {
		t.Fatalf("MarkForHandoff: %v", err)
	}
	cn.GetStateMachine().Transition(StateIdle)
	if !cn.TryAcquire() {
		t.Fatal("Get could not acquire the idle connection")
	}
	gen := cn.HandoffOwnerGen()

	if cn.ClaimIdleForHandoff() {
		t.Fatal("ClaimIdleForHandoff succeeded on a connection that is in use")
	}
	if got := cn.GetStateMachine().GetState(); got != StateInUse {
		t.Fatalf("state = %v, want %v (the holder's connection was taken)", got, StateInUse)
	}
	if !cn.ShouldHandoff() {
		t.Fatal("handoff mark lost: OnPut would not queue the handoff when the holder returns the connection")
	}
	if cn.HandedOffSince(gen) {
		t.Fatal("owner generation bumped by a failed claim")
	}
}

// A claimed idle connection is out of circulation and still marked, which
// queueHandoff requires; MarkHandoffQueued then clears only the mark.
func TestClaimIdleForHandoffTakesIdleConn(t *testing.T) {
	cn := NewConn(nil)
	if err := cn.MarkForHandoff("new-endpoint:6379", 1); err != nil {
		t.Fatalf("MarkForHandoff: %v", err)
	}
	cn.GetStateMachine().Transition(StateIdle)
	gen := cn.HandoffOwnerGen()

	if !cn.ClaimIdleForHandoff() {
		t.Fatal("ClaimIdleForHandoff failed on an idle connection")
	}
	if got := cn.GetStateMachine().GetState(); got != StateUnusable {
		t.Fatalf("state = %v, want %v", got, StateUnusable)
	}
	if !cn.ShouldHandoff() {
		t.Fatal("claim cleared the handoff mark; queueHandoff would refuse the connection")
	}
	if !cn.HandedOffSince(gen) {
		t.Fatal("claim did not bump the owner generation")
	}

	cn.MarkHandoffQueued()
	if cn.ShouldHandoff() {
		t.Fatal("still marked ShouldHandoff after MarkHandoffQueued")
	}
	if cn.GetHandoffEndpoint() != "new-endpoint:6379" {
		t.Fatalf("endpoint = %q, want it kept for the worker", cn.GetHandoffEndpoint())
	}
	if got := cn.GetStateMachine().GetState(); got != StateUnusable {
		t.Fatalf("MarkHandoffQueued changed the state to %v", got)
	}
}
