package pool

import (
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"testing"
	"time"
)

// TestMarkQueuedForHandoffRollbackDoesNotResurrectClearedState is a regression
// test for the handoff CAS-rollback race.
//
// When MarkQueuedForHandoff cannot transition the connection to UNUSABLE it
// rolls back the handoff metadata it just CAS'd. A concurrent handoff worker
// may have completed the handoff and run ClearHandoffState in that exact
// window. The rollback must NOT clobber the worker's cleared state by
// resurrecting ShouldHandoff=true — doing so wedges the connection forever,
// because OnGet rejects any connection reporting ShouldHandoff.
//
// The offending interleaving (the clear landing between the metadata CAS and
// the rollback store) is timing-dependent, so this hammers it many times. With
// the previous plain-Store rollback a resurrected ShouldHandoff is observed
// within these iterations; with the CAS rollback it never is.
func TestMarkQueuedForHandoffRollbackDoesNotResurrectClearedState(t *testing.T) {
	iterations := 20000
	if testing.Short() {
		iterations = 2000
	}

	for i := 0; i < iterations; i++ {
		cn := NewConn(nil)
		if err := cn.MarkForHandoff("new-endpoint:6379", int64(i+1)); err != nil {
			t.Fatalf("iter %d: MarkForHandoff: %v", i, err)
		}
		// Force MarkQueuedForHandoff down its rollback path: StateClosed is not a
		// valid source for the UNUSABLE transition, so TryTransition fails and the
		// metadata rollback runs.
		cn.GetStateMachine().Transition(StateClosed)

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			_ = cn.MarkQueuedForHandoff()
		}()
		go func() {
			defer wg.Done()
			<-start
			// Represents a worker that finished the handoff and cleared state.
			cn.ClearHandoffState()
		}()
		close(start)
		wg.Wait()

		// ClearHandoffState always runs, so the connection's final handoff state
		// must be cleared. A resurrected ShouldHandoff means the rollback
		// clobbered the worker's clear.
		if cn.ShouldHandoff() {
			t.Fatalf("iter %d: ShouldHandoff resurrected after ClearHandoffState — rollback clobbered the cleared state", i)
		}
	}
}

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

// handoffDuringPutHook queues the connection for a handoff on Put, then plays
// the handoff worker that picks it up at once: it re-initializes the
// connection (INITIALIZING) and runs an init command through a
// SingleConnPool, which holds it IN_USE. All of this happens before putConn
// returns from the hook, the worst case of the real interleaving.
type handoffDuringPutHook struct{}

func (handoffDuringPutHook) OnGet(context.Context, *Conn, bool) (bool, error) { return true, nil }

func (handoffDuringPutHook) OnPut(_ context.Context, cn *Conn) (bool, bool, error) {
	if err := cn.MarkForHandoff("new-endpoint:6379", 1); err != nil {
		return false, true, err
	}
	if err := cn.MarkQueuedForHandoff(); err != nil {
		return false, true, err
	}
	cn.GetStateMachine().Transition(StateInitializing) // worker: SetNetConnAndInitConn
	if _, err := NewSingleConnPool(nil, cn).Get(context.Background()); err != nil {
		return false, true, err
	}
	return true, false, nil
}

func (handoffDuringPutHook) OnRemove(context.Context, *Conn, error) {}

// After the OnPut hook hands the connection to a handoff worker, putConn must
// not release it: the IN_USE state it would release belongs to the worker's
// init command, not to the caller that returned the connection. Releasing it
// makes the connection IDLE in the pool mid-init, so a Get takes it while the
// worker still writes init commands on it.
func TestPutDoesNotReleaseConnHandedToHandoffWorker(t *testing.T) {
	p := NewConnPool(&Options{
		Dialer:             func(context.Context) (net.Conn, error) { return &net.TCPConn{}, nil },
		PoolSize:           2,
		MaxConcurrentDials: 2,
		DialTimeout:        time.Second,
	})
	defer p.Close()
	p.AddPoolHook(handoffDuringPutHook{})

	ctx := context.Background()
	cn, err := p.newConn(ctx, true)
	if err != nil {
		t.Fatalf("newConn: %v", err)
	}
	cn.GetStateMachine().Transition(StateInUse) // the caller holds it

	p.putConnWithoutTurn(ctx, cn)

	if got := cn.GetStateMachine().GetState(); got != StateInUse {
		t.Fatalf("state after Put = %v, want %v (the worker's state)", got, StateInUse)
	}
	if cn.TryAcquire() {
		t.Fatal("a Get acquired the connection while the handoff worker holds it")
	}
}

// A MOVING handoff claims an idle connection (IDLE -> UNUSABLE) straight from
// the idle list. If queueing the handoff then fails, the connection is removed
// with RemoveWithoutTurn. It was never acquired, so the removal must decrement
// the idle count, not the used count.
func TestRemoveClaimedIdleConnRecordsIdleState(t *testing.T) {
	p := NewConnPool(&Options{
		Dialer:             func(context.Context) (net.Conn, error) { return &net.TCPConn{}, nil },
		PoolSize:           2,
		MaxConcurrentDials: 2,
		DialTimeout:        time.Second,
	})
	defer p.Close()

	ctx := context.Background()
	idle, err := p.newConn(ctx, true)
	if err != nil {
		t.Fatalf("newConn: %v", err)
	}
	idle.GetStateMachine().Transition(StateInUse)
	p.putConnWithoutTurn(ctx, idle) // now in idleConns, counted idle

	used, err := p.newConn(ctx, true)
	if err != nil {
		t.Fatalf("newConn: %v", err)
	}
	used.GetStateMachine().Transition(StateInUse)

	var mu sync.Mutex
	got := map[*Conn][]string{}
	SetAllMetricCallbacks(&MetricCallbacks{
		ConnectionCount: func(_ context.Context, delta int, cn *Conn, state string, _ bool) {
			if delta < 0 {
				mu.Lock()
				got[cn] = append(got[cn], state)
				mu.Unlock()
			}
		},
	})
	defer SetAllMetricCallbacks(nil)

	if !idle.ClaimIdleForHandoff() {
		t.Fatal("ClaimIdleForHandoff failed on an idle connection")
	}
	p.RemoveWithoutTurn(ctx, idle, errors.New("handoff queue full"))
	p.RemoveWithoutTurn(ctx, used, errors.New("handoff failed"))

	mu.Lock()
	defer mu.Unlock()
	if want := []string{MetricStateIdle}; !slices.Equal(got[idle], want) {
		t.Fatalf("claimed idle conn removal decremented %v, want %v", got[idle], want)
	}
	if want := []string{MetricStateUsed}; !slices.Equal(got[used], want) {
		t.Fatalf("used conn removal decremented %v, want %v", got[used], want)
	}
	if n := p.IdleLen(); n != 0 {
		t.Fatalf("IdleLen = %d, want 0", n)
	}
}

// SingleConnPool runs commands on a connection that a background owner
// holds: initConn (INITIALIZING), including a handoff re-init, and re-auth
// (UNUSABLE). Such a connection can sit in its pool's idle list. Returning it
// after one command must restore the owner's state, not make it IDLE: an IDLE
// connection is handed to the next Get while the owner is still sending its
// remaining commands on it, and the two read each other's replies.
func TestSingleConnPoolPutRestoresOwnerState(t *testing.T) {
	ctx := context.Background()
	for _, owner := range []ConnState{StateInitializing, StateUnusable} {
		cn := NewConn(nil)
		cn.GetStateMachine().Transition(owner)
		p := NewSingleConnPool(nil, cn)

		got, err := p.Get(ctx)
		if err != nil || got != cn {
			t.Fatalf("%v: Get = (%v, %v)", owner, got, err)
		}
		if s := cn.GetStateMachine().GetState(); s != StateInUse {
			t.Fatalf("%v: state during the command = %v, want %v", owner, s, StateInUse)
		}
		p.Put(ctx, cn)

		if s := cn.GetStateMachine().GetState(); s != owner {
			t.Fatalf("%v: state after Put = %v, want %v (released to the pool mid-init)", owner, s, owner)
		}
		if cn.TryAcquire() {
			t.Fatalf("%v: pool Get acquired a connection its owner still holds", owner)
		}
	}
}

func TestSingleConnPoolRemoveRestoresOwnerState(t *testing.T) {
	ctx := context.Background()
	cn := NewConn(nil)
	cn.GetStateMachine().Transition(StateInitializing)
	p := NewSingleConnPool(nil, cn)

	if _, err := p.Get(ctx); err != nil {
		t.Fatalf("Get: %v", err)
	}
	p.Remove(ctx, cn, errors.New("bad conn"))

	if s := cn.GetStateMachine().GetState(); s != StateInitializing {
		t.Fatalf("state after Remove = %v, want %v", s, StateInitializing)
	}
	if _, err := p.Get(ctx); err == nil {
		t.Fatal("Get after Remove succeeded")
	}
}

// A connection the caller already holds (IN_USE) stays IN_USE.
func TestSingleConnPoolPutKeepsHeldConn(t *testing.T) {
	ctx := context.Background()
	cn := NewConn(nil)
	cn.GetStateMachine().Transition(StateInUse)
	p := NewSingleConnPool(nil, cn)

	if _, err := p.Get(ctx); err != nil {
		t.Fatalf("Get: %v", err)
	}
	p.Put(ctx, cn)
	if s := cn.GetStateMachine().GetState(); s != StateInUse {
		t.Fatalf("state after Put = %v, want %v", s, StateInUse)
	}
}
