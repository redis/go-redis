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
