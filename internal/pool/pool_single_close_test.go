package pool

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestSingleConnPoolGetDoesNotReopenClosedConn(t *testing.T) {
	cn := NewConn(nil)
	cn.Close()
	pool := NewSingleConnPool(nil, cn)

	got, err := pool.Get(context.Background())
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("Get() error = %v, want %v", err, ErrClosed)
	}
	if got != nil {
		t.Fatalf("Get() conn = %p, want nil", got)
	}
	if state := cn.GetStateMachine().GetState(); state != StateClosed {
		t.Fatalf("connection state after Get() = %s, want %s", state, StateClosed)
	}
}

func TestSingleConnPoolGetRestoresOwnerState(t *testing.T) {
	for _, ownerState := range []ConnState{
		StateCreated,
		StateInitializing,
		StateIdle,
		StateInUse,
		StateUnusable,
	} {
		t.Run(ownerState.String(), func(t *testing.T) {
			cn := NewConn(nil)
			cn.GetStateMachine().Transition(ownerState)
			pool := NewSingleConnPool(nil, cn)

			got, err := pool.Get(context.Background())
			if err != nil {
				t.Fatalf("Get() error = %v", err)
			}
			if got != cn {
				t.Fatalf("Get() conn = %p, want %p", got, cn)
			}
			if state := cn.GetStateMachine().GetState(); state != StateInUse {
				t.Fatalf("connection state during lease = %s, want %s", state, StateInUse)
			}

			pool.Put(context.Background(), cn)
			if state := cn.GetStateMachine().GetState(); state != ownerState {
				t.Fatalf("connection state after Put() = %s, want owner state %s", state, ownerState)
			}
		})
	}
}

func TestSingleConnPoolPutAndRemovePreserveClosedState(t *testing.T) {
	for _, release := range []struct {
		name string
		fn   func(*SingleConnPool, *Conn)
	}{
		{"put", func(p *SingleConnPool, cn *Conn) { p.Put(context.Background(), cn) }},
		{"remove", func(p *SingleConnPool, cn *Conn) { p.Remove(context.Background(), cn, ErrClosed) }},
	} {
		t.Run(release.name, func(t *testing.T) {
			cn := NewConn(nil)
			cn.GetStateMachine().Transition(StateIdle)
			pool := NewSingleConnPool(nil, cn)
			if _, err := pool.Get(context.Background()); err != nil {
				t.Fatalf("Get() error = %v", err)
			}

			cn.Close()
			release.fn(pool, cn)
			if state := cn.GetStateMachine().GetState(); state != StateClosed {
				t.Fatalf("connection state after %s = %s, want %s", release.name, state, StateClosed)
			}
		})
	}
}

func TestSingleConnPoolGetRacingCloseLeavesConnClosed(t *testing.T) {
	for i := 0; i < 500; i++ {
		cn := NewConn(nil)
		pool := NewSingleConnPool(nil, cn)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)

		go func() {
			defer wg.Done()
			<-start
			got, err := pool.Get(context.Background())
			if err == nil {
				pool.Put(context.Background(), got)
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			cn.Close()
		}()
		close(start)
		wg.Wait()

		if state := cn.GetStateMachine().GetState(); state != StateClosed {
			t.Fatalf("iteration %d: connection state = %s, want %s", i, state, StateClosed)
		}
	}
}
