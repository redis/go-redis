package pool

import (
	"context"
	"errors"
	"testing"
)

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
