package pool

import (
	"context"
	"errors"
	"testing"
)

func TestStickyConnPool_BasicFlow(t *testing.T) {
	ctx := context.Background()
	mp := &mockPooler{}
	p := NewStickyConnPool(mp)

	if _, err := p.NewConn(ctx); err != nil {
		t.Fatalf("NewConn: %v", err)
	}
	if err := p.CloseConn(ctx, NewConn(nil), "reason", "IDLE"); err != nil {
		t.Fatalf("CloseConn: %v", err)
	}
	if p.Size() != 1 {
		t.Errorf("Size = %d, want 1", p.Size())
	}
	if p.Stats() == nil {
		t.Error("Stats nil")
	}
	p.AddPoolHook(nil)
	p.RemovePoolHook(nil)

	// stateDefault: Len 0.
	if p.Len() != 0 {
		t.Errorf("Len(default) = %d, want 0", p.Len())
	}

	cn, err := p.Get(ctx) // default -> inited
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if p.Len() != 1 {
		t.Errorf("Len(inited) = %d, want 1", p.Len())
	}
	p.Put(ctx, cn)
	if p.IdleLen() != 1 {
		t.Errorf("IdleLen = %d, want 1", p.IdleLen())
	}
	cn2, err := p.Get(ctx) // inited -> read from channel
	if err != nil || cn2 != cn {
		t.Fatalf("Get(inited) = %v, %v", cn2, err)
	}
	p.Put(ctx, cn2)

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// stateClosed: Get returns ErrClosed, Len 0.
	if _, err := p.Get(ctx); err != ErrClosed {
		t.Errorf("Get(closed) = %v, want ErrClosed", err)
	}
	if p.Len() != 0 {
		t.Errorf("Len(closed) = %d, want 0", p.Len())
	}
}

func TestStickyConnPool_SharedRefcount(t *testing.T) {
	mp := &mockPooler{}
	p := NewStickyConnPool(mp)
	p2 := NewStickyConnPool(p) // same instance, shared=2
	if p2 != p {
		t.Fatal("NewStickyConnPool(sticky) should reuse the instance")
	}
	if err := p2.Close(); err != nil { // shared 2 -> 1, no-op close
		t.Fatalf("Close(shared): %v", err)
	}
	if err := p.Close(); err != nil { // shared 1 -> 0, actually closes
		t.Fatalf("Close(final): %v", err)
	}
}

func TestStickyConnPool_RemoveAndReset(t *testing.T) {
	ctx := context.Background()
	mp := &mockPooler{}
	p := NewStickyConnPool(mp)

	// Reset when healthy is a no-op.
	if err := p.Reset(ctx); err != nil {
		t.Fatalf("Reset(healthy) = %v", err)
	}

	cn, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	p.Remove(ctx, cn, errors.New("bad conn"))

	// After Remove, badConnError is set; Get surfaces it.
	if _, err := p.Get(ctx); err == nil {
		t.Error("Get after Remove should return bad conn error")
	}

	// Reset drains the bad conn and returns the pool to default state.
	if err := p.Reset(ctx); err != nil {
		t.Fatalf("Reset: %v", err)
	}
	if len(mp.removeErr) == 0 {
		t.Error("Reset should remove the bad connection from the underlying pool")
	}
}

func TestStickyConnPool_PutRemoveAfterClose(t *testing.T) {
	ctx := context.Background()
	mp := &mockPooler{}
	p := NewStickyConnPool(mp)
	cn, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Sending on the closed channel panics internally and is recovered,
	// falling back to the underlying pool.
	p.Put(ctx, cn)
	if mp.putCount == 0 {
		t.Error("Put after Close should fall back to underlying pool.Put")
	}

	p.RemoveWithoutTurn(ctx, cn, errors.New("late"))
	if len(mp.removeErr) == 0 {
		t.Error("Remove after Close should fall back to underlying pool.Remove")
	}
}
