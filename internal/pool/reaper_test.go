package pool

import (
	"context"
	"net"
	"testing"
	"time"
)

func reaperPool(t *testing.T, o Options) *ConnPool {
	t.Helper()
	o.PoolSize = 4
	o.MaxConcurrentDials = 4
	o.DialTimeout = time.Second
	o.PoolTimeout = time.Second
	o.Dialer = func(context.Context) (net.Conn, error) {
		client, server := net.Pipe()
		t.Cleanup(func() { _ = server.Close() })
		return client, nil
	}
	p := NewConnPool(&o)
	t.Cleanup(func() { _ = p.Close() })
	return p
}

func fillIdle(t *testing.T, p *ConnPool, n int) []*Conn {
	t.Helper()
	ctx := context.Background()
	cns := make([]*Conn, 0, n)
	for i := 0; i < n; i++ {
		cn, err := p.Get(ctx)
		if err != nil {
			t.Fatal(err)
		}
		cns = append(cns, cn)
	}
	for _, cn := range cns {
		p.Put(ctx, cn)
	}
	return cns
}

func eventually(t *testing.T, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal(msg)
}

func TestReaperClosesExpiredIdle(t *testing.T) {
	p := reaperPool(t, Options{
		ConnMaxLifetime:             50 * time.Millisecond,
		ConnMaxLifetimeReapInterval: 10 * time.Millisecond,
	})
	fillIdle(t, p, 3)
	eventually(t, func() bool { return p.Len() == 0 && p.IdleLen() == 0 },
		"expired idle conns were not reaped")
}

func TestReaperOffByDefault(t *testing.T) {
	p := reaperPool(t, Options{ConnMaxLifetime: 20 * time.Millisecond})
	fillIdle(t, p, 3)
	time.Sleep(100 * time.Millisecond)
	if p.Len() != 3 {
		t.Fatalf("len = %d, want 3: no reaper, conns stay until Get", p.Len())
	}
}

func TestReaperKeepsUnexpiredAndInUse(t *testing.T) {
	p := reaperPool(t, Options{ConnMaxLifetime: time.Hour})
	ctx := context.Background()
	fillIdle(t, p, 2)
	if n := p.reapExpiredIdle(ctx); n != 0 {
		t.Fatalf("reaped %d unexpired conns", n)
	}

	held, err := p.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Put(ctx, held)
	held.expiresAt = time.Now().Add(-time.Second)
	if n := p.reapExpiredIdle(ctx); n != 0 {
		t.Fatalf("reaped %d, want 0: in-use conn must not be touched", n)
	}
	if p.Len() != 2 {
		t.Fatalf("len = %d, want 2", p.Len())
	}
}

func TestReaperSkipsNonIdleState(t *testing.T) {
	p := reaperPool(t, Options{ConnMaxLifetime: time.Hour})
	cns := fillIdle(t, p, 2)
	for _, cn := range cns {
		cn.expiresAt = time.Now().Add(-time.Second)
	}
	// Simulate a handoff worker owning one idle-listed conn.
	if _, err := cns[0].GetStateMachine().TryTransition([]ConnState{StateCreated, StateIdle}, StateUnusable); err != nil {
		t.Fatal(err)
	}
	if n := p.reapExpiredIdle(context.Background()); n != 1 {
		t.Fatalf("reaped %d, want 1 (unusable one skipped)", n)
	}
}

func TestReaperRefillsMinIdle(t *testing.T) {
	p := reaperPool(t, Options{
		ConnMaxLifetime:             50 * time.Millisecond,
		ConnMaxLifetimeReapInterval: 10 * time.Millisecond,
		MinIdleConns:                2,
	})
	eventually(t, func() bool { return p.IdleLen() >= 2 }, "initial min idle not filled")
	first := make(map[uint64]bool)
	p.connsMu.Lock()
	for id := range p.conns {
		first[id] = true
	}
	p.connsMu.Unlock()

	eventually(t, func() bool {
		p.connsMu.Lock()
		defer p.connsMu.Unlock()
		if len(p.conns) < 2 {
			return false
		}
		for id := range p.conns {
			if first[id] {
				return false
			}
		}
		return true
	}, "expired min-idle conns were not replaced")
}

func TestReaperStopsOnClose(t *testing.T) {
	p := reaperPool(t, Options{
		ConnMaxLifetime:             time.Hour,
		ConnMaxLifetimeReapInterval: time.Millisecond,
	})
	_ = p.Close()
	if n := p.reapExpiredIdle(context.Background()); n != 0 {
		t.Fatalf("reaped %d after close", n)
	}
}

func TestReaperClosesIdleTimeExpired(t *testing.T) {
	// No ConnMaxLifetime: the reaper must still start for ConnMaxIdleTime.
	p := reaperPool(t, Options{
		ConnMaxIdleTime:             50 * time.Millisecond,
		ConnMaxLifetimeReapInterval: 10 * time.Millisecond,
	})
	fillIdle(t, p, 4)
	eventually(t, func() bool { return p.Len() == 0 && p.IdleLen() == 0 },
		"idle-time expired conns were not reaped")
}

func TestReaperShrinksAfterSpikeToMinIdle(t *testing.T) {
	p := reaperPool(t, Options{
		ConnMaxIdleTime:             60 * time.Millisecond,
		ConnMaxLifetimeReapInterval: 10 * time.Millisecond,
		MinIdleConns:                1,
	})
	fillIdle(t, p, 4) // the "spike"
	// All idle-expired conns close; the floor is refilled with a fresh one.
	eventually(t, func() bool { return p.Len() == 1 && p.IdleLen() == 1 },
		"pool did not shrink to MinIdleConns after the spike")
}

func TestReaperKeepsRecentlyUsedIdle(t *testing.T) {
	p := reaperPool(t, Options{ConnMaxIdleTime: time.Hour})
	fillIdle(t, p, 2)
	if n := p.reapExpiredIdle(context.Background()); n != 0 {
		t.Fatalf("reaped %d recently used conns", n)
	}
}
