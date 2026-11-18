package pool

import (
	"context"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"weak"
)

func reaperPool(t *testing.T, o Options) *ConnPool {
	t.Helper()
	p := newReaperPool(t, o)
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// newReaperPool builds the pool without registering a Close cleanup, so a test
// can drop it.
func newReaperPool(t *testing.T, o Options) *ConnPool {
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
	return NewConnPool(&o)
}

// fillIdle takes n conns and returns them as IDLE, the state a conn is in
// after the client has used it and Put it back.
func fillIdle(t *testing.T, p *ConnPool, n int) []*Conn {
	t.Helper()
	ctx := context.Background()
	cns := make([]*Conn, 0, n)
	for i := 0; i < n; i++ {
		cn, err := p.Get(ctx)
		if err != nil {
			t.Fatal(err)
		}
		cn.GetStateMachine().Transition(StateInUse)
		cns = append(cns, cn)
	}
	for _, cn := range cns {
		p.Put(ctx, cn)
		if s := cn.GetStateMachine().GetState(); s != StateIdle {
			t.Fatalf("conn state after Put = %v, want IDLE", s)
		}
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
	cns := fillIdle(t, p, 3)
	eventually(t, func() bool { return p.Len() == 0 && p.IdleLen() == 0 },
		"expired idle conns were not reaped")
	// Conns leave the pool under the lock and are closed just after it.
	eventually(t, func() bool {
		for _, cn := range cns {
			if !cn.IsClosed() {
				return false
			}
		}
		return true
	}, "reaped conns were not closed")
}

func TestReaperOffByDefault(t *testing.T) {
	p := reaperPool(t, Options{ConnMaxLifetime: 20 * time.Millisecond})
	if p.reaperDone != nil {
		t.Fatal("reaper started without ConnMaxLifetimeReapInterval")
	}
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

func TestReaperSkipsConnsClaimedByWorker(t *testing.T) {
	for name, claim := range map[string]func(*Conn) bool{
		// A re-auth or handoff worker moves IDLE -> UNUSABLE.
		"unusable": func(cn *Conn) bool {
			_, err := cn.GetStateMachine().TryTransition([]ConnState{StateIdle}, StateUnusable)
			return err == nil
		},
		// Anything that claims the conn the way Get does.
		"in-use": func(cn *Conn) bool { return cn.TryAcquire() },
	} {
		t.Run(name, func(t *testing.T) {
			p := reaperPool(t, Options{ConnMaxLifetime: time.Hour})
			cns := fillIdle(t, p, 2)
			for _, cn := range cns {
				cn.expiresAt = time.Now().Add(-time.Second)
			}
			if !claim(cns[0]) {
				t.Fatal("could not claim conn")
			}
			if n := p.reapExpiredIdle(context.Background()); n != 1 {
				t.Fatalf("reaped %d, want 1 (claimed conn skipped)", n)
			}
			if cns[0].IsClosed() {
				t.Fatal("claimed conn was closed under its owner")
			}
			if !cns[1].IsClosed() {
				t.Fatal("expired idle conn was not closed")
			}
		})
	}
}

func TestReaperRefillsMinIdle(t *testing.T) {
	p := reaperPool(t, Options{
		ConnMaxLifetime:             50 * time.Millisecond,
		ConnMaxLifetimeReapInterval: 10 * time.Millisecond,
		MinIdleConns:                2,
	})
	// MinIdle conns are CREATED, never used: the reaper must take those too.
	eventually(t, func() bool { return p.Len() == 2 }, "initial min idle not filled")
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

func TestReaperGoroutineStopsOnClose(t *testing.T) {
	p := reaperPool(t, Options{
		ConnMaxLifetime:             time.Hour,
		ConnMaxLifetimeReapInterval: time.Millisecond,
	})
	if p.reaperDone == nil {
		t.Fatal("reaper not started")
	}
	_ = p.Close()
	select {
	case <-p.reaperDone:
	case <-time.After(2 * time.Second):
		t.Fatal("reaper goroutine still running after Close")
	}
}

func TestReaperDoesNotKeepDroppedPoolAlive(t *testing.T) {
	p := newReaperPool(t, Options{
		ConnMaxLifetime:             time.Hour,
		ConnMaxLifetimeReapInterval: time.Millisecond,
	})
	done := p.reaperDone
	wp := weak.Make(p)
	p = nil //nolint:ineffassign,wastedassign

	deadline := time.Now().Add(5 * time.Second)
	for wp.Value() != nil && time.Now().Before(deadline) {
		runtime.GC()
		time.Sleep(10 * time.Millisecond)
	}
	if wp.Value() != nil {
		t.Fatal("dropped pool was not collected: reaper holds it")
	}
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reaper goroutine did not exit after the pool was collected")
	}
}

func TestReaperRunsHooksAndMetricsOnce(t *testing.T) {
	var idleDown, closedStale atomic.Int32
	metrics := &MetricCallbacks{
		ConnectionCount: func(_ context.Context, delta int, _ *Conn, state string, _ bool) {
			if delta == -1 && state == MetricStateIdle {
				idleDown.Add(1)
			}
		},
		ConnectionClosed: func(_ context.Context, _ *Conn, reason string, _ error) {
			if reason == CloseReasonStale {
				closedStale.Add(1)
			}
		},
	}
	p := reaperPool(t, Options{ConnMaxLifetime: time.Hour, Metrics: metrics})
	hook := &countRemoveHook{}
	p.AddPoolHook(hook)

	cns := fillIdle(t, p, 3)
	for _, cn := range cns {
		cn.expiresAt = time.Now().Add(-time.Second)
	}
	if n := p.reapExpiredIdle(context.Background()); n != 3 {
		t.Fatalf("reaped %d, want 3", n)
	}
	// A second pass finds nothing: no double events.
	if n := p.reapExpiredIdle(context.Background()); n != 0 {
		t.Fatalf("second pass reaped %d, want 0", n)
	}
	if got := hook.removes.Load(); got != 3 {
		t.Fatalf("OnRemove calls = %d, want 3", got)
	}
	if got := idleDown.Load(); got != 3 {
		t.Fatalf("idle -1 count callbacks = %d, want 3", got)
	}
	if got := closedStale.Load(); got != 3 {
		t.Fatalf("ConnectionClosed(stale) = %d, want 3", got)
	}
}

type countRemoveHook struct{ removes atomic.Int32 }

func (h *countRemoveHook) OnGet(context.Context, *Conn, bool) (bool, error) { return true, nil }

func (h *countRemoveHook) OnPut(context.Context, *Conn) (bool, bool, error) {
	return true, false, nil
}
func (h *countRemoveHook) OnRemove(context.Context, *Conn, error) { h.removes.Add(1) }

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

func TestReaperTakeKeepsSurvivorOrderAndCounters(t *testing.T) {
	p := reaperPool(t, Options{ConnMaxLifetime: time.Hour})
	fillIdle(t, p, 4)
	idle := append([]*Conn(nil), p.idleConns...)
	past := time.Now().Add(-time.Second)
	idle[0].expiresAt = past
	idle[2].expiresAt = past

	if n := p.reapExpiredIdle(context.Background()); n != 2 {
		t.Fatalf("reaped %d, want 2", n)
	}
	p.connsMu.Lock()
	defer p.connsMu.Unlock()
	if len(p.idleConns) != 2 || p.idleConns[0] != idle[1] || p.idleConns[1] != idle[3] {
		t.Fatalf("survivors out of order or wrong: %v", p.idleConns)
	}
	if got := p.idleConnsLen.Load(); got != 2 {
		t.Fatalf("idleConnsLen = %d, want 2", got)
	}
	if got := p.poolSize.Load(); got != 2 {
		t.Fatalf("poolSize = %d, want 2", got)
	}
	if len(p.conns) != 2 {
		t.Fatalf("conns = %d, want 2", len(p.conns))
	}
	// The dropped conns must not stay reachable through the backing array.
	tail := p.idleConns[:cap(p.idleConns)][len(p.idleConns):4]
	for i, c := range tail {
		if c != nil {
			t.Fatalf("tail[%d] still references a dropped conn", i)
		}
	}
}

// Get/Put churn against a fast reaper: accounting must stay consistent and a
// Get must never hand out a closed conn.
func TestReaperConcurrentWithGetPut(t *testing.T) {
	for _, fifo := range []bool{false, true} {
		name := "lifo"
		if fifo {
			name = "fifo"
		}
		t.Run(name, func(t *testing.T) {
			p := reaperPool(t, Options{
				PoolFIFO:                    fifo,
				ConnMaxLifetime:             2 * time.Millisecond,
				ConnMaxLifetimeJitter:       time.Millisecond,
				ConnMaxLifetimeReapInterval: 200 * time.Microsecond,
			})
			ctx := context.Background()
			stop := time.Now().Add(500 * time.Millisecond)
			var wg sync.WaitGroup
			for w := 0; w < 8; w++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for time.Now().Before(stop) {
						cn, err := p.Get(ctx)
						if err != nil {
							t.Errorf("Get: %v", err)
							return
						}
						if cn.IsClosed() {
							t.Error("Get returned a closed conn")
							return
						}
						cn.GetStateMachine().Transition(StateInUse)
						time.Sleep(50 * time.Microsecond)
						p.Put(ctx, cn)
					}
				}()
			}
			wg.Wait()

			p.connsMu.Lock()
			defer p.connsMu.Unlock()
			if got, want := p.idleConnsLen.Load(), int32(len(p.idleConns)); got != want {
				t.Fatalf("idleConnsLen = %d, len(idleConns) = %d", got, want)
			}
			if got, want := p.poolSize.Load(), int32(len(p.conns)); got != want {
				t.Fatalf("poolSize = %d, len(conns) = %d", got, want)
			}
		})
	}
}
