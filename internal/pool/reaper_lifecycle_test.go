package pool

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type funcHook struct{ onRemove func(*Conn) }

func (h *funcHook) OnGet(context.Context, *Conn, bool) (bool, error) { return true, nil }
func (h *funcHook) OnPut(context.Context, *Conn) (bool, bool, error) { return true, false, nil }
func (h *funcHook) OnRemove(_ context.Context, cn *Conn, _ error)    { h.onRemove(cn) }

// Hooks must see the conn already CLOSED, so per-conn trackers that check
// IsClosed (the streaming re-auth hook) cannot re-create state after OnRemove.
func TestReaperMarksConnClosedBeforeRemoveHooks(t *testing.T) {
	p := reaperPool(t, Options{ConnMaxLifetime: time.Hour})
	var closedAtHook atomic.Int32
	p.AddPoolHook(&funcHook{onRemove: func(cn *Conn) {
		if cn.IsClosed() {
			closedAtHook.Add(1)
		}
	}})
	cns := fillIdle(t, p, 3)
	for _, cn := range cns {
		cn.expiresAt = time.Now().Add(-time.Second)
	}
	if n := p.reapExpiredIdle(context.Background()); n != 3 {
		t.Fatalf("reaped %d, want 3", n)
	}
	if got := closedAtHook.Load(); got != 3 {
		t.Fatalf("hook saw a closed conn %d times, want 3", got)
	}
	for _, cn := range cns {
		if !cn.IsClosed() {
			t.Fatal("conn not closed")
		}
	}
}

// Close must not return while the reaper is still running hooks for conns it
// already removed from the pool.
func TestReaperCloseWaitsForInFlightPass(t *testing.T) {
	p := reaperPool(t, Options{
		ConnMaxLifetime:             20 * time.Millisecond,
		ConnMaxLifetimeReapInterval: 5 * time.Millisecond,
	})
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	p.AddPoolHook(&funcHook{onRemove: func(*Conn) {
		once.Do(func() { close(entered) })
		<-release
	}})
	fillIdle(t, p, 1)

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("reaper never reached the removal hook")
	}
	closed := make(chan struct{})
	go func() {
		_ = p.Close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("Close returned while the reaper was still running hooks")
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	select {
	case <-closed:
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not return after the reaper finished")
	}
}

// If the replacement dials after a reap fail, a later tick must retry them even
// though the pool is empty and nothing is expired.
func TestReaperRetriesMinIdleAfterFailedDial(t *testing.T) {
	var fail atomic.Bool
	p := NewConnPool(&Options{
		PoolSize:                    4,
		MaxConcurrentDials:          4,
		DialTimeout:                 time.Second,
		PoolTimeout:                 time.Second,
		DialerRetries:               1,
		DialerRetryTimeout:          time.Millisecond,
		ConnMaxLifetime:             60 * time.Millisecond,
		ConnMaxLifetimeReapInterval: 10 * time.Millisecond,
		MinIdleConns:                1,
		Dialer: func(context.Context) (net.Conn, error) {
			if fail.Load() {
				return nil, errors.New("dial refused")
			}
			c, s := net.Pipe()
			t.Cleanup(func() { _ = s.Close() })
			return c, nil
		},
	})
	t.Cleanup(func() { _ = p.Close() })

	eventually(t, func() bool { return p.Len() == 1 }, "min idle not filled")
	fail.Store(true)
	eventually(t, func() bool { return p.Len() == 0 }, "expired conn was not reaped")
	time.Sleep(50 * time.Millisecond) // let failed replacement dials settle
	fail.Store(false)
	eventually(t, func() bool { return p.Len() == 1 && p.IdleLen() == 1 },
		"idle floor was not refilled after the server recovered")
}

// idleConnsLen is incremented under connsMu on Put, so a concurrent reaper pass
// can never see it go negative.
func TestReaperIdleCounterNeverNegative(t *testing.T) {
	p := reaperPool(t, Options{
		ConnMaxLifetime:             time.Millisecond,
		ConnMaxLifetimeReapInterval: 100 * time.Microsecond,
	})
	ctx := context.Background()
	stop := time.Now().Add(400 * time.Millisecond)
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for time.Now().Before(stop) {
				cn, err := p.Get(ctx)
				if err != nil {
					t.Errorf("Get: %v", err)
					return
				}
				cn.GetStateMachine().Transition(StateInUse)
				p.Put(ctx, cn)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for time.Now().Before(stop) {
			if n := p.idleConnsLen.Load(); n < 0 {
				t.Errorf("idleConnsLen went negative: %d", n)
				return
			}
		}
	}()
	wg.Wait()
}
