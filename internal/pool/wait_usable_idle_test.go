package pool

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWaitForUsableIdleChecksReadyBeforeTicker(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	cn := NewConn(client)
	cn.GetStateMachine().Transition(StateIdle)
	p := &ConnPool{
		closedCh:  make(chan struct{}),
		idleConns: []*Conn{cn},
	}
	if err := p.waitForUsableIdle(context.Background(), time.Now().Add(5*time.Millisecond), false); err != nil {
		t.Fatalf("ready idle connection should not wait for ticker: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.waitForUsableIdle(ctx, time.Now().Add(time.Second), false); err != context.Canceled {
		t.Fatalf("cancelled wait returned %v, want context.Canceled", err)
	}
}

func TestUsableIdlePollIntervalRespectsCallerDeadline(t *testing.T) {
	if got := usableIdlePollInterval(context.Background(), time.Second); got != 10*time.Millisecond {
		t.Fatalf("unbounded caller interval = %v, want 10ms", got)
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(30*time.Millisecond))
	defer cancel()
	if got := usableIdlePollInterval(ctx, time.Second); got <= 0 || got > 7500*time.Microsecond {
		t.Fatalf("short-deadline interval = %v, want (0, 7.5ms]", got)
	}
	expiredCtx, expiredCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer expiredCancel()
	if got := usableIdlePollInterval(expiredCtx, time.Second); got != 0 {
		t.Fatalf("expired caller interval = %v, want 0", got)
	}
}


// A handoff worker can own idle-list entries indefinitely. They must not
// prevent a waiting Get from checking whether it can dial a replacement.
// A later ready connection must win over an earlier re-auth entry, and must
// not wait for the full maintenance deadline.
func TestWaitForUsableIdleScansPastReauth(t *testing.T) {
	firstClient, firstServer := net.Pipe()
	defer firstClient.Close()
	defer firstServer.Close()
	secondClient, secondServer := net.Pipe()
	defer secondClient.Close()
	defer secondServer.Close()
	reauth := NewConn(firstClient)
	reauth.GetStateMachine().Transition(StateUnusable)
	ready := NewConn(secondClient)
	ready.GetStateMachine().Transition(StateIdle)
	p := &ConnPool{
		closedCh: make(chan struct{}),
		idleConns: []*Conn{reauth, ready},
	}
	if err := p.waitForUsableIdle(context.Background(), time.Now().Add(100*time.Millisecond), false); err != nil {
		t.Fatalf("ready connection behind re-auth was overlooked: %v", err)
	}
}

func TestWaitForUsableIdleHandoffOnly(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	cn := NewConn(client)
	cn.GetStateMachine().Transition(StateUnusable)
	if err := cn.MarkForHandoff("replacement:6379", 1); err != nil {
		t.Fatal(err)
	}
	p := &ConnPool{
		closedCh:  make(chan struct{}),
		idleConns: []*Conn{cn},
	}
	if err := p.waitForUsableIdle(context.Background(), time.Now().Add(80*time.Millisecond), false); err != nil {
		t.Fatalf("handoff-only idle list should permit a replacement dial: %v", err)
	}
}

type blockingIdleHealthConn struct {
	net.Conn
	block   atomic.Bool
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (c *blockingIdleHealthConn) SetDeadline(t time.Time) error {
	if t.IsZero() && c.block.Load() {
		c.once.Do(func() { close(c.entered) })
		<-c.release
	}
	return c.Conn.SetDeadline(t)
}

// With no pool hooks, a health check cannot requeue the popped connection.
// Another caller must be free to dial against the remaining pool capacity.
func TestHooklessHealthCheckDoesNotBlockSpareDial(t *testing.T) {
	for _, try := range []bool{false, true} {
		name := "Get"
		if try {
			name = "TryGet"
		}
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			client, server := net.Pipe()
			defer server.Close()
			held := &blockingIdleHealthConn{
				Conn: client, entered: make(chan struct{}), release: make(chan struct{}),
			}
			var dials atomic.Int32
			p := NewConnPool(&Options{
				PoolSize: 2, MaxActiveConns: 2, MaxConcurrentDials: 2,
				PoolTimeout: 80 * time.Millisecond,
				Dialer: func(context.Context) (net.Conn, error) {
					if dials.Add(1) == 1 {
						return held, nil
					}
					c, peer := net.Pipe()
					t.Cleanup(func() { _ = peer.Close() })
					return c, nil
				},
			})
			t.Cleanup(func() { _ = p.Close() })
			original, err := p.Get(ctx)
			if err != nil {
				t.Fatal(err)
			}
			original.GetStateMachine().Transition(StateInUse)
			p.Put(ctx, original)
			held.block.Store(true)
			firstDone := make(chan *Conn, 1)
			firstErr := make(chan error, 1)
			go func() {
				cn, err := p.Get(ctx)
				firstDone <- cn
				firstErr <- err
			}()
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(held.release) }) }
			defer release()
			select {
			case <-held.entered:
			case <-time.After(time.Second):
				t.Fatal("idle health check did not block")
			}
			var replacement *Conn
			if try {
				replacement, err = p.TryGet(ctx)
			} else {
				replacement, err = p.Get(ctx)
			}
			if err != nil || replacement == nil || replacement == original {
				t.Fatalf("%s did not dial spare capacity: conn=%p err=%v", name, replacement, err)
			}
			p.Put(ctx, replacement)
			release()
			select {
			case got := <-firstDone:
				if gotErr := <-firstErr; gotErr != nil || got != original {
					t.Fatalf("original idle acquisition: conn=%p err=%v", got, gotErr)
				}
				p.Put(ctx, got)
			case <-time.After(time.Second):
				t.Fatal("blocked health check did not finish")
			}
			if got := dials.Load(); got != 2 {
				t.Fatalf("dial count = %d, want 2", got)
			}
		})
	}
}
