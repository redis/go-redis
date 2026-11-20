package pubsub

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

// newReAuthTestManager is a manager over srv whose connections report
// RESP3 when resp3 is set, and that retires the live connection when
// replace is set (see RecheckConn).
func newReAuthTestManager(t *testing.T, srv *fakeServer, resp3 bool, replace *atomic.Bool) *Manager {
	t.Helper()
	hooks := testHooks(srv)
	hooks.newConn = func(ctx context.Context, addr string) (*pool.Conn, error) {
		cn, err := srv.dial(ctx, addr)
		if err == nil {
			cn.SetResp3(resp3)
		}
		return cn, err
	}
	hooks.shouldReplaceConn = func(cn *pool.Conn) bool { return replace.Load() || cn.ShouldHandoff() }
	cfg := testConfig("node-a:6379")
	cfg.RESP2 = !resp3
	m := NewManager(cfg, hooks)
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// A re-auth callback is taken from the connection it was scheduled for.
// If the manager replaced the connection before the callback runs, the
// AUTH must not be sent to the replacement, which authenticated when it
// was dialed (issue #4302). The current connection's callback still
// writes AUTH in place, and its +OK reply is consumed silently.
func TestReAuthSkipsReplacedConnection(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	var replace atomic.Bool
	m := newReAuthTestManager(t, srv, true, &replace)

	h, err := m.Subscribe(ctx, "ch")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc1 := srv.waitDial(t)
	fsc1.expectCmd(t, "subscribe", "ch")
	waitForConfirmation(t, h.Events(), "subscribe", "ch")

	staleReAuth := fsc1.poolConn.GetPubSubReAuth()
	if staleReAuth == nil {
		t.Fatal("first connection has no re-auth callback")
	}

	replace.Store(true)
	m.RecheckConn()
	fsc2 := srv.waitDial(t)
	replace.Store(false)
	fsc2.expectCmd(t, "subscribe", "ch")
	fsc1.expectClosed(t)

	if err := staleReAuth("user", "old-pass"); err != nil {
		t.Fatalf("stale re-auth: %v", err)
	}
	fsc2.expectNoCmd(t, 50*time.Millisecond)

	current := fsc2.poolConn.GetPubSubReAuth()
	if current == nil {
		t.Fatal("replacement connection has no re-auth callback")
	}
	if err := current("user", "new-pass"); err != nil {
		t.Fatalf("current re-auth: %v", err)
	}
	fsc2.expectCmd(t, "auth", "user", "new-pass")

	// The +OK reply must not surface as a subscriber event.
	fsc2.write(t, "+OK\r\n")
	fsc2.sendMessage(t, "message", "ch", "after-auth")
	for {
		ev := recvEvent(t, h.Events())
		if _, ok := ev.(*Subscription); ok {
			continue // the replay's confirmation
		}
		if msg, ok := ev.(*Message); !ok || msg.Payload != "after-auth" {
			t.Fatalf("event after AUTH reply = %#v, want the message", ev)
		}
		break
	}
}

// A password-only credential sends the single-argument AUTH.
func TestReAuthPasswordOnly(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	var replace atomic.Bool
	m := newReAuthTestManager(t, srv, true, &replace)

	if _, err := m.Subscribe(ctx, "ch"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "ch")

	if err := fsc.poolConn.GetPubSubReAuth()("", "secret"); err != nil {
		t.Fatalf("re-auth: %v", err)
	}
	fsc.expectCmd(t, "auth", "secret")
}

// A subscribed RESP2 connection cannot run AUTH: new credentials replace
// the connection instead, and the replacement resubscribes.
func TestReAuthRESP2ReplacesConnection(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	var replace atomic.Bool
	m := newReAuthTestManager(t, srv, false, &replace)

	if _, err := m.Subscribe(ctx, "ch"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc1 := srv.waitDial(t)
	fsc1.expectCmd(t, "subscribe", "ch")

	if err := fsc1.poolConn.GetPubSubReAuth()("user", "pass"); err != nil {
		t.Fatalf("re-auth: %v", err)
	}
	fsc2 := srv.waitDial(t)
	fsc2.expectCmd(t, "subscribe", "ch")
	fsc1.expectClosed(t)
}

// ReAuth on a closed manager reports ErrClosed.
func TestReAuthClosedManager(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	var replace atomic.Bool
	m := newReAuthTestManager(t, srv, true, &replace)

	if _, err := m.Subscribe(ctx, "ch"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "ch")
	reAuth := fsc.poolConn.GetPubSubReAuth()
	_ = m.Close()
	if err := reAuth("user", "pass"); err != pool.ErrClosed {
		t.Fatalf("re-auth after Close = %v, want pool.ErrClosed", err)
	}
}
