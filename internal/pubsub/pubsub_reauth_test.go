package pubsub

import (
	"context"
	"strings"
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
// the connection instead. The old connection is unsubscribed and fenced
// before the replacement resubscribes, and it is read up to the fence
// pong, so a message the server queued on it is still delivered.
func TestReAuthRESP2ReplacesConnection(t *testing.T) {
	t.Run("ConfirmedBeforeRotation", func(t *testing.T) {
		testReAuthRESP2Drain(t, true)
	})
	// The subscribe confirmation is still unread on the old connection
	// when the rotation swaps it out. It must still confirm the handle,
	// or the messages after it on the old connection are skipped.
	t.Run("ConfirmationUnreadAtRotation", func(t *testing.T) {
		testReAuthRESP2Drain(t, false)
	})
}

func testReAuthRESP2Drain(t *testing.T, confirmFirst bool) {
	ctx := context.Background()
	srv := newFakeServer()
	var replace atomic.Bool
	m := newReAuthTestManager(t, srv, false, &replace)

	h, err := m.Subscribe(ctx, "ch")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	ch := h.Channel()
	fsc1 := srv.waitDial(t)
	fsc1.expectCmd(t, "subscribe", "ch")
	if confirmFirst {
		fsc1.sendConfirm(t, "subscribe", "ch", 1)
		fsc1.sendMessage(t, "message", "ch", "before")
		if msg := recvMsg(t, ch); msg.Payload != "before" {
			t.Fatalf("got %q, want \"before\"", msg.Payload)
		}
	}

	if err := fsc1.poolConn.GetPubSubReAuth()("user", "pass"); err != nil {
		t.Fatalf("re-auth: %v", err)
	}
	fsc2 := srv.waitDial(t)
	fsc1.expectCmd(t, "unsubscribe")
	fsc1.expectCmd(t, "punsubscribe")
	fence := fsc1.waitCmd(t)
	if len(fence) != 2 || fence[0] != "ping" || !strings.HasPrefix(fence[1], "go-redis-rotation-") {
		t.Fatalf("fence = %v, want [ping go-redis-rotation-...]", fence)
	}
	fsc2.expectCmd(t, "subscribe", "ch")

	if !confirmFirst {
		fsc1.sendConfirm(t, "subscribe", "ch", 1)
	}
	// Queued on the old connection ahead of the fence: still delivered.
	fsc1.sendMessage(t, "message", "ch", "queued")
	if msg := recvMsg(t, ch); msg.Payload != "queued" {
		t.Fatalf("got %q, want \"queued\"", msg.Payload)
	}
	fsc1.pong(fence)
	fsc1.expectClosed(t)

	fsc2.sendConfirm(t, "subscribe", "ch", 1)
	fsc2.sendMessage(t, "message", "ch", "after")
	if msg := recvMsg(t, ch); msg.Payload != "after" {
		t.Fatalf("got %q, want \"after\"", msg.Payload)
	}
}

// The drain covers patterns and shard channels too: the fence releases
// every kind in use (SUNSUBSCRIBE only when shard channels are), and a
// pattern or shard message queued on the old connection is delivered.
func TestReAuthRESP2DrainKinds(t *testing.T) {
	for _, tc := range []struct {
		name      string
		subscribe func(m *Manager, ctx context.Context) (PubSuber, error)
		kind      string
		fence     []string // commands before the fence PING
		message   []string // message frame kind and fields
	}{
		{
			name: "SSubscribe",
			subscribe: func(m *Manager, ctx context.Context) (PubSuber, error) {
				return m.SSubscribe(ctx, "sch")
			},
			kind:    "ssubscribe",
			fence:   []string{"unsubscribe", "punsubscribe", "sunsubscribe"},
			message: []string{"smessage", "sch", "queued"},
		},
		{
			name: "PSubscribe",
			subscribe: func(m *Manager, ctx context.Context) (PubSuber, error) {
				return m.PSubscribe(ctx, "p.*")
			},
			kind:    "psubscribe",
			fence:   []string{"unsubscribe", "punsubscribe"},
			message: []string{"pmessage", "p.*", "p.x", "queued"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			srv := newFakeServer()
			var replace atomic.Bool
			m := newReAuthTestManager(t, srv, false, &replace)

			h, err := tc.subscribe(m, ctx)
			if err != nil {
				t.Fatalf("subscribe: %v", err)
			}
			ch := h.Channel()
			fsc1 := srv.waitDial(t)
			name := tc.message[1]
			fsc1.expectCmd(t, tc.kind, name)
			fsc1.sendConfirm(t, tc.kind, name, 1)

			if err := fsc1.poolConn.GetPubSubReAuth()("user", "pass"); err != nil {
				t.Fatalf("re-auth: %v", err)
			}
			fsc2 := srv.waitDial(t)
			for _, cmd := range tc.fence {
				fsc1.expectCmd(t, cmd)
			}
			fence := fsc1.waitCmd(t)
			if len(fence) != 2 || fence[0] != "ping" {
				t.Fatalf("fence = %v, want [ping ...]", fence)
			}
			fsc2.expectCmd(t, tc.kind, name)

			fsc1.sendMessage(t, tc.message[0], tc.message[1:]...)
			if msg := recvMsg(t, ch); msg.Payload != "queued" {
				t.Fatalf("got %q, want \"queued\"", msg.Payload)
			}
			fsc1.pong(fence)
			fsc1.expectClosed(t)
		})
	}
}

// A rotation is invisible to subscribers: the replacement's confirmations
// for names the handle already has produce no subscribe events, and the
// rotation's own unsubscribes on the old connection produce none either.
func TestReAuthRESP2NoSubscriptionEvents(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	var replace atomic.Bool
	m := newReAuthTestManager(t, srv, false, &replace)

	h, err := m.Subscribe(ctx, "ch")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	events := h.ChannelWithSubscriptions()
	fsc1 := srv.waitDial(t)
	fsc1.expectCmd(t, "subscribe", "ch")
	fsc1.sendConfirm(t, "subscribe", "ch", 1)
	expectEvent(t, events, func(ev any) bool {
		sub, ok := ev.(*Subscription)
		return ok && sub.Kind == "subscribe" && sub.Channel == "ch"
	})

	if err := fsc1.poolConn.GetPubSubReAuth()("user", "pass"); err != nil {
		t.Fatalf("re-auth: %v", err)
	}
	fsc2 := srv.waitDial(t)
	fsc1.expectCmd(t, "unsubscribe")
	fsc1.expectCmd(t, "punsubscribe")
	fence := fsc1.waitCmd(t)
	fsc2.expectCmd(t, "subscribe", "ch")
	fsc1.sendConfirm(t, "unsubscribe", "ch", 0)
	fsc1.pong(fence)
	fsc1.expectClosed(t)
	fsc2.sendConfirm(t, "subscribe", "ch", 1)
	fsc2.sendMessage(t, "message", "ch", "after")

	// The first event after the rotation is the message.
	expectEvent(t, events, func(ev any) bool {
		msg, ok := ev.(*Message)
		if !ok {
			t.Fatalf("got %T %v before the message, want no subscription event", ev, ev)
		}
		return msg.Payload == "after"
	})
}

func expectEvent(t *testing.T, events <-chan any, match func(any) bool) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("event channel closed")
			}
			if match(ev) {
				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for an event")
		}
	}
}

// A replaced RESP2 connection whose fence pong never arrives is closed
// after rotationDrainTimeout, and the read loop moves on to the
// replacement.
func TestReAuthRESP2DrainTimeout(t *testing.T) {
	old := rotationDrainTimeout
	rotationDrainTimeout = 100 * time.Millisecond
	t.Cleanup(func() { rotationDrainTimeout = old })

	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	var replace atomic.Bool
	m := newReAuthTestManager(t, srv, false, &replace)

	h, err := m.Subscribe(ctx, "ch")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	ch := h.Channel()
	fsc1 := srv.waitDial(t)
	fsc1.expectCmd(t, "subscribe", "ch")

	if err := fsc1.poolConn.GetPubSubReAuth()("user", "pass"); err != nil {
		t.Fatalf("re-auth: %v", err)
	}
	fsc2 := srv.waitDial(t)
	fsc2.expectCmd(t, "subscribe", "ch")
	fsc1.expectClosed(t)

	fsc2.sendMessage(t, "message", "ch", "after")
	if msg := recvMsg(t, ch); msg.Payload != "after" {
		t.Fatalf("got %q, want \"after\"", msg.Payload)
	}
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
