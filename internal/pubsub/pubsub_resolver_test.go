package pubsub

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

// redirectingResolver retires the connection once (on the first frame
// read after replace is armed) and resolves every reconnect to target.
type redirectingResolver struct {
	target  string
	replace atomic.Bool
}

func (r *redirectingResolver) ShouldReplace(*pool.Conn) bool { return r.replace.Swap(false) }

func (r *redirectingResolver) Resolve(context.Context, *pool.Conn, string) string {
	return r.target
}

func newResolverTestManager(t *testing.T, srv *fakeServer, cfg Config, r AddrResolver) *Manager {
	t.Helper()
	m := NewManager(
		cfg,
		srv.dial,
		func(cn *pool.Conn) error { return cn.Close() },
		func(context.Context, *pool.Conn, *proto.Reader) error { return nil },
		testIsBadConn,
		nil,
		r,
	)
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// drainToMessage returns the next *Message on events, skipping other
// event kinds.
func drainToMessage(t *testing.T, events <-chan any) *Message {
	t.Helper()
	for {
		if msg, ok := recvEvent(t, events).(*Message); ok {
			return msg
		}
	}
}

// TestAddrResolverPluggable pins the resolver seam: the manager retires
// a connection when the resolver says so and dials the address the
// resolver returns, knowing nothing about the reason — here a test
// policy, in the client the maintenance-handoff one. The frame that
// triggered the replacement is still delivered.
func TestAddrResolverPluggable(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	r := &redirectingResolver{target: "node-b:6379"}
	m := newResolverTestManager(t, srv, testConfig("node-a:6379"), r)

	h, err := m.Subscribe(ctx, "ch")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc1 := srv.waitDial(t)
	if fsc1.addr != "node-a:6379" {
		t.Fatalf("first dial went to %q, want node-a:6379", fsc1.addr)
	}
	fsc1.expectCmd(t, "subscribe", "ch")
	waitForConfirmation(t, h.Events(), "subscribe", "ch")

	// Arm the resolver: the next frame read retires the connection, and
	// the reconnect dials where the resolver points.
	r.replace.Store(true)
	fsc1.sendMessage(t, "message", "ch", "trigger")
	fsc2 := srv.waitDial(t)
	if fsc2.addr != "node-b:6379" {
		t.Fatalf("reconnect dialed %q, want the resolver's node-b:6379", fsc2.addr)
	}
	fsc2.expectCmd(t, "subscribe", "ch")
	fsc1.expectClosed(t)
	if msg := drainToMessage(t, h.Events()); msg.Payload != "trigger" {
		t.Fatalf("got %q, want the frame that triggered the replacement", msg.Payload)
	}
}

// TestNilAddrResolverIgnoresHandoffMarks pins that the manager itself
// knows nothing about handoffs: without a resolver, a connection marked
// for handoff is neither retired nor redirected.
func TestNilAddrResolverIgnoresHandoffMarks(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newResolverTestManager(t, srv, testConfig("node-a:6379"), nil)

	h, err := m.Subscribe(ctx, "ch")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "ch")
	waitForConfirmation(t, h.Events(), "subscribe", "ch")

	if err := fsc.poolConn.MarkForHandoff("node-b:6379", 1); err != nil {
		t.Fatalf("MarkForHandoff: %v", err)
	}
	fsc.sendMessage(t, "message", "ch", "still-here")
	if msg := drainToMessage(t, h.Events()); msg.Payload != "still-here" {
		t.Fatalf("got %q, want \"still-here\"", msg.Payload)
	}
	select {
	case fsc2 := <-srv.dialCh:
		t.Fatalf("manager without a resolver redialed to %q on a handoff mark", fsc2.addr)
	case <-time.After(200 * time.Millisecond):
	}
}
