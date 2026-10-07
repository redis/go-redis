package pubsub

import (
	"context"
	"errors"
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

// lockCheckingResolver is a redirectingResolver that also records
// whether each callback found the manager lock held — the
// serialization AddrResolver promises, which lets a resolver decide
// both callbacks from shared state without locking of its own.
type lockCheckingResolver struct {
	redirectingResolver
	m        *Manager
	calls    atomic.Int32
	unlocked atomic.Int32 // callbacks that found the manager lock free
}

func (r *lockCheckingResolver) ShouldReplace(cn *pool.Conn) bool {
	r.observe()
	return r.redirectingResolver.ShouldReplace(cn)
}

func (r *lockCheckingResolver) Resolve(ctx context.Context, prev *pool.Conn, current string) string {
	r.observe()
	return r.redirectingResolver.Resolve(ctx, prev, current)
}

// observe counts a callback, noting whether the manager lock was free:
// TryLock succeeds only then (and is undone right away).
func (r *lockCheckingResolver) observe() {
	r.calls.Add(1)
	if r.m.mu.TryLock() {
		r.m.mu.Unlock()
		r.unlocked.Add(1)
	}
}

// TestResolverCallbacksRunUnderManagerLock pins the serialization the
// AddrResolver contract promises: ShouldReplace (consulted after a
// frame read) and Resolve (consulted by a reconnect) both run under the
// manager lock, so they can never overlap.
func TestResolverCallbacksRunUnderManagerLock(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	r := &lockCheckingResolver{redirectingResolver: redirectingResolver{target: "node-b:6379"}}
	m := newResolverTestManager(t, srv, testConfig("node-a:6379"), r)
	r.m = m

	h, err := m.Subscribe(ctx, "ch")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc1 := srv.waitDial(t)
	fsc1.expectCmd(t, "subscribe", "ch")
	waitForConfirmation(t, h.Events(), "subscribe", "ch")

	// The frame read with the resolver armed consults ShouldReplace; the
	// reconnect it starts consults Resolve.
	r.replace.Store(true)
	fsc1.sendMessage(t, "message", "ch", "trigger")
	fsc2 := srv.waitDial(t)
	if fsc2.addr != "node-b:6379" {
		t.Fatalf("reconnect dialed %q, want the resolver's node-b:6379", fsc2.addr)
	}
	fsc2.expectCmd(t, "subscribe", "ch")
	fsc1.expectClosed(t)

	if got := r.calls.Load(); got < 2 {
		t.Fatalf("resolver consulted %d time(s), want both ShouldReplace and Resolve", got)
	}
	if n := r.unlocked.Load(); n != 0 {
		t.Fatalf("%d resolver callback(s) ran without the manager lock", n)
	}
}

// dropConnByWriteFailure makes the manager drop fsc's connection the
// way a broken socket does: the server stops reading after one more
// frame (a sacrificial ping), so the ping after it blocks on the
// synchronous pipe and hits the write timeout. The handoff mark is set
// between the two pings, so no frame is read with the mark in place:
// the drop, not the live-replacement check, must carry it over.
func dropConnByWriteFailure(t *testing.T, h PubSuber, fsc *fakeServerConn, handoffTo string) {
	t.Helper()
	ctx := context.Background()
	fsc.pauseAfterNext()
	if err := h.PingSilent(ctx); err != nil {
		t.Fatalf("arming ping: %v", err)
	}
	fsc.expectCmd(t, "ping")
	if err := fsc.poolConn.MarkForHandoff(handoffTo, 1); err != nil {
		t.Fatalf("MarkForHandoff: %v", err)
	}
	if err := h.PingSilent(ctx); err == nil {
		t.Fatal("ping with a blocked write succeeded, want error")
	}
	fsc.paused.Store(false)
	fsc.expectClosed(t)
}

// TestLostConnectionRestoreFollowsHandoff pins that a connection lost
// to a failed write still steers its restoration: the read loop's
// redial shows the resolver the lost connection, so a maintenance
// handoff it had received (in the client's resolver: a MarkForHandoff)
// redirects the dial instead of reconnecting to the endpoint being
// vacated and staying there.
func TestLostConnectionRestoreFollowsHandoff(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	cfg := testConfig("node-a:6379")
	cfg.WriteTimeout = 200 * time.Millisecond
	m := newResolverTestManager(t, srv, cfg, testHandoffResolver{})

	h, err := m.Subscribe(ctx, "ch")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc1 := srv.waitDial(t)
	fsc1.expectCmd(t, "subscribe", "ch")
	waitForConfirmation(t, h.Events(), "subscribe", "ch")

	dropConnByWriteFailure(t, h, fsc1, "node-b:6379")

	// With "ch" registered the read loop restores the connection — at
	// the handoff endpoint.
	fsc2 := srv.waitDial(t)
	if fsc2.addr != "node-b:6379" {
		t.Fatalf("restoration dialed %q, want the handoff endpoint node-b:6379", fsc2.addr)
	}
	fsc2.expectCmd(t, "subscribe", "ch")
	waitForConfirmation(t, h.Events(), "subscribe", "ch")
}

// TestForegroundRestoreFollowsHandoff pins the same for the other
// restoration path: with nothing subscribed the read loop has no reason
// to redial, so the next Subscribe restores the connection itself —
// and consults the resolver with the lost connection exactly like a
// reconnect, rather than dialing the lost connection's address.
func TestForegroundRestoreFollowsHandoff(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	cfg := testConfig("node-a:6379")
	cfg.WriteTimeout = 200 * time.Millisecond
	m := newResolverTestManager(t, srv, cfg, testHandoffResolver{})

	// A ping-only handle: its ping dials, but nothing is registered.
	h := m.NewHandle()
	if err := h.PingSilent(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	fsc1 := srv.waitDial(t)
	if fsc1.addr != "node-a:6379" {
		t.Fatalf("first dial went to %q, want node-a:6379", fsc1.addr)
	}
	fsc1.expectCmd(t, "ping")

	dropConnByWriteFailure(t, h, fsc1, "node-b:6379")

	// Nothing subscribed: the read loop must not redial on its own.
	select {
	case fsc := <-srv.dialCh:
		t.Fatalf("read loop redialed %q with nothing subscribed", fsc.addr)
	case <-time.After(100 * time.Millisecond):
	}

	// The foreground Subscribe restores the connection — at the handoff
	// endpoint.
	if _, err := h.Subscribe(ctx, "ch"); err != nil {
		t.Fatalf("Subscribe after the drop: %v", err)
	}
	fsc2 := srv.waitDial(t)
	if fsc2.addr != "node-b:6379" {
		t.Fatalf("restoration dialed %q, want the handoff endpoint node-b:6379", fsc2.addr)
	}
	fsc2.expectCmd(t, "subscribe", "ch")
	waitForConfirmation(t, h.Events(), "subscribe", "ch")
}

// rotatingResolver mimics a resolver that picks another endpoint when
// there is no previous connection to learn from — the shape of a
// resolver that rotates through candidates until one dials.
type rotatingResolver struct {
	target   string
	nilPrevs atomic.Int32 // Resolve calls that saw no previous connection
}

func (*rotatingResolver) ShouldReplace(*pool.Conn) bool { return false }

func (r *rotatingResolver) Resolve(_ context.Context, prev *pool.Conn, current string) string {
	if prev != nil {
		return current
	}
	r.nilPrevs.Add(1)
	return r.target
}

// TestFailedFirstDialResolvesLaterDials pins the resolver contract
// when the very first dial fails: no connection was ever dropped, yet
// every later dial must still consult the resolver (with a nil
// previous connection), or a Subscribe retried against a dead address
// could never reach the working one the resolver knows. The scenario
// is sticky on purpose: Manager.Subscribe rolls back the registration
// its failed call made, so no background reconnect runs and only the
// next foreground call can dial.
func TestFailedFirstDialResolvesLaterDials(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	var deadDials atomic.Int32
	newConn := func(ctx context.Context, addr string) (*pool.Conn, error) {
		if addr == "dead:6379" {
			deadDials.Add(1)
			return nil, errors.New("connection refused")
		}
		return srv.dial(ctx, addr)
	}
	r := &rotatingResolver{target: "node-b:6379"}
	m := NewManager(testConfig("dead:6379"), newConn, func(cn *pool.Conn) error { return cn.Close() },
		func(context.Context, *pool.Conn, *proto.Reader) error { return nil },
		testIsBadConn, nil, r)
	t.Cleanup(func() { _ = m.Close() })

	// The first dial goes to the configured address, unresolved, and
	// fails; the call's registration is rolled back with it.
	if _, err := m.Subscribe(ctx, "ch"); err == nil {
		t.Fatal("Subscribe against the dead address succeeded, want a dial error")
	}
	if n := deadDials.Load(); n != 1 {
		t.Fatalf("dead address dialed %d times, want 1", n)
	}

	// The next dial is resolved — with no previous connection to show —
	// and lands on the endpoint the resolver picked.
	h, err := m.Subscribe(ctx, "ch")
	if err != nil {
		t.Fatalf("second Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	if fsc.addr != "node-b:6379" {
		t.Fatalf("second dial went to %q, want the resolver's node-b:6379", fsc.addr)
	}
	fsc.expectCmd(t, "subscribe", "ch")
	waitForConfirmation(t, h.Events(), "subscribe", "ch")
	if n := deadDials.Load(); n != 1 {
		t.Fatalf("dead address dialed %d times in total, want only the first", n)
	}
	if n := r.nilPrevs.Load(); n == 0 {
		t.Fatal("resolver was never consulted with a nil previous connection")
	}
}
