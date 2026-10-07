package pubsub

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

// TestFreshDialConfirmationReachesNewOwner pins that a subscriber whose
// Subscribe dials the connection is registered before the replay is
// written: the replay's confirmation is broadcast to the owners at the
// moment it arrives, and the new handle must be among them (with the
// replay written outside the lock, an explicit write after it could
// miss a confirmation that landed in between).
func TestFreshDialConfirmationReachesNewOwner(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newTestManager(t, srv, testConfig("node:6379"))

	// A retained registration without a connection, as a failed connect
	// leaves behind on a caller-held handle.
	a := m.NewHandle().(*handle)
	m.mu.Lock()
	a.channels["x"] = struct{}{}
	m.registerHandleLocked(m.subscribers, "x", a)
	m.mu.Unlock()

	// b's Subscribe dials; the replay writes x once, for both owners.
	b, err := m.Subscribe(ctx, "x")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "x")
	fsc.expectNoCmd(t, 100*time.Millisecond)
	waitForConfirmation(t, a.Events(), "subscribe", "x")
	waitForConfirmation(t, b.Events(), "subscribe", "x")
}

// TestExpiredContextDoesNotDropConnection pins that a caller whose
// context is already done gets its error back without touching the
// shared connection: a write on its behalf would fail before any byte
// is sent, and dropping the connection for it would cost every other
// subscriber a reconnect.
func TestExpiredContextDoesNotDropConnection(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newTestManager(t, srv, testConfig("node:6379"))

	h, err := m.Subscribe(ctx, "a")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	ch := h.Channel()
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "a")

	expired, cancel := context.WithCancel(ctx)
	cancel()
	// Repeat: acquireIO's select is not biased, so a single attempt may
	// legitimately report the cancellation from the acquire itself.
	for range 20 {
		if err := h.Ping(expired); !errors.Is(err, context.Canceled) {
			t.Fatalf("Ping with a cancelled context = %v, want context.Canceled", err)
		}
		if _, err := h.Subscribe(expired, "b"); !errors.Is(err, context.Canceled) {
			t.Fatalf("Subscribe with a cancelled context = %v, want context.Canceled", err)
		}
	}
	if channels, _, _ := h.Subscriptions(); len(channels) != 1 || channels[0] != "a" {
		t.Fatalf("subscriptions after cancelled Subscribe = %v, want [a]", channels)
	}

	// The connection is intact: nothing was written, nothing redialed,
	// and delivery continues.
	fsc.expectNoCmd(t, 100*time.Millisecond)
	select {
	case fsc2 := <-srv.dialCh:
		t.Fatalf("a cancelled caller cost a reconnect to %s", fsc2.addr)
	default:
	}
	fsc.sendMessage(t, "message", "a", "alive")
	if msg := recvMsg(t, ch); msg.Payload != "alive" {
		t.Fatalf("got %q, want \"alive\"", msg.Payload)
	}
}

// TestCallerDeadlineDoesNotBoundSharedWrite pins the other half of the
// expired-context fix: a caller's deadline that passes while its write
// is in flight must not fail that write, because the shared connection
// would be dropped for every subscriber. The write is bounded by the
// manager's WriteTimeout alone; here it stalls past the caller's
// deadline, then completes, and the connection stays up.
func TestCallerDeadlineDoesNotBoundSharedWrite(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	cfg := testConfig("node:6379")
	cfg.WriteTimeout = 5 * time.Second
	m := newTestManager(t, srv, cfg)

	h, err := m.Subscribe(ctx, "a")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "a")
	fsc.sendConfirm(t, "subscribe", "a", 1)
	waitForConfirmation(t, h.Events(), "subscribe", "a")

	// Stall the server after the arming ping: the Subscribe write below
	// blocks on the pipe, past the caller's deadline, until the server
	// resumes well within WriteTimeout.
	fsc.pauseAfterNext()
	if err := m.Ping(ctx); err != nil {
		t.Fatalf("arming ping: %v", err)
	}
	fsc.expectCmd(t, "ping")
	go func() {
		time.Sleep(200 * time.Millisecond)
		fsc.paused.Store(false)
	}()

	short, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if _, err := h.Subscribe(short, "b"); err != nil {
		t.Fatalf("Subscribe whose deadline passed mid-write: %v", err)
	}
	fsc.expectCmd(t, "subscribe", "b")
	fsc.sendConfirm(t, "subscribe", "b", 2)
	waitForConfirmation(t, h.Events(), "subscribe", "b")

	// The connection survived: no redial.
	select {
	case fsc2 := <-srv.dialCh:
		t.Fatalf("a caller's deadline cost a reconnect to %s", fsc2.addr)
	default:
	}
}

// TestIdleReadIgnoresRelaxedTimeout pins the read loop's blocking read
// against maintenance windows: the manager reads with no deadline, and
// a relaxed timeout set on the connection (a MOVING/MIGRATING window)
// must not turn that into a finite one — an idle subscription would
// time out, look broken and be reconnected although the socket is
// healthy. The connection stays, and delivery continues afterwards.
func TestIdleReadIgnoresRelaxedTimeout(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newTestManager(t, srv, testConfig("node:6379"))

	h, err := m.Subscribe(ctx, "idle")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	ch := h.Channel()
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "idle")

	// A relaxed window far shorter than the idle period that follows.
	fsc.poolConn.SetRelaxedTimeout(50*time.Millisecond, 50*time.Millisecond)
	select {
	case fsc2 := <-srv.dialCh:
		t.Fatalf("idle connection reconnected to %s under a relaxed timeout", fsc2.addr)
	case <-time.After(300 * time.Millisecond):
	}

	fsc.sendMessage(t, "message", "idle", "still-here")
	if msg := recvMsg(t, ch); msg.Payload != "still-here" {
		t.Fatalf("got %q, want \"still-here\"", msg.Payload)
	}
}

// TestHealthCheckWithoutPingTimeout pins that a non-positive PingTimeout
// disables the probe's deadline and the reply requirement instead of
// failing every probe: pings still go out, and an unanswered one does
// not drop the connection.
func TestHealthCheckWithoutPingTimeout(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer() // pings unanswered
	cfg := testConfig("node:6379")
	cfg.HealthCheckInterval = 20 * time.Millisecond
	cfg.PingTimeout = -1
	m := newTestManager(t, srv, cfg)

	if _, err := m.Subscribe(ctx, "hc"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "hc")
	fsc.sendConfirm(t, "subscribe", "hc", 1)

	fsc.expectCmd(t, "ping")
	fsc.expectCmd(t, "ping")
	select {
	case fsc2 := <-srv.dialCh:
		t.Fatalf("reconnected to %s with PingTimeout disabled", fsc2.addr)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestUnsubscribeUnownedNameConfirms pins that an unsubscribe of a name
// the handle does not own, or a bare unsubscribe on a handle owning
// nothing, still produces the confirmation the server would have sent,
// so a Receive awaiting it returns instead of hanging; nothing is
// written for them.
func TestUnsubscribeUnownedNameConfirms(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newTestManager(t, srv, testConfig("node:6379"))

	h, err := m.Subscribe(ctx, "a")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "a")
	waitForConfirmation(t, h.Events(), "subscribe", "a")

	if err := h.Unsubscribe(ctx, "b"); err != nil {
		t.Fatalf("Unsubscribe of an unowned name: %v", err)
	}
	sub, ok := recvEvent(t, h.Events()).(*Subscription)
	if !ok || sub.Kind != "unsubscribe" || sub.Channel != "b" || sub.Count != 1 {
		t.Fatalf("event = %#v, want unsubscribe b with count 1", sub)
	}
	fsc.expectNoCmd(t, 100*time.Millisecond)

	empty := m.NewHandle()
	if err := empty.Unsubscribe(ctx); err != nil {
		t.Fatalf("bare Unsubscribe on an empty handle: %v", err)
	}
	sub, ok = recvEvent(t, empty.Events()).(*Subscription)
	if !ok || sub.Kind != "unsubscribe" || sub.Channel != "" || sub.Count != 0 {
		t.Fatalf("event = %#v, want a bare unsubscribe confirmation with count 0", sub)
	}
	fsc.expectNoCmd(t, 100*time.Millisecond)
}

// TestChannelSizeIgnoresNonPositive pins that WithChannelSize with a
// non-positive size leaves the configured buffer alone: zero would make
// every later delivery stream unbuffered, a negative size would panic.
func TestChannelSizeIgnoresNonPositive(t *testing.T) {
	srv := newFakeServer()
	m := newTestManager(t, srv, testConfig("node:6379"))

	for _, size := range []int{0, -1} {
		m.NewHandle().Channel(func(c PubSubConfiger) { c.UpdateChannelSize(size) })
		if got, _ := m.consumerSettings(); got != 16 {
			t.Fatalf("ChanSize after UpdateChannelSize(%d) = %d, want the configured 16", size, got)
		}
	}
	m.NewHandle().Channel(func(c PubSubConfiger) { c.UpdateChannelSize(7) })
	if got, _ := m.consumerSettings(); got != 7 {
		t.Fatalf("ChanSize after UpdateChannelSize(7) = %d, want 7", got)
	}
}

// stickyResolver always wants the connection replaced and resolves
// every reconnect to target.
type stickyResolver struct{ target string }

func (stickyResolver) ShouldReplace(*pool.Conn) bool                        { return true }
func (r stickyResolver) Resolve(context.Context, *pool.Conn, string) string { return r.target }

// TestReplacementRetriesBackOff pins that a replacement whose dial keeps
// failing is not retried on every frame: attempts follow the reconnect
// backoff while frames keep flowing from the connection being retired.
func TestReplacementRetriesBackOff(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	cfg := testConfig("node-a:6379")
	cfg.MinRetryBackoff = 100 * time.Millisecond
	cfg.ReconnectMaxBackoff = 100 * time.Millisecond

	var attempts atomic.Int32
	newConn := func(ctx context.Context, addr string) (*pool.Conn, error) {
		if addr == "dead:6379" {
			attempts.Add(1)
			return nil, errors.New("connection refused")
		}
		return srv.dial(ctx, addr)
	}
	m := NewManager(cfg, newConn, func(cn *pool.Conn) error { return cn.Close() },
		func(context.Context, *pool.Conn, *proto.Reader) error { return nil },
		testIsBadConn, nil, stickyResolver{target: "dead:6379"})
	t.Cleanup(func() { _ = m.Close() })

	h, err := m.Subscribe(ctx, "ch")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "ch")
	waitForConfirmation(t, h.Events(), "subscribe", "ch")

	// Every frame read asks for a replacement; the dial to the redirect
	// target fails at once. Without the backoff each frame would pay an
	// attempt.
	const frames = 10
	for i := range frames {
		fsc.sendMessage(t, "message", "ch", fmt.Sprint(i))
	}
	for i := range frames {
		if msg := drainToMessage(t, h.Events()); msg.Payload != fmt.Sprint(i) {
			t.Fatalf("got %q, want %d", msg.Payload, i)
		}
	}
	if n := attempts.Load(); n == 0 || n >= frames {
		t.Fatalf("replacement dial attempts = %d for %d frames, want a few, not one per frame", n, frames)
	}
}

// TestManagerCloseClearsHandleOwnership pins that the manager's
// teardown leaves no handle owning anything: a caller retaining a
// handle across the client's Close must not read back names the
// manager has already dropped along with the connection. (A handle's
// own Close detaches its names one by one on the way out; the
// manager's teardown closes handles wholesale.)
func TestManagerCloseClearsHandleOwnership(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newTestManager(t, srv, testConfig("node:6379"))

	h, err := m.Subscribe(ctx, "ch")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	if _, err := h.PSubscribe(ctx, "p.*"); err != nil {
		t.Fatalf("PSubscribe: %v", err)
	}
	if _, err := h.SSubscribe(ctx, "sch"); err != nil {
		t.Fatalf("SSubscribe: %v", err)
	}
	srv.waitDial(t)
	if c, p, s := h.Subscriptions(); len(c) != 1 || len(p) != 1 || len(s) != 1 {
		t.Fatalf("Subscriptions before Close = %v %v %v, want one name each", c, p, s)
	}

	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if c, p, s := h.Subscriptions(); len(c)+len(p)+len(s) != 0 {
		t.Fatalf("Subscriptions after the manager closed = %v %v %v, want none", c, p, s)
	}
	// Closing a handle of a closed manager stays a nil no-op.
	if err := h.Close(); err != nil {
		t.Fatalf("handle Close after the manager closed = %v, want nil", err)
	}
}
