package pubsub

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

// subscribeResult is what an asynchronous Manager.Subscribe returned.
type subscribeResult struct {
	h   PubSuber
	err error
}

// subscribeAsync runs m.Subscribe in a goroutine, reporting its result.
func subscribeAsync(m *Manager, channels ...string) <-chan subscribeResult {
	res := make(chan subscribeResult, 1)
	go func() {
		h, err := m.Subscribe(context.Background(), channels...)
		res <- subscribeResult{h: h, err: err}
	}()
	return res
}

func awaitSubscribe(t *testing.T, res <-chan subscribeResult) subscribeResult {
	t.Helper()
	select {
	case r := <-res:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Subscribe to return")
		return subscribeResult{}
	}
}

// expectNoGateHit asserts that no further dial reaches the gate within d.
func (s *fakeServer) expectNoGateHit(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case <-s.gateHit:
		t.Fatal("a second dial reached the gate; dials must be single-flight")
	case <-time.After(d):
	}
}

// TestDialReleasesManagerLock pins that a dial runs with the manager
// lock released: while one subscriber's dial is parked, everything that
// takes the lock — a new handle, Subscriptions, CloseIfIdle, and a
// writer such as a handle's Close — keeps working instead of waiting out
// the dial.
func TestDialReleasesManagerLock(t *testing.T) {
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newTestManager(t, srv, testConfig("node:6379"))

	gate := make(chan struct{})
	srv.setDialGate(gate)

	res := subscribeAsync(m, "a")
	srv.waitGateHit(t)

	done := make(chan struct{})
	go func() {
		defer close(done)
		h := m.NewHandle()
		if channels, _, _ := h.Subscriptions(); len(channels) != 0 {
			t.Errorf("fresh handle owns %v, want nothing", channels)
		}
		// Not idle: the dialing subscriber's handle is registered.
		if m.CloseIfIdle() {
			t.Error("CloseIfIdle closed a manager with a subscribe in flight")
		}
		if err := h.Close(); err != nil {
			t.Errorf("handle Close during the dial: %v", err)
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("lock-taking operations blocked behind the in-flight dial")
	}

	// Release the dial: the subscribe completes on the dialed connection.
	close(gate)
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "a")
	if r := awaitSubscribe(t, res); r.err != nil {
		t.Fatalf("Subscribe: %v", r.err)
	}
}

// TestCloseDuringDialDoesNotWait pins that Close neither waits for an
// in-flight dial nor leaks its connection: Close returns while the dial
// is parked and aborts it, so no connection comes out of it, and the
// subscriber gets pool.ErrClosed.
func TestCloseDuringDialDoesNotWait(t *testing.T) {
	srv := newFakeServer()
	m := newTestManager(t, srv, testConfig("node:6379"))

	gate := make(chan struct{})
	srv.setDialGate(gate)
	defer close(gate)

	res := subscribeAsync(m, "a")
	srv.waitGateHit(t)

	closed := make(chan error, 1)
	go func() { closed <- m.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close waited for the in-flight dial")
	}

	if r := awaitSubscribe(t, res); !errors.Is(r.err, pool.ErrClosed) {
		t.Fatalf("Subscribe racing Close = (%v, %v), want pool.ErrClosed", r.h, r.err)
	}
	select {
	case fsc := <-srv.dialCh:
		t.Fatalf("the aborted dial produced a connection to %s", fsc.addr)
	default:
	}
}

// TestDialFinishingAfterCloseIsClosed pins the other side of the race:
// a dial that completes despite Close (its dial function ignored the
// cancellation) has its connection closed instead of installed.
func TestDialFinishingAfterCloseIsClosed(t *testing.T) {
	srv := newFakeServer()
	gate := make(chan struct{})
	hit := make(chan struct{}, 1)
	newConn := func(ctx context.Context, addr string) (*pool.Conn, error) {
		hit <- struct{}{}
		<-gate // deaf to ctx
		return srv.dial(context.Background(), addr)
	}
	m := NewManager(testConfig("node:6379"), newConn, func(cn *pool.Conn) error { return cn.Close() },
		func(context.Context, *pool.Conn, *proto.Reader) error { return nil },
		testIsBadConn, nil, nil, nil)
	t.Cleanup(func() { _ = m.Close() })

	res := subscribeAsync(m, "a")
	<-hit
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	close(gate)
	srv.waitDial(t).expectClosed(t)
	if r := awaitSubscribe(t, res); !errors.Is(r.err, pool.ErrClosed) {
		t.Fatalf("Subscribe racing Close = (%v, %v), want pool.ErrClosed", r.h, r.err)
	}
}

// TestAbandonedDialIsDiscarded pins that a dial every foreground caller
// gave up on no longer demands its connection: with nothing subscribed
// once it completes, it is closed instead of installed, rather than
// kept open — read loop, health checks and all — with nobody using it.
func TestAbandonedDialIsDiscarded(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(ctx context.Context, m *Manager) error
	}{
		{"subscribe", func(ctx context.Context, m *Manager) error {
			_, err := m.Subscribe(ctx, "a")
			return err
		}},
		{"ping", func(ctx context.Context, m *Manager) error {
			h := m.NewHandle()
			defer h.Close()
			return h.Ping(ctx)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newFakeServer()
			srv.autoConfirm = true
			srv.autoPong = true
			m := newTestManager(t, srv, testConfig("node:6379"))

			gate := make(chan struct{})
			srv.setDialGate(gate)

			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			if err := tc.call(ctx, m); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("call with an expired wait = %v, want context.DeadlineExceeded", err)
			}

			close(gate)
			srv.waitDial(t).expectClosed(t)
		})
	}
}

// TestHandleClosedDuringDialReleasesConn pins that a handle closed while
// its own call awaits the dial does not leave the dialed connection
// behind: the Close found no connection to release, and the dial —
// still demanded by the waiting call — installs one, so the call,
// finding its handle closed, releases it in the Close's stead.
func TestHandleClosedDuringDialReleasesConn(t *testing.T) {
	for _, tc := range []struct {
		name string
		call func(h PubSuber) error
	}{
		{"ping", func(h PubSuber) error { return h.Ping(context.Background()) }},
		{"client setname", func(h PubSuber) error { return h.ClientSetName(context.Background(), "n") }},
		{"subscribe", func(h PubSuber) error {
			_, err := h.Subscribe(context.Background(), "a")
			return err
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv := newFakeServer()
			srv.autoConfirm = true
			srv.autoPong = true
			m := newTestManager(t, srv, testConfig("node:6379"))

			gate := make(chan struct{})
			srv.setDialGate(gate)

			h := m.NewHandle()
			res := make(chan error, 1)
			go func() { res <- tc.call(h) }()
			srv.waitGateHit(t)

			if err := h.Close(); err != nil {
				t.Fatalf("handle Close during the dial: %v", err)
			}
			close(gate)
			srv.waitDial(t).expectClosed(t)

			select {
			case err := <-res:
				if !errors.Is(err, pool.ErrClosed) {
					t.Fatalf("call on a handle closed mid-dial = %v, want pool.ErrClosed", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("call did not return after its handle closed")
			}
		})
	}
}

// TestDialWaitPrefersSettledOutcome pins that a wait whose ctx is done
// by the time the call has settled reports the call's outcome: the dial
// was installed, so reporting ctx's error would have the caller roll
// back names the install's replay already subscribed.
func TestDialWaitPrefersSettledOutcome(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	call := &dialCall{done: make(chan struct{})}
	close(call.done)
	// Both select cases are ready; repeat to cover the random pick.
	for range 100 {
		if err := call.wait(ctx); err != nil {
			t.Fatalf("wait on a settled call = %v, want its nil outcome", err)
		}
	}
}

// TestConcurrentSubscribesDialOnce pins the single-flight dial: two
// subscribers racing on a connectionless manager produce one dial; the
// second joins the first's, and both names are subscribed on the one
// connection (by its replay, or by the second subscriber's own write).
func TestConcurrentSubscribesDialOnce(t *testing.T) {
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newTestManager(t, srv, testConfig("node:6379"))

	gate := make(chan struct{})
	srv.setDialGate(gate)

	resA := subscribeAsync(m, "a")
	srv.waitGateHit(t)
	resB := subscribeAsync(m, "b")
	// b must join a's dial rather than start its own.
	srv.expectNoGateHit(t, 200*time.Millisecond)

	close(gate)
	fsc := srv.waitDial(t)
	got := map[string]bool{}
	for !got["a"] || !got["b"] {
		cmd := fsc.waitCmd(t)
		if cmd[0] != "subscribe" {
			t.Fatalf("unexpected command %v", cmd)
		}
		for _, name := range cmd[1:] {
			if got[name] {
				t.Fatalf("%s subscribed twice", name)
			}
			got[name] = true
		}
	}
	for _, res := range []<-chan subscribeResult{resA, resB} {
		if r := awaitSubscribe(t, res); r.err != nil {
			t.Fatalf("Subscribe: %v", r.err)
		}
	}
	fsc.expectNoCmd(t, 100*time.Millisecond)
	select {
	case fsc2 := <-srv.dialCh:
		t.Fatalf("second dial to %s; dials must be single-flight", fsc2.addr)
	default:
	}
}

// TestDialRetriesWithBackoff pins the dialer's retries: a failed attempt
// is retried DialRetries times, re-targeted through resolveAddress and
// paced by the backoff, before the failure is reported.
func TestDialRetriesWithBackoff(t *testing.T) {
	srv := newFakeServer()
	srv.autoConfirm = true
	cfg := testConfig("node-a:6379")
	cfg.DialRetries = 2
	cfg.MinRetryBackoff = 20 * time.Millisecond
	cfg.ReconnectMaxBackoff = 20 * time.Millisecond

	var attempts []time.Time
	var addrs []string
	newConn := func(ctx context.Context, addr string) (*pool.Conn, error) {
		attempts = append(attempts, time.Now())
		addrs = append(addrs, addr)
		if len(attempts) <= 2 {
			return nil, errors.New("connection refused")
		}
		return srv.dial(ctx, addr)
	}
	resolve := func(_ *pool.Conn, current string) string {
		if current == "node-a:6379" {
			return "node-b:6379"
		}
		return current
	}
	m := NewManager(cfg, newConn, func(cn *pool.Conn) error { return cn.Close() },
		func(context.Context, *pool.Conn, *proto.Reader) error { return nil },
		testIsBadConn, nil, nil, resolve)
	t.Cleanup(func() { _ = m.Close() })

	if _, err := m.Subscribe(context.Background(), "a"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	srv.waitDial(t).expectCmd(t, "subscribe", "a")

	// The attempts are sequential on the dial goroutine, all done by now.
	if len(attempts) != 3 {
		t.Fatalf("dial attempts = %d, want 1 + DialRetries = 3", len(attempts))
	}
	if addrs[0] != "node-a:6379" || addrs[1] != "node-b:6379" {
		t.Fatalf("attempt targets = %v, want the first unresolved and the retries resolved", addrs)
	}
	for i := 1; i < len(attempts); i++ {
		if gap := attempts[i].Sub(attempts[i-1]); gap < 15*time.Millisecond {
			t.Fatalf("retry %d came %v after the previous attempt, want the backoff", i, gap)
		}
	}
}

// TestDialRetryBackoffPolicy pins that a configured DialRetryBackoff
// paces a dial's retries — with the 0-based attempt index — instead of
// the reconnect backoff, which keeps bounding only the pause between
// dials.
func TestDialRetryBackoffPolicy(t *testing.T) {
	srv := newFakeServer()
	srv.autoConfirm = true
	cfg := testConfig("node:6379")
	cfg.DialRetries = 2
	cfg.MinRetryBackoff = time.Millisecond
	cfg.ReconnectMaxBackoff = time.Millisecond

	var asked []int
	cfg.DialRetryBackoff = func(attempt int) time.Duration {
		asked = append(asked, attempt)
		return 40 * time.Millisecond
	}

	var attempts []time.Time
	newConn := func(ctx context.Context, addr string) (*pool.Conn, error) {
		attempts = append(attempts, time.Now())
		if len(attempts) <= 2 {
			return nil, errors.New("connection refused")
		}
		return srv.dial(ctx, addr)
	}
	m := NewManager(cfg, newConn, func(cn *pool.Conn) error { return cn.Close() },
		func(context.Context, *pool.Conn, *proto.Reader) error { return nil },
		testIsBadConn, nil, nil, nil)
	t.Cleanup(func() { _ = m.Close() })

	if _, err := m.Subscribe(context.Background(), "a"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	srv.waitDial(t).expectCmd(t, "subscribe", "a")

	if len(attempts) != 3 {
		t.Fatalf("dial attempts = %d, want 1 + DialRetries = 3", len(attempts))
	}
	if !slices.Equal(asked, []int{0, 1}) {
		t.Fatalf("DialRetryBackoff asked for attempts %v, want [0 1]", asked)
	}
	for i := 1; i < len(attempts); i++ {
		if gap := attempts[i].Sub(attempts[i-1]); gap < 35*time.Millisecond {
			t.Fatalf("retry %d came %v after the previous attempt, want DialRetryBackoff's 40ms", i, gap)
		}
	}
}

// TestDialCarriesCallerContextValues pins that every dial of the shared
// connection sees the context values of the Subscribe that established
// it — a CredentialsProviderContext reads them during the handshake —
// with the caller's cancellation detached: the background redial after
// a lost connection runs with the same values, though the subscribing
// context is long cancelled.
func TestDialCarriesCallerContextValues(t *testing.T) {
	type tenantKey struct{}
	srv := newFakeServer()
	srv.autoConfirm = true

	type seen struct {
		tenant any
		ctxErr error
	}
	dials := make(chan seen, 4)
	newConn := func(ctx context.Context, addr string) (*pool.Conn, error) {
		dials <- seen{tenant: ctx.Value(tenantKey{}), ctxErr: ctx.Err()}
		return srv.dial(ctx, addr)
	}
	m := NewManager(testConfig("node:6379"), newConn, func(cn *pool.Conn) error { return cn.Close() },
		func(context.Context, *pool.Conn, *proto.Reader) error { return nil },
		testIsBadConn, nil, nil, nil)
	t.Cleanup(func() { _ = m.Close() })

	ctx, cancel := context.WithCancel(context.WithValue(context.Background(), tenantKey{}, "acme"))
	if _, err := m.Subscribe(ctx, "a"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	cancel()
	fsc1 := srv.waitDial(t)
	fsc1.expectCmd(t, "subscribe", "a")
	if d := <-dials; d.tenant != "acme" || d.ctxErr != nil {
		t.Fatalf("first dial saw tenant %v (ctx err %v), want \"acme\" and a live context", d.tenant, d.ctxErr)
	}

	_ = fsc1.conn.Close()
	srv.waitDial(t).expectCmd(t, "subscribe", "a")
	if d := <-dials; d.tenant != "acme" || d.ctxErr != nil {
		t.Fatalf("redial saw tenant %v (ctx err %v), want \"acme\" with the caller's cancellation detached", d.tenant, d.ctxErr)
	}
}

// stallWrite subscribes "a" on a fresh manager, then stops the fake
// server reading so the next write parks on the pipe, and starts that
// write (a Subscribe of "b") in the background. It returns the handle,
// its message channel, the server conn and the background result.
func stallWrite(t *testing.T, m *Manager, srv *fakeServer) (PubSuber, <-chan *Message, *fakeServerConn, <-chan error) {
	t.Helper()
	ctx := context.Background()

	h, err := m.Subscribe(ctx, "a")
	if err != nil {
		t.Fatalf("Subscribe a: %v", err)
	}
	ch := h.Channel()
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "a")

	// A paused server stops reading between frames, so a write then
	// blocks on the synchronous pipe until its deadline or the connection
	// closes. The read loop may already be parked inside a pipe Read from
	// before the pause, which consumes one more write: write until one
	// parks, at most two attempts.
	fsc.paused.Store(true)
	t.Cleanup(func() { fsc.paused.Store(false) })

	var stalled <-chan error
	for i := range 2 {
		res := make(chan error, 1)
		name := fmt.Sprintf("stall-%d", i)
		go func() {
			_, err := m.Subscribe(ctx, name)
			res <- err
		}()
		select {
		case err := <-res:
			if err != nil {
				t.Fatalf("probe Subscribe %s: %v", name, err)
			}
			// Consumed by the pending Read; the loop is parked now.
			continue
		case <-time.After(100 * time.Millisecond):
			stalled = res
		}
		break
	}
	if stalled == nil {
		t.Fatal("no write parked on the paused server")
	}
	return h, ch, fsc, stalled
}

// TestCloseAbortsStalledWrite pins that Close does not wait out a write
// stalled on a peer that stopped reading: writes hold the manager lock,
// so Close fails the stalled write by closing the connection before
// taking the lock, and returns at once instead of after WriteTimeout.
func TestCloseAbortsStalledWrite(t *testing.T) {
	srv := newFakeServer()
	srv.autoConfirm = true
	cfg := testConfig("node:6379")
	cfg.WriteTimeout = 10 * time.Second // far beyond the test's patience
	m := newTestManager(t, srv, cfg)

	_, _, _, stalled := stallWrite(t, m, srv)

	start := time.Now()
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("Close took %v behind a stalled write, want immediate", took)
	}

	// Closing the connection fails the parked write.
	select {
	case err := <-stalled:
		if err == nil {
			t.Fatal("the stalled Subscribe succeeded after Close")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the stalled Subscribe never returned after Close")
	}

	// Close aborted the write; it must not have started a reconnect.
	select {
	case fsc := <-srv.dialCh:
		t.Fatalf("Close's aborted write redialed %s", fsc.addr)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestStalledWriteFailsAtDeadline pins that a write stalled on a peer
// that stopped reading holds the manager lock no longer than its
// deadline: it fails at WriteTimeout, the desynced connection is dropped,
// and the restoration replays the registry on a fresh one, where
// delivery resumes.
func TestStalledWriteFailsAtDeadline(t *testing.T) {
	srv := newFakeServer()
	srv.autoConfirm = true
	cfg := testConfig("node:6379")
	cfg.WriteTimeout = 200 * time.Millisecond
	m := newTestManager(t, srv, cfg)

	_, ch, fsc1, stalled := stallWrite(t, m, srv)

	select {
	case err := <-stalled:
		if err == nil {
			t.Fatal("the stalled Subscribe succeeded on a peer that stopped reading")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the stalled write outlived its WriteTimeout")
	}
	// A paused fake server never reads, so it only notices the close
	// once resumed.
	fsc1.paused.Store(false)
	fsc1.expectClosed(t)

	fsc2 := srv.waitDial(t)
	if cmd := fsc2.waitCmd(t); cmd[0] != "subscribe" || !slices.Contains(cmd[1:], "a") {
		t.Fatalf("replay = %v, want a subscribe carrying a", cmd)
	}
	fsc2.sendMessage(t, "message", "a", "after")
	if msg := recvMsg(t, ch); msg.Payload != "after" {
		t.Fatalf("got %q after the restoration, want \"after\"", msg.Payload)
	}
}

// TestHealthCheckReplyTimeoutReconnects pins the reply side of the
// health check: a PING the server never answers is a dead or
// unresponsive connection, so once PingTimeout passes without any
// inbound frame the manager drops it and reconnects with the
// subscription replay. (Before, only a failed PING write triggered a
// reconnect; a silently dead socket went unnoticed.)
func TestHealthCheckReplyTimeoutReconnects(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer() // no autoPong: pings go unanswered
	cfg := testConfig("node:6379")
	cfg.HealthCheckInterval = 20 * time.Millisecond
	cfg.PingTimeout = 100 * time.Millisecond
	m := newTestManager(t, srv, cfg)

	if _, err := m.Subscribe(ctx, "hc"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc1 := srv.waitDial(t)
	fsc1.expectCmd(t, "subscribe", "hc")
	fsc1.sendConfirm(t, "subscribe", "hc", 1)

	// The idle connection is pinged; the ping is swallowed.
	fsc1.expectCmd(t, "ping")

	// No reply within PingTimeout: the connection is replaced and the
	// subscription replayed on the new one.
	fsc2 := srv.waitDial(t)
	fsc2.expectCmd(t, "subscribe", "hc")
	fsc1.expectClosed(t)
}

// TestHealthCheckAnyFrameSatisfiesReplyWait pins that the reply wait
// is about liveness, not the pong itself: replies are ordered, so any
// inbound frame after the PING (a message here) proves the server is
// responding, and the connection is kept.
func TestHealthCheckAnyFrameSatisfiesReplyWait(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	cfg := testConfig("node:6379")
	cfg.HealthCheckInterval = 20 * time.Millisecond
	cfg.PingTimeout = 100 * time.Millisecond
	m := newTestManager(t, srv, cfg)

	h, err := m.Subscribe(ctx, "hc")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	ch := h.Channel()
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "hc")
	fsc.sendConfirm(t, "subscribe", "hc", 1)

	// Answer the first ping with traffic instead of a pong, then let the
	// server answer later pings normally.
	fsc.expectCmd(t, "ping")
	fsc.sendMessage(t, "message", "hc", "alive")
	fsc.autoPong.Store(true)
	if msg := recvMsg(t, ch); msg.Payload != "alive" {
		t.Fatalf("got %q, want \"alive\"", msg.Payload)
	}

	// Well past PingTimeout: no reconnect happened.
	select {
	case fsc2 := <-srv.dialCh:
		t.Fatalf("unexpected reconnect to %s: inbound traffic must satisfy the reply wait", fsc2.addr)
	case <-time.After(300 * time.Millisecond):
	}
}

// TestLateConfirmationIsNotRetried pins that an unconfirmed subscribe
// is never re-sent on a healthy connection: replies are ordered, so a
// confirmation that is merely late is still on its way, and a retry
// would only draw a duplicate confirmation. The late reply then settles
// the name with exactly one event.
func TestLateConfirmationIsNotRetried(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer() // no autoConfirm: the test answers by hand
	cfg := testConfig("node:6379")
	cfg.SubscribeRetryInterval = 30 * time.Millisecond
	m := newTestManager(t, srv, cfg)

	h, err := m.Subscribe(ctx, "x")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "x")

	// Several retry intervals pass with the confirmation outstanding:
	// nothing is re-sent.
	fsc.expectNoCmd(t, 150*time.Millisecond)

	fsc.sendConfirm(t, "subscribe", "x", 1)
	waitForConfirmation(t, h.Events(), "subscribe", "x")

	// Settled: still nothing to re-send, and no second confirmation.
	fsc.expectNoCmd(t, 100*time.Millisecond)
	select {
	case ev := <-h.Events():
		t.Fatalf("unexpected event after the late confirmation: %#v", ev)
	default:
	}
}

// TestHealthCheckReplyWaitHonorsRelaxedTimeout pins the maintenance-
// window interplay: with a relaxed timeout active on the connection (a
// MOVING/MIGRATING window), the reply wait stretches to it, so a server
// that is slow to answer during the window is not mistaken for a dead
// one. An answer that never comes still fails the connection — once the
// stretched wait has elapsed.
func TestHealthCheckReplyWaitHonorsRelaxedTimeout(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer() // no autoPong: pings go unanswered
	cfg := testConfig("node:6379")
	cfg.HealthCheckInterval = 20 * time.Millisecond
	cfg.PingTimeout = 50 * time.Millisecond
	m := newTestManager(t, srv, cfg)

	if _, err := m.Subscribe(ctx, "hc"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc1 := srv.waitDial(t)
	fsc1.expectCmd(t, "subscribe", "hc")
	fsc1.sendConfirm(t, "subscribe", "hc", 1)

	// The maintenance window relaxes the connection's timeouts far
	// beyond PingTimeout.
	fsc1.poolConn.SetRelaxedTimeout(400*time.Millisecond, 400*time.Millisecond)

	// The idle connection is pinged; the ping is swallowed. With the
	// plain PingTimeout the reconnect would land within 50ms.
	fsc1.expectCmd(t, "ping")
	select {
	case fsc2 := <-srv.dialCh:
		t.Fatalf("reconnected to %s inside the relaxed window", fsc2.addr)
	case <-time.After(250 * time.Millisecond):
	}

	// The stretched wait elapses with the ping still unanswered: the
	// connection is replaced and the subscription replayed.
	fsc2 := srv.waitDial(t)
	fsc2.expectCmd(t, "subscribe", "hc")
	fsc1.expectClosed(t)
}

// TestHealthPingExpiredContextKeepsConnection pins that a health probe
// whose deadline expired before it got the manager lock is skipped,
// not written: a write with a born-expired deadline fails before
// sending a byte — which would drop a healthy connection for every
// subscriber.
func TestHealthPingExpiredContextKeepsConnection(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newTestManager(t, srv, testConfig("node:6379"))

	h, err := m.Subscribe(ctx, "ch")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "ch")
	waitForConfirmation(t, h.Events(), "subscribe", "ch")

	expired, cancel := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer cancel()
	var errs []error
	for range 32 {
		_, err := m.healthPing(expired)
		errs = append(errs, err)
	}

	// Nothing was written, the connection was neither dropped nor
	// replaced, and it still delivers.
	fsc.expectNoCmd(t, 50*time.Millisecond)
	select {
	case fsc2 := <-srv.dialCh:
		t.Fatalf("healthy connection was replaced (dial to %q) after expired probes", fsc2.addr)
	case <-time.After(200 * time.Millisecond):
	}
	fsc.sendMessage(t, "message", "ch", "alive")
	if msg := drainToMessage(t, h.Events()); msg.Payload != "alive" {
		t.Fatalf("got %q, want \"alive\"", msg.Payload)
	}
	for _, err := range errs {
		if !errors.Is(err, errPingNotSent) {
			t.Fatalf("healthPing with an expired context = %v, want errPingNotSent", err)
		}
	}
}

// redirectingResolver retires the connection once (on the first frame
// read after replace is armed) and resolves every reconnect to target.
type redirectingResolver struct {
	target  string
	replace atomic.Bool
}

func (r *redirectingResolver) ShouldReplace(*pool.Conn) bool { return r.replace.Swap(false) }

func (r *redirectingResolver) Resolve(*pool.Conn, string) string {
	return r.target
}

func newResolverTestManager(
	t *testing.T,
	srv *fakeServer,
	cfg Config,
	shouldReplaceConn func(cn *pool.Conn) bool,
	resolveAddress func(prev *pool.Conn, current string) string,
) *Manager {
	t.Helper()
	m := NewManager(
		cfg,
		srv.dial,
		func(cn *pool.Conn) error { return cn.Close() },
		func(context.Context, *pool.Conn, *proto.Reader) error { return nil },
		testIsBadConn,
		nil,
		shouldReplaceConn,
		resolveAddress,
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

// TestResolverPluggable pins the resolver seam: the manager retires
// a connection when the resolver says so and dials the address the
// resolver returns, knowing nothing about the reason — here a test
// policy, in the client the maintenance-handoff one. The frame that
// triggered the replacement is still delivered.
func TestResolverPluggable(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	r := &redirectingResolver{target: "node-b:6379"}
	m := newResolverTestManager(t, srv, testConfig("node-a:6379"), r.ShouldReplace, r.Resolve)

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

// TestRecheckConnRetiresIdleConn pins RecheckConn: an owner whose
// shouldReplaceConn answer changed without a frame arriving (the
// failover client's master switch) gets the idle connection retired at
// once — the health check is off in testConfig, so no frame would ever
// come to trigger it.
func TestRecheckConnRetiresIdleConn(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	r := &redirectingResolver{target: "node-b:6379"}
	m := newResolverTestManager(t, srv, testConfig("node-a:6379"), r.ShouldReplace, r.Resolve)

	h, err := m.Subscribe(ctx, "ch")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc1 := srv.waitDial(t)
	fsc1.expectCmd(t, "subscribe", "ch")
	waitForConfirmation(t, h.Events(), "subscribe", "ch")

	r.replace.Store(true)
	m.RecheckConn()
	fsc2 := srv.waitDial(t)
	if fsc2.addr != "node-b:6379" {
		t.Fatalf("reconnect dialed %q, want the resolver's node-b:6379", fsc2.addr)
	}
	fsc2.expectCmd(t, "subscribe", "ch")
	fsc1.expectClosed(t)
}

// TestRecheckConnWithoutConnIsNoop pins that RecheckConn is safe with no
// live connection: before the first subscribe and after Close.
func TestRecheckConnWithoutConnIsNoop(t *testing.T) {
	srv := newFakeServer()
	r := &redirectingResolver{target: "node-b:6379"}
	m := newResolverTestManager(t, srv, testConfig("node-a:6379"), r.ShouldReplace, r.Resolve)

	r.replace.Store(true)
	m.RecheckConn()
	_ = m.Close()
	m.RecheckConn()
	select {
	case fsc := <-srv.dialCh:
		t.Fatalf("RecheckConn without a live connection dialed %q", fsc.addr)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestReplacementDialKeepsDelivering pins that retiring a connection
// does not stall delivery: the replacement is dialed in the background,
// so while its dial is parked the read loop keeps delivering from the
// connection being retired, which is closed only once the replacement
// is subscribed.
func TestReplacementDialKeepsDelivering(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	r := &redirectingResolver{target: "node-b:6379"}
	m := newResolverTestManager(t, srv, testConfig("node-a:6379"), r.ShouldReplace, r.Resolve)

	h, err := m.Subscribe(ctx, "ch")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc1 := srv.waitDial(t)
	fsc1.expectCmd(t, "subscribe", "ch")
	waitForConfirmation(t, h.Events(), "subscribe", "ch")

	gate := make(chan struct{})
	srv.setDialGate(gate)
	r.replace.Store(true)
	fsc1.sendMessage(t, "message", "ch", "trigger")
	srv.waitGateHit(t)
	if msg := drainToMessage(t, h.Events()); msg.Payload != "trigger" {
		t.Fatalf("got %q, want the frame that triggered the replacement", msg.Payload)
	}

	// The replacement's dial is parked: the old connection still serves.
	fsc1.sendMessage(t, "message", "ch", "during")
	if msg := drainToMessage(t, h.Events()); msg.Payload != "during" {
		t.Fatalf("got %q while the replacement dialed, want \"during\"", msg.Payload)
	}
	select {
	case <-fsc1.done:
		t.Fatal("the old connection was closed before its replacement was installed")
	default:
	}

	close(gate)
	fsc2 := srv.waitDial(t)
	if fsc2.addr != "node-b:6379" {
		t.Fatalf("replacement dialed %q, want node-b:6379", fsc2.addr)
	}
	fsc2.expectCmd(t, "subscribe", "ch")
	fsc1.expectClosed(t)
	fsc2.sendMessage(t, "message", "ch", "after")
	if msg := drainToMessage(t, h.Events()); msg.Payload != "after" {
		t.Fatalf("got %q on the replacement, want \"after\"", msg.Payload)
	}
}

// TestNilResolverIgnoresHandoffMarks pins that the manager itself
// knows nothing about handoffs: without a resolver, a connection marked
// for handoff is neither retired nor redirected.
func TestNilResolverIgnoresHandoffMarks(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newResolverTestManager(t, srv, testConfig("node-a:6379"), nil, nil)

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
// whether each Resolve found the manager lock held — the serialization
// the manager promises resolveAddress. ShouldReplace is only counted:
// it runs without the lock, and a TryLock there could not tell "free"
// from "held by some other path" anyway.
type lockCheckingResolver struct {
	redirectingResolver
	m               *Manager
	shouldCalls     atomic.Int32
	resolveCalls    atomic.Int32
	resolveUnlocked atomic.Int32 // Resolve calls that found the manager lock free
}

func (r *lockCheckingResolver) ShouldReplace(cn *pool.Conn) bool {
	r.shouldCalls.Add(1)
	return r.redirectingResolver.ShouldReplace(cn)
}

// Resolve notes whether the manager lock was free: TryLock succeeds
// only then (and is undone right away).
func (r *lockCheckingResolver) Resolve(prev *pool.Conn, current string) string {
	r.resolveCalls.Add(1)
	if r.m.mu.TryLock() {
		r.m.mu.Unlock()
		r.resolveUnlocked.Add(1)
	}
	return r.redirectingResolver.Resolve(prev, current)
}

// TestResolveAddressRunsUnderManagerLock pins the callback contract:
// resolveAddress (consulted by a reconnect) runs under the manager
// lock, while shouldReplaceConn (consulted after a frame read) is
// lock-free and still drives the retirement.
func TestResolveAddressRunsUnderManagerLock(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	r := &lockCheckingResolver{redirectingResolver: redirectingResolver{target: "node-b:6379"}}
	m := newResolverTestManager(t, srv, testConfig("node-a:6379"), r.ShouldReplace, r.Resolve)
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

	if r.shouldCalls.Load() == 0 {
		t.Fatal("ShouldReplace was never consulted")
	}
	if r.resolveCalls.Load() == 0 {
		t.Fatal("Resolve was never consulted")
	}
	if n := r.resolveUnlocked.Load(); n != 0 {
		t.Fatalf("%d Resolve call(s) ran without the manager lock", n)
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
	m := newResolverTestManager(t, srv, cfg, testShouldHandoff, testResolveHandoff)

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
	m := newResolverTestManager(t, srv, cfg, testShouldHandoff, testResolveHandoff)

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

func (r *rotatingResolver) Resolve(prev *pool.Conn, current string) string {
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
		testIsBadConn, nil, r.ShouldReplace, r.Resolve)
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

// TestExpiredContextDoesNotDropConnection pins that a caller whose
// context is already done never costs the shared connection: dropping it
// would cost every other subscriber a reconnect. A Ping gets its error
// back with nothing written. A Subscribe on a held handle keeps its
// intent — a client's Subscribe hands out the PubSub whatever it
// returns — and, the write ignoring the caller's ctx, subscribes.
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
	// Repeated: no Ping may write or redial on the caller's behalf.
	for range 20 {
		if err := h.Ping(expired); !errors.Is(err, context.Canceled) {
			t.Fatalf("Ping with a cancelled context = %v, want context.Canceled", err)
		}
	}
	fsc.expectNoCmd(t, 100*time.Millisecond)

	if _, err := h.Subscribe(expired, "b"); err != nil {
		t.Fatalf("Subscribe with a cancelled context on a live connection = %v, want nil", err)
	}
	fsc.expectCmd(t, "subscribe", "b")
	if channels, _, _ := h.Subscriptions(); len(channels) != 2 {
		t.Fatalf("subscriptions after cancelled Subscribe = %v, want [a b]", channels)
	}

	// The connection is intact: nothing redialed, and delivery continues.
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

// TestExpiredContextSubscribeKeepsIntent pins that a held handle's
// Subscribe with a context already done, and no connection yet, keeps
// its registration: a client's Subscribe hands out the PubSub without
// the error, so dropping the names would leave a live PubSub that never
// subscribes. The dial it starts outlives the abandoned wait and its
// replay subscribes them.
func TestExpiredContextSubscribeKeepsIntent(t *testing.T) {
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newTestManager(t, srv, testConfig("node:6379"))

	expired, cancel := context.WithCancel(context.Background())
	cancel()
	h := m.NewHandle()
	if _, err := h.Subscribe(expired, "ch"); !errors.Is(err, context.Canceled) {
		t.Fatalf("Subscribe with a cancelled context = %v, want context.Canceled", err)
	}
	if channels, _, _ := h.Subscriptions(); len(channels) != 1 || channels[0] != "ch" {
		t.Fatalf("subscriptions after cancelled Subscribe = %v, want [ch]", channels)
	}

	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "ch")
	waitForConfirmation(t, h.Events(), "subscribe", "ch")
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

// stickyResolver always wants the connection replaced and resolves
// every reconnect to target.
type stickyResolver struct{ target string }

func (stickyResolver) ShouldReplace(*pool.Conn) bool       { return true }
func (r stickyResolver) Resolve(*pool.Conn, string) string { return r.target }

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
	r := stickyResolver{target: "dead:6379"}
	m := NewManager(cfg, newConn, func(cn *pool.Conn) error { return cn.Close() },
		func(context.Context, *pool.Conn, *proto.Reader) error { return nil },
		testIsBadConn, nil, r.ShouldReplace, r.Resolve)
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

// TestFailedReplacementRetriedWithoutTraffic pins that a failed
// replacement of a live connection is retried when its backoff ends even
// if no frame arrives meanwhile: with the health check disabled a quiet
// connection produces none, and waiting for one could miss the handoff
// cutover entirely.
func TestFailedReplacementRetriedWithoutTraffic(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	cfg := testConfig("node-a:6379") // health check disabled
	cfg.MinRetryBackoff = 50 * time.Millisecond
	cfg.ReconnectMaxBackoff = 50 * time.Millisecond

	var targetUp atomic.Bool
	failed := make(chan struct{}, 1)
	newConn := func(ctx context.Context, addr string) (*pool.Conn, error) {
		if addr == "next:6379" && !targetUp.Load() {
			select {
			case failed <- struct{}{}:
			default:
			}
			return nil, errors.New("connection refused")
		}
		return srv.dial(ctx, addr)
	}
	r := stickyResolver{target: "next:6379"}
	m := NewManager(cfg, newConn, func(cn *pool.Conn) error { return cn.Close() },
		func(context.Context, *pool.Conn, *proto.Reader) error { return nil },
		testIsBadConn, nil, r.ShouldReplace, r.Resolve)
	t.Cleanup(func() { _ = m.Close() })

	h, err := m.Subscribe(ctx, "ch")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "ch")
	waitForConfirmation(t, h.Events(), "subscribe", "ch")

	// The confirmation was the one frame: it asked for the replacement,
	// whose dial failed. The connection stays quiet from here on.
	select {
	case <-failed:
	case <-time.After(5 * time.Second):
		t.Fatal("the replacement was never attempted")
	}
	targetUp.Store(true)

	next := srv.waitDial(t)
	if next.addr != "next:6379" {
		t.Fatalf("replacement dialed %s, want next:6379", next.addr)
	}
	next.expectCmd(t, "subscribe", "ch")
	fsc.expectClosed(t)
}
