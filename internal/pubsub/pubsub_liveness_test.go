package pubsub

import (
	"context"
	"errors"
	"testing"
	"time"
)

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
// whose deadline expired while it waited for the I/O slot is skipped,
// not written: acquireIO's select is not biased, so an expired probe
// can still win the slot, and a write with a born-expired deadline
// fails before sending a byte — which would drop a healthy connection
// for every subscriber. Many attempts, since the hazard is a coin flip
// per call.
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
