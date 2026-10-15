package pubsub

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

// waitForPong drains events until a *Pong arrives, discarding other
// events (confirmations from autoConfirm, messages).
func waitForPong(t *testing.T, events <-chan any) *Pong {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("events closed while waiting for a pong")
			}
			if pong, ok := ev.(*Pong); ok {
				return pong
			}
		case <-deadline:
			t.Fatal("timed out waiting for a pong")
		}
	}
}

// pongsUntil drains events until a *Message with each of the given
// payloads has arrived, returning how many pongs were seen on the way.
// The marker messages are written after the pong frames on their
// connections, so per-connection ordering guarantees that once every
// marker arrived, any pong that was going to surface already has.
func pongsUntil(t *testing.T, events <-chan any, payloads ...string) int {
	t.Helper()
	want := make(map[string]struct{}, len(payloads))
	for _, p := range payloads {
		want[p] = struct{}{}
	}
	pongs := 0
	deadline := time.After(5 * time.Second)
	for len(want) > 0 {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("events closed while draining to the marker message(s)")
			}
			switch ev := ev.(type) {
			case *Pong:
				pongs++
			case *Message:
				delete(want, ev.Payload)
			}
		case <-deadline:
			t.Fatalf("timed out waiting for marker message(s) %v", want)
		}
	}
	return pongs
}

// TestPingPongDelivery pins the standalone pong-delivery contract: one
// pong per handle Ping on Events, none for PingSilent, and none for the
// manager-level ping (no waiter).
func TestPingPongDelivery(t *testing.T) {
	ctx := context.Background()

	t.Run("Handle", func(t *testing.T) {
		srv := newFakeServer()
		srv.autoConfirm = true
		m := newTestManager(t, srv, testConfig("node:6379"))

		h, err := m.Subscribe(ctx, "ch")
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		events := h.Events()

		fsc := srv.waitDial(t)
		fsc.expectCmd(t, "subscribe", "ch")

		// Ping surfaces its pong on Events, payload included.
		if err := h.Ping(ctx, "hello"); err != nil {
			t.Fatalf("Ping: %v", err)
		}
		fsc.expectCmd(t, "ping", "hello")
		fsc.sendMessage(t, "pong", "hello")
		if pong := waitForPong(t, events); pong.Payload != "hello" {
			t.Fatalf("pong payload = %q, want \"hello\"", pong.Payload)
		}

		// PingSilent writes the PING but its pong never surfaces.
		if err := h.PingSilent(ctx); err != nil {
			t.Fatalf("PingSilent: %v", err)
		}
		fsc.expectCmd(t, "ping")
		fsc.write(t, "+PONG\r\n")
		fsc.sendMessage(t, "message", "ch", "marker")
		if n := pongsUntil(t, events, "marker"); n != 0 {
			t.Fatalf("PingSilent surfaced %d pong(s), want 0", n)
		}
	})

	t.Run("Manager", func(t *testing.T) {
		srv := newFakeServer()
		srv.autoConfirm = true
		m := newTestManager(t, srv, testConfig("node:6379"))

		h, err := m.Subscribe(ctx, "ch")
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		events := h.Events()

		fsc := srv.waitDial(t)
		fsc.expectCmd(t, "subscribe", "ch")

		// A manager-level ping has no waiter: its pong is consumed
		// silently even though a handle lives on the connection.
		if err := m.Ping(ctx); err != nil {
			t.Fatalf("Ping: %v", err)
		}
		fsc.expectCmd(t, "ping")
		fsc.write(t, "+PONG\r\n")
		fsc.sendMessage(t, "message", "ch", "marker")
		if n := pongsUntil(t, events, "marker"); n != 0 {
			t.Fatalf("manager Ping surfaced %d pong(s), want 0", n)
		}
	})
}

// expectPong drains events until a *Pong arrives and asserts its
// payload.
func expectPong(t *testing.T, events <-chan any, payload string) {
	t.Helper()
	if pong := waitForPong(t, events); pong.Payload != payload {
		t.Fatalf("pong payload = %q, want %q", pong.Payload, payload)
	}
}

// TestPingPongFIFOAttribution pins pong correlation: pongs answer pings
// in FIFO order on the one connection, so each handle receives exactly
// the pong of its own ping — never another handle's.
func TestPingPongFIFOAttribution(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newTestManager(t, srv, testConfig("node:6379"))

	a, err := m.Subscribe(ctx, "cha")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	b, err := m.Subscribe(ctx, "chb")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "cha")
	fsc.expectCmd(t, "subscribe", "chb")

	if err := a.Ping(ctx, "for-a"); err != nil {
		t.Fatalf("Ping a: %v", err)
	}
	fsc.expectCmd(t, "ping", "for-a")
	if err := b.Ping(ctx, "for-b"); err != nil {
		t.Fatalf("Ping b: %v", err)
	}
	fsc.expectCmd(t, "ping", "for-b")

	// The server answers in write order; each pong must reach only the
	// handle whose ping it answers.
	fsc.sendMessage(t, "pong", "for-a")
	fsc.sendMessage(t, "pong", "for-b")
	expectPong(t, a.Events(), "for-a")
	expectPong(t, b.Events(), "for-b")

	fsc.sendMessage(t, "message", "cha", "marker")
	fsc.sendMessage(t, "message", "chb", "marker")
	if n := pongsUntil(t, a.Events(), "marker"); n != 0 {
		t.Fatalf("handle a saw %d extra pong(s), want 0", n)
	}
	if n := pongsUntil(t, b.Events(), "marker"); n != 0 {
		t.Fatalf("handle b saw %d extra pong(s), want 0", n)
	}
}

// TestPingPongSilentInterleaved pins that a silent ping between two
// waited pings consumes its own pong without shifting attribution: the
// handle sees its two pongs, in order, and never the silent one.
func TestPingPongSilentInterleaved(t *testing.T) {
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

	if err := h.Ping(ctx, "one"); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	fsc.expectCmd(t, "ping", "one")
	if err := h.PingSilent(ctx, "silent"); err != nil {
		t.Fatalf("PingSilent: %v", err)
	}
	fsc.expectCmd(t, "ping", "silent")
	if err := h.Ping(ctx, "two"); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	fsc.expectCmd(t, "ping", "two")

	fsc.sendMessage(t, "pong", "one")
	fsc.sendMessage(t, "pong", "silent")
	fsc.sendMessage(t, "pong", "two")
	expectPong(t, h.Events(), "one")
	expectPong(t, h.Events(), "two")

	fsc.sendMessage(t, "message", "ch", "marker")
	if n := pongsUntil(t, h.Events(), "marker"); n != 0 {
		t.Fatalf("handle saw %d extra pong(s), want 0", n)
	}
}

// TestPingPongReconnectFlush pins the reconnect flush: a pong wait whose
// ping died with the connection is dropped, so it neither swallows nor
// misdirects pongs on the replacement connection.
func TestPingPongReconnectFlush(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newTestManager(t, srv, testConfig("node:6379"))

	a, err := m.Subscribe(ctx, "cha")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	b, err := m.Subscribe(ctx, "chb")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc1 := srv.waitDial(t)
	fsc1.expectCmd(t, "subscribe", "cha")
	fsc1.expectCmd(t, "subscribe", "chb")

	// a's ping is written but never answered: the server dies first.
	if err := a.Ping(ctx, "lost"); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	fsc1.expectCmd(t, "ping", "lost")
	_ = fsc1.conn.Close()

	// The reconnect replays the registry on the new connection (one
	// subscribe carrying both names, registry map order).
	fsc2 := srv.waitDial(t)
	if cmd := fsc2.waitCmd(t); cmd[0] != "subscribe" || len(cmd) != 3 {
		t.Fatalf("replay command = %v, want subscribe with 2 names", cmd)
	}

	// Without the flush, a's stale head-of-queue entry would swallow
	// the pong b is waiting for.
	if err := b.Ping(ctx, "fresh"); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	fsc2.expectCmd(t, "ping", "fresh")
	fsc2.sendMessage(t, "pong", "fresh")
	expectPong(t, b.Events(), "fresh")

	fsc2.sendMessage(t, "message", "cha", "marker")
	if n := pongsUntil(t, a.Events(), "marker"); n != 0 {
		t.Fatalf("handle a saw %d pong(s) after its wait died, want 0", n)
	}
}

// TestPingPongClosedHandleSlot pins that closing a handle keeps its
// queue slots in place: each slot still consumes its pong silently, so
// attribution never shifts onto a later waiter.
func TestPingPongClosedHandleSlot(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newTestManager(t, srv, testConfig("node:6379"))

	a, err := m.Subscribe(ctx, "cha")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	b, err := m.Subscribe(ctx, "chb")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "cha")
	fsc.expectCmd(t, "subscribe", "chb")

	// a pings, then closes before the pong arrives.
	if err := a.Ping(ctx, "for-a"); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	fsc.expectCmd(t, "ping", "for-a")
	if err := a.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	fsc.expectCmd(t, "unsubscribe", "cha")

	if err := b.Ping(ctx, "for-b"); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	fsc.expectCmd(t, "ping", "for-b")

	// a's pong must be consumed by its (now empty) slot — if the slot
	// were removed instead, b would receive "for-a" here.
	fsc.sendMessage(t, "pong", "for-a")
	fsc.sendMessage(t, "pong", "for-b")
	expectPong(t, b.Events(), "for-b")
}

// TestClientSetNamePongDelivery pins that CLIENT SETNAME's +OK reply
// parses as a pong and surfaces only on the handle that issued it.
func TestClientSetNamePongDelivery(t *testing.T) {
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
	bystander := m.NewHandle().Events()

	if err := h.ClientSetName(ctx, "conn-name"); err != nil {
		t.Fatalf("ClientSetName: %v", err)
	}
	fsc.expectCmd(t, "client", "setname", "conn-name")
	fsc.write(t, "+OK\r\n")
	expectPong(t, h.Events(), "OK")

	select {
	case ev := <-bystander:
		t.Fatalf("SETNAME reply delivered to a handle that did not ask: %#v", ev)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestPingPongErrorReplySettlesWaiter pins that an error reply settles
// the rejected PING's ledger entry: the stale waiter must not swallow
// or misdirect the next pong.
func TestPingPongErrorReplySettlesWaiter(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newTestManager(t, srv, testConfig("node:6379"))

	a, err := m.Subscribe(ctx, "cha")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	b, err := m.Subscribe(ctx, "chb")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "cha")
	fsc.expectCmd(t, "subscribe", "chb")

	// a's ping is rejected; b's is answered.
	if err := a.Ping(ctx, "for-a"); err != nil {
		t.Fatalf("Ping a: %v", err)
	}
	fsc.expectCmd(t, "ping", "for-a")
	fsc.sendError(t, "NOPERM no permission to run 'ping'")

	if err := b.Ping(ctx, "for-b"); err != nil {
		t.Fatalf("Ping b: %v", err)
	}
	fsc.expectCmd(t, "ping", "for-b")
	fsc.sendMessage(t, "pong", "for-b")
	expectPong(t, b.Events(), "for-b")

	fsc.sendMessage(t, "message", "cha", "marker")
	if n := pongsUntil(t, a.Events(), "marker"); n != 0 {
		t.Fatalf("handle a saw %d pong(s) after its ping was rejected, want 0", n)
	}
}

// TestPingPongErrorReplySilentSlot pins that a rejected silent ping
// settles its own nil entry instead of leaking it (repeatedly rejected
// health-check pings would otherwise grow the ledger forever and
// swallow later pongs).
func TestPingPongErrorReplySilentSlot(t *testing.T) {
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

	if err := h.PingSilent(ctx, "silent"); err != nil {
		t.Fatalf("PingSilent: %v", err)
	}
	fsc.expectCmd(t, "ping", "silent")
	fsc.sendError(t, "NOPERM no permission to run 'ping'")

	if err := h.Ping(ctx, "real"); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	fsc.expectCmd(t, "ping", "real")
	fsc.sendMessage(t, "pong", "real")
	expectPong(t, h.Events(), "real")
}

// TestSubscribeErrorReplyKeepsPongAttribution pins that subscribe
// writes are ledgered too: a rejected SUBSCRIBE settles its own entry,
// never a queued ping's.
func TestSubscribeErrorReplyKeepsPongAttribution(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer() // no autoConfirm: the test answers by hand
	m := newTestManager(t, srv, testConfig("node:6379"))

	a, err := m.Subscribe(ctx, "cha")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "cha")

	b, err := m.Subscribe(ctx, "chb")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc.expectCmd(t, "subscribe", "chb")

	if err := a.Ping(ctx, "for-a"); err != nil {
		t.Fatalf("Ping a: %v", err)
	}
	fsc.expectCmd(t, "ping", "for-a")
	if err := b.Ping(ctx, "for-b"); err != nil {
		t.Fatalf("Ping b: %v", err)
	}
	fsc.expectCmd(t, "ping", "for-b")

	// cha is confirmed, chb's subscribe is rejected, both pings answered.
	fsc.sendConfirm(t, "subscribe", "cha", 1)
	fsc.sendError(t, "NOPERM no access to 'chb'")
	fsc.sendMessage(t, "pong", "for-a")
	fsc.sendMessage(t, "pong", "for-b")
	expectPong(t, a.Events(), "for-a")
	expectPong(t, b.Events(), "for-b")
}

// TestClosedHandleCommandsDoNotDial pins the closed-handle gate for the
// pong-shaped commands: Ping, PingSilent and ClientSetName on a closed
// handle fail with ErrClosed under the manager lock — the same lock
// Close holds — and never redial the connection Close released.
func TestClosedHandleCommandsDoNotDial(t *testing.T) {
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

	// Closing the last subscriber releases the shared connection.
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	fsc.expectClosed(t)

	if err := h.Ping(ctx); !errors.Is(err, pool.ErrClosed) {
		t.Fatalf("Ping on closed handle = %v, want ErrClosed", err)
	}
	if err := h.PingSilent(ctx); !errors.Is(err, pool.ErrClosed) {
		t.Fatalf("PingSilent on closed handle = %v, want ErrClosed", err)
	}
	if err := h.ClientSetName(ctx, "n"); !errors.Is(err, pool.ErrClosed) {
		t.Fatalf("ClientSetName on closed handle = %v, want ErrClosed", err)
	}
	select {
	case <-srv.dialCh:
		t.Fatal("a closed handle's command dialed a new connection")
	case <-time.After(100 * time.Millisecond):
	}
}

// TestConnReleaseFailsPongWaiters pins the one-reply-per-Ping contract
// across a deliberate connection release: when the last subscription is
// removed while another live handle awaits a pong, the release must
// fail that wait with an error event — not strand it forever.
func TestConnReleaseFailsPongWaiters(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newTestManager(t, srv, testConfig("node:6379"))

	b, err := m.Subscribe(ctx, "ch")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "ch")

	// a's ping is in flight when b removes the final subscription.
	a := m.NewHandle()
	if err := a.Ping(ctx, "lost"); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	fsc.expectCmd(t, "ping", "lost")
	if err := b.Unsubscribe(ctx, "ch"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	fsc.expectClosed(t)

	// The waiter is failed explicitly instead of waiting forever.
	expectErrorEvent(t, a.Events())
}

// expectErrorEvent drains events until an error event arrives,
// discarding other events (confirmations, messages).
func expectErrorEvent(t *testing.T, events <-chan any) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("events closed while waiting for an error event")
			}
			if _, isErr := ev.(error); isErr {
				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for an error event")
		}
	}
}

// TestWriteFailureFailsPongWaiters pins ledger settlement on a
// write-failure drop: when a later command's failed write forces the
// manager to drop the connection, an outstanding pong wait is failed
// with an error event — not stranded until some future connect wipes
// the ledger.
func TestWriteFailureFailsPongWaiters(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	m := newTestManager(t, srv, testConfig("node:6379"))

	// A ping-only handle: nothing is registered, so no reconnect replay
	// runs after the drop to settle the wait as a side effect.
	h := m.NewHandle()
	if err := h.Ping(ctx, "lost"); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "ping", "lost")

	// Pause the server's read loop right after it consumes the next
	// frame — a sacrificial ping — so the write under test then blocks
	// on the synchronous pipe and hits the write timeout.
	fsc.pauseAfterNext()
	if err := m.Ping(ctx); err != nil {
		t.Fatalf("arming ping: %v", err)
	}
	fsc.expectCmd(t, "ping")
	if err := h.Ping(ctx, "blocked"); err == nil {
		t.Fatal("Ping with a blocked write succeeded, want error")
	}

	// The drop settles the first ping's wait with an error event.
	expectErrorEvent(t, h.Events())

	fsc.paused.Store(false)
	fsc.expectClosed(t)

	// With nothing subscribed, the drop must not trigger a redial.
	select {
	case <-srv.dialCh:
		t.Fatal("the dropped ping-only connection was redialed")
	case <-time.After(100 * time.Millisecond):
	}
}

// TestReconnectFailsDeadPongWaiters pins ledger settlement across a
// reconnect: a pong wait whose connection died is failed with an error
// event when the replacement takes over — its reply can never arrive —
// and the fresh ledger attributes new pongs cleanly.
func TestReconnectFailsDeadPongWaiters(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newTestManager(t, srv, testConfig("node:6379"))

	h, err := m.Subscribe(ctx, "ch")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc1 := srv.waitDial(t)
	fsc1.expectCmd(t, "subscribe", "ch")

	// The ping is written but never answered: the server dies first.
	if err := h.Ping(ctx, "lost"); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	fsc1.expectCmd(t, "ping", "lost")
	_ = fsc1.conn.Close()

	// The reconnect replays the registry and fails the dead wait.
	fsc2 := srv.waitDial(t)
	fsc2.expectCmd(t, "subscribe", "ch")
	expectErrorEvent(t, h.Events())

	// The replacement's ledger starts clean: a fresh ping gets its pong.
	if err := h.Ping(ctx, "fresh"); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	fsc2.expectCmd(t, "ping", "fresh")
	fsc2.sendMessage(t, "pong", "fresh")
	expectPong(t, h.Events(), "fresh")
}

// waitForConfirmation drains events until a *Subscription with the
// given kind and channel arrives, discarding other events.
func waitForConfirmation(t *testing.T, events <-chan any, kind, channel string) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("events closed while waiting for a confirmation")
			}
			if sub, ok := ev.(*Subscription); ok && sub.Kind == kind && sub.Channel == channel {
				return
			}
		case <-deadline:
			t.Fatalf("timed out waiting for %s %s confirmation", kind, channel)
		}
	}
}

// subsUntil drains events until a *Message with each of the given
// payloads has arrived, returning how many *Subscription confirmations
// were seen on the way.
func subsUntil(t *testing.T, events <-chan any, payloads ...string) int {
	t.Helper()
	want := make(map[string]struct{}, len(payloads))
	for _, p := range payloads {
		want[p] = struct{}{}
	}
	subs := 0
	deadline := time.After(5 * time.Second)
	for len(want) > 0 {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("events closed while draining to the marker message(s)")
			}
			switch ev := ev.(type) {
			case *Subscription:
				subs++
			case *Message:
				delete(want, ev.Payload)
			}
		case <-deadline:
			t.Fatalf("timed out waiting for marker message(s) %v", want)
		}
	}
	return subs
}

// TestSubscribeConfirmationTargetsWriter pins targeted confirmation
// delivery: when a second handle subscribes to a channel another handle
// already owns, the re-written SUBSCRIBE's confirmation reaches only
// the new subscriber, never the existing owner.
func TestSubscribeConfirmationTargetsWriter(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newTestManager(t, srv, testConfig("node:6379"))

	a, err := m.Subscribe(ctx, "ch")
	if err != nil {
		t.Fatalf("Subscribe a: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "ch")
	waitForConfirmation(t, a.Events(), "subscribe", "ch")

	b, err := m.Subscribe(ctx, "ch")
	if err != nil {
		t.Fatalf("Subscribe b: %v", err)
	}
	fsc.expectCmd(t, "subscribe", "ch")
	waitForConfirmation(t, b.Events(), "subscribe", "ch")

	fsc.sendMessage(t, "message", "ch", "marker")
	if n := subsUntil(t, a.Events(), "marker"); n != 0 {
		t.Fatalf("first owner saw %d confirmation(s) of another handle's subscribe, want 0", n)
	}
}

// TestReplayConfirmationBroadcasts pins broadcast delivery for replay
// writes: the reconnect replay's confirmation reaches every registered
// owner of the name.
func TestReplayConfirmationBroadcasts(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newTestManager(t, srv, testConfig("node:6379"))

	a, err := m.Subscribe(ctx, "ch")
	if err != nil {
		t.Fatalf("Subscribe a: %v", err)
	}
	fsc1 := srv.waitDial(t)
	fsc1.expectCmd(t, "subscribe", "ch")
	waitForConfirmation(t, a.Events(), "subscribe", "ch")

	b, err := m.Subscribe(ctx, "ch")
	if err != nil {
		t.Fatalf("Subscribe b: %v", err)
	}
	fsc1.expectCmd(t, "subscribe", "ch")
	waitForConfirmation(t, b.Events(), "subscribe", "ch")

	_ = fsc1.conn.Close()

	fsc2 := srv.waitDial(t)
	fsc2.expectCmd(t, "subscribe", "ch")
	waitForConfirmation(t, a.Events(), "subscribe", "ch")
	waitForConfirmation(t, b.Events(), "subscribe", "ch")
}

// TestUnsubscribeRaceConfirmationReachesNobody pins the orphan
// unsubscribe race: a re-subscribe re-creates the registry entry while
// the UNSUBSCRIBE confirmation is still in flight, and that late
// confirmation must reach nobody — the new subscriber's first event is
// its own subscribe confirmation. ("keep" holds the connection open:
// unsubscribing the last name would release it.)
func TestUnsubscribeRaceConfirmationReachesNobody(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer() // no autoConfirm: the test answers by hand
	m := newTestManager(t, srv, testConfig("node:6379"))

	a, err := m.Subscribe(ctx, "ch", "keep")
	if err != nil {
		t.Fatalf("Subscribe a: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "ch", "keep")
	fsc.sendConfirm(t, "subscribe", "ch", 1)
	fsc.sendConfirm(t, "subscribe", "keep", 2)
	waitForConfirmation(t, a.Events(), "subscribe", "keep")

	// a leaves ch (last owner: the orphan UNSUBSCRIBE is written), and b
	// re-subscribes before the server's confirmation arrives.
	if err := a.Unsubscribe(ctx, "ch"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	fsc.expectCmd(t, "unsubscribe", "ch")
	b, err := m.Subscribe(ctx, "ch")
	if err != nil {
		t.Fatalf("Subscribe b: %v", err)
	}
	fsc.expectCmd(t, "subscribe", "ch")

	// The server answers in write order.
	fsc.sendConfirm(t, "unsubscribe", "ch", 1)
	fsc.sendConfirm(t, "subscribe", "ch", 2)

	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev, ok := <-b.Events():
			if !ok {
				t.Fatal("events closed while waiting for b's confirmation")
			}
			sub, isSub := ev.(*Subscription)
			if !isSub {
				continue
			}
			if sub.Kind != "subscribe" || sub.Channel != "ch" {
				t.Fatalf("b's first confirmation = %v, want its own subscribe ch", sub)
			}
			return
		case <-deadline:
			t.Fatal("timed out waiting for b's subscribe confirmation")
		}
	}
}

// TestEmptyPatternMessageRouting pins that pattern routing is driven by
// the frame kind, not by a non-empty Pattern field: PSUBSCRIBE "" is a
// valid subscription whose pmessage deliveries carry an empty pattern,
// and they must reach the pattern subscriber — never the subscriber of
// the empty channel, who gets its own message frame.
func TestEmptyPatternMessageRouting(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newTestManager(t, srv, testConfig("node:6379"))

	p, err := m.PSubscribe(ctx, "")
	if err != nil {
		t.Fatalf("PSubscribe: %v", err)
	}
	c, err := m.Subscribe(ctx, "")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "psubscribe", "")
	fsc.expectCmd(t, "subscribe", "")

	// PUBLISH "" ... fans out as a pmessage to the pattern subscriber
	// and a message to the channel subscriber.
	fsc.sendMessage(t, "pmessage", "", "", "via-pattern")
	fsc.sendMessage(t, "message", "", "via-channel")

	firstMessage := func(events <-chan any) *Message {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for {
			select {
			case ev, ok := <-events:
				if !ok {
					t.Fatal("events closed while waiting for a message")
				}
				if msg, ok := ev.(*Message); ok {
					return msg
				}
			case <-deadline:
				t.Fatal("timed out waiting for a message")
			}
		}
	}
	if msg := firstMessage(p.Events()); msg.Payload != "via-pattern" {
		t.Fatalf("pattern subscriber's first message = %q, want \"via-pattern\"", msg.Payload)
	}
	if msg := firstMessage(c.Events()); msg.Payload != "via-channel" {
		t.Fatalf("channel subscriber's first message = %q, want \"via-channel\"", msg.Payload)
	}
}

// TestUnsubscribeRaceDoesNotRequestReload pins that the topology-reload
// heuristic consults the ledger: the confirmation of the manager's own
// UNSUBSCRIBE finding subscribers again (a resubscribe raced it) is not
// a slot migration and must not request a reload.
func TestUnsubscribeRaceDoesNotRequestReload(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer() // no autoConfirm: the test answers by hand
	reloads := make(chan struct{}, 16)
	m := newTestManagerReload(t, srv, testConfig("node:6379"), func() {
		select {
		case reloads <- struct{}{}:
		default:
		}
	})

	a, err := m.Subscribe(ctx, "ch", "keep")
	if err != nil {
		t.Fatalf("Subscribe a: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "ch", "keep")
	fsc.sendConfirm(t, "subscribe", "ch", 1)
	fsc.sendConfirm(t, "subscribe", "keep", 2)
	waitForConfirmation(t, a.Events(), "subscribe", "keep")

	if err := a.Unsubscribe(ctx, "ch"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	fsc.expectCmd(t, "unsubscribe", "ch")
	b, err := m.Subscribe(ctx, "ch")
	if err != nil {
		t.Fatalf("Subscribe b: %v", err)
	}
	fsc.expectCmd(t, "subscribe", "ch")

	fsc.sendConfirm(t, "unsubscribe", "ch", 1)
	fsc.sendConfirm(t, "subscribe", "ch", 2)
	// b's confirmation fences the earlier unsubscribe confirmation.
	waitForConfirmation(t, b.Events(), "subscribe", "ch")

	select {
	case <-reloads:
		t.Fatal("own unsubscribe confirmation requested a topology reload")
	default:
	}
}

// TestServerInitiatedUnsubscribeRequestsReload pins the positive half
// of the heuristic: an UNMATCHED unsubscribe confirmation (answering no
// write of ours) that still finds subscribers is a slot migration and
// must request a reload.
func TestServerInitiatedUnsubscribeRequestsReload(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	reloads := make(chan struct{}, 16)
	m := newTestManagerReload(t, srv, testConfig("node:6379"), func() {
		select {
		case reloads <- struct{}{}:
		default:
		}
	})

	if _, err := m.SSubscribe(ctx, "{a}ch"); err != nil {
		t.Fatalf("SSubscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "ssubscribe", "{a}ch")

	// Server-initiated: no SUNSUBSCRIBE was written.
	fsc.sendConfirm(t, "sunsubscribe", "{a}ch", 0)
	select {
	case <-reloads:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the reload request")
	}
}

// TestFanoutClonesMessages pins per-destination independence: handles
// sharing a channel receive their own message values (PayloadSlice
// included), so one consumer's mutation can't corrupt another's.
func TestFanoutClonesMessages(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newTestManager(t, srv, testConfig("node:6379"))

	a, err := m.Subscribe(ctx, "ch")
	if err != nil {
		t.Fatalf("Subscribe a: %v", err)
	}
	b, err := m.Subscribe(ctx, "ch")
	if err != nil {
		t.Fatalf("Subscribe b: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "ch")
	fsc.expectCmd(t, "subscribe", "ch")

	// A message whose payload is a slice (RESP3 array payload).
	fsc.write(t, "*3\r\n$7\r\nmessage\r\n$2\r\nch\r\n*2\r\n$3\r\none\r\n$3\r\ntwo\r\n")

	nextMessage := func(events <-chan any) *Message {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for {
			select {
			case ev, ok := <-events:
				if !ok {
					t.Fatal("events closed while waiting for a message")
				}
				if msg, ok := ev.(*Message); ok {
					return msg
				}
			case <-deadline:
				t.Fatal("timed out waiting for a message")
			}
		}
	}
	ma := nextMessage(a.Events())
	mb := nextMessage(b.Events())
	if ma == mb {
		t.Fatal("both handles received the same *Message instance")
	}

	// One consumer's mutation must stay its own.
	ma.PayloadSlice[0] = "mutated"
	ma.Payload = "mutated"
	if mb.PayloadSlice[0] != "one" || mb.Payload != "" {
		t.Fatalf("mutation leaked across handles: %+v", mb)
	}
}

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

// TestConfirmationCountIsPerHandle pins that Subscription.Count is the
// receiving handle's own name count, as on a dedicated connection — not
// the server's connection-wide count, which includes other handles'
// names — for server confirmations and synthesized ones alike.
func TestConfirmationCountIsPerHandle(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer() // answers by hand
	m := newTestManager(t, srv, testConfig("node:6379"))

	a, err := m.Subscribe(ctx, "a")
	if err != nil {
		t.Fatalf("Subscribe a: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "a")
	fsc.sendConfirm(t, "subscribe", "a", 1)
	waitForConfirmation(t, a.Events(), "subscribe", "a")

	b, err := m.Subscribe(ctx, "b")
	if err != nil {
		t.Fatalf("Subscribe b: %v", err)
	}
	fsc.expectCmd(t, "subscribe", "b")
	// The server counts the connection: a and b.
	fsc.sendConfirm(t, "subscribe", "b", 2)
	sub, ok := recvEvent(t, b.Events()).(*Subscription)
	if !ok || sub.Kind != "subscribe" || sub.Channel != "b" || sub.Count != 1 {
		t.Fatalf("event = %#v, want subscribe b with count 1", sub)
	}

	// A synthesized confirmation (b does not own a, so nothing is
	// written) counts the same way.
	if err := b.Unsubscribe(ctx, "a"); err != nil {
		t.Fatalf("Unsubscribe a on b: %v", err)
	}
	sub, ok = recvEvent(t, b.Events()).(*Subscription)
	if !ok || sub.Kind != "unsubscribe" || sub.Channel != "a" || sub.Count != 1 {
		t.Fatalf("event = %#v, want unsubscribe a with count 1", sub)
	}
	fsc.expectNoCmd(t, 100*time.Millisecond)
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
