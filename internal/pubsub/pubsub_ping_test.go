package pubsub

import (
	"context"
	"testing"
	"time"
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
