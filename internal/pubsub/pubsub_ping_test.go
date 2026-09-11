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
