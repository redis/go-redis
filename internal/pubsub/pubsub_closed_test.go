package pubsub

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

// TestUnsubscribeAfterCloseReturnsErrClosed pins that a closed handle's
// unsubscribes report the close, like its subscribes and pings.
func TestUnsubscribeAfterCloseReturnsErrClosed(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newTestManager(t, srv, testConfig("node:6379"))

	h, err := m.Subscribe(ctx, "u")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	srv.waitDial(t)
	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for name, unsub := range map[string]func(context.Context, ...string) error{
		"Unsubscribe": h.Unsubscribe, "PUnsubscribe": h.PUnsubscribe, "SUnsubscribe": h.SUnsubscribe,
	} {
		if err := unsub(ctx, "u"); !errors.Is(err, pool.ErrClosed) {
			t.Fatalf("%s after Close = %v, want pool.ErrClosed", name, err)
		}
	}

	// The same once the manager closed the handle.
	h2 := m.NewHandle()
	_ = m.Close()
	if err := h2.Unsubscribe(ctx, "u"); !errors.Is(err, pool.ErrClosed) {
		t.Fatalf("Unsubscribe after manager Close = %v, want pool.ErrClosed", err)
	}
}

// TestClientSetNameRESP2Subscribed pins that on RESP2 ClientSetName on a
// subscribed shared connection fails up front, writing nothing, instead
// of returning nil and letting the server's refusal surface later on
// Events. Unsubscribed, it is written as before.
func TestClientSetNameRESP2Subscribed(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	cfg := testConfig("node:6379")
	cfg.RESP2 = true
	m := newTestManager(t, srv, cfg)

	idle := m.NewHandle()
	if err := idle.ClientSetName(ctx, "n1"); err != nil {
		t.Fatalf("ClientSetName with nothing subscribed: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "client", "setname", "n1")

	if _, err := m.Subscribe(ctx, "s"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc.expectCmd(t, "subscribe", "s")
	if err := idle.ClientSetName(ctx, "n2"); !errors.Is(err, ErrClientSetNameSubscribed) {
		t.Fatalf("ClientSetName while subscribed = %v, want ErrClientSetNameSubscribed", err)
	}
	fsc.expectNoCmd(t, 50*time.Millisecond)
}
