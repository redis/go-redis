package pubsub

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"
)

// TestSubscribeAfterFailedDialSelfHeals pins the wake-on-kept-
// registration contract: a Subscribe on a caller-held handle whose dial
// fails keeps its registration (see handleSubscribe) AND wakes the read
// loop, whose reconnect cycle then retries the dial until the server is
// back and replays the registry. Without the wake, the read loop —
// parked idle after the previous unsubscribe released the connection —
// would never run again, and the kept registration would never be
// established (the health checker skips a nil conn, so nothing else
// retries).
func TestSubscribeAfterFailedDialSelfHeals(t *testing.T) {
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

	// Unsubscribing the last name releases the connection; the read
	// loop errors out of its blocked read and parks idle.
	if err := h.Unsubscribe(ctx, "a"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	fsc.expectClosed(t)
	// Give the read loop time to observe the closed conn and park; the
	// fix must hold regardless (an unparked loop retries on its own),
	// but the interesting regression case is the parked one.
	time.Sleep(50 * time.Millisecond)

	// Subscribe while the server is unreachable: the error surfaces to
	// the caller, but the registration is kept for the reconnect
	// replay.
	dialErr := errors.New("server down")
	srv.setDialErr(dialErr)
	if _, err := h.Subscribe(ctx, "b"); !errors.Is(err, dialErr) {
		t.Fatalf("Subscribe error = %v, want %v", err, dialErr)
	}

	// The server comes back. The woken read loop must redial on its own
	// and replay the kept registration — no further user calls.
	srv.setDialErr(nil)
	fsc2 := srv.waitDial(t)
	fsc2.expectCmd(t, "subscribe", "b")

	// End to end: the replayed subscription delivers.
	ch := h.Channel()
	fsc2.sendMessage(t, "message", "b", "healed")
	if msg := recvMsg(t, ch); msg.Payload != "healed" {
		t.Fatalf("got %q, want \"healed\"", msg.Payload)
	}
}

// TestRejectedSubscribeReconciles pins the Pending/Subscribed
// reconciliation (see subscription): a subscribe the server rejects
// answers with an error reply instead of a confirmation — writes are
// fire-and-forget — so the name stays Pending and the health checker
// re-sends it until the server accepts. Confirmed names are never
// re-sent, and unsubscribing a Pending name stops its retries.
func TestRejectedSubscribeReconciles(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	cfg := testConfig("node:6379")
	cfg.HealthCheckInterval = 10 * time.Millisecond
	m := newTestManager(t, srv, cfg)

	h, err := m.Subscribe(ctx, "ok")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "ok")
	fsc.sendConfirm(t, "subscribe", "ok", 1)

	// The live health checker interleaves pings freely; only
	// subscription commands are asserted.
	waitCmdSkipPing := func() []string {
		t.Helper()
		for {
			cmd := fsc.waitCmd(t)
			if len(cmd) > 0 && cmd[0] == "ping" {
				continue
			}
			return cmd
		}
	}
	expectOnlyPings := func(d time.Duration) {
		t.Helper()
		deadline := time.After(d)
		for {
			select {
			case cmd := <-fsc.cmds:
				if len(cmd) == 0 || cmd[0] != "ping" {
					t.Fatalf("unexpected command %v", cmd)
				}
			case <-deadline:
				return
			}
		}
	}

	// The server rejects the subscribe: the name stays Pending and is
	// re-sent — alone, without the confirmed one.
	if _, err := h.Subscribe(ctx, "denied"); err != nil {
		t.Fatalf("Subscribe denied: %v", err)
	}
	if got := waitCmdSkipPing(); !slices.Equal(got, []string{"subscribe", "denied"}) {
		t.Fatalf("command = %v, want [subscribe denied]", got)
	}
	fsc.sendError(t, "NOPERM this user has no permissions")
	if got := waitCmdSkipPing(); !slices.Equal(got, []string{"subscribe", "denied"}) {
		t.Fatalf("resync sent %v, want [subscribe denied]", got)
	}

	// Still rejected: the retry keeps coming until the server accepts.
	fsc.sendError(t, "NOPERM this user has no permissions")
	if got := waitCmdSkipPing(); !slices.Equal(got, []string{"subscribe", "denied"}) {
		t.Fatalf("resync sent %v, want [subscribe denied]", got)
	}
	fsc.sendConfirm(t, "subscribe", "denied", 2)

	// Confirmed: drain re-sends racing the confirmation, then require
	// quiet (~20 health cycles would re-send any surviving Pending).
	drainDeadline := time.After(100 * time.Millisecond)
drain:
	for {
		select {
		case cmd := <-fsc.cmds:
			if len(cmd) > 0 && cmd[0] == "ping" {
				continue
			}
			if !slices.Equal(cmd, []string{"subscribe", "denied"}) {
				t.Fatalf("unexpected command %v", cmd)
			}
		case <-drainDeadline:
			break drain
		}
	}
	expectOnlyPings(200 * time.Millisecond)

	// End to end: the reconciled subscription delivers.
	ch := h.Channel()
	fsc.sendMessage(t, "message", "denied", "granted")
	if msg := recvMsg(t, ch); msg.Payload != "granted" {
		t.Fatalf("got %q, want \"granted\"", msg.Payload)
	}

	// Unsubscribing a Pending name stops its retries: the entry — and
	// its state — dies with the last handle.
	if _, err := h.Subscribe(ctx, "gone"); err != nil {
		t.Fatalf("Subscribe gone: %v", err)
	}
	if got := waitCmdSkipPing(); !slices.Equal(got, []string{"subscribe", "gone"}) {
		t.Fatalf("command = %v, want [subscribe gone]", got)
	}
	if err := h.Unsubscribe(ctx, "gone"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	// Re-sends may interleave until the unsubscribe lands.
	for {
		cmd := waitCmdSkipPing()
		if slices.Equal(cmd, []string{"unsubscribe", "gone"}) {
			break
		}
		if !slices.Equal(cmd, []string{"subscribe", "gone"}) {
			t.Fatalf("unexpected command %v", cmd)
		}
	}
	expectOnlyPings(200 * time.Millisecond)
}

// TestPingWriteFailureDropsConn pins the write-failure handling of the
// non-subscription writers: a PING or CLIENT SETNAME that fails mid-write
// may have partially reached the server, desyncing the RESP stream — the
// connection must be dropped (like the subscribe/unsubscribe paths do)
// so the read loop reconnects and replays the registry, instead of the
// server parsing the next command as the truncated one's payload.
func TestPingWriteFailureDropsConn(t *testing.T) {
	for _, tc := range []struct {
		name  string
		write func(ctx context.Context, m *Manager) error
	}{
		{"Ping", func(ctx context.Context, m *Manager) error { return m.Ping(ctx) }},
		{"ClientSetName", func(ctx context.Context, m *Manager) error { return m.ClientSetName(ctx, "n") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			srv := newFakeServer()
			cfg := testConfig("node:6379")
			cfg.WriteTimeout = 50 * time.Millisecond
			m := newTestManager(t, srv, cfg)

			if _, err := m.Subscribe(ctx, "a"); err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			fsc1 := srv.waitDial(t)
			fsc1.expectCmd(t, "subscribe", "a")
			fsc1.sendConfirm(t, "subscribe", "a", 1)

			// Park the pipe: the server's read loop checks paused only
			// between frames, so its one in-flight read swallows the
			// first write — the next write then blocks on the unread
			// pipe until WriteTimeout and fails with a possibly
			// half-written command on the wire.
			fsc1.paused.Store(true)
			var writeErr error
			for range 3 {
				if writeErr = tc.write(ctx, m); writeErr != nil {
					break
				}
			}
			if writeErr == nil {
				t.Fatal("expected a write error")
			}

			// The failed write dropped the connection; the read loop
			// reconnects and replays the registry.
			fsc2 := srv.waitDial(t)
			fsc2.expectCmd(t, "subscribe", "a")

			// The replaced conn was closed; resume its read loop so it
			// observes the close.
			fsc1.paused.Store(false)
			fsc1.expectClosed(t)
		})
	}
}
