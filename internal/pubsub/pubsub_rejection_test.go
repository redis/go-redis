package pubsub

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"
)

// waitForError drains events until an error event arrives.
func waitForError(t *testing.T, events <-chan any) error {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatal("events closed while waiting for an error reply")
			}
			if err, isErr := ev.(error); isErr {
				return err
			}
		case <-deadline:
			t.Fatal("timed out waiting for an error reply")
		}
	}
}

// expectSoloSubscribes consumes n commands and asserts each is a
// single-name subscribe, returning the names seen.
func expectSoloSubscribes(t *testing.T, fsc *fakeServerConn, n int) map[string]bool {
	t.Helper()
	names := map[string]bool{}
	for range n {
		cmd := fsc.waitCmd(t)
		if len(cmd) != 2 || cmd[0] != "subscribe" {
			t.Fatalf("got %v, want a single-name subscribe", cmd)
		}
		names[cmd[1]] = true
	}
	return names
}

// TestRejectedNameDoesNotPoisonBatch pins failure isolation on
// rejection: the server refuses a multi-name SUBSCRIBE as a whole, so
// after a rejection the names are retried one by one — the good one
// gets established and only the bad one keeps being retried — and the
// rejection reaches its owners alone, never a bystander.
func TestRejectedNameDoesNotPoisonBatch(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer() // answers by hand
	cfg := testConfig("node:6379")
	cfg.SubscribeRetryInterval = 30 * time.Millisecond
	m := newTestManager(t, srv, cfg)

	h, err := m.Subscribe(ctx, "good", "bad")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "good", "bad")

	bystander, err := m.Subscribe(ctx, "other")
	if err != nil {
		t.Fatalf("Subscribe other: %v", err)
	}
	fsc.expectCmd(t, "subscribe", "other")
	fsc.sendConfirm(t, "subscribe", "other", 1)
	waitForConfirmation(t, bystander.Events(), "subscribe", "other")

	// The whole command is refused: the issuer hears it, and both names
	// are retried — one by one.
	fsc.sendError(t, "NOPERM no access to 'bad'")
	if err := waitForError(t, h.Events()); !strings.Contains(err.Error(), "NOPERM") {
		t.Fatalf("issuer got %v, want the NOPERM reply", err)
	}
	retried := map[string]bool{}
	for range 2 {
		cmd := fsc.waitCmd(t)
		if len(cmd) != 2 || cmd[0] != "subscribe" {
			t.Fatalf("retry sent %v, want a single-name subscribe", cmd)
		}
		retried[cmd[1]] = true
		switch cmd[1] {
		case "good":
			fsc.sendConfirm(t, "subscribe", "good", 2)
		case "bad":
			fsc.sendError(t, "NOPERM no access to 'bad'")
		}
	}
	if !retried["good"] || !retried["bad"] {
		t.Fatalf("retried %v, want good and bad", retried)
	}
	waitForConfirmation(t, h.Events(), "subscribe", "good")

	// From here only the bad name is retried, alone, and only its owner
	// hears each rejection.
	if cmd := fsc.waitCmd(t); !slices.Equal(cmd, []string{"subscribe", "bad"}) {
		t.Fatalf("retry sent %v, want [subscribe bad] alone", cmd)
	}
	fsc.sendError(t, "NOPERM no access to 'bad'")
	if err := waitForError(t, h.Events()); !strings.Contains(err.Error(), "NOPERM") {
		t.Fatalf("owner got %v, want the NOPERM reply", err)
	}
	select {
	case ev := <-bystander.Events():
		t.Fatalf("a rejection of another handle's name reached a bystander: %#v", ev)
	case <-time.After(100 * time.Millisecond):
	}

	// The established name delivers.
	fsc.sendMessage(t, "message", "good", "m1")
	for {
		ev := recvEvent(t, h.Events())
		if msg, ok := ev.(*Message); ok {
			if msg.Channel != "good" || msg.Payload != "m1" {
				t.Fatalf("got %v, want m1 on good", msg)
			}
			break
		}
	}
}

// TestReplayWritesKnownRejectedNamesAlone pins that the knowledge of a
// rejection survives a reconnect: the replay batches the names that were
// never rejected and writes a known-rejected one alone, so the good
// names are re-established right away instead of after a retry round.
func TestReplayWritesKnownRejectedNamesAlone(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer() // answers by hand
	cfg := testConfig("node:6379")
	cfg.SubscribeRetryInterval = time.Hour // no retry interferes
	m := newTestManager(t, srv, cfg)

	h, err := m.Subscribe(ctx, "good")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc1 := srv.waitDial(t)
	fsc1.expectCmd(t, "subscribe", "good")
	fsc1.sendConfirm(t, "subscribe", "good", 1)
	waitForConfirmation(t, h.Events(), "subscribe", "good")

	if _, err := h.Subscribe(ctx, "bad"); err != nil {
		t.Fatalf("Subscribe bad: %v", err)
	}
	fsc1.expectCmd(t, "subscribe", "bad")
	fsc1.sendError(t, "NOPERM no access to 'bad'")
	if err := waitForError(t, h.Events()); !strings.Contains(err.Error(), "NOPERM") {
		t.Fatalf("got %v, want the NOPERM reply", err)
	}

	// Break the connection: the replay on the new one writes the good
	// name and the known-rejected one as separate commands.
	_ = fsc1.conn.Close()
	fsc2 := srv.waitDial(t)
	replayed := expectSoloSubscribes(t, fsc2, 2)
	if !replayed["good"] || !replayed["bad"] {
		t.Fatalf("replayed %v, want good and bad in separate commands", replayed)
	}

	// The good name is confirmed on its own; the bad one is rejected on
	// its own and still reaches only its owner.
	fsc2.sendConfirm(t, "subscribe", "good", 1)
	fsc2.sendError(t, "NOPERM no access to 'bad'")
	waitForConfirmation(t, h.Events(), "subscribe", "good")
	if err := waitForError(t, h.Events()); !strings.Contains(err.Error(), "NOPERM") {
		t.Fatalf("got %v, want the NOPERM reply", err)
	}
}
