package pubsub

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/hashtag"
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

// TestUnsubscribeErrorReachesIssuer pins the attribution of a rejected
// orphan UNSUBSCRIBE (an ACL revoking the command, say): the error
// reaches the handle that released the name even though the name has
// left the registry by then — and not a handle that re-added the name
// meanwhile, which an owner lookup would mistake for its target.
func TestUnsubscribeErrorReachesIssuer(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer() // no autoConfirm: the test answers by hand
	m := newTestManager(t, srv, testConfig("node:6379"))

	// keep holds the shared connection open once ch is gone.
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
	// re-adds ch before the server's reply arrives.
	if err := a.Unsubscribe(ctx, "ch"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	fsc.expectCmd(t, "unsubscribe", "ch")
	b, err := m.Subscribe(ctx, "ch")
	if err != nil {
		t.Fatalf("Subscribe b: %v", err)
	}
	fsc.expectCmd(t, "subscribe", "ch")

	// The server answers in write order: the unsubscribe is refused.
	fsc.sendError(t, "NOPERM this user has no permissions to run the 'unsubscribe' command")
	fsc.sendConfirm(t, "subscribe", "ch", 2)

	if err := waitForError(t, a.Events()); !strings.Contains(err.Error(), "NOPERM") {
		t.Fatalf("a got %v, want the NOPERM reply to its unsubscribe", err)
	}
	// b sees its own confirmation and nothing of a's rejection.
	for {
		switch ev := recvEvent(t, b.Events()).(type) {
		case error:
			t.Fatalf("b received a's unsubscribe rejection: %v", ev)
		case *Subscription:
			if ev.Kind == "subscribe" && ev.Channel == "ch" {
				return
			}
		}
	}
}

// TestManagerUnsubscribeErrorReachesEveryReleaser pins the same for a
// manager-level Unsubscribe spanning handles: every handle that
// released the name gets the rejection.
func TestManagerUnsubscribeErrorReachesEveryReleaser(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	m := newTestManager(t, srv, testConfig("node:6379"))

	h1, err := m.Subscribe(ctx, "x")
	if err != nil {
		t.Fatalf("Subscribe h1: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "x")
	fsc.sendConfirm(t, "subscribe", "x", 1)
	waitForConfirmation(t, h1.Events(), "subscribe", "x")

	h2, err := m.Subscribe(ctx, "x", "keep")
	if err != nil {
		t.Fatalf("Subscribe h2: %v", err)
	}
	fsc.expectCmd(t, "subscribe", "x", "keep")
	fsc.sendConfirm(t, "subscribe", "x", 1)
	fsc.sendConfirm(t, "subscribe", "keep", 2)
	waitForConfirmation(t, h2.Events(), "subscribe", "keep")

	if err := m.Unsubscribe(ctx, "x"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	fsc.expectCmd(t, "unsubscribe", "x")
	fsc.sendError(t, "NOPERM this user has no permissions to run the 'unsubscribe' command")

	for i, h := range []PubSuber{h1, h2} {
		if err := waitForError(t, h.Events()); !strings.Contains(err.Error(), "NOPERM") {
			t.Fatalf("h%d got %v, want the NOPERM reply to the unsubscribe", i+1, err)
		}
	}
}

// errorBeforeMessage drains events up to the next *Message and returns
// the error event seen on the way, if any.
func errorBeforeMessage(t *testing.T, events <-chan any) error {
	t.Helper()
	var got error
	for {
		switch ev := recvEvent(t, events).(type) {
		case error:
			got = ev
		case *Message:
			return got
		}
	}
}

// TestManagerSUnsubscribeErrorStaysWithinSlot pins per-slot
// attribution: a manager-level SUnsubscribe spanning handles whose
// shard channels hash to different slots is written as one command per
// slot, and the error reply of one slot (a MOVED, say) reaches only the
// handles that released the names that command carried — not the
// releasers of a slot that succeeded.
func TestManagerSUnsubscribeErrorStaysWithinSlot(t *testing.T) {
	ctx := context.Background()
	if s1, s2 := hashtag.Slot("{a}one"), hashtag.Slot("{b}one"); s1 == s2 {
		t.Fatalf("test channels share slot %d; pick different tags", s1)
	}
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newTestManager(t, srv, testConfig("node:6379"))

	// Each handle owns one shard channel plus a marker channel that
	// keeps the connection alive and orders the assertions below.
	hA, err := m.SSubscribe(ctx, "{a}one")
	if err != nil {
		t.Fatalf("SSubscribe hA: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "ssubscribe", "{a}one")
	if _, err := hA.Subscribe(ctx, "markA"); err != nil {
		t.Fatalf("Subscribe markA: %v", err)
	}
	fsc.expectCmd(t, "subscribe", "markA")
	hB, err := m.SSubscribe(ctx, "{b}one")
	if err != nil {
		t.Fatalf("SSubscribe hB: %v", err)
	}
	fsc.expectCmd(t, "ssubscribe", "{b}one")
	if _, err := hB.Subscribe(ctx, "markB"); err != nil {
		t.Fatalf("Subscribe markB: %v", err)
	}
	fsc.expectCmd(t, "subscribe", "markB")
	waitForConfirmation(t, hA.Events(), "subscribe", "markA")
	waitForConfirmation(t, hB.Events(), "subscribe", "markB")
	releaser := map[string]PubSuber{"{a}one": hA, "{b}one": hB}
	marker := map[string]string{"{a}one": "markA", "{b}one": "markB"}

	// One call orphans both shard channels: two SUNSUBSCRIBE commands,
	// one per slot, in an order that depends on map iteration.
	fsc.autoConfirm.Store(false)
	if err := m.SUnsubscribe(ctx, "{a}one", "{b}one"); err != nil {
		t.Fatalf("SUnsubscribe: %v", err)
	}
	first, second := fsc.waitCmd(t), fsc.waitCmd(t)
	for _, cmd := range [][]string{first, second} {
		if len(cmd) != 2 || cmd[0] != "sunsubscribe" {
			t.Fatalf("command = %v, want a single-name sunsubscribe", cmd)
		}
	}

	// The server refuses the first slot and confirms the second, then
	// the markers: a handle's events up to its marker hold the error iff
	// its slot was the refused one.
	fsc.sendError(t, "MOVED 15495 node-b:6379")
	fsc.sendConfirm(t, "sunsubscribe", second[1], 0)
	fsc.sendMessage(t, "message", marker[first[1]], "m")
	fsc.sendMessage(t, "message", marker[second[1]], "m")

	if err := errorBeforeMessage(t, releaser[first[1]].Events()); err == nil || !strings.Contains(err.Error(), "MOVED") {
		t.Fatalf("releaser of the refused slot %q got %v, want the MOVED reply", first[1], err)
	}
	if err := errorBeforeMessage(t, releaser[second[1]].Events()); err != nil {
		t.Fatalf("releaser of the confirmed slot %q received the other slot's error: %v", second[1], err)
	}
}
