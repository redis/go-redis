package pubsub

import (
	"context"
	"errors"
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

// TestRetryWriteBudgetStartsWithLockInHand pins that a rejected-name
// retry's write is not bounded by the budget spent getting to it: a
// budget shared with the wait for the manager lock could be spent by
// the time the write starts, and a socket deadline already in the past
// fails the write before a byte is sent — dropping a live connection
// for every subscriber. The wait is stretched past the whole budget
// here by holding the manager lock.
func TestRetryWriteBudgetStartsWithLockInHand(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	m := newTestManager(t, srv, testConfig("node:6379"))

	h, err := m.Subscribe(ctx, "ch")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "ch")
	fsc.sendError(t, "NOPERM no permissions to access this channel")
	if err := waitForError(t, h.Events()); !strings.Contains(err.Error(), "NOPERM") {
		t.Fatalf("got %v, want the NOPERM reply", err)
	}

	// Stall the retry between the slot and the write for longer than
	// its whole budget, then let it go.
	m.mu.Lock()
	done := make(chan struct{})
	go func() {
		defer close(done)
		m.resubscribeRejected(50 * time.Millisecond)
	}()
	time.Sleep(150 * time.Millisecond)
	m.mu.Unlock()
	<-done

	// The live connection stays, and the retry went out on it.
	select {
	case fsc2 := <-srv.dialCh:
		t.Fatalf("live connection was replaced (dial to %q) by a retry whose wait outlived its budget", fsc2.addr)
	case <-time.After(100 * time.Millisecond):
	}
	fsc.expectCmd(t, "subscribe", "ch")
}

// TestPendingReconcilesWithHealthCheckDisabled pins that disabling the
// health check does not disable subscription recovery: a subscribe
// rejected with an error reply on a healthy connection is re-sent by
// the dedicated retry loop (see Manager.retryRejected), which runs
// whether or not the health checker is parked.
func TestPendingReconcilesWithHealthCheckDisabled(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	cfg := testConfig("node:6379") // HealthCheckInterval -1: checker parked
	cfg.SubscribeRetryInterval = 20 * time.Millisecond
	m := newTestManager(t, srv, cfg)

	if _, err := m.Subscribe(ctx, "denied"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "denied")
	fsc.sendError(t, "NOPERM this user has no permissions")

	// Still rejected: the resync keeps re-sending on its cadence.
	fsc.expectCmd(t, "subscribe", "denied")
	fsc.sendError(t, "NOPERM this user has no permissions")
	fsc.expectCmd(t, "subscribe", "denied")
	fsc.sendConfirm(t, "subscribe", "denied", 1)

	// Confirmed: drain re-sends racing the confirmation, then require
	// quiet — nothing is Pending, so the resync loop must write nothing.
	drainDeadline := time.After(100 * time.Millisecond)
drain:
	for {
		select {
		case cmd := <-fsc.cmds:
			if !slices.Equal(cmd, []string{"subscribe", "denied"}) {
				t.Fatalf("unexpected command %v", cmd)
			}
			fsc.sendConfirm(t, "subscribe", "denied", 1)
		case <-drainDeadline:
			break drain
		}
	}
	fsc.expectNoCmd(t, 100*time.Millisecond)
}

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

// TestRejectedSubscribeReconciles pins the Rejected reconciliation (see
// subState): a subscribe the server rejects answers with an error reply
// instead of a confirmation — writes are fire-and-forget — so the name
// becomes Rejected and the retry loop re-sends it until the server
// accepts, with the health checker's pings interleaving. Confirmed
// names are never re-sent, and unsubscribing a Rejected name stops its
// retries.
func TestRejectedSubscribeReconciles(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoPong = true
	cfg := testConfig("node:6379")
	cfg.HealthCheckInterval = 10 * time.Millisecond
	cfg.SubscribeRetryInterval = 10 * time.Millisecond
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

// TestPendingResyncSurvivesChurn pins retry liveness under unrelated
// traffic: a rejected subscribe must still be re-sent even when other
// handles write subscription commands more often than the retry
// interval.
func TestPendingResyncSurvivesChurn(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	cfg := testConfig("node:6379") // HealthCheckInterval -1: checker parked
	cfg.SubscribeRetryInterval = 100 * time.Millisecond
	m := newTestManager(t, srv, cfg)

	if _, err := m.Subscribe(ctx, "bad"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "bad")
	fsc.sendError(t, "NOPERM this user has no permissions") // stays Pending

	c, err := m.Subscribe(ctx, "churn")
	if err != nil {
		t.Fatalf("Subscribe churn: %v", err)
	}
	fsc.expectCmd(t, "subscribe", "churn")

	// Churn faster than the resync interval; the re-send of "bad" must
	// arrive anyway.
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case cmd := <-fsc.cmds:
			if cmd[0] == "subscribe" && slices.Contains(cmd[1:], "bad") {
				return
			}
		case <-tick.C:
			if _, err := c.Subscribe(ctx, "churn"); err != nil {
				t.Fatalf("churn subscribe: %v", err)
			}
		case <-deadline:
			t.Fatal("pending subscribe was never re-sent under churn")
		}
	}
}

// TestSubscribeSkipsNamesCoveredByConnectReplay pins the fresh-connect
// dedup: a Subscribe that has to dial registers its names first, so the
// replay carries them along with every retained registration and no
// second write follows — one would draw a second confirmation
// (duplicate events, two ledger entries).
func TestSubscribeSkipsNamesCoveredByConnectReplay(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newTestManager(t, srv, testConfig("node:6379"))

	// Seed a retained registration — what a failed connect leaves behind
	// on a caller-held handle — without dialing.
	h := m.NewHandle().(*handle)
	m.mu.Lock()
	h.channels["kept"] = struct{}{}
	m.registerHandleLocked(m.subscribers, "kept", h)
	m.mu.Unlock()

	// Subscribing to the retained name plus a new one dials fresh: both
	// are registered before the dial, so the replay carries both in one
	// command (map order) and nothing else is written.
	if _, err := h.Subscribe(ctx, "kept", "new"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	replay := fsc.waitCmd(t)
	if replay[0] != "subscribe" || len(replay) != 3 ||
		!slices.Contains(replay[1:], "kept") || !slices.Contains(replay[1:], "new") {
		t.Fatalf("replay command = %v, want one subscribe carrying kept and new", replay)
	}
	fsc.expectNoCmd(t, 100*time.Millisecond)

	// Exactly one confirmation per name surfaces on Events.
	seen := map[string]int{}
	for range 2 {
		sub, ok := recvEvent(t, h.Events()).(*Subscription)
		if !ok || sub.Kind != "subscribe" {
			t.Fatalf("event = %#v, want a subscribe confirmation", sub)
		}
		seen[sub.Channel]++
	}
	if seen["kept"] != 1 || seen["new"] != 1 {
		t.Fatalf("confirmations = %v, want exactly one per name", seen)
	}
	select {
	case ev := <-h.Events():
		t.Fatalf("duplicate event after both confirmations: %#v", ev)
	case <-time.After(100 * time.Millisecond):
	}
}
