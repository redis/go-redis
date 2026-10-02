package pubsub

import (
	"context"
	"testing"
	"time"
)

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

// TestRetrySupersedesLostWaiter pins waiter retirement: when a
// confirmation is lost on a healthy connection and the resync re-sends
// the name, the original write's waiter is retired — otherwise the
// retry's confirmation would settle the stale original and leave the
// retry's own waiter shifting the attribution of every confirmation
// after it.
func TestRetrySupersedesLostWaiter(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer() // no autoConfirm: the test answers by hand
	cfg := testConfig("node:6379")
	cfg.PendingResyncFallback = 100 * time.Millisecond
	m := newTestManager(t, srv, cfg)

	// x's confirmation never arrives; only the retry's does.
	a, err := m.Subscribe(ctx, "x")
	if err != nil {
		t.Fatalf("Subscribe a: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "x")
	fsc.expectCmd(t, "subscribe", "x") // the resync retry
	fsc.sendConfirm(t, "subscribe", "x", 1)
	waitForConfirmation(t, a.Events(), "subscribe", "x")

	// Attribution behind the settled retry must be intact: each later
	// subscriber still receives exactly its own confirmation.
	b, err := m.Subscribe(ctx, "y")
	if err != nil {
		t.Fatalf("Subscribe b: %v", err)
	}
	fsc.expectCmd(t, "subscribe", "y")
	fsc.sendConfirm(t, "subscribe", "y", 2)
	waitForConfirmation(t, b.Events(), "subscribe", "y")

	c, err := m.Subscribe(ctx, "x")
	if err != nil {
		t.Fatalf("Subscribe c: %v", err)
	}
	fsc.expectCmd(t, "subscribe", "x")
	fsc.sendConfirm(t, "subscribe", "x", 2)
	waitForConfirmation(t, c.Events(), "subscribe", "x")

	// A surviving stale waiter would consume c's confirmation as the
	// retry's broadcast reply, duplicating it to the existing owner.
	fsc.sendMessage(t, "message", "x", "marker")
	if n := subsUntil(t, a.Events(), "marker"); n != 0 {
		t.Fatalf("existing owner saw %d confirmation(s) from a stale retry waiter, want 0", n)
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
