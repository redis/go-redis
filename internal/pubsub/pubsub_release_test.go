package pubsub

import (
	"context"
	"testing"
	"time"
)

// TestSharedNameUnsubscribeKeepsMessagesAhead pins the release of a name
// another handle keeps: no UNSUBSCRIBE is sent, but a barrier PING is,
// and a message the server sent before its pong still reaches the
// releasing handle, ahead of the unsubscribe confirmation.
func TestSharedNameUnsubscribeKeepsMessagesAhead(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	m := newTestManager(t, srv, testConfig("node:6379"))

	leaver, keeper, fsc := subscribeShared(t, m, srv, "x")

	if err := leaver.Unsubscribe(ctx, "x"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	fsc.expectCmd(t, "ping")
	// Published before the server read the PING.
	fsc.sendMessage(t, "message", "x", "ahead")
	fsc.pong([]string{"ping"})
	fsc.sendMessage(t, "message", "x", "after")

	if ev := recvEvent(t, leaver.Events()); !isMessage(ev, "ahead") {
		t.Fatalf("first event = %#v, want the message sent ahead of the barrier", ev)
	}
	if sub, ok := recvEvent(t, leaver.Events()).(*Subscription); !ok || sub.Kind != "unsubscribe" || sub.Count != 0 {
		t.Fatalf("second event = %#v, want the unsubscribe confirmation", sub)
	}
	for _, want := range []string{"ahead", "after"} {
		if ev := recvEvent(t, keeper.Events()); !isMessage(ev, want) {
			t.Fatalf("keeper got %#v, want %q", ev, want)
		}
	}
	select {
	case ev := <-leaver.Events():
		t.Fatalf("leaver got %#v after its unsubscribe confirmation", ev)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestSharedNameUnsubscribeSettlesOnDrop pins that a barrier lost with
// the connection still settles: the releaser gets its confirmation and
// the reconnect replays only the keeper's name.
func TestSharedNameUnsubscribeSettlesOnDrop(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	m := newTestManager(t, srv, testConfig("node:6379"))

	leaver, _, fsc := subscribeShared(t, m, srv, "x")

	if err := leaver.Unsubscribe(ctx, "x"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	fsc.expectCmd(t, "ping")
	_ = fsc.conn.Close()

	if sub, ok := recvEvent(t, leaver.Events()).(*Subscription); !ok || sub.Kind != "unsubscribe" {
		t.Fatalf("event = %#v, want the synthesized unsubscribe confirmation", sub)
	}
	fsc2 := srv.waitDial(t)
	fsc2.expectCmd(t, "subscribe", "x")
}

// subscribeShared subscribes two handles to name, one after the other,
// each confirmed.
func subscribeShared(t *testing.T, m *Manager, srv *fakeServer, name string) (first, second PubSuber, fsc *fakeServerConn) {
	t.Helper()
	ctx := context.Background()
	first, err := m.Subscribe(ctx, name)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc = srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", name)
	fsc.sendConfirm(t, "subscribe", name, 1)
	recvEvent(t, first.Events())

	second, err = m.Subscribe(ctx, name)
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc.expectCmd(t, "subscribe", name)
	fsc.sendConfirm(t, "subscribe", name, 1)
	recvEvent(t, second.Events())
	return first, second, fsc
}
