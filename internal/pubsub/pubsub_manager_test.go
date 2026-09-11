package pubsub

import (
	"context"
	"errors"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/hashtag"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

func TestManagerFanout(t *testing.T) {
	ctx := context.Background()

	t.Run("MessageToSubscribedHandle", func(t *testing.T) {
		srv := newFakeServer()
		m := newTestManager(t, srv, testConfig("node:6379"))

		h, err := m.Subscribe(ctx, "ch1")
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		ch := h.Channel()

		fsc := srv.waitDial(t)
		if fsc.addr != "node:6379" {
			t.Fatalf("dialed %q, want %q", fsc.addr, "node:6379")
		}
		fsc.expectCmd(t, "subscribe", "ch1")

		fsc.sendMessage(t, "message", "ch1", "hello")
		msg := recvMsg(t, ch)
		if msg.Channel != "ch1" || msg.Payload != "hello" {
			t.Fatalf("got %q on %q, want \"hello\" on \"ch1\"", msg.Payload, msg.Channel)
		}
	})

	t.Run("MessageToEveryHandleOnTheChannel", func(t *testing.T) {
		srv := newFakeServer()
		m := newTestManager(t, srv, testConfig("node:6379"))

		h1, err := m.Subscribe(ctx, "shared")
		if err != nil {
			t.Fatalf("Subscribe h1: %v", err)
		}
		h2, err := m.Subscribe(ctx, "shared")
		if err != nil {
			t.Fatalf("Subscribe h2: %v", err)
		}
		ch1, ch2 := h1.Channel(), h2.Channel()

		fsc := srv.waitDial(t)
		fsc.expectCmd(t, "subscribe", "shared")
		fsc.expectCmd(t, "subscribe", "shared") // re-sent, harmless

		fsc.sendMessage(t, "message", "shared", "both")
		if msg := recvMsg(t, ch1); msg.Payload != "both" {
			t.Fatalf("h1 got %q, want \"both\"", msg.Payload)
		}
		if msg := recvMsg(t, ch2); msg.Payload != "both" {
			t.Fatalf("h2 got %q, want \"both\"", msg.Payload)
		}
	})

	t.Run("PatternAndShardNamespacesAreSeparate", func(t *testing.T) {
		srv := newFakeServer()
		m := newTestManager(t, srv, testConfig("node:6379"))

		hp, err := m.PSubscribe(ctx, "news.*")
		if err != nil {
			t.Fatalf("PSubscribe: %v", err)
		}
		hs, err := m.SSubscribe(ctx, "news.tech")
		if err != nil {
			t.Fatalf("SSubscribe: %v", err)
		}
		chp, chs := hp.Channel(), hs.Channel()

		fsc := srv.waitDial(t)
		fsc.expectCmd(t, "psubscribe", "news.*")
		fsc.expectCmd(t, "ssubscribe", "news.tech")

		fsc.sendMessage(t, "pmessage", "news.*", "news.tech", "p-payload")
		if msg := recvMsg(t, chp); msg.Pattern != "news.*" || msg.Payload != "p-payload" {
			t.Fatalf("pattern handle got %+v, want pattern=news.* payload=p-payload", msg)
		}

		fsc.sendMessage(t, "smessage", "news.tech", "s-payload")
		if msg := recvMsg(t, chs); msg.Payload != "s-payload" {
			t.Fatalf("shard handle got %q, want \"s-payload\"", msg.Payload)
		}

		// The smessage must not reach the pattern handle and vice versa.
		select {
		case msg := <-chp:
			t.Fatalf("pattern handle received cross-namespace message %+v", msg)
		case <-time.After(50 * time.Millisecond):
		}
	})

	t.Run("ConfirmationsOnChannelWithSubscriptions", func(t *testing.T) {
		srv := newFakeServer()
		m := newTestManager(t, srv, testConfig("node:6379"))

		h, err := m.Subscribe(ctx, "conf")
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		events := h.ChannelWithSubscriptions()

		fsc := srv.waitDial(t)
		fsc.expectCmd(t, "subscribe", "conf")

		fsc.sendConfirm(t, "subscribe", "conf", 1)
		select {
		case ev := <-events:
			sub, ok := ev.(*Subscription)
			if !ok || sub.Kind != "subscribe" || sub.Channel != "conf" || sub.Count != 1 {
				t.Fatalf("event = %#v, want subscribe confirmation for conf", ev)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for the confirmation")
		}
	})

	t.Run("ChannelModesAreMutuallyExclusive", func(t *testing.T) {
		srv := newFakeServer()
		m := newTestManager(t, srv, testConfig("node:6379"))

		h, err := m.Subscribe(ctx, "modes")
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		srv.waitDial(t)
		h.Channel()

		defer func() {
			if recover() == nil {
				t.Fatal("ChannelWithSubscriptions after Channel did not panic")
			}
		}()
		h.ChannelWithSubscriptions()
	})
}

func TestManagerUnsubscribe(t *testing.T) {
	ctx := context.Background()

	srv := newFakeServer()
	m := newTestManager(t, srv, testConfig("node:6379"))

	h1, err := m.Subscribe(ctx, "shared")
	if err != nil {
		t.Fatalf("Subscribe h1: %v", err)
	}
	h2, err := m.Subscribe(ctx, "shared")
	if err != nil {
		t.Fatalf("Subscribe h2: %v", err)
	}
	if _, err := h2.Subscribe(ctx, "keep"); err != nil {
		t.Fatalf("Subscribe keep: %v", err)
	}
	ch2 := h2.Channel()

	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "shared")
	fsc.expectCmd(t, "subscribe", "shared")
	fsc.expectCmd(t, "subscribe", "keep")

	// h1 leaving "shared" must not unsubscribe it server-side: h2 still
	// listens — no command. And a handle owning nothing stays usable and
	// can subscribe again.
	if err := h1.Unsubscribe(ctx, "shared"); err != nil {
		t.Fatalf("h1 Unsubscribe: %v", err)
	}
	fsc.expectNoCmd(t, 100*time.Millisecond)
	if _, err := h1.Subscribe(ctx, "revived"); err != nil {
		t.Fatalf("Subscribe after drain: %v", err)
	}
	fsc.expectCmd(t, "subscribe", "revived")

	// The last owner leaving sends the server-side unsubscribe and the
	// removal is immediate: messages the server sent before processing
	// the UNSUBSCRIBE find no registered handle and are discarded, and
	// the confirmation fans out to nobody.
	if err := h2.Unsubscribe(ctx, "shared"); err != nil {
		t.Fatalf("h2 Unsubscribe shared: %v", err)
	}
	fsc.expectCmd(t, "unsubscribe", "shared")
	fsc.sendMessage(t, "message", "shared", "in-flight")
	fsc.sendConfirm(t, "unsubscribe", "shared", 1)
	fsc.sendMessage(t, "message", "shared", "late")
	select {
	case msg := <-ch2:
		t.Fatalf("message %q delivered after Unsubscribe returned", msg.Payload)
	case <-time.After(50 * time.Millisecond):
	}

	// Draining the last subscriptions quiesces the connection as soon as
	// the final UNSUBSCRIBE is written — no confirmations needed.
	if err := h1.Unsubscribe(ctx); err != nil {
		t.Fatalf("h1 Unsubscribe all: %v", err)
	}
	fsc.expectCmd(t, "unsubscribe", "revived")
	if err := h2.Unsubscribe(ctx, "keep"); err != nil {
		t.Fatalf("h2 Unsubscribe keep: %v", err)
	}
	fsc.expectCmd(t, "unsubscribe", "keep")
	fsc.expectClosed(t)

	// Only an explicit Close ends a handle.
	if err := h2.Close(); err != nil {
		t.Fatalf("h2 Close: %v", err)
	}
	if _, err := h2.Subscribe(ctx, "again"); !errors.Is(err, pool.ErrClosed) {
		t.Fatalf("Subscribe on closed handle = %v, want pool.ErrClosed", err)
	}
	expectChanClosed(t, ch2)

	// The next Subscribe redials lazily (un-parking the read loop).
	h3, err := m.Subscribe(ctx, "fresh")
	if err != nil {
		t.Fatalf("Subscribe after idle: %v", err)
	}
	ch3 := h3.Channel()
	fsc2 := srv.waitDial(t)
	fsc2.expectCmd(t, "subscribe", "fresh")
	fsc2.sendMessage(t, "message", "fresh", "hello")
	if msg := recvMsg(t, ch3); msg.Payload != "hello" {
		t.Fatalf("got %q, want \"hello\"", msg.Payload)
	}
}

func TestManagerReconnect(t *testing.T) {
	ctx := context.Background()

	t.Run("BrokenConnRedialsAndReplays", func(t *testing.T) {
		srv := newFakeServer()
		m := newTestManager(t, srv, testConfig("node:6379"))

		h, err := m.Subscribe(ctx, "ch1")
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		if _, err := h.PSubscribe(ctx, "pat.*"); err != nil {
			t.Fatalf("PSubscribe: %v", err)
		}
		ch := h.Channel()

		fsc1 := srv.waitDial(t)
		fsc1.expectCmd(t, "subscribe", "ch1")
		fsc1.expectCmd(t, "psubscribe", "pat.*")

		// Sever the connection: the read loop reconnects and replays
		// every registered subscription on the fresh conn.
		_ = fsc1.conn.Close()
		fsc2 := srv.waitDial(t)
		fsc2.expectCmd(t, "subscribe", "ch1")
		fsc2.expectCmd(t, "psubscribe", "pat.*")

		fsc2.sendMessage(t, "message", "ch1", "back")
		if msg := recvMsg(t, ch); msg.Payload != "back" {
			t.Fatalf("got %q, want \"back\"", msg.Payload)
		}
	})

	t.Run("ShardOnlyReconnectReplays", func(t *testing.T) {
		// The shard registry replays like the other two: a reconnect
		// with only SSUBSCRIBE subscriptions must restore them (#3806
		// was exactly this hole — a shard-only reconnect resubscribed
		// nothing).
		srv := newFakeServer()
		m := newTestManager(t, srv, testConfig("node:6379"))

		h, err := m.SSubscribe(ctx, "{a}ch")
		if err != nil {
			t.Fatalf("SSubscribe: %v", err)
		}
		ch := h.Channel()
		fsc1 := srv.waitDial(t)
		fsc1.expectCmd(t, "ssubscribe", "{a}ch")

		_ = fsc1.conn.Close()
		fsc2 := srv.waitDial(t)
		fsc2.expectCmd(t, "ssubscribe", "{a}ch")

		fsc2.sendMessage(t, "smessage", "{a}ch", "back")
		if msg := recvMsg(t, ch); msg.Payload != "back" {
			t.Fatalf("got %q, want \"back\"", msg.Payload)
		}
	})

	t.Run("DialFailureRetriesAndReportsViaHook", func(t *testing.T) {
		srv := newFakeServer()
		hookCalls := make(chan struct{}, 100)
		m := NewManager(
			testConfig("node:6379"),
			srv.dial,
			func(cn *pool.Conn) error { return cn.Close() },
			func(ctx context.Context, cn *pool.Conn, rd *proto.Reader) error { return nil },
			testIsBadConn,
			func() {
				select {
				case hookCalls <- struct{}{}:
				default:
				}
			},
		)
		t.Cleanup(func() { _ = m.Close() })

		if _, err := m.Subscribe(ctx, "ch1"); err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		fsc1 := srv.waitDial(t)
		fsc1.expectCmd(t, "subscribe", "ch1")

		// Sever the conn while dials fail: every failed reconnect attempt
		// fires the hook (the cluster wires it to a topology reload).
		srv.setDialErr(errors.New("node is down"))
		_ = fsc1.conn.Close()
		select {
		case <-hookCalls:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for the reconnect-failure hook")
		}

		// Once the node is back the loop converges and replays.
		srv.setDialErr(nil)
		fsc2 := srv.waitDial(t)
		fsc2.expectCmd(t, "subscribe", "ch1")
	})

	t.Run("ErrorReplyKeepsTheConn", func(t *testing.T) {
		srv := newFakeServer()
		m := newTestManager(t, srv, testConfig("node:6379"))

		h, err := m.Subscribe(ctx, "ch1")
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		ch := h.Channel()
		raw := m.NewHandle() // raw-events subscriber view
		fsc := srv.waitDial(t)
		fsc.expectCmd(t, "subscribe", "ch1")

		// A RESP error reply (e.g. NOPERM) is not treated as a broken
		// connection: no redial, the stream keeps working, and — since
		// it cannot be attributed to one subscriber — it is fanned out
		// to every handle's events stream, where Receive surfaces it.
		fsc.sendError(t, "NOPERM this user has no permissions")
		select {
		case fsc2 := <-srv.dialCh:
			t.Fatalf("unexpected reconnect to %q after an error reply", fsc2.addr)
		case <-time.After(100 * time.Millisecond):
		}
		select {
		case ev := <-raw.Events():
			err, ok := ev.(error)
			if !ok || !strings.Contains(err.Error(), "NOPERM") {
				t.Fatalf("raw event = %#v, want the NOPERM error reply", ev)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for the error reply on Events")
		}

		// The message channel view skips the error and keeps delivering.
		fsc.sendMessage(t, "message", "ch1", "still-alive")
		if msg := recvMsg(t, ch); msg.Payload != "still-alive" {
			t.Fatalf("got %q, want \"still-alive\"", msg.Payload)
		}
	})
}

// TestManagerUnsubscribeWriteFailureSelfHeals pins the failure path of
// an orphan unsubscribe: the removal happens before the fallible write,
// so a write error must drop the connection — the command may have
// partially reached the server, and a healthy conn would stay
// subscribed to names nobody owns while a retry would be a silent
// no-op. The next connect replays only what is still registered:
// exactly the state the caller asked for.
func TestManagerUnsubscribeWriteFailureSelfHeals(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	m := newTestManager(t, srv, testConfig("node:6379"))
	t.Cleanup(func() { _ = m.Close() })

	h, err := m.Subscribe(ctx, "ch1")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "ch1")
	fsc.sendConfirm(t, "subscribe", "ch1", 1)
	if sub, ok := recvEvent(t, h.Events()).(*Subscription); !ok || sub.Kind != "subscribe" {
		t.Fatalf("first event = %#v, want the subscribe confirmation", sub)
	}

	// Pause the server's read loop. The pause takes effect after the
	// pending read consumes one more frame, so a sacrificial ping arms
	// it; the orphan UNSUBSCRIBE write then blocks on the synchronous
	// pipe and hits the write timeout.
	fsc.paused.Store(true)
	if err := m.Ping(ctx); err != nil {
		t.Fatalf("arming ping: %v", err)
	}
	if err := h.Unsubscribe(ctx, "ch1"); err == nil {
		t.Fatal("Unsubscribe with a blocked write succeeded, want error")
	}
	fsc.paused.Store(false)

	// The synthesized confirmation was delivered before the failed
	// write.
	if sub, ok := recvEvent(t, h.Events()).(*Subscription); !ok ||
		sub.Kind != "unsubscribe" || sub.Channel != "ch1" || sub.Count != 0 {
		t.Fatalf("event = %#v, want unsubscribe ch1 count 0", sub)
	}

	// With nothing registered the manager stays disconnected (the read
	// loop parks idle); the next Subscribe redials, and the fresh
	// connection replays nothing beyond it — the handle is revivable.
	if _, err := h.Subscribe(ctx, "ch2"); err != nil {
		t.Fatalf("Subscribe after self-heal: %v", err)
	}
	fsc2 := srv.waitDial(t)
	fsc2.expectCmd(t, "subscribe", "ch2")
	fsc2.expectNoCmd(t, 50*time.Millisecond)
}

// TestManagerRedirectReplyRequestsReload pins the stale-topology
// reaction: a MOVED reply to a fire-and-forget SSUBSCRIBE means the
// routing view is stale — the manager requests a refresh (the re-route
// sweep then moves the already-registered channel) while the reply
// still fans out and the connection stays up.
func TestManagerRedirectReplyRequestsReload(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	reloads := make(chan struct{}, 16)
	m := newTestManagerReload(t, srv, testConfig("node-a:6379"), func() {
		select {
		case reloads <- struct{}{}:
		default:
		}
	})
	t.Cleanup(func() { _ = m.Close() })

	h, err := m.SSubscribe(ctx, "{a}ch")
	if err != nil {
		t.Fatalf("SSubscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "ssubscribe", "{a}ch")

	fsc.sendError(t, "MOVED 866 node-b:6379")
	select {
	case <-reloads:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the reload request")
	}
	if ev, ok := recvEvent(t, h.Events()).(error); !ok || !strings.Contains(ev.Error(), "MOVED") {
		t.Fatalf("event = %#v, want the MOVED error reply", ev)
	}
	select {
	case fsc2 := <-srv.dialCh:
		t.Fatalf("unexpected reconnect to %q after a MOVED reply", fsc2.addr)
	case <-time.After(100 * time.Millisecond):
	}
}

// TestManagerSubscribeSelfHeals pins the contract for subscribe-time
// failures: clients hand out the PubSub without checking
// the subscribe error, so the intent must survive a failed dial and be
// replayed by the read loop once the node is back.
func TestManagerSubscribeSelfHeals(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	m := newTestManager(t, srv, testConfig("node:6379"))
	t.Cleanup(func() { _ = m.Close() })

	h := m.NewHandle()
	ch := h.Channel()

	srv.setDialErr(errors.New("node is down"))
	if _, err := h.Subscribe(ctx, "ch1"); err == nil {
		t.Fatal("Subscribe with failing dial succeeded, want error")
	}

	// The node comes back: the reconnect loop replays the registered
	// intent and delivery starts without any further caller action.
	srv.setDialErr(nil)
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "ch1")
	fsc.sendConfirm(t, "subscribe", "ch1", 1)
	fsc.sendMessage(t, "message", "ch1", "healed")
	if msg := recvMsg(t, ch); msg.Payload != "healed" {
		t.Fatalf("got %q, want \"healed\"", msg.Payload)
	}
}

// TestManagerSubscribeRollsBackFreshHandle pins the counterpart: a
// handle created by the failing Manager.Subscribe call itself is
// returned as nil, so nobody could consume or Close it — its
// registration must be rolled back, leaving the manager idle.
func TestManagerSubscribeRollsBackFreshHandle(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	m := newTestManager(t, srv, testConfig("node:6379"))

	srv.setDialErr(errors.New("node is down"))
	if _, err := m.Subscribe(ctx, "ch1"); err == nil {
		t.Fatal("Subscribe with failing dial succeeded, want error")
	}

	m.mu.Lock()
	handles, subs := len(m.handles), len(m.subscribers)
	m.mu.Unlock()
	if handles != 0 || subs != 0 {
		t.Fatalf("after rolled-back Subscribe: %d handles, %d subscriptions registered, want 0, 0", handles, subs)
	}
	if !m.CloseIfIdle() {
		t.Fatal("manager not idle after rolled-back Subscribe")
	}
}

// TestManagerHandoff pins the maintenance-notification reaction: a
// connection marked for handoff (MOVING) makes the manager redirect to
// the handoff endpoint on the next read and replay the subscriptions
// there.
func TestManagerHandoff(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	m := newTestManager(t, srv, testConfig("old:6379"))

	h, err := m.Subscribe(ctx, "ch1")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	ch := h.Channel()

	fsc1 := srv.waitDial(t)
	fsc1.expectCmd(t, "subscribe", "ch1")

	// The MOVING push handler marks the conn; the manager reacts after
	// the next successful read (here: an unrelated confirmation frame).
	if err := fsc1.poolConn.MarkForHandoff("new:6379", 1); err != nil {
		t.Fatalf("MarkForHandoff: %v", err)
	}
	fsc1.sendConfirm(t, "subscribe", "ch1", 1)

	fsc2 := srv.waitDial(t)
	if fsc2.addr != "new:6379" {
		t.Fatalf("handoff redialed %q, want %q", fsc2.addr, "new:6379")
	}
	fsc2.expectCmd(t, "subscribe", "ch1")
	fsc1.expectClosed(t)

	// The redirect is sticky: a later reconnect also lands on the new
	// endpoint.
	_ = fsc2.conn.Close()
	fsc3 := srv.waitDial(t)
	if fsc3.addr != "new:6379" {
		t.Fatalf("post-handoff reconnect dialed %q, want %q", fsc3.addr, "new:6379")
	}
	fsc3.expectCmd(t, "subscribe", "ch1")

	fsc3.sendMessage(t, "message", "ch1", "moved")
	if msg := recvMsg(t, ch); msg.Payload != "moved" {
		t.Fatalf("got %q, want \"moved\"", msg.Payload)
	}
}

// TestManagerShardedSlotSplit pins that multi-channel SSUBSCRIBE writes
// are split into one command per hash slot: a cluster server rejects a
// command spanning slots with CROSSSLOT even when it owns every slot
// involved.
func TestManagerShardedSlotSplit(t *testing.T) {
	ctx := context.Background()

	if s1, s2 := hashtag.Slot("{a}one"), hashtag.Slot("{b}one"); s1 == s2 {
		t.Fatalf("test channels share slot %d; pick different tags", s1)
	}

	srv := newFakeServer()
	m := newTestManager(t, srv, testConfig("node:6379"))

	if _, err := m.SSubscribe(ctx, "{a}one", "{a}two", "{b}one"); err != nil {
		t.Fatalf("SSubscribe: %v", err)
	}
	fsc := srv.waitDial(t)

	// One command per slot, written in first-appearance order of the
	// channels: the server confirms in write order, so subscribers see
	// their confirmations in subscription order (pubsub_test.go's
	// sharded spec asserts it).
	fsc.expectCmd(t, "ssubscribe", "{a}one", "{a}two")
	fsc.expectCmd(t, "ssubscribe", "{b}one")
	fsc.expectNoCmd(t, 50*time.Millisecond)
}

func TestManagerPing(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	m := newTestManager(t, srv, testConfig("node:6379"))

	// Ping dials lazily.
	if err := m.Ping(ctx); err != nil {
		t.Fatalf("Ping without conn: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "ping")

	h, err := m.Subscribe(ctx, "ch1")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc.expectCmd(t, "subscribe", "ch1")

	if err := h.Ping(ctx, "payload"); err != nil {
		t.Fatalf("Ping with payload: %v", err)
	}
	fsc.expectCmd(t, "ping", "payload")

	// The pong reaches only the handle that pinged (on its raw Events
	// stream — Receive surfaces it; a subscriber never sees a pong it
	// didn't request).
	bystander := m.NewHandle().Events()
	fsc.sendMessage(t, "pong", "payload")
	if pong, ok := recvEvent(t, h.Events()).(*Pong); !ok || pong.Payload != "payload" {
		t.Fatalf("event = %#v, want *Pong with payload", pong)
	}
	select {
	case ev := <-bystander:
		t.Fatalf("pong delivered to a handle that did not ping: %#v", ev)
	case <-time.After(50 * time.Millisecond):
	}

	// A manager-level ping (the health check's) has no waiter: its pong
	// is consumed silently.
	if err := m.Ping(ctx); err != nil {
		t.Fatalf("manager Ping: %v", err)
	}
	fsc.expectCmd(t, "ping")
	fsc.sendMessage(t, "pong", "")
	select {
	case ev := <-h.Events():
		t.Fatalf("health pong delivered to a subscriber: %#v", ev)
	case ev := <-bystander:
		t.Fatalf("health pong delivered to a bystander: %#v", ev)
	case <-time.After(50 * time.Millisecond):
	}
}

// TestManagerSlowConsumerDrop pins the drop policy: with a handle's
// events buffer full and nobody consuming, further deliveries are
// dropped without stalling the fan-out. A second handle on another
// channel serves as an ordering fence: the single listen goroutine fans
// out FIFO, so receiving the fence message proves the burst fan-outs
// completed.
func TestManagerSlowConsumerDrop(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	cfg := testConfig("node:6379")
	cfg.ChanSize = 1 // events buffer size
	m := newTestManager(t, srv, cfg)

	slow, err := m.Subscribe(ctx, "slow")
	if err != nil {
		t.Fatalf("Subscribe slow: %v", err)
	}
	fence, err := m.Subscribe(ctx, "fence")
	if err != nil {
		t.Fatalf("Subscribe fence: %v", err)
	}
	fenceCh := fence.Channel()

	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "slow")
	fsc.expectCmd(t, "subscribe", "fence")

	// slow has no consumer yet: m1 fills its 1-slot events buffer, m2
	// and m3 are dropped.
	fsc.sendMessage(t, "message", "slow", "m1")
	fsc.sendMessage(t, "message", "slow", "m2")
	fsc.sendMessage(t, "message", "slow", "m3")
	fsc.sendMessage(t, "message", "fence", "done")

	if msg := recvMsg(t, fenceCh); msg.Payload != "done" {
		t.Fatalf("fence got %q, want \"done\"", msg.Payload)
	}

	// The consumer catches up: the buffered message arrives, the dropped
	// ones are gone, and the stream continues.
	slowCh := slow.Channel()
	if msg := recvMsg(t, slowCh); msg.Payload != "m1" {
		t.Fatalf("buffered message = %q, want \"m1\"", msg.Payload)
	}
	fsc.sendMessage(t, "message", "slow", "m4")
	if msg := recvMsg(t, slowCh); msg.Payload != "m4" {
		t.Fatalf("got %q after drops, want \"m4\"", msg.Payload)
	}
}

// TestManagerHealthCheck pins the health checker: a silent connection
// gets a PING after HealthCheckInterval, and a failing ping triggers a
// reconnect with the subscription replay.
func TestManagerHealthCheck(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	cfg := testConfig("node:6379")
	cfg.HealthCheckInterval = 20 * time.Millisecond
	cfg.WriteTimeout = 100 * time.Millisecond
	m := newTestManager(t, srv, cfg)

	if _, err := m.Subscribe(ctx, "hc"); err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc1 := srv.waitDial(t)
	fsc1.expectCmd(t, "subscribe", "hc")

	// The idle connection is pinged after the interval.
	fsc1.expectCmd(t, "ping")

	// Pausing the server's reads leaves the pipe unread, so the next
	// health-check ping write hits its deadline; the failed ping makes
	// the health checker reconnect and replay the subscription.
	fsc1.paused.Store(true)
	fsc2 := srv.waitDial(t)
	fsc2.expectCmd(t, "subscribe", "hc")
	fsc2.expectCmd(t, "ping")

	// The replaced conn was closed by the reconnect; resume its read
	// loop so it observes the close.
	fsc1.paused.Store(false)
	fsc1.expectClosed(t)
}

// TestHealthCheckRuntimeUpdates pins that UpdateHealthCheckInterval
// (WithChannelHealthCheckInterval) applies at runtime: a positive value
// revives a disabled checker, re-arms a running one immediately, and
// <= 0 parks it.
func TestHealthCheckRuntimeUpdates(t *testing.T) {
	ctx := context.Background()

	// setup subscribes one handle and hands back the handle plus the
	// server conn, with the subscribe command already consumed.
	setup := func(t *testing.T, interval time.Duration) (PubSuber, *fakeServerConn) {
		t.Helper()
		srv := newFakeServer()
		cfg := testConfig("node:6379")
		cfg.HealthCheckInterval = interval
		m := newTestManager(t, srv, cfg)

		h, err := m.Subscribe(ctx, "hc")
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		fsc := srv.waitDial(t)
		fsc.expectCmd(t, "subscribe", "hc")
		// Confirm like a real server would: an unconfirmed name stays
		// Pending and the health checker re-sends it (see
		// resubscribePending), which would interleave with the exact
		// command sequences asserted below.
		fsc.sendConfirm(t, "subscribe", "hc", 1)
		return h, fsc
	}

	// expectQuiet drains straggler pings (a tick already fired when the
	// update landed) and then requires 300ms of silence.
	expectQuiet := func(t *testing.T, fsc *fakeServerConn) {
		t.Helper()
		deadline := time.After(5 * time.Second)
		for {
			select {
			case cmd := <-fsc.cmds:
				if len(cmd) == 0 || cmd[0] != "ping" {
					t.Fatalf("unexpected command %v", cmd)
				}
			case <-time.After(300 * time.Millisecond):
				return
			case <-deadline:
				t.Fatal("health checker kept pinging after being disabled")
			}
		}
	}

	t.Run("ReviveDisabled", func(t *testing.T) {
		h, fsc := setup(t, -1) // health check disabled: the checker parks
		h.Channel(func(c PubSubConfiger) {
			c.UpdateHealthCheckInterval(30 * time.Millisecond)
		})
		fsc.expectCmd(t, "ping")
	})

	t.Run("RetuneRunning", func(t *testing.T) {
		// Armed for an hour — only the cfgChanged wake can re-arm it in
		// time for the expectCmd's window.
		h, fsc := setup(t, time.Hour)
		h.Channel(func(c PubSubConfiger) {
			c.UpdateHealthCheckInterval(30 * time.Millisecond)
		})
		fsc.expectCmd(t, "ping")
	})

	t.Run("DisableRunning", func(t *testing.T) {
		h, fsc := setup(t, 20*time.Millisecond)
		fsc.expectCmd(t, "ping") // the checker is live
		h.Channel(func(c PubSubConfiger) {
			c.UpdateHealthCheckInterval(-1)
		})
		expectQuiet(t, fsc)
	})
}

// TestPingTimeoutAppliesAtRuntime pins that UpdatePingTimeout
// (WithChannelPingTimeout) bounds the health-check ping: with the pipe
// parked, only the ping's context deadline can fail the blocked write,
// and the failed ping makes the checker reconnect. Both the configured
// PingTimeout and the WriteTimeout are far beyond the test's 5s
// patience, so a reconnect dial within it proves the runtime update took
// effect.
func TestPingTimeoutAppliesAtRuntime(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	cfg := testConfig("node:6379")
	cfg.HealthCheckInterval = 20 * time.Millisecond
	cfg.WriteTimeout = 30 * time.Second
	cfg.PingTimeout = 30 * time.Second
	m := newTestManager(t, srv, cfg)

	h, err := m.Subscribe(ctx, "pt")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc1 := srv.waitDial(t)
	fsc1.expectCmd(t, "subscribe", "pt")
	// Confirm like a real server would, so the health checker's Pending
	// reconciliation has nothing to re-send here.
	fsc1.sendConfirm(t, "subscribe", "pt", 1)

	h.Channel(func(c PubSubConfiger) {
		c.UpdatePingTimeout(50 * time.Millisecond)
	})

	// Park the pipe: the next ping write blocks until its context — the
	// updated PingTimeout — expires; the checker then reconnects and
	// replays the subscription.
	fsc1.paused.Store(true)
	fsc2 := srv.waitDial(t)
	fsc2.expectCmd(t, "subscribe", "pt")

	// The replaced conn was closed by the reconnect; resume its read
	// loop so it observes the close.
	fsc1.paused.Store(false)
	fsc1.expectClosed(t)
}

// TestReconnectTimeoutAppliesAtRuntime pins that UpdateReconnectTimeout
// (WithChannelReconnectTimeout) bounds the health checker's re-dial:
// with the dial gate shut, only the reconnect context's deadline can
// fail the dial, and the failed reconnect fires the onReconnectFailure
// hook. The configured 30s ReconnectTimeout is far beyond the test's 5s
// patience, so the hook firing within it proves the runtime update took
// effect.
func TestReconnectTimeoutAppliesAtRuntime(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	cfg := testConfig("node:6379")
	cfg.HealthCheckInterval = 20 * time.Millisecond
	cfg.PingTimeout = 50 * time.Millisecond
	cfg.WriteTimeout = 30 * time.Second
	cfg.ReconnectTimeout = 30 * time.Second

	hookFired := make(chan struct{}, 16)
	m := newTestManagerReload(t, srv, cfg, func() {
		select {
		case hookFired <- struct{}{}:
		default:
		}
	})

	h, err := m.Subscribe(ctx, "rt")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc1 := srv.waitDial(t)
	fsc1.expectCmd(t, "subscribe", "rt")
	// Confirm like a real server would, so the health checker's Pending
	// reconciliation has nothing to re-send here.
	fsc1.sendConfirm(t, "subscribe", "rt", 1)

	h.Channel(func(c PubSubConfiger) {
		c.UpdateReconnectTimeout(50 * time.Millisecond)
	})

	// Gate future dials shut, then park the pipe: the next health-check
	// ping write fails (PingTimeout), the checker reconnects, and its
	// dial parks at the gate until the reconnect context — the updated
	// ReconnectTimeout — expires.
	gate := make(chan struct{})
	srv.setDialGate(gate)
	// Release every parked dial on the way out: the read loop's own
	// deadline-free reconnect parks at the gate holding the manager
	// lock, which the cleanup's Close needs.
	defer close(gate)

	fsc1.paused.Store(true)
	defer fsc1.paused.Store(false)

	srv.waitGateHit(t)
	select {
	case <-hookFired:
		// The gated dial was aborted by the reconnect deadline.
	case <-time.After(5 * time.Second):
		t.Fatal("reconnect not bounded by the updated ReconnectTimeout")
	}
}

// TestManagerLevelUnsubscribe pins the manager-level (all handles)
// unsubscribe variants and the reconnect-failure hook setter.
func TestManagerLevelUnsubscribe(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	hookCalls := make(chan struct{}, 100)
	m := newTestManagerReload(t, srv, testConfig("node:6379"), func() {
		select {
		case hookCalls <- struct{}{}:
		default:
		}
	})

	h1, err := m.Subscribe(ctx, "x")
	if err != nil {
		t.Fatalf("Subscribe h1: %v", err)
	}
	ch1 := h1.Channel()
	h2, err := m.Subscribe(ctx, "x")
	if err != nil {
		t.Fatalf("Subscribe h2: %v", err)
	}
	if _, err := h2.Subscribe(ctx, "y"); err != nil {
		t.Fatalf("Subscribe y: %v", err)
	}
	if _, err := h2.PSubscribe(ctx, "p.*"); err != nil {
		t.Fatalf("PSubscribe: %v", err)
	}
	if _, err := h2.SSubscribe(ctx, "s1"); err != nil {
		t.Fatalf("SSubscribe: %v", err)
	}

	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "x")
	fsc.expectCmd(t, "subscribe", "x")
	fsc.expectCmd(t, "subscribe", "y")
	fsc.expectCmd(t, "psubscribe", "p.*")
	fsc.expectCmd(t, "ssubscribe", "s1")

	// Manager-level Unsubscribe removes the channel from every handle:
	// one server-side UNSUBSCRIBE for the last live owner (h1 leaves
	// synthetically, its channel stays open).
	if err := m.Unsubscribe(ctx, "x"); err != nil {
		t.Fatalf("Unsubscribe: %v", err)
	}
	fsc.expectCmd(t, "unsubscribe", "x")
	fsc.sendConfirm(t, "unsubscribe", "x", 3)
	select {
	case _, ok := <-ch1:
		if !ok {
			t.Fatal("h1 channel closed by an unsubscribe (only Close ends it)")
		}
	case <-time.After(50 * time.Millisecond):
	}

	// No names means every pattern of every handle.
	if err := m.PUnsubscribe(ctx); err != nil {
		t.Fatalf("PUnsubscribe: %v", err)
	}
	fsc.expectCmd(t, "punsubscribe", "p.*")
	fsc.sendConfirm(t, "punsubscribe", "p.*", 2)

	if err := m.SUnsubscribe(ctx, "s1"); err != nil {
		t.Fatalf("SUnsubscribe: %v", err)
	}
	fsc.expectCmd(t, "sunsubscribe", "s1")
	fsc.sendConfirm(t, "sunsubscribe", "s1", 1)

	// h2 still owns "y", so the connection stays up and the
	// reconnect-failure hook fires when it breaks while dials fail.
	srv.setDialErr(errors.New("node is down"))
	_ = fsc.conn.Close()
	select {
	case <-hookCalls:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for the reconnect-failure hook")
	}
	srv.setDialErr(nil)
}

// TestManagerPumpExitsOnClose pins the pump lifecycle: a pump parked on
// a send to an abandoned consumer channel must exit when the handle
// closes — nobody will ever drain the channel, so without the done
// signal the goroutine (and its backlog) would leak per closed PubSub.
func TestManagerPumpExitsOnClose(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newTestManager(t, srv, testConfig("node:6379"))
	t.Cleanup(func() { _ = m.Close() })

	h, err := m.Subscribe(ctx, "ch1")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "ch1")

	ch := h.Channel(func(c PubSubConfiger) { c.UpdateChannelSize(1) })

	// One message fills the consumer buffer, the next parks the pump on
	// the blocking send; the consumer never reads.
	for range 3 {
		fsc.sendMessage(t, "message", "ch1", "unread")
	}
	waitUntil(t, "the pump to park on the full channel", func() bool {
		return len(ch) == 1 && pumpGoroutines() >= 1
	})

	if err := h.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	waitUntil(t, "the pump goroutine to exit", func() bool {
		return pumpGoroutines() == 0
	})
}

// pumpGoroutines counts live consumer-pump goroutines in a full stack
// dump — exact, unlike a runtime.NumGoroutine baseline, which drifts
// with unrelated goroutines winding down from other tests.
func pumpGoroutines() int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	return strings.Count(string(buf[:n]), "internal/pubsub.pump[")
}

// waitUntil polls cond until it holds or a 5s deadline expires.
func waitUntil(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestManagerClosedGuards pins that Ping and ClientSetName refuse to
// run on a closed manager or a closed handle — dialing after Close
// would create a connection nothing can ever close again.
func TestManagerClosedGuards(t *testing.T) {
	ctx := context.Background()

	t.Run("ManagerClosed", func(t *testing.T) {
		srv := newFakeServer()
		m := newTestManager(t, srv, testConfig("node:6379"))

		if err := m.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := m.Ping(ctx); !errors.Is(err, pool.ErrClosed) {
			t.Fatalf("Ping after Close = %v, want pool.ErrClosed", err)
		}
		if err := m.ClientSetName(ctx, "name"); !errors.Is(err, pool.ErrClosed) {
			t.Fatalf("ClientSetName after Close = %v, want pool.ErrClosed", err)
		}
		select {
		case fsc := <-srv.dialCh:
			t.Fatalf("a dial reached the server after Close (addr %s)", fsc.addr)
		default:
		}
	})

	t.Run("HandleClosed", func(t *testing.T) {
		srv := newFakeServer()
		m := newTestManager(t, srv, testConfig("node:6379"))
		t.Cleanup(func() { _ = m.Close() })

		h := m.NewHandle()
		if err := h.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		if err := h.Ping(ctx); !errors.Is(err, pool.ErrClosed) {
			t.Fatalf("Ping on closed handle = %v, want pool.ErrClosed", err)
		}
		if err := h.ClientSetName(ctx, "name"); !errors.Is(err, pool.ErrClosed) {
			t.Fatalf("ClientSetName on closed handle = %v, want pool.ErrClosed", err)
		}
	})
}

func TestManagerClose(t *testing.T) {
	ctx := context.Background()

	t.Run("CloseTearsEverythingDown", func(t *testing.T) {
		srv := newFakeServer()
		m := newTestManager(t, srv, testConfig("node:6379"))

		h, err := m.Subscribe(ctx, "ch1")
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		ch := h.Channel()
		fsc := srv.waitDial(t)
		fsc.expectCmd(t, "subscribe", "ch1")

		if err := m.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		fsc.expectClosed(t)
		expectChanClosed(t, ch)

		if _, err := m.Subscribe(ctx, "again"); !errors.Is(err, pool.ErrClosed) {
			t.Fatalf("Subscribe after Close = %v, want pool.ErrClosed", err)
		}
		if err := m.Close(); !errors.Is(err, pool.ErrClosed) {
			t.Fatalf("second Close = %v, want pool.ErrClosed", err)
		}
	})

	t.Run("CloseIfIdle", func(t *testing.T) {
		srv := newFakeServer()
		srv.autoConfirm = true
		m := newTestManager(t, srv, testConfig("node:6379"))

		h, err := m.Subscribe(ctx, "ch1")
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		srv.waitDial(t)

		if m.CloseIfIdle() {
			t.Fatal("CloseIfIdle closed a manager with live subscriptions")
		}
		if err := h.Close(); err != nil {
			t.Fatalf("handle Close: %v", err)
		}
		// The close wrote an UNSUBSCRIBE whose confirmation completes
		// the deregistration asynchronously; idle follows.
		deadline := time.Now().Add(5 * time.Second)
		for !m.CloseIfIdle() {
			if time.Now().After(deadline) {
				t.Fatal("CloseIfIdle never closed the drained manager")
			}
			time.Sleep(5 * time.Millisecond)
		}
		if _, err := m.Subscribe(ctx, "again"); !errors.Is(err, pool.ErrClosed) {
			t.Fatalf("Subscribe after CloseIfIdle = %v, want pool.ErrClosed", err)
		}
	})
}
