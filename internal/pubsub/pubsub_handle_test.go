package pubsub

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

// messageFrame builds a message frame without a testing.T, so flooder
// goroutines can inject it via writeRaw.
func messageFrame(channel, payload string) string {
	return fmt.Sprintf("*3\r\n$7\r\nmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
		len(channel), channel, len(payload), payload)
}

// drainCmds discards every command the manager writes on fsc, so a test
// issuing more commands than the cmds buffer holds never stalls the
// fake server's read loop.
func drainCmds(fsc *fakeServerConn) {
	go func() {
		for {
			select {
			case <-fsc.cmds:
			case <-fsc.done:
				return
			}
		}
	}()
}

// TestHandleLocking interleaves the handle's entry points with the
// manager's fan-out and teardown. The handle's own mu guards only the
// consumer view (pump, msgCh, allCh); closed and the subscription sets
// stay under the manager's mu — these tests exist to fail under -race
// (or panic on send-to-closed / concurrent map access) if that split
// regresses.
func TestHandleLocking(t *testing.T) {
	ctx := context.Background()

	t.Run("CloseRacesFanout", func(t *testing.T) {
		srv := newFakeServer()
		srv.autoConfirm = true
		m := newTestManager(t, srv, testConfig("node:6379"))

		// keeper shares the channel: h.Close then unsubscribes nothing
		// server-side, and the fan-out stays busy after the close.
		keeper, err := m.Subscribe(ctx, "busy")
		if err != nil {
			t.Fatalf("Subscribe keeper: %v", err)
		}
		keepCh := keeper.Channel()

		h, err := m.Subscribe(ctx, "busy")
		if err != nil {
			t.Fatalf("Subscribe h: %v", err)
		}
		hCh := h.Channel()

		fsc := srv.waitDial(t)
		fsc.expectCmd(t, "subscribe", "busy")
		fsc.expectCmd(t, "subscribe", "busy")

		// Drain keeper in the background, watching for the post-close
		// sentinel; the range ends when the manager's cleanup closes it.
		gotSentinel := make(chan struct{})
		go func() {
			for msg := range keepCh {
				if msg.Payload == "after-close" {
					close(gotSentinel)
					return
				}
			}
		}()

		// Flood deliveries while the handle closes: the listen loop's
		// deliver into h races closeLocked ending the stream.
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			frame := messageFrame("busy", "m")
			for range 500 {
				fsc.writeRaw(frame)
			}
		}()

		recvMsg(t, hCh) // the handle is live and receiving when Close hits
		if err := h.Close(); err != nil {
			t.Fatalf("Close: %v", err)
		}
		expectChanClosed(t, hCh)
		wg.Wait()

		// The surviving handle still receives. The sentinel is re-sent
		// until it lands: while the flood backlog drains, a full events
		// buffer may legitimately drop it.
		deadline := time.After(5 * time.Second)
		for {
			fsc.writeRaw(messageFrame("busy", "after-close"))
			select {
			case <-gotSentinel:
				return
			case <-deadline:
				t.Fatal("timed out waiting for the surviving handle to receive")
			case <-time.After(20 * time.Millisecond):
			}
		}
	})

	t.Run("SubscriptionsRaceRegistryUpdates", func(t *testing.T) {
		// No autoConfirm: each churn command would produce a confirmation
		// frame, and the burst can overflow the fake server's write buffer
		// while the churn holds the manager lock — stalling the test into
		// write-timeout reconnects. The races under test (Subscriptions
		// vs. the registry updates) don't involve confirmations.
		srv := newFakeServer()
		m := newTestManager(t, srv, testConfig("node:6379"))

		h, err := m.Subscribe(ctx, "base")
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		drainCmds(srv.waitDial(t))

		// One goroutine churns the handle's subscription sets (mutated
		// under the manager's mu) while the main goroutine reads them
		// through Subscriptions.
		var churnErr error
		done := make(chan struct{})
		go func() {
			defer close(done)
			for i := range 100 {
				name := fmt.Sprintf("ch-%d", i)
				if _, churnErr = h.Subscribe(ctx, name); churnErr != nil {
					return
				}
				if churnErr = h.Unsubscribe(ctx, name); churnErr != nil {
					return
				}
				if _, churnErr = h.PSubscribe(ctx, name+".*"); churnErr != nil {
					return
				}
				if churnErr = h.PUnsubscribe(ctx, name+".*"); churnErr != nil {
					return
				}
			}
		}()

	loop:
		for {
			channels, _, _ := h.Subscriptions()
			if !slices.Contains(channels, "base") {
				t.Fatal("base channel missing from Subscriptions during churn")
			}
			select {
			case <-done:
				break loop
			default:
			}
		}
		if churnErr != nil {
			t.Fatalf("churn: %v", churnErr)
		}

		// The sets are updated synchronously with the calls, so after the
		// churn only the initial subscription remains.
		channels, patterns, schannels := h.Subscriptions()
		if len(channels) != 1 || channels[0] != "base" || len(patterns) != 0 || len(schannels) != 0 {
			t.Fatalf("Subscriptions = %v, %v, %v; want [base], [], []", channels, patterns, schannels)
		}
	})

	t.Run("HandleCloseRacesManagerClose", func(t *testing.T) {
		for range 30 {
			srv := newFakeServer()
			srv.autoConfirm = true
			m := newTestManager(t, srv, testConfig("node:6379"))

			h, err := m.Subscribe(ctx, "x")
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			ch := h.Channel()
			srv.waitDial(t)

			// Both teardowns run closeLocked on the same handle; whichever
			// interleaving wins, the streams must end exactly once.
			var wg sync.WaitGroup
			wg.Add(2)
			go func() { defer wg.Done(); _ = h.Close() }()
			go func() { defer wg.Done(); _ = m.Close() }()
			wg.Wait()

			expectChanClosed(t, ch)
			if err := m.Close(); !errors.Is(err, pool.ErrClosed) {
				t.Fatalf("second manager Close = %v, want pool.ErrClosed", err)
			}
		}
	})

	t.Run("ConcurrentChannelCallsShareOnePump", func(t *testing.T) {
		srv := newFakeServer()
		srv.autoConfirm = true
		m := newTestManager(t, srv, testConfig("node:6379"))

		h, err := m.Subscribe(ctx, "cc")
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		fsc := srv.waitDial(t)
		fsc.expectCmd(t, "subscribe", "cc")

		const n = 8
		chs := make([]<-chan *Message, n)
		start := make(chan struct{})
		var wg sync.WaitGroup
		for i := range n {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				chs[i] = h.Channel()
			}()
		}
		close(start)
		wg.Wait()

		for i := 1; i < n; i++ {
			if chs[i] != chs[0] {
				t.Fatalf("Channel call %d returned a different channel", i)
			}
		}

		fsc.sendMessage(t, "message", "cc", "hello")
		if msg := recvMsg(t, chs[0]); msg.Payload != "hello" {
			t.Fatalf("got %q, want \"hello\"", msg.Payload)
		}
	})

	t.Run("SendTimeoutDropsForSlowConsumer", func(t *testing.T) {
		srv := newFakeServer()
		srv.autoConfirm = true
		m := newTestManager(t, srv, testConfig("node:6379"))

		h, err := m.Subscribe(ctx, "st")
		if err != nil {
			t.Fatalf("Subscribe: %v", err)
		}
		ch := h.Channel(func(c PubSubConfiger) {
			c.UpdateChannelSize(1)
			c.UpdateSendTimeout(25 * time.Millisecond)
		})
		if cap(ch) != 1 {
			t.Fatalf("channel cap = %d, want 1 (UpdateChannelSize)", cap(ch))
		}

		fsc := srv.waitDial(t)
		fsc.expectCmd(t, "subscribe", "st")

		// Two rounds: the second proves the timeout keeps working after a
		// drop (a one-shot timer would park the pump forever from then on).
		for round := 1; round <= 2; round++ {
			kept := fmt.Sprintf("kept-%d", round)
			// kept fills the 1-slot buffer; the next message parks the pump,
			// whose send then times out and is dropped.
			fsc.sendMessage(t, "message", "st", kept)
			fsc.sendMessage(t, "message", "st", fmt.Sprintf("dropped-%d", round))
			time.Sleep(500 * time.Millisecond)

			if msg := recvMsg(t, ch); msg.Payload != kept {
				t.Fatalf("round %d: got %q, want %q", round, msg.Payload, kept)
			}
			// The buffer is free again: the next message must arrive — and be
			// this one, proving the parked message was dropped, not delayed.
			after := fmt.Sprintf("after-%d", round)
			fsc.sendMessage(t, "message", "st", after)
			if msg := recvMsg(t, ch); msg.Payload != after {
				t.Fatalf("round %d: got %q, want %q", round, msg.Payload, after)
			}
		}
	})

	t.Run("ChannelRacesClose", func(t *testing.T) {
		for range 20 {
			srv := newFakeServer()
			srv.autoConfirm = true
			m := newTestManager(t, srv, testConfig("node:6379"))

			h, err := m.Subscribe(ctx, "race")
			if err != nil {
				t.Fatalf("Subscribe: %v", err)
			}
			srv.waitDial(t)

			// Channel takes h.mu, Close takes the manager's mu — they never
			// nest, so whichever order lands the pump must terminate and
			// close its channel.
			var ch <-chan *Message
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				ch = h.Channel()
			}()
			_ = h.Close()
			wg.Wait()

			expectChanClosed(t, ch)
		}
	})
}

// TestChannelOptionConfigUpdates pins the ChannelOption write path into
// the manager config: on the first Channel call every update applies
// (the pump hasn't started when options run); on later calls the
// size/send-timeout updates are gated — their Channel call can no
// longer be affected — while the health-check settings still go
// through.
func TestChannelOptionConfigUpdates(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newTestManager(t, srv, testConfig("node:6379"))

	h, err := m.Subscribe(ctx, "cfg")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}

	snapshot := func() Config {
		m.mu.RLock()
		defer m.mu.RUnlock()
		return m.cfg
	}

	h.Channel(func(c PubSubConfiger) {
		c.UpdateChannelSize(5)
		c.UpdateSendTimeout(time.Second)
		c.UpdatePingTimeout(2 * time.Second)
		c.UpdateReconnectTimeout(3 * time.Second)
	})
	cfg := snapshot()
	if cfg.ChanSize != 5 || cfg.SendTimeout != time.Second ||
		cfg.PingTimeout != 2*time.Second || cfg.ReconnectTimeout != 3*time.Second {
		t.Fatalf("first-call updates: cfg = %+v", cfg)
	}

	h.Channel(func(c PubSubConfiger) {
		c.UpdateChannelSize(999)
		c.UpdateSendTimeout(999 * time.Second)
		c.UpdatePingTimeout(4 * time.Second)
		c.UpdateReconnectTimeout(5 * time.Second)
	})
	cfg = snapshot()
	if cfg.ChanSize != 5 || cfg.SendTimeout != time.Second {
		t.Fatalf("size/send-timeout not gated after the pump started: cfg = %+v", cfg)
	}
	if cfg.PingTimeout != 4*time.Second || cfg.ReconnectTimeout != 5*time.Second {
		t.Fatalf("ungated updates skipped on a later call: cfg = %+v", cfg)
	}
}

// TestChannelSizeAppliesManagerWide pins the documented WithChannelSize
// scope: the size belongs to the shared manager, so handles and
// consumer views created after the update use it, while existing
// streams keep the size they were created with.
func TestChannelSizeAppliesManagerWide(t *testing.T) {
	ctx := context.Background()
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newTestManager(t, srv, testConfig("node:6379")) // ChanSize 16

	h1, err := m.Subscribe(ctx, "wide")
	if err != nil {
		t.Fatalf("Subscribe: %v", err)
	}
	ch1 := h1.Channel(func(c PubSubConfiger) { c.UpdateChannelSize(5) })

	// h1's consumer channel snapshotted the updated size; its events
	// stream predates the update and keeps the original.
	if cap(ch1) != 5 {
		t.Fatalf("updater's channel cap = %d, want 5", cap(ch1))
	}
	if cap(h1.Events()) != 16 {
		t.Fatalf("pre-update events cap = %d, want 16", cap(h1.Events()))
	}

	// A handle created after the update — and its view — use the new
	// size, whether it comes from NewHandle or Subscribe.
	h2 := m.NewHandle()
	if cap(h2.Events()) != 5 {
		t.Fatalf("new handle events cap = %d, want 5", cap(h2.Events()))
	}
	if got := cap(h2.Channel()); got != 5 {
		t.Fatalf("new handle channel cap = %d, want 5", got)
	}

	h3, err := m.Subscribe(ctx, "wide2")
	if err != nil {
		t.Fatalf("Subscribe h3: %v", err)
	}
	if cap(h3.Events()) != 5 {
		t.Fatalf("subscribed handle events cap = %d, want 5", cap(h3.Events()))
	}
}
