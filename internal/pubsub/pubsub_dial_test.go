package pubsub

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

// subscribeResult is what an asynchronous Manager.Subscribe returned.
type subscribeResult struct {
	h   PubSuber
	err error
}

// subscribeAsync runs m.Subscribe in a goroutine, reporting its result.
func subscribeAsync(m *Manager, channels ...string) <-chan subscribeResult {
	res := make(chan subscribeResult, 1)
	go func() {
		h, err := m.Subscribe(context.Background(), channels...)
		res <- subscribeResult{h: h, err: err}
	}()
	return res
}

func awaitSubscribe(t *testing.T, res <-chan subscribeResult) subscribeResult {
	t.Helper()
	select {
	case r := <-res:
		return r
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for Subscribe to return")
		return subscribeResult{}
	}
}

// expectNoGateHit asserts that no further dial reaches the gate within d.
func (s *fakeServer) expectNoGateHit(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case <-s.gateHit:
		t.Fatal("a second dial reached the gate; dials must be single-flight")
	case <-time.After(d):
	}
}

// TestDialReleasesManagerLock pins that a dial runs with the state lock
// released: while one subscriber's dial is parked, everything that takes
// only the state lock — a new handle, Subscriptions, CloseIfIdle — keeps
// working instead of waiting out the dial. Writers, a handle's Close
// among them, hold the I/O slot and therefore do wait; one is started
// during the dial and must complete once the dial ends.
func TestDialReleasesManagerLock(t *testing.T) {
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newTestManager(t, srv, testConfig("node:6379"))

	gate := make(chan struct{})
	srv.setDialGate(gate)

	res := subscribeAsync(m, "a")
	srv.waitGateHit(t)

	// State-lock-only operations must not wait for the parked dial.
	var h PubSuber
	done := make(chan struct{})
	go func() {
		defer close(done)
		h = m.NewHandle()
		if channels, _, _ := h.Subscriptions(); len(channels) != 0 {
			t.Errorf("fresh handle owns %v, want nothing", channels)
		}
		// Not idle: the dialing subscriber's handle is registered.
		if m.CloseIfIdle() {
			t.Error("CloseIfIdle closed a manager with a subscribe in flight")
		}
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("state-lock operations blocked behind the in-flight dial")
	}

	// A writer queued behind the dial completes once the dial ends.
	closed := make(chan error, 1)
	go func() { closed <- h.Close() }()

	// Release the dial: the subscribe completes on the dialed connection,
	// then the queued Close runs.
	close(gate)
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "a")
	if r := awaitSubscribe(t, res); r.err != nil {
		t.Fatalf("Subscribe: %v", r.err)
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("handle Close queued behind the dial: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("handle Close never completed after the dial ended")
	}
}

// TestCloseDuringDialDoesNotWait pins that Close neither waits for an
// in-flight dial nor leaks its connection: Close returns while the dial
// is parked, the subscriber gets pool.ErrClosed once the dial ends, and
// the connection the dial produced is closed by its owner.
func TestCloseDuringDialDoesNotWait(t *testing.T) {
	srv := newFakeServer()
	m := newTestManager(t, srv, testConfig("node:6379"))

	gate := make(chan struct{})
	srv.setDialGate(gate)

	res := subscribeAsync(m, "a")
	srv.waitGateHit(t)

	closed := make(chan error, 1)
	go func() { closed <- m.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close waited for the in-flight dial")
	}

	// The dial still produces a connection; finding the manager closed,
	// its owner must close it rather than install it.
	close(gate)
	fsc := srv.waitDial(t)
	fsc.expectClosed(t)
	if r := awaitSubscribe(t, res); !errors.Is(r.err, pool.ErrClosed) {
		t.Fatalf("Subscribe racing Close = (%v, %v), want pool.ErrClosed", r.h, r.err)
	}
}

// TestConcurrentSubscribesDialOnce pins the single-flight dial: two
// subscribers racing on a connectionless manager produce one dial; the
// second waits for the I/O slot the dialer holds and then writes its
// own subscribe on the shared connection.
func TestConcurrentSubscribesDialOnce(t *testing.T) {
	srv := newFakeServer()
	srv.autoConfirm = true
	m := newTestManager(t, srv, testConfig("node:6379"))

	gate := make(chan struct{})
	srv.setDialGate(gate)

	resA := subscribeAsync(m, "a")
	srv.waitGateHit(t)
	resB := subscribeAsync(m, "b")
	// b must wait for a's dial rather than start its own.
	srv.expectNoGateHit(t, 200*time.Millisecond)

	close(gate)
	fsc := srv.waitDial(t)
	// Both names are written on the one connection: a by the dialer, b
	// by the waiter once the connection is installed.
	got := map[string]bool{}
	for range 2 {
		cmd := fsc.waitCmd(t)
		if len(cmd) != 2 || cmd[0] != "subscribe" {
			t.Fatalf("unexpected command %v", cmd)
		}
		got[cmd[1]] = true
	}
	if !got["a"] || !got["b"] {
		t.Fatalf("subscribed %v, want a and b", got)
	}
	for _, res := range []<-chan subscribeResult{resA, resB} {
		if r := awaitSubscribe(t, res); r.err != nil {
			t.Fatalf("Subscribe: %v", r.err)
		}
	}
	select {
	case fsc2 := <-srv.dialCh:
		t.Fatalf("second dial to %s; dials must be single-flight", fsc2.addr)
	default:
	}
}
