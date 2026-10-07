package pubsub

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

// stallWrite subscribes "a" on a fresh manager, then stops the fake
// server reading so the next write parks on the pipe, and starts that
// write (a Subscribe of "b") in the background. It returns the handle,
// its message channel, the server conn and the background result.
func stallWrite(t *testing.T, m *Manager, srv *fakeServer) (PubSuber, <-chan *Message, *fakeServerConn, <-chan error) {
	t.Helper()
	ctx := context.Background()

	h, err := m.Subscribe(ctx, "a")
	if err != nil {
		t.Fatalf("Subscribe a: %v", err)
	}
	ch := h.Channel()
	fsc := srv.waitDial(t)
	fsc.expectCmd(t, "subscribe", "a")

	// A paused server stops reading between frames, so a write then
	// blocks on the synchronous pipe until its deadline or the connection
	// closes. The read loop may already be parked inside a pipe Read from
	// before the pause, which consumes one more write: write until one
	// parks, at most two attempts.
	fsc.paused.Store(true)
	t.Cleanup(func() { fsc.paused.Store(false) })

	var stalled <-chan error
	for i := range 2 {
		res := make(chan error, 1)
		name := fmt.Sprintf("stall-%d", i)
		go func() {
			_, err := m.Subscribe(ctx, name)
			res <- err
		}()
		select {
		case err := <-res:
			if err != nil {
				t.Fatalf("probe Subscribe %s: %v", name, err)
			}
			// Consumed by the pending Read; the loop is parked now.
			continue
		case <-time.After(100 * time.Millisecond):
			stalled = res
		}
		break
	}
	if stalled == nil {
		t.Fatal("no write parked on the paused server")
	}
	return h, ch, fsc, stalled
}

// TestStalledWriteDoesNotBlockManager pins that socket writes run with
// the state lock released: while a subscribe write is stalled on a peer
// that stopped reading, inbound traffic still fans out and Close
// returns at once instead of waiting out WriteTimeout.
func TestStalledWriteDoesNotBlockManager(t *testing.T) {
	srv := newFakeServer()
	srv.autoConfirm = true
	cfg := testConfig("node:6379")
	cfg.WriteTimeout = 10 * time.Second // far beyond the test's patience
	m := newTestManager(t, srv, cfg)

	_, ch, fsc, stalled := stallWrite(t, m, srv)

	// The server's writer still runs: a message arrives and is delivered
	// while the write is parked.
	fsc.sendMessage(t, "message", "a", "during")
	if msg := recvMsg(t, ch); msg.Payload != "during" {
		t.Fatalf("got %q during the stalled write, want \"during\"", msg.Payload)
	}

	start := time.Now()
	if err := m.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("Close took %v behind a stalled write, want immediate", took)
	}

	// Closing the connection fails the parked write.
	select {
	case err := <-stalled:
		if err == nil {
			t.Fatal("the stalled Subscribe succeeded after Close")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the stalled Subscribe never returned after Close")
	}
}

// TestWriterWaitHonorsContext pins that a writer queued behind a
// stalled write waits on the I/O slot with its own context: it returns
// the deadline error on time rather than blocking for WriteTimeout.
func TestWriterWaitHonorsContext(t *testing.T) {
	srv := newFakeServer()
	srv.autoConfirm = true
	cfg := testConfig("node:6379")
	cfg.WriteTimeout = 10 * time.Second
	m := newTestManager(t, srv, cfg)

	h, _, _, _ := stallWrite(t, m, srv)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := h.Subscribe(ctx, "c")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Subscribe behind a stalled write = %v, want context.DeadlineExceeded", err)
	}
	if took := time.Since(start); took > time.Second {
		t.Fatalf("Subscribe returned after %v, want about the 100ms deadline", took)
	}
}
