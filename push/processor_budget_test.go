package push

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/proto"
)

// deadlineReader models a socket under one hard read deadline: the first Read
// returns what the kernel already holds, later Reads return the rest of the
// stream unless the deadline has passed, as a net.Conn read past its deadline
// fails even when the bytes have arrived. reset arms a fresh deadline, as each
// drain call does.
type deadlineReader struct {
	first, rest []byte
	cap         time.Duration
	deadline    time.Time
}

func (r *deadlineReader) reset() { r.deadline = time.Now().Add(r.cap) }

func (r *deadlineReader) Read(p []byte) (int, error) {
	if len(r.first) > 0 {
		n := copy(p, r.first)
		r.first = r.first[n:]
		return n, nil
	}
	if time.Now().After(r.deadline) {
		return 0, testTimeoutError{}
	}
	if len(r.rest) == 0 {
		return 0, testTimeoutError{} // nothing more on the socket
	}
	n := copy(p, r.rest)
	r.rest = r.rest[n:]
	return n, nil
}

// slowHandler spends d per notification, like a CSC invalidation handler that
// also enqueues refreshes.
type slowHandler struct {
	d       time.Duration
	handled int
}

func (h *slowHandler) HandlePushNotification(context.Context, NotificationHandlerContext, []interface{}) error {
	time.Sleep(h.d)
	h.handled++
	return nil
}

// A buffered drain must not spend its caller's whole read deadline on handlers:
// with many complete frames buffered and slow handlers, the old loop handled them
// all, then had to read the rest of a partial frame after the deadline passed and
// failed mid-frame, so the caller dropped a healthy connection. The drain now
// stops at a frame boundary once its budget is spent; the frames left buffered
// are handled by the next drain.
func TestProcessor_BufferedDrainStopsAtBoundaryWhenBudgetSpent(t *testing.T) {
	frame := []byte(">2\r\n$10\r\ninvalidate\r\n$1\r\nk\r\n")
	const complete = 40
	stream := append(bytes.Repeat(frame, complete), frame...) // one more frame, split below
	// Partial last frame: header and name buffered (">2\r\n$10\r\ninvalidate\r\n"), so
	// the name peek succeeds and only the frame's ReadReply needs another read.
	split := len(stream) - len(frame) + 21

	rd := &deadlineReader{first: stream[:split], rest: stream[split:], cap: 50 * time.Millisecond}
	r := proto.NewReaderSize(rd, len(stream)+64) // the first Read buffers every complete frame
	p := NewProcessor()
	h := &slowHandler{d: 2 * time.Millisecond} // 40 frames x 2ms = 80ms, past the 50ms cap
	if err := p.RegisterHandler("invalidate", h, false); err != nil {
		t.Fatal(err)
	}

	rd.reset()
	if err := p.ProcessPendingNotificationsBuffered(context.Background(), NotificationHandlerContext{}, r); err != nil {
		t.Fatalf("first drain failed mid-frame after handling %d frames: %v", h.handled, err)
	}
	if h.handled == 0 || h.handled >= complete {
		t.Fatalf("first drain handled %d frames, want a budget-bounded prefix of %d", h.handled, complete)
	}
	if r.Buffered() == 0 {
		t.Fatal("frames left after the budget must stay buffered for the next drain")
	}

	// Later drains, each with a fresh deadline, finish the backlog and the split frame.
	for i := 0; i < 100 && h.handled < complete+1; i++ {
		rd.reset()
		if err := p.ProcessPendingNotificationsBuffered(context.Background(), NotificationHandlerContext{}, r); err != nil {
			t.Fatalf("drain %d failed after %d frames: %v", i+2, h.handled, err)
		}
	}
	if h.handled != complete+1 {
		t.Fatalf("handled %d frames in total, want %d", h.handled, complete+1)
	}
	if r.Buffered() != 0 {
		t.Fatalf("%d bytes left buffered after the backlog drained", r.Buffered())
	}
}
