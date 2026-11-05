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
	if time.Now().After(r.deadline) || len(r.rest) == 0 {
		return 0, testTimeoutError{}
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

var invalidateFrame = []byte(">2\r\n$10\r\ninvalidate\r\n$1\r\nk\r\n")

// newDrainCase buffers complete invalidation frames followed by one frame split
// after its name (">2\r\n$10\r\ninvalidate\r\n" buffered, the rest on the
// socket), behind a deadline of cap.
func newDrainCase(t *testing.T, complete int, cap, handlerTime time.Duration) (*proto.Reader, *deadlineReader, *Processor, *slowHandler) {
	t.Helper()
	stream := append(bytes.Repeat(invalidateFrame, complete), invalidateFrame...)
	split := len(stream) - len(invalidateFrame) + 21
	rd := &deadlineReader{first: stream[:split], rest: stream[split:], cap: cap}
	r := proto.NewReaderSize(rd, len(stream)+64) // the first Read buffers every complete frame
	p := NewProcessor()
	h := &slowHandler{d: handlerTime}
	if err := p.RegisterHandler("invalidate", h, false); err != nil {
		t.Fatal(err)
	}
	return r, rd, p, h
}

// Handler time counts against the caller's one read deadline. With slow handlers
// the old loop handled every buffered frame, then read the rest of a partly
// buffered frame after the deadline passed and failed mid-frame, so the caller
// dropped a healthy connection. The drain now handles the complete frames, leaves
// the partly buffered one untouched, and the next drain completes it.
func TestProcessor_BufferedDrainLeavesPartialPushFrame(t *testing.T) {
	const complete = 40
	r, rd, p, h := newDrainCase(t, complete, 50*time.Millisecond, 2*time.Millisecond) // 80ms of handlers

	rd.reset()
	if err := p.ProcessPendingNotificationsBuffered(context.Background(), NotificationHandlerContext{}, r); err != nil {
		t.Fatalf("first drain failed after handling %d frames: %v", h.handled, err)
	}
	if h.handled != complete {
		t.Fatalf("first drain handled %d frames, want the %d complete ones", h.handled, complete)
	}
	if r.Buffered() != 21 {
		t.Fatalf("%d bytes buffered after the first drain, want the 21-byte prefix of the split frame", r.Buffered())
	}

	rd.reset()
	if err := p.ProcessPendingNotificationsBuffered(context.Background(), NotificationHandlerContext{}, r); err != nil {
		t.Fatalf("second drain: %v", err)
	}
	if h.handled != complete+1 || r.Buffered() != 0 {
		t.Fatalf("after the second drain: %d frames handled, %d bytes buffered; want %d and 0", h.handled, r.Buffered(), complete+1)
	}
}

// The same holds under a deadline shorter than one handler (the 1ms speculative
// probe): the drain never reads after its first frame, so no read can land past
// the deadline.
func TestProcessor_BufferedDrainShortDeadline(t *testing.T) {
	r, rd, p, h := newDrainCase(t, 1, time.Millisecond, 3*time.Millisecond)
	rd.reset()
	if err := p.ProcessPendingNotificationsBuffered(context.Background(), NotificationHandlerContext{}, r); err != nil {
		t.Fatalf("drain failed after handling %d frames: %v", h.handled, err)
	}
	if h.handled != 1 || r.Buffered() != 21 {
		t.Fatalf("%d frames handled, %d bytes buffered; want 1 and the 21-byte prefix", h.handled, r.Buffered())
	}
}

// A partly buffered frame that starts with an attribute is not left behind: the
// pool's put-path check would parse the attribute without a read deadline. The
// drain reads it under its deadline as before, and a timeout there is reported.
func TestProcessor_BufferedDrainDoesNotLeavePartialAttribute(t *testing.T) {
	first := append(append([]byte{}, invalidateFrame...), "|1\r\n+me"...)
	rd := &deadlineReader{first: first, rest: []byte("ta\r\n+v\r\n"), cap: time.Millisecond}
	r := proto.NewReaderSize(rd, 256)
	p := NewProcessor()
	h := &slowHandler{d: 12 * time.Millisecond} // the deadline passes during the first handler
	if err := p.RegisterHandler("invalidate", h, false); err != nil {
		t.Fatal(err)
	}
	rd.reset()
	err := p.ProcessPendingNotificationsBuffered(context.Background(), NotificationHandlerContext{}, r)
	if err == nil {
		if b, _ := r.Peek(1); len(b) > 0 && b[0] == proto.RespAttr {
			t.Fatal("a partly buffered attribute was left for the put-path check")
		}
	}
	if h.handled != 1 {
		t.Fatalf("handled %d frames, want 1", h.handled)
	}
}

// A fully buffered attribute in front of a partly buffered push frame does not
// get the push frame started: the attribute is discarded from the buffer (no
// read), and the whole-frame check then applies to the push frame itself.
func TestProcessor_BufferedDrainChecksPushAfterAttribute(t *testing.T) {
	attr := "|1\r\n+meta\r\n+v\r\n"
	first := append(append(append([]byte{}, invalidateFrame...), attr...), invalidateFrame[:21]...)
	rd := &deadlineReader{first: first, rest: invalidateFrame[21:], cap: time.Millisecond}
	r := proto.NewReaderSize(rd, 256)
	p := NewProcessor()
	h := &slowHandler{d: 3 * time.Millisecond} // the deadline passes during the first handler
	if err := p.RegisterHandler("invalidate", h, false); err != nil {
		t.Fatal(err)
	}
	rd.reset()
	if err := p.ProcessPendingNotificationsBuffered(context.Background(), NotificationHandlerContext{}, r); err != nil {
		t.Fatalf("drain failed after handling %d frames: %v", h.handled, err)
	}
	if h.handled != 1 {
		t.Fatalf("handled %d frames, want 1", h.handled)
	}
	if b, _ := r.Peek(1); r.Buffered() != 21 || len(b) == 0 || b[0] != proto.RespPush {
		t.Fatalf("%d bytes buffered, want the 21-byte prefix of the push frame", r.Buffered())
	}
}
