package pool

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/proto"
)

// socketTimeout is a retryable net.Error, the shape a socket read deadline produces.
type socketTimeout struct{}

func (socketTimeout) Error() string   { return "i/o timeout" }
func (socketTimeout) Timeout() bool   { return true }
func (socketTimeout) Temporary() bool { return true }

// dataThenErrorReader returns its payload together with an error, the way a
// transport can report a complete reply and a timeout in the same read.
type dataThenErrorReader struct {
	data []byte
	err  error
	done bool
}

func (r *dataThenErrorReader) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	r.done = true
	return copy(p, r.data), r.err
}

func newDeadlineContext(t *testing.T, d time.Duration) (context.Context, time.Time) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("context has no deadline")
	}
	return ctx, deadline
}

// expiredDeadlineContext returns a deadline that has already passed, so the
// context timer has fired, the way a socket read that timed out looks.
func expiredDeadlineContext(t *testing.T) (context.Context, time.Time) {
	t.Helper()
	ctx, deadline := newDeadlineContext(t, time.Millisecond)
	time.Sleep(5 * time.Millisecond)
	if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("context did not expire: %v", ctx.Err())
	}
	return ctx, deadline
}

// A timeout that arrives together with data belongs to the next read; wrapping it
// would make bufio cache a context error and hand it to the following command.
func TestDeadlineReaderDoesNotWrapErrorWithData(t *testing.T) {
	ctx, deadline := expiredDeadlineContext(t)
	raw := socketTimeout{}
	r := &deadlineReader{
		reader:   &dataThenErrorReader{data: []byte("+OK\r\n"), err: raw},
		ctx:      ctx,
		deadline: deadline,
	}

	buf := make([]byte, len("+OK\r\n"))
	n, err := r.Read(buf)
	if n != len(buf) {
		t.Fatalf("Read returned %d bytes, want %d", n, len(buf))
	}
	var contextErr *ContextDeadlineError
	if errors.As(err, &contextErr) {
		t.Fatalf("a timeout that arrived with data was wrapped as a context error: %v", err)
	}
	if !errors.Is(err, raw) {
		t.Fatalf("Read returned %v, want the raw socket timeout", err)
	}
}

// The socket timer can fire just before the context timer publishes its error;
// a timeout on a deadline that came from the context must still be wrapped.
func TestContextDeadlineErrorWrapsAtTheDeadlineBoundary(t *testing.T) {
	ctx, deadline := newDeadlineContext(t, time.Hour)

	err := contextDeadlineError(ctx, deadline, socketTimeout{})
	var contextErr *ContextDeadlineError
	if !errors.As(err, &contextErr) {
		t.Fatalf("timeout at the context deadline was not wrapped: %v", err)
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wrapped error does not match context.DeadlineExceeded: %v", err)
	}
}

// The cached error from a successful command must not surface as a context
// deadline on the next command: shouldRetry has to see a retryable socket timeout.
func TestCachedReadErrorIsNotAContextDeadline(t *testing.T) {
	ctx, deadline := expiredDeadlineContext(t)
	raw := socketTimeout{}
	rd := proto.NewReader(&deadlineReader{
		reader:   &dataThenErrorReader{data: []byte("+OK\r\n"), err: raw},
		ctx:      ctx,
		deadline: deadline,
	})

	if _, err := rd.ReadReply(); err != nil {
		t.Fatalf("first reply: %v", err)
	}
	_, err := rd.ReadReply()
	var contextErr *ContextDeadlineError
	if errors.As(err, &contextErr) {
		t.Fatalf("the next command received the previous read's context error: %v", err)
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("expected a retryable socket timeout, got %v", err)
	}
}
