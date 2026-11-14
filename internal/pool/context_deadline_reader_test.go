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
// transport can report bytes and a timeout in the same read.
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

// A timeout that arrives together with the tail of an incomplete reply belongs to
// the command still being parsed; its context classification must be kept.
func TestPartialReplyKeepsContextDeadline(t *testing.T) {
	ctx, deadline := expiredDeadlineContext(t)
	rd := proto.NewReader(&deadlineReader{
		reader:   &dataThenErrorReader{data: []byte("$5\r\nhe"), err: socketTimeout{}},
		ctx:      ctx,
		deadline: deadline,
	})

	_, err := rd.ReadReply()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("partial reply lost its context deadline classification: %v", err)
	}
}

// scriptedConn replays scripted reads, so a read can return bytes and a timeout at
// the same time — the transport shape that leaves a cached error behind.
type scriptedConn struct {
	reads []struct {
		data []byte
		err  error
	}
}

func (c *scriptedConn) Read(p []byte) (int, error) {
	if len(c.reads) == 0 {
		return 0, io.EOF
	}
	step := c.reads[0]
	c.reads = c.reads[1:]
	return copy(p, step.data), step.err
}

func (c *scriptedConn) Write(p []byte) (int, error)      { return len(p), nil }
func (c *scriptedConn) Close() error                     { return nil }
func (c *scriptedConn) LocalAddr() net.Addr              { return scriptedAddr{} }
func (c *scriptedConn) RemoteAddr() net.Addr             { return scriptedAddr{} }
func (c *scriptedConn) SetDeadline(time.Time) error      { return nil }
func (c *scriptedConn) SetReadDeadline(time.Time) error  { return nil }
func (c *scriptedConn) SetWriteDeadline(time.Time) error { return nil }

type scriptedAddr struct{}

func (scriptedAddr) Network() string { return "scripted" }
func (scriptedAddr) String() string  { return "scripted" }

// A transport can return a complete reply and a timeout together. The reply parses
// fine, but bufio caches the wrapped timeout; the next command must not inherit it.
func TestCachedReadErrorIsDroppedBeforeNextCommand(t *testing.T) {
	conn := &scriptedConn{reads: []struct {
		data []byte
		err  error
	}{
		{[]byte("+OK\r\n"), socketTimeout{}},
		{[]byte("+OK\r\n"), nil},
	}}
	cn := NewConn(conn)
	t.Cleanup(func() { _ = cn.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	time.Sleep(5 * time.Millisecond)

	// The first command gets its reply even though a context timeout came with it.
	err := cn.WithReader(ctx, time.Second, func(rd *proto.Reader) error {
		_, err := rd.ReadReply()
		return err
	})
	if err != nil {
		t.Fatalf("first command: a complete reply must parse despite the timeout: %v", err)
	}

	// The connection is fine: the next command must read the next reply instead of
	// the previous command's cached context error.
	var reply string
	err = cn.WithReader(context.Background(), time.Second, func(rd *proto.Reader) error {
		value, err := rd.ReadReply()
		if err != nil {
			return err
		}
		reply, _ = value.(string)
		return nil
	})
	if err != nil {
		t.Fatalf("second command inherited the previous command's error: %v", err)
	}
	if reply != "OK" {
		t.Fatalf("second command read %q, want OK", reply)
	}
}
