package redis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/push"
)

// fakeTimeout is a net.Error reporting a timeout.
type fakeTimeout struct{}

func (fakeTimeout) Error() string   { return "i/o timeout" }
func (fakeTimeout) Timeout() bool   { return true }
func (fakeTimeout) Temporary() bool { return true }

// TestDrainErrorClassificationContract pins the drain-path classification
// (isBadConn with allowTimeout=false): a net timeout is FATAL — the drain loop
// only blocks mid-frame, so a timeout means a partially consumed frame and a
// desynced conn that must be removed, never re-pooled. EOF/conn errors and
// context.DeadlineExceeded are fatal as before.
func TestDrainErrorClassificationContract(t *testing.T) {
	const addr = "localhost:6379"

	timeoutErr := &net.OpError{Op: "read", Net: "tcp", Err: fakeTimeout{}}
	if !isBadConn(timeoutErr, false, addr) {
		t.Error("net i/o timeout must be fatal on the drain path (conn removed)")
	}
	if !isBadConn(io.EOF, false, addr) {
		t.Error("io.EOF must be fatal (conn removed)")
	}
	if !isBadConn(context.DeadlineExceeded, false, addr) {
		t.Error("context.DeadlineExceeded must be fatal")
	}
}

// invalidateFrame builds a RESP3 `>` push frame: ["invalidate", [key]].
func invalidateFrame(key string) []byte {
	return []byte(fmt.Sprintf(">2\r\n$10\r\ninvalidate\r\n*1\r\n$%d\r\n%s\r\n", len(key), key))
}

type recordingHandler struct {
	mu sync.Mutex
	n  int
}

func (h *recordingHandler) HandlePushNotification(_ context.Context, _ push.NotificationHandlerContext, _ []interface{}) error {
	h.mu.Lock()
	h.n++
	h.mu.Unlock()
	return nil
}

func (h *recordingHandler) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.n
}

// newReaderBufferedPushConn returns a conn with `frame` buffered in proto.Reader
// but no socket-visible data (net.Pipe has no syscall.Conn, so MaybeHasData is
// false) — the coalesced-push case (invalidate left in cn.rd after a prior reply).
func newReaderBufferedPushConn(t *testing.T, frame []byte) (*pool.Conn, func()) {
	t.Helper()
	server, client := net.Pipe()
	cn := pool.NewConn(client)
	go func() { _, _ = server.Write(frame) }()
	// PeekReplyType fills the bufio buffer without consuming the frame.
	if err := cn.WithReader(context.Background(), time.Second, func(rd *proto.Reader) error {
		_, err := rd.PeekReplyType()
		return err
	}); err != nil {
		_ = server.Close()
		_ = client.Close()
		t.Fatalf("priming reader buffer: %v", err)
	}
	return cn, func() { _ = server.Close(); _ = client.Close() }
}

// TestDrainPushNotifications_ConsumesReaderBufferedPush is a regression guard: a
// push buffered in proto.Reader (no socket data) must still drain — the gate
// checks HasBufferedData(), not only MaybeHasData().
func TestDrainPushNotifications_ConsumesReaderBufferedPush(t *testing.T) {
	rec := &recordingHandler{}
	proc := push.NewProcessor()
	if err := proc.RegisterHandler("invalidate", rec, false); err != nil {
		t.Fatalf("register handler: %v", err)
	}
	c := &baseClient{opt: &Options{Protocol: 3}, pushProcessor: proc}

	cn, cleanup := newReaderBufferedPushConn(t, invalidateFrame("foo"))
	defer cleanup()

	// Assert HasBufferedData, not MaybeHasData: the latter is false on Unix
	// (net.Pipe has no syscall.Conn) but true on the non-Unix stub.
	if !cn.HasBufferedData() {
		t.Fatal("precondition: frame was not buffered in the reader")
	}

	if err := c.drainPushNotifications(cn); err != nil {
		t.Fatalf("drainPushNotifications returned error: %v", err)
	}
	if rec.count() == 0 {
		t.Fatal("reader-buffered invalidate was not consumed/dispatched (gate skipped it)")
	}
}

// erroringProcessor returns a semantic (non-connection) error from
// ProcessPendingNotifications, delegating other methods to the embedded Processor.
type erroringProcessor struct{ *push.Processor }

func (erroringProcessor) ProcessPendingNotifications(_ context.Context, _ push.NotificationHandlerContext, _ *proto.Reader) error {
	return errors.New("semantic boom")
}

// TestDrainPushNotifications_CustomProcessorErrorNotFatal is a regression guard: a
// custom processor's semantic error must not be treated as connection-fatal (else
// isBadConn removes the conn).
func TestDrainPushNotifications_CustomProcessorErrorNotFatal(t *testing.T) {
	proc := erroringProcessor{push.NewProcessor()}
	c := &baseClient{opt: &Options{Protocol: 3}, pushProcessor: proc}

	cn, cleanup := newReaderBufferedPushConn(t, invalidateFrame("foo"))
	defer cleanup()

	if err := c.drainPushNotifications(cn); err != nil {
		t.Fatalf("custom-processor semantic error must not be fatal, got: %v", err)
	}
}

// TestBackgroundDrainerLifecycle verifies start registers a handle, double-start
// is a no-op, and stop removes the registration and joins the goroutine.
func TestBackgroundDrainerLifecycle(t *testing.T) {
	cp := pool.NewConnPool(&pool.Options{
		Dialer:   func(context.Context) (net.Conn, error) { return nil, errors.New("no dial in lifecycle test") },
		PoolSize: 1,
	})
	defer cp.Close()
	c := &baseClient{opt: &Options{Protocol: 3}, connPool: cp}

	c.startBackgroundDrainer()
	v, ok := cscDrainHandles.Load(c)
	if !ok {
		t.Fatal("startBackgroundDrainer did not register a drain handle")
	}
	h := v.(*cscDrainHandle)

	// Double-start must not replace the handle.
	c.startBackgroundDrainer()
	if v2, _ := cscDrainHandles.Load(c); v2.(*cscDrainHandle) != h {
		t.Fatal("double start replaced the drain handle")
	}

	c.stopBackgroundDrainer()
	if _, still := cscDrainHandles.Load(c); still {
		t.Fatal("stopBackgroundDrainer did not remove the registration")
	}
	// stop joins the goroutine, so done must already be closed.
	select {
	case <-h.done:
	default:
		t.Fatal("stopBackgroundDrainer returned before the drainer goroutine exited")
	}

	// Stop again: idempotent, no panic.
	c.stopBackgroundDrainer()
}

// TestCscDrainIntervalClampsMinimum: sub-millisecond DrainInterval values are
// clamped to cscMinDrainInterval (unreliable timers would silently loosen the
// staleness bound); values at or above the floor pass through.
func TestCscDrainIntervalClampsMinimum(t *testing.T) {
	sub := &baseClient{opt: &Options{
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{DrainInterval: 100 * time.Microsecond},
	}}
	if got := sub.cscDrainInterval(); got != cscMinDrainInterval {
		t.Fatalf("sub-ms DrainInterval must clamp to %v, got %v", cscMinDrainInterval, got)
	}

	above := &baseClient{opt: &Options{
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{DrainInterval: 10 * time.Millisecond},
	}}
	if got := above.cscDrainInterval(); got != 10*time.Millisecond {
		t.Fatalf("above-floor DrainInterval must pass through, got %v", got)
	}

	unset := &baseClient{opt: &Options{Protocol: 3}}
	if got := unset.cscDrainInterval(); got != cscDrainSkipWindow {
		t.Fatalf("unset DrainInterval must default to %v, got %v", cscDrainSkipWindow, got)
	}
}

// TestInvalidateHandlerCountsInvalidationBytes: the SharedTracking invalidate
// handler must feed RESPInvalidationBytesRead like the Broadcast and
// PerConnection handlers do, or the metric reads 0 under the default strategy.
func TestInvalidateHandlerCountsInvalidationBytes(t *testing.T) {
	h := &invalidateHandler{cache: NewLocalCache(CacheConfig{MaxEntries: 16}), db: 0}

	before := RESPInvalidationBytesRead()
	err := h.HandlePushNotification(context.Background(), push.NotificationHandlerContext{},
		[]interface{}{"invalidate", []interface{}{"foo", []byte("quux")}})
	if err != nil {
		t.Fatalf("HandlePushNotification: %v", err)
	}
	want := int64(len("foo") + len("quux"))
	if got := RESPInvalidationBytesRead() - before; got != want {
		t.Fatalf("invalidation bytes delta: got %d want %d", got, want)
	}
}

// drainablePooler is a non-*pool.ConnPool Pooler implementing idleConnDrainer.
type drainablePooler struct {
	pool.Pooler
	mu     sync.Mutex
	called int
}

func (d *drainablePooler) DrainIdleConns(_ context.Context, _ *pool.DrainState, _ func(cn *pool.Conn) error) {
	d.mu.Lock()
	d.called++
	d.mu.Unlock()
}

func (d *drainablePooler) calls() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.called
}

// pubsubMessageFrame builds a RESP3 `>` push frame: ["message", ch, payload].
func pubsubMessageFrame(ch, payload string) []byte {
	return []byte(fmt.Sprintf(">3\r\n$7\r\nmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
		len(ch), ch, len(payload), payload))
}

// TestDrainPushNotifications_LeavesPubSubFrames: the drain loop must not
// consume pub/sub-reserved push frames — they belong to the pub/sub system
// (same guard as the built-in processor, cf. PR #3842).
func TestDrainPushNotifications_LeavesPubSubFrames(t *testing.T) {
	proc := push.NewProcessor()
	c := &baseClient{opt: &Options{Protocol: 3}, pushProcessor: proc}

	cn, cleanup := newReaderBufferedPushConn(t, pubsubMessageFrame("ch", "hello"))
	defer cleanup()

	if err := c.drainPushNotifications(cn); err != nil {
		t.Fatalf("drainPushNotifications returned error: %v", err)
	}
	if !cn.HasBufferedData() {
		t.Fatal("pub/sub message frame was consumed by the drain loop; it must stay buffered")
	}
}

// TestDrainPushNotifications_MidFrameTimeoutIsFatal: a hard-deadline timeout
// while consuming a frame leaves the conn desynced (frame tail still on the
// socket); the drain must surface it as fatal so the pool removes the conn.
func TestDrainPushNotifications_MidFrameTimeoutIsFatal(t *testing.T) {
	proc := push.NewProcessor()
	c := &baseClient{opt: &Options{Protocol: 3}, pushProcessor: proc}

	// Complete header + name, truncated key payload: ReadReply consumes the
	// prefix then blocks for the tail that never arrives.
	partial := []byte(">2\r\n$10\r\ninvalidate\r\n*1\r\n$3\r\nfo")
	cn, cleanup := newReaderBufferedPushConn(t, partial)
	defer cleanup()

	if err := c.drainPushNotifications(cn); err == nil {
		t.Fatal("mid-frame timeout must be fatal (non-nil) so the conn is removed, got nil")
	}
}

// TestBackgroundDrainerUsesOptionalInterface: any Pooler implementing
// idleConnDrainer gets background draining, not only *pool.ConnPool.
func TestBackgroundDrainerUsesOptionalInterface(t *testing.T) {
	dp := &drainablePooler{}
	c := &baseClient{opt: &Options{
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{DrainInterval: time.Millisecond},
	}, connPool: dp}

	c.startBackgroundDrainer()
	defer c.stopBackgroundDrainer()

	deadline := time.After(2 * time.Second)
	for dp.calls() == 0 {
		select {
		case <-deadline:
			t.Fatal("drainer never called the pooler's DrainIdleConns")
		default:
			time.Sleep(time.Millisecond)
		}
	}
}
