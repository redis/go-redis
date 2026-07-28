package redis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
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

	processed, err := c.drainPushNotifications(cn)
	if err != nil {
		t.Fatalf("drainPushNotifications returned error: %v", err)
	}
	if !processed {
		t.Fatal("drain with buffered data must report processed=true")
	}
	if rec.count() == 0 {
		t.Fatal("reader-buffered invalidate was not consumed/dispatched (gate skipped it)")
	}
}

// erroringProcessor returns err from ProcessPendingNotifications, delegating
// other methods to the embedded Processor. Wrapping the built-in processor
// this way still classifies as CUSTOM in drainPushNotifications (the type
// assertion is exact) — which is the point: a wrapper gives no
// no-bytes-consumed guarantee either.
type erroringProcessor struct {
	*push.Processor
	err error
}

func (p erroringProcessor) ProcessPendingNotifications(_ context.Context, _ push.NotificationHandlerContext, _ *proto.Reader) error {
	return p.err
}

// TestDrainPushNotifications_CustomProcessorErrorIsFatal: a custom processor
// (any non-*push.Processor, including a wrapper around the built-in one) gives
// no guarantee that no bytes were consumed before its error, so the reader may
// be mid-frame — the error must be connection-fatal (non-nil), exactly like
// the built-in processor's mid-frame errors, so the drainer removes the conn
// instead of re-pooling a possibly desynced reader. (This inverts the earlier
// not-fatal behavior, which re-pooled the conn on the same evidence.)
func TestDrainPushNotifications_CustomProcessorErrorIsFatal(t *testing.T) {
	proc := erroringProcessor{push.NewProcessor(), errors.New("semantic boom")}
	c := &baseClient{opt: &Options{Protocol: 3}, pushProcessor: proc}

	cn, cleanup := newReaderBufferedPushConn(t, invalidateFrame("foo"))
	defer cleanup()

	if _, err := c.drainPushNotifications(cn); err == nil {
		t.Fatal("custom-processor drain error must be fatal so the conn is removed, got nil")
	}
}

// TestBackgroundDrainerLifecycle verifies start stores a handle on the client,
// double-start is a no-op, and stop joins the goroutine. The handle is
// intentionally RETAINED (not cleared) after stop: cscPoolHook is read on the
// command hot path, so niling the CSC fields under a concurrent Close would race;
// repeat stops are made idempotent by teardownOnce instead.
func TestBackgroundDrainerLifecycle(t *testing.T) {
	cp := pool.NewConnPool(&pool.Options{
		Dialer:   func(context.Context) (net.Conn, error) { return nil, errors.New("no dial in lifecycle test") },
		PoolSize: 1,
	})
	defer cp.Close()
	c := &baseClient{opt: &Options{Protocol: 3}, connPool: cp}

	c.startBackgroundDrainer()
	h := c.cscDrainHandle
	if h == nil {
		t.Fatal("startBackgroundDrainer did not store a drain handle")
	}

	// Double-start must not replace the handle.
	c.startBackgroundDrainer()
	if c.cscDrainHandle != h {
		t.Fatal("double start replaced the drain handle")
	}

	c.stopBackgroundDrainer()
	// The handle is retained (see doc above), but the goroutine must have exited.
	if c.cscDrainHandle != h {
		t.Fatal("stopBackgroundDrainer must retain the drain handle")
	}
	select {
	case <-h.done:
	default:
		t.Fatal("stopBackgroundDrainer returned before the drainer goroutine exited")
	}

	// Stop again: idempotent, no panic, no double-close of the stop channel.
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

// TestInvalidateHandlerDecodesPayloads: the SharedTracking invalidate handler
// must evict for both string and []byte key names in the push payload.
func TestInvalidateHandlerDecodesPayloads(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	cache.Set("get:foo", []string{dbNamespacedKey(0, "foo")}, []byte("1"))
	cache.Set("get:quux", []string{dbNamespacedKey(0, "quux")}, []byte("2"))
	h := &invalidateHandler{cache: cache, db: 0}

	err := h.HandlePushNotification(context.Background(), push.NotificationHandlerContext{},
		[]interface{}{"invalidate", []interface{}{"foo", []byte("quux")}})
	if err != nil {
		t.Fatalf("HandlePushNotification: %v", err)
	}
	if n := cache.Len(); n != 0 {
		t.Fatalf("both entries should be invalidated, Len=%d", n)
	}
}

// TestInvalidateHandlerNilPayloadFlushes: a nil <keys> payload (emitted by the
// server on FLUSHDB/FLUSHALL) must flush the entire cache.
func TestInvalidateHandlerNilPayloadFlushes(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 16})
	cache.Set("get:foo", []string{dbNamespacedKey(0, "foo")}, []byte("1"))
	cache.Set("get:quux", []string{dbNamespacedKey(0, "quux")}, []byte("2"))
	h := &invalidateHandler{cache: cache, db: 0}

	err := h.HandlePushNotification(context.Background(), push.NotificationHandlerContext{},
		[]interface{}{"invalidate", nil})
	if err != nil {
		t.Fatalf("HandlePushNotification: %v", err)
	}
	if n := cache.Len(); n != 0 {
		t.Fatalf("nil payload must flush the whole cache, Len=%d", n)
	}
}

// boundCache reads the handler's current cache binding under its lock.
func boundCache(h *invalidateHandler) Cache {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.cache
}

// TestInvalidateHandlerReleasedOnClose: closing a client that OWNS its cache
// (ClientSideCacheConfig) must release the invalidate handler's BINDING. An
// application-supplied processor outlives the client; a handler left bound to
// the dead cache would make a successor client's registration fail and
// silently disable its CSC. The handler itself stays registered — and
// protected — so application code cannot unregister invalidation out from
// under a live client.
func TestInvalidateHandlerReleasedOnClose(t *testing.T) {
	p := NewPushNotificationProcessor()

	c1 := NewClient(&Options{
		Addr:                      "localhost:1", // never dialed
		Protocol:                  3,
		PushNotificationProcessor: p,
		ClientSideCacheConfig:     &ClientSideCacheConfig{MaxEntries: 16},
	})
	if c1.baseClient.csc == nil {
		t.Fatal("first client should have CSC attached")
	}
	ih, ok := p.GetHandler(invalidatePushName).(*invalidateHandler)
	if !ok {
		t.Fatal("invalidate handler should be registered while the client lives")
	}
	// The handler is protected: user-level unregistration must FAIL, so a live
	// client's invalidation can't be silently removed by application code.
	if err := p.UnregisterHandler(invalidatePushName); err == nil {
		t.Fatal("UnregisterHandler must fail for the protected invalidate handler")
	}

	if err := c1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	// Still registered, but the binding is released.
	if p.GetHandler(invalidatePushName) == nil {
		t.Fatal("handler must stay registered (protected) after Close; only its binding is released")
	}
	if boundCache(ih) != nil {
		t.Fatal("owned-cache binding must be released on Close")
	}

	// A successor client reusing the processor rebinds the handler.
	c2 := NewClient(&Options{
		Addr:                      "localhost:1",
		Protocol:                  3,
		PushNotificationProcessor: p,
		ClientSideCacheConfig:     &ClientSideCacheConfig{MaxEntries: 16},
	})
	t.Cleanup(func() { _ = c2.Close() })
	if c2.baseClient.csc == nil {
		t.Fatal("successor client must be able to attach CSC after the first Close")
	}
	if boundCache(ih) != c2.baseClient.csc {
		t.Fatal("successor client must rebind the released handler to its own cache")
	}
}

// TestInvalidateHandlerRetainedForSharedCache: with an explicitly supplied
// (shared) cache the client does not own it — other clients on the same
// processor may still rely on the handler, so Close must leave it registered.
func TestInvalidateHandlerRetainedForSharedCache(t *testing.T) {
	p := NewPushNotificationProcessor()
	shared := NewLocalCache(CacheConfig{MaxEntries: 16})

	c1 := NewClient(&Options{
		Addr:                      "localhost:1",
		Protocol:                  3,
		PushNotificationProcessor: p,
		ClientSideCache:           shared,
	})
	c2 := NewClient(&Options{
		Addr:                      "localhost:1",
		Protocol:                  3,
		PushNotificationProcessor: p,
		ClientSideCache:           shared,
	})
	t.Cleanup(func() { _ = c2.Close() })

	if c1.baseClient.csc == nil || c2.baseClient.csc == nil {
		t.Fatal("both clients should share CSC on the same cache+processor")
	}
	if err := c1.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	ih, ok := p.GetHandler(invalidatePushName).(*invalidateHandler)
	if !ok {
		t.Fatal("shared-cache handler must survive one client's Close: the second client still needs invalidations")
	}
	if boundCache(ih) != shared {
		t.Fatal("shared-cache binding must not be released by a non-owning client's Close")
	}
}

// newDampingClient builds a baseClient whose drainer ticks every millisecond,
// draining the given conns round-robin (one per pass) through an
// always-erroring custom processor.
func newDampingClient(cns ...*pool.Conn) *baseClient {
	return &baseClient{
		opt: &Options{
			Protocol:              3,
			ClientSideCacheConfig: &ClientSideCacheConfig{DrainInterval: time.Millisecond},
		},
		connPool:      &drainablePooler{cns: cns},
		pushProcessor: erroringProcessor{push.NewProcessor(), errors.New("always fails")},
	}
}

// waitDrainerSelfStop asserts the drainer stops itself (damping) and that CSC
// serving is disabled.
func waitDrainerSelfStop(t *testing.T, c *baseClient) {
	t.Helper()
	h := c.cscDrainHandle
	if h == nil {
		t.Fatal("drainer did not start")
	}
	select {
	case <-h.done:
		// Drainer self-stopped after the damping threshold.
	case <-time.After(5 * time.Second):
		t.Fatal("drainer did not self-stop on persistent custom-processor errors")
	}
	if c.cscActive.Load() {
		t.Fatal("cscActive must be false after the damping threshold: stale hits must not be served")
	}
}

// TestBackgroundDrainerDisablesCSCOnPersistentCustomErrors: a custom processor
// that fails every drain would otherwise remove (and force a redial of) a conn
// per tick forever. After cscDrainCustomErrCap consecutive failures the drainer
// must disable CSC serving (cscActive=false) and stop, instead of churning.
func TestBackgroundDrainerDisablesCSCOnPersistentCustomErrors(t *testing.T) {
	cn, cleanup := newReaderBufferedPushConn(t, invalidateFrame("foo"))
	defer cleanup()

	c := newDampingClient(cn)
	c.startBackgroundDrainer()
	t.Cleanup(c.stopBackgroundDrainer)
	waitDrainerSelfStop(t, c)
}

// TestBackgroundDrainerDampingSurvivesCleanConns: in a real pool, each fatal
// drain removes its conn and the freshly dialed replacement has nothing
// buffered — its drain is a no-op. Such clean drains must NOT reset the
// damping counter, or a persistently failing processor would churn conns
// forever without ever tripping the cap.
func TestBackgroundDrainerDampingSurvivesCleanConns(t *testing.T) {
	pushy, cleanup1 := newReaderBufferedPushConn(t, invalidateFrame("foo"))
	defer cleanup1()
	// A conn with nothing buffered and nothing on the socket: its drain is a
	// clean no-op (net.Pipe has no syscall.Conn, so MaybeHasData is false).
	server, client := net.Pipe()
	defer func() { _ = server.Close(); _ = client.Close() }()
	clean := pool.NewConn(client)

	// Alternate failing and clean conns per drain pass.
	c := newDampingClient(pushy, clean)
	c.startBackgroundDrainer()
	t.Cleanup(c.stopBackgroundDrainer)
	waitDrainerSelfStop(t, c)
}

// drainablePooler is a non-*pool.ConnPool Pooler implementing idleConnDrainer.
// With cns set, each DrainIdleConns pass hands the callback one conn,
// round-robin; without, passes are just counted.
type drainablePooler struct {
	pool.Pooler
	cns []*pool.Conn

	mu     sync.Mutex
	called int
}

func (d *drainablePooler) DrainIdleConns(_ context.Context, _ *pool.DrainState, fn func(cn *pool.Conn) error) {
	d.mu.Lock()
	n := d.called
	d.called++
	d.mu.Unlock()
	if len(d.cns) > 0 {
		_ = fn(d.cns[n%len(d.cns)])
	}
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

	if _, err := c.drainPushNotifications(cn); err != nil {
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

	if _, err := c.drainPushNotifications(cn); err == nil {
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

// TestBackgroundDrainerCleanupOnGC verifies the runtime.AddCleanup safety net:
// a client that starts a drainer and is then dropped WITHOUT Close must have its
// drainer goroutine stopped once the *Client wrapper is garbage-collected.
func TestBackgroundDrainerCleanupOnGC(t *testing.T) {
	cp := pool.NewConnPool(&pool.Options{
		Dialer:   func(context.Context) (net.Conn, error) { return nil, errors.New("no dial in cleanup test") },
		PoolSize: 1,
	})
	defer cp.Close()

	// Build a *Client with a running drainer, register the cleanup, and return
	// ONLY its done channel so the *Client becomes unreachable when this returns.
	done := func() <-chan struct{} {
		c := &Client{baseClient: &baseClient{opt: &Options{Protocol: 3}, connPool: cp}}
		c.baseClient.startBackgroundDrainer()
		h := c.baseClient.cscDrainHandle
		if h == nil {
			t.Fatal("drainer did not start")
		}
		cscRegisterCleanups(c)
		return h.done
	}()

	deadline := time.After(10 * time.Second)
	for {
		runtime.GC()
		select {
		case <-done:
			return // cleanup fired and the goroutine exited
		case <-time.After(50 * time.Millisecond):
		}
		select {
		case <-deadline:
			t.Fatal("drainer goroutine did not stop after the client was GC'd")
		default:
		}
	}
}
