package redis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/otel"
	"github.com/redis/go-redis/v9/internal/pool"
)

// fdCountHook counts Get/Put on the pipeline pool — used to prove the held
// full-duplex connection actually cycles through the pool's per-conn hooks on
// lease/return, which is the mechanism maintnotifications and streaming-creds
// re-auth rely on.
type fdCountHook struct{ gets, puts atomic.Int64 }

func (h *fdCountHook) OnGet(_ context.Context, _ *pool.Conn, _ bool) (bool, error) {
	h.gets.Add(1)
	return true, nil
}

func (h *fdCountHook) OnPut(_ context.Context, _ *pool.Conn) (bool, bool, error) {
	h.puts.Add(1)
	return true, false, nil
}
func (h *fdCountHook) OnRemove(_ context.Context, _ *pool.Conn, _ error) {}

// TestFullDuplexReturnRunsPoolHooks verifies that on lease/return the FD
// connection passes back through the pipeline pool's PoolHook Get/Put path.
// maintnotifications (step 4) and streaming-creds re-auth (step 5) are exactly
// such pool hooks, so this is the concrete evidence they get a chance to run.
func TestFullDuplexReturnRunsPoolHooks(t *testing.T) {
	ctx := context.Background()

	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	pp := c.getPipelinePool()
	if pp == nil {
		t.Fatal("no pipeline pool")
	}
	hook := &fdCountHook{}
	pp.AddPoolHook(hook) // must be installed before the engine leases its conn

	// Fast idle-return so it fires within the test window; max-hold left at its
	// default (5s) so it does not fire in this <2s test — the idle path is what
	// we are proving.
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex:            true,
		FullDuplexIdleTimeout: 40 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	// The engine leases a conn at startup and, finding no work, idle-returns it —
	// so a Get and a Put should both be observed by the pool hook.
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		if hook.gets.Load() >= 1 && hook.puts.Load() >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if g, p := hook.gets.Load(), hook.puts.Load(); g < 1 || p < 1 {
		t.Fatalf("FD conn did not cycle through pool hooks (gets=%d puts=%d) — maintnotif/re-auth hooks would never run", g, p)
	}

	// And a command still works after the return (re-lease → another Get).
	if err := ap.Set(ctx, "fd:hook:k", "v", 0).Err(); err != nil {
		t.Fatalf("post-return set: %v", err)
	}
	if v, err := ap.Get(ctx, "fd:hook:k").Result(); err != nil || v != "v" {
		t.Fatalf("post-return get: v=%q err=%v", v, err)
	}
}

func fdTestClient(addr string) *Client {
	return NewClient(&Options{
		Addr:                    addr,
		Protocol:                3,
		PipelinePoolSize:        4,
		PipelineReadBufferSize:  64 * 1024,
		PipelineWriteBufferSize: 64 * 1024,
		PoolSize:                4,
	})
}

// TestFullDuplexConsumesInvalidationPush proves the ordered full-duplex reader
// drains an interleaved RESP3 push (a CLIENT TRACKING invalidation) instead of
// misreading it as a command reply. CLIENT TRACKING ON is issued on the single
// held FD connection, a key is primed (tracked) through FD, then a SECOND client
// mutates it — the server pushes an `invalidate` frame onto the FD conn. If the
// reader did not consume the push, one of the follow-up GET readReply calls
// would parse the push frame as its reply and return a wrong value or error.
func TestFullDuplexConsumesInvalidationPush(t *testing.T) {
	ctx := context.Background()
	addr := ":6379"

	c := fdTestClient(addr)
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis at %s: %v", addr, err)
	}

	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active (ap.fd is nil)")
	}

	key := "fd:pushtest:key"
	if err := c.Set(ctx, key, "v0", 0).Err(); err != nil {
		t.Fatalf("seed set: %v", err)
	}

	if err := ap.Do(ctx, "CLIENT", "TRACKING", "ON").Err(); err != nil {
		t.Fatalf("CLIENT TRACKING ON: %v", err)
	}
	if v, err := ap.Get(ctx, key).Result(); err != nil || v != "v0" {
		t.Fatalf("prime GET: v=%q err=%v", v, err)
	}

	other := NewClient(&Options{Addr: addr})
	defer other.Close()
	if err := other.Set(ctx, key, "v1", 0).Err(); err != nil {
		t.Fatalf("invalidating set: %v", err)
	}

	time.Sleep(150 * time.Millisecond) // let the invalidation push arrive/buffer

	for i := 0; i < 100; i++ {
		v, err := ap.Get(ctx, key).Result()
		if err != nil {
			t.Fatalf("GET %d after invalidation: %v (push misread as reply?)", i, err)
		}
		if v != "v1" {
			t.Fatalf("GET %d: got %q want %q (FIFO misaligned by unconsumed push)", i, v, "v1")
		}
	}
}

// TestFullDuplexOrderedManyGoroutines is a correctness/-race check: many
// concurrent goroutines each run a SET then GET of their own key through the
// ordered full-duplex stream and must read back exactly what they wrote
// (per-caller order + reply/command alignment hold).
func TestFullDuplexOrderedManyGoroutines(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	const workers, iters = 64, 200
	errCh := make(chan error, workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			key := "fd:ord:" + itoa(w)
			for i := 0; i < iters; i++ {
				val := itoa(w) + ":" + itoa(i)
				if err := ap.Set(ctx, key, val, 0).Err(); err != nil {
					errCh <- err
					return
				}
				got, err := ap.Get(ctx, key).Result()
				if err != nil {
					errCh <- err
					return
				}
				if got != val {
					errCh <- &fdOrderErr{w, i, val, got}
					return
				}
			}
			errCh <- nil
		}(w)
	}
	for w := 0; w < workers; w++ {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
}

// TestFullDuplexRecoversFromConnKill is the retry fault-injection test: many
// goroutines run continuous SET-then-GET of their own key through the ordered
// full-duplex stream while a second client repeatedly kills the connection
// (CLIENT KILL TYPE normal — SKIPME skips the killer). The engine must re-issue
// the unacked tail on a fresh connection: every worker keeps reading back
// exactly what it wrote (per-caller order + alignment survive the failure), and
// nothing hangs.
func TestFullDuplexRecoversFromConnKill(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	killer := NewClient(&Options{Addr: ":6379"})
	defer killer.Close()
	if err := killer.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis for killer: %v", err)
	}

	const workers = 16
	var stop atomic.Bool
	done := make(chan error, workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			key := "fd:kill:" + itoa(w)
			for n := 0; !stop.Load(); n++ {
				val := itoa(w) + ":" + itoa(n)
				if err := ap.Set(ctx, key, val, 0).Err(); err != nil {
					done <- fmt.Errorf("worker %d set #%d: %w", w, n, err)
					return
				}
				got, err := ap.Get(ctx, key).Result()
				if err != nil {
					done <- fmt.Errorf("worker %d get #%d: %w", w, n, err)
					return
				}
				if got != val {
					done <- fmt.Errorf("worker %d get #%d: got %q want %q (misaligned across recovery)", w, n, got, val)
					return
				}
			}
			done <- nil
		}(w)
	}

	// Let load build, then kill the full-duplex connection a few times mid-stream.
	time.Sleep(120 * time.Millisecond)
	for i := 0; i < 3; i++ {
		if err := killer.Do(ctx, "CLIENT", "KILL", "TYPE", "normal").Err(); err != nil {
			t.Logf("CLIENT KILL #%d: %v", i, err)
		}
		time.Sleep(90 * time.Millisecond)
	}
	time.Sleep(120 * time.Millisecond)
	stop.Store(true)

	deadline := time.After(15 * time.Second)
	for w := 0; w < workers; w++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-deadline:
			t.Fatal("timeout: a worker hung after a connection kill (retry deadlock / lost completion?)")
		}
	}
}

// TestFullDuplexIdleReturnsConn verifies lease/return: after an idle gap the
// held connection goes back to the pipeline pool (so its per-conn hooks can
// run), and the next command re-leases it and still reads correctly.
func TestFullDuplexIdleReturnsConn(t *testing.T) {
	ctx := context.Background()

	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	// Fast idle-return; max-hold pushed out so only the idle path fires here.
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex:            true,
		FullDuplexIdleTimeout: 60 * time.Millisecond,
		FullDuplexMaxHold:     10 * time.Second,
	})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}
	pp := c.getPipelinePool()
	if pp == nil {
		t.Fatal("no pipeline pool")
	}

	if err := ap.Set(ctx, "fd:idle:k", "v", 0).Err(); err != nil {
		t.Fatalf("initial set: %v", err)
	}
	// Immediately after the command the engine still holds the conn (idle gap not
	// yet elapsed) — it is checked out of the pool, so IdleLen is 0. This is the
	// baseline that makes the return below meaningful (the assertion would not
	// hold if lease/return were removed — the conn would stay held, never idle).
	if got := pp.IdleLen(); got != 0 {
		t.Fatalf("expected the FD conn held (IdleLen=0) right after a command, got %d", got)
	}

	returned := false
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		if pp.IdleLen() >= 1 {
			returned = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !returned {
		t.Fatalf("pipeline conn not returned to the pool after idle (IdleLen=%d)", pp.IdleLen())
	}

	if v, err := ap.Get(ctx, "fd:idle:k").Result(); err != nil || v != "v" {
		t.Fatalf("re-lease GET: v=%q err=%v", v, err)
	}
}

// TestFullDuplexMaxHoldRecycles forces periodic clean returns under continuous
// load (max-hold) and asserts correctness holds, nothing hangs, and the
// connection is recycled through the same small pool (no leak).
func TestFullDuplexMaxHoldRecycles(t *testing.T) {
	ctx := context.Background()

	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	// Force frequent max-hold recycles; idle pushed out so only max-hold fires
	// under the continuous load below.
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex:            true,
		FullDuplexMaxHold:     80 * time.Millisecond,
		FullDuplexIdleTimeout: 10 * time.Second,
	})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}
	pp := c.getPipelinePool()

	const workers = 8
	var stop atomic.Bool
	done := make(chan error, workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			key := "fd:mh:" + itoa(w)
			for n := 0; !stop.Load(); n++ {
				val := itoa(w) + ":" + itoa(n)
				if err := ap.Set(ctx, key, val, 0).Err(); err != nil {
					done <- err
					return
				}
				got, err := ap.Get(ctx, key).Result()
				if err != nil {
					done <- err
					return
				}
				if got != val {
					done <- fmt.Errorf("worker %d n%d: got %q want %q across recycle", w, n, got, val)
					return
				}
			}
			done <- nil
		}(w)
	}
	time.Sleep(500 * time.Millisecond) // several 80ms max-hold recycles
	if pp != nil {
		if n := pp.Len(); n > workers {
			t.Fatalf("pipeline pool grew under recycling (Len=%d) — conn leak?", n)
		}
	}
	stop.Store(true)

	// Prove the max-hold path actually fired (else this test would be green over
	// an inert feature).
	if r := ap.fd.recycles.Load(); r == 0 {
		t.Fatal("no max-hold recycles observed — the recycle path did not fire")
	}
	deadline := time.After(15 * time.Second)
	for w := 0; w < workers; w++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-deadline:
			t.Fatal("timeout: worker hung under max-hold recycling")
		}
	}
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [20]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

type fdOrderErr struct {
	w, i      int
	want, got string
}

func (e *fdOrderErr) Error() string {
	return "per-caller order/alignment broken: worker=" + itoa(e.w) + " iter=" + itoa(e.i) +
		" want=" + e.want + " got=" + e.got
}

// TestFullDuplexConfigDefaults pins the zero-value resolution. A zero
// FullDuplexWindow MUST become the default, never 0: the writer's backpressure
// gate is `for inflight.len() >= window`, so window==0 would block the writer on
// the first submit (0 >= 0). This is a construction check — no server needed.
func TestFullDuplexConfigDefaults(t *testing.T) {
	c := fdTestClient(":6379")
	defer c.Close()
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}
	if ap.fd.window != fdDefaultWindow {
		t.Fatalf("zero FullDuplexWindow resolved to %d, want default %d (window 0 deadlocks the writer)", ap.fd.window, fdDefaultWindow)
	}
	if ap.fd.idle != fdDefaultIdle {
		t.Fatalf("zero FullDuplexIdleTimeout resolved to %s, want default %s", ap.fd.idle, fdDefaultIdle)
	}
	if ap.fd.maxHold != fdDefaultMaxHold {
		t.Fatalf("zero FullDuplexMaxHold resolved to %s, want default %s", ap.fd.maxHold, fdDefaultMaxHold)
	}
	if cap(ap.fd.ch) != fdDefaultWindow {
		t.Fatalf("queue capacity %d, want window %d", cap(ap.fd.ch), fdDefaultWindow)
	}
}

// TestFullDuplexValidateRejects covers the config surface: Validate must reject
// the contradictory standalone combos and negative tunings. Pure — no server.
func TestFullDuplexValidateRejects(t *testing.T) {
	bad := []struct {
		name string
		cfg  AutoPipelineOptions
		// wantMsg, when set, must appear in the error — proves the FD-specific
		// branch fired rather than a generic check (the FD checks run first).
		wantMsg string
	}{
		{"unordered", AutoPipelineOptions{FullDuplex: true, Unordered: true}, "FullDuplex requires an ordered stream"},
		{"concurrent-batches", AutoPipelineOptions{FullDuplex: true, MaxConcurrentBatches: 2}, "FullDuplex requires MaxConcurrentBatches"},
		{"neg-window", AutoPipelineOptions{FullDuplex: true, FullDuplexWindow: -1}, "FullDuplexWindow"},
		{"neg-idle", AutoPipelineOptions{FullDuplex: true, FullDuplexIdleTimeout: -1}, "FullDuplexIdleTimeout"},
		{"neg-maxhold", AutoPipelineOptions{FullDuplex: true, FullDuplexMaxHold: -1}, "FullDuplexMaxHold"},
	}
	for _, tc := range bad {
		err := tc.cfg.Validate()
		if err == nil {
			t.Errorf("%s: Validate() = nil, want error", tc.name)
			continue
		}
		if tc.wantMsg != "" && !strings.Contains(err.Error(), tc.wantMsg) {
			t.Errorf("%s: Validate() = %q, want it to contain %q", tc.name, err, tc.wantMsg)
		}
	}
	good := []struct {
		name string
		cfg  AutoPipelineOptions
	}{
		{"default", AutoPipelineOptions{FullDuplex: true}},
		{"ordered-1batch", AutoPipelineOptions{FullDuplex: true, MaxConcurrentBatches: 1}},
		{"tuned", AutoPipelineOptions{FullDuplex: true, FullDuplexWindow: 1024, FullDuplexIdleTimeout: time.Second, FullDuplexMaxHold: time.Second}},
	}
	for _, tc := range good {
		if err := tc.cfg.Validate(); err != nil {
			t.Errorf("%s: Validate() = %v, want nil", tc.name, err)
		}
	}
}

// fdProcessCounterHook counts ProcessHook / ProcessPipelineHook invocations and
// always calls next — the shape of an observability hook (redisotel).
type fdProcessCounterHook struct {
	process  atomic.Int64
	pipeline atomic.Int64
	sawNext  atomic.Int64
}

func (h *fdProcessCounterHook) DialHook(next DialHook) DialHook { return next }
func (h *fdProcessCounterHook) ProcessHook(next ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error {
		h.process.Add(1)
		err := next(ctx, cmd)
		h.sawNext.Add(1)
		return err
	}
}

func (h *fdProcessCounterHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return func(ctx context.Context, cmds []Cmder) error {
		h.pipeline.Add(1)
		return next(ctx, cmds)
	}
}

// TestFullDuplexRunsProcessHooks proves per-command observability works on the FD
// path: with a ProcessHook registered, every FD-dispatched command runs through
// the hook chain (redisotel spans/metrics + custom hooks fire), next() is called,
// and results are still correct. Without this, the FD engine's raw conn I/O would
// bypass hooks entirely.
func TestFullDuplexRunsProcessHooks(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	hook := &fdProcessCounterHook{}
	c.AddHook(hook)
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	base := hook.process.Load()
	const n = 50
	for i := 0; i < n; i++ {
		if err := ap.Set(ctx, "fdhook:"+itoa(i), itoa(i), 0).Err(); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}
	for i := 0; i < n; i++ {
		if v, err := ap.Get(ctx, "fdhook:"+itoa(i)).Result(); err != nil || v != itoa(i) {
			t.Fatalf("get %d: v=%q err=%v", i, v, err)
		}
	}
	// 2n commands (n SET + n GET), each must have run the hook chain with next().
	if got := hook.process.Load() - base; got < 2*n {
		t.Fatalf("ProcessHook fired %d times for %d FD commands — hooks not running on the FD path", got, 2*n)
	}
	if got := hook.sawNext.Load(); got < 2*n {
		t.Fatalf("hook next() reached %d times, want >= %d — chain not completing on FD", got, 2*n)
	}
}

// fdShortCircuitHook returns err WITHOUT calling next for one command name — a
// fail-fast / cache / circuit-breaker style hook. Selective (only the target
// command) so it does not also short-circuit the connection handshake.
type fdShortCircuitHook struct {
	name  string
	err   error
	calls atomic.Int64
}

func (h *fdShortCircuitHook) DialHook(next DialHook) DialHook { return next }
func (h *fdShortCircuitHook) ProcessHook(next ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error {
		if cmd.Name() == h.name {
			h.calls.Add(1)
			return h.err // short-circuit: next is never called
		}
		return next(ctx, cmd)
	}
}

func (h *fdShortCircuitHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}

// TestFullDuplexHookShortCircuit exercises the short-circuit path (the one where
// host and reader both finalize the command). It must be race-free (-race), the
// caller must see the hook's error, and the command must still execute on the
// wire (FD cannot un-send an already-queued command). Regression guard for the
// data race where the host released the caller before the reader finished writing
// the command.
func TestFullDuplexHookShortCircuit(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	// Separate hook-free client to verify the writes actually reached the server.
	verify := NewClient(&Options{Addr: ":6379"})
	defer verify.Close()

	sentinel := errors.New("short-circuited by hook")
	hook := &fdShortCircuitHook{name: "set", err: sentinel}
	c.AddHook(hook)
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	const n = 30
	for i := 0; i < n; i++ {
		if err := ap.Set(ctx, "fdsc:"+itoa(i), itoa(i), 0).Err(); !errors.Is(err, sentinel) {
			t.Fatalf("set %d: err=%v, want the short-circuit sentinel", i, err)
		}
	}
	if hook.calls.Load() < n {
		t.Fatalf("short-circuit hook fired %d times, want >= %d", hook.calls.Load(), n)
	}
	// The commands still ran on the wire despite the short-circuit — verified from
	// a hook-free client. This also proves the reader stayed aligned (a desync
	// would have corrupted or errored these writes).
	for i := 0; i < n; i++ {
		if v, err := verify.Get(ctx, "fdsc:"+itoa(i)).Result(); err != nil || v != itoa(i) {
			t.Fatalf("verify get %d: v=%q err=%v — command did not execute or stream desynced", i, v, err)
		}
	}
}

// delayReplyProxy is a tiny TCP proxy that forwards client->server immediately
// but delays every server->client chunk by `delay`, simulating reply latency so
// the FD reader lags the writer. Used to force backpressure without touching the
// server (DEBUG SLEEP is often disabled).
func delayReplyProxy(t *testing.T, backend string, delay time.Duration) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	var mu sync.Mutex
	var conns []net.Conn // active endpoints, closed by stop() to kill live sessions
	track := func(c net.Conn) {
		mu.Lock()
		conns = append(conns, c)
		mu.Unlock()
	}
	go func() {
		for {
			client, err := ln.Accept()
			if err != nil {
				return
			}
			go func(client net.Conn) {
				srv, err := net.Dial("tcp", backend)
				if err != nil {
					client.Close()
					return
				}
				track(client)
				track(srv)
				go io.Copy(srv, client) // client -> server: immediate
				buf := make([]byte, 64*1024)
				for {
					n, rerr := srv.Read(buf)
					if n > 0 {
						b := append([]byte(nil), buf[:n]...)
						time.Sleep(delay) // server -> client: delayed
						if _, werr := client.Write(b); werr != nil {
							break
						}
					}
					if rerr != nil {
						break
					}
				}
				srv.Close()
				client.Close()
			}(client)
		}
	}()
	return ln.Addr().String(), func() {
		ln.Close()
		mu.Lock()
		for _, c := range conns {
			c.Close() // kill live sessions so in-flight replies never arrive
		}
		mu.Unlock()
	}
}

// TestFullDuplexBackpressure proves the bounded in-flight window actually applies
// backpressure: with reply latency injected so the reader lags, a caller
// submitting far more than the window BLOCKS once the channel + in-flight are
// full — outstanding stays bounded, memory does not grow without limit — and once
// the reads catch up everything drains with the correct values (no drops/reorders
// under backpressure).
func TestFullDuplexBackpressure(t *testing.T) {
	ctx := context.Background()
	if err := NewClient(&Options{Addr: ":6379"}).Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	const delay = 25 * time.Millisecond
	paddr, stop := delayReplyProxy(t, "127.0.0.1:6379", delay)
	defer stop()

	c := fdTestClient(paddr)
	defer c.Close()

	const window, maxBatch = 32, 4
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex:       true,
		FullDuplexWindow: window,
		MaxBatchSize:     maxBatch,
	})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}
	// Prime the session so the engine has leased a conn + a live in-flight deque.
	if err := ap.Set(ctx, "bp:prime", "x", 0).Err(); err != nil {
		t.Fatalf("prime: %v", err)
	}

	const N = 500
	key := func(i int) string { return fmt.Sprintf("bp:%d", i) }
	val := func(i int) string { return fmt.Sprintf("v%d", i) }

	// Burst N submits. Reply latency (delay) means the reader lags, so in-flight
	// fills to the window, the writer blocks, the channel fills, and ap.Set blocks
	// once ~channel+in-flight are outstanding.
	var accepted atomic.Int64
	cmds := make([]*StatusCmd, N)
	submittedAll := make(chan struct{})
	go func() {
		for i := 0; i < N; i++ {
			cmds[i] = ap.Set(ctx, key(i), val(i), 0) // blocks here once full = backpressure
			accepted.Add(1)
		}
		close(submittedAll)
	}()

	// The burst saturates in microseconds; replies do not start returning for
	// `delay`. Sample well within that window: outstanding must be bounded, not N.
	time.Sleep(delay / 3)
	acc := accepted.Load()
	peak := ap.fd.curInflight.Load().peakLen()
	if acc >= N {
		t.Fatalf("no backpressure: accepted %d of %d before any reply returned (unbounded submit)", acc, N)
	}
	if acc > 4*window {
		t.Fatalf("outstanding %d exceeds the expected bound (~channel %d + in-flight %d+%d); backpressure too loose", acc, window, window, maxBatch)
	}
	if peak > window+maxBatch {
		t.Fatalf("in-flight peak %d exceeded window+maxBatch=%d — deque not bounded", peak, window+maxBatch)
	}
	t.Logf("under reply latency: accepted=%d/%d (bounded to ~channel+in-flight), in-flight peak=%d (<= %d)", acc, N, peak, window+maxBatch)

	// Reads catch up: the submitter finishes and every command lands.
	select {
	case <-submittedAll:
	case <-time.After(15 * time.Second):
		t.Fatalf("submitter did not drain after backpressure (accepted %d/%d)", accepted.Load(), N)
	}
	for i := 0; i < N; i++ {
		if err := cmds[i].Err(); err != nil {
			t.Fatalf("cmd %d failed after backpressure: %v", i, err)
		}
	}
	// Correctness after backpressure: sampled keys hold the values we set.
	for _, i := range []int{0, 1, N / 3, N / 2, N - 2, N - 1} {
		if got, err := ap.Get(ctx, key(i)).Result(); err != nil || got != val(i) {
			t.Fatalf("post-backpressure GET %s: got=%q err=%v want=%q", key(i), got, err, val(i))
		}
	}
	// Final peak sanity over the whole run.
	if peak := ap.fd.curInflight.Load().peakLen(); peak > window+maxBatch {
		t.Fatalf("final in-flight peak %d exceeded window+maxBatch=%d", peak, window+maxBatch)
	}
}

// TestFullDuplexMidStreamRedisError proves a per-command Redis error (WRONGTYPE)
// is delivered to THAT command and does NOT desync the stream: valid commands
// interleaved before and after it still get their own correct replies.
func TestFullDuplexMidStreamRedisError(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	const K = 64
	key := func(i int) string { return fmt.Sprintf("mse:%d", i) }
	val := func(i int) string { return fmt.Sprintf("mv%d", i) }
	for i := 0; i < K; i++ {
		if err := ap.Set(ctx, key(i), val(i), 0).Err(); err != nil {
			t.Fatalf("prewrite %d: %v", i, err)
		}
	}
	// A string key that LPUSH will reject with WRONGTYPE.
	if err := ap.Set(ctx, "mse:str", "s", 0).Err(); err != nil {
		t.Fatalf("prewrite str: %v", err)
	}

	// Interleave, all in flight: GET (valid) then LPUSH on the string key (errors).
	gets := make([]*StringCmd, K)
	errs := make([]*IntCmd, K)
	for i := 0; i < K; i++ {
		gets[i] = ap.Get(ctx, key(i))
		errs[i] = ap.LPush(ctx, "mse:str", "x") // WRONGTYPE, mid-stream
	}
	// The erroring commands must all carry a Redis error (not a conn teardown)...
	for i := 0; i < K; i++ {
		e := errs[i].Err()
		if e == nil {
			t.Fatalf("LPush %d on a string key returned no error (expected WRONGTYPE)", i)
		}
		if !isRedisError(e) {
			t.Fatalf("LPush %d error is not a Redis error (stream torn down?): %v", i, e)
		}
	}
	// ...and every interleaved GET must still return its own correct value.
	for i := 0; i < K; i++ {
		got, e := gets[i].Result()
		if e != nil || got != val(i) {
			t.Fatalf("GET %s after mid-stream error: got=%q err=%v want=%q (stream desynced by the error?)", key(i), got, e, val(i))
		}
	}
}

// TestFullDuplexCloseWhileBackpressured proves Close unblocks a caller that is
// blocked on a full window (backpressure): the submitter must return promptly
// (ctx.Done bail in submit), not hang, and pending commands fail rather than
// wedge.
func TestFullDuplexCloseWhileBackpressured(t *testing.T) {
	ctx := context.Background()
	if err := NewClient(&Options{Addr: ":6379"}).Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	const delay = 30 * time.Millisecond
	paddr, stop := delayReplyProxy(t, "127.0.0.1:6379", delay)
	defer stop()

	c := fdTestClient(paddr)
	defer c.Close()
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex: true, FullDuplexWindow: 32, MaxBatchSize: 4,
	})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}
	if err := ap.Set(ctx, "bpc:prime", "x", 0).Err(); err != nil {
		t.Fatalf("prime: %v", err)
	}

	const N = 5000
	done := make(chan struct{})
	go func() {
		for i := 0; i < N; i++ {
			ap.Set(ctx, fmt.Sprintf("bpc:%d", i), "v", 0) // blocks once the window is full
		}
		close(done)
	}()
	time.Sleep(delay / 2) // submitter now blocked on backpressure

	select {
	case <-done:
		t.Fatal("submitter finished before Close — backpressure did not engage")
	default:
	}

	if err := ap.Close(); err != nil {
		t.Fatalf("Close while backpressured: %v", err)
	}
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("submitter did not unblock after Close — Close did not release the backpressure wait")
	}
}

// TestFDReqsNoRetry covers the retry guard: a tail containing a NoRetry command
// (RawWriteToCmd) must NOT be replayed on a fresh connection — matching the
// half-duplex pipeline's cmdsContainNoRetry guard. Pure, no server.
func TestFDReqsNoRetry(t *testing.T) {
	ctx := context.Background()
	retryable := fdReq{cmd: NewStatusCmd(ctx, "set", "k", "v")}
	if fdReqsNoRetry([]fdReq{retryable}) {
		t.Fatal("a plain SET was reported NoRetry — the tail would wrongly be failed instead of replayed")
	}
	noretry := fdReq{cmd: NewRawWriteToCmd(ctx, io.Discard, "ping")}
	if !noretry.cmd.NoRetry() {
		t.Fatal("precondition: RawWriteToCmd.NoRetry() should be true")
	}
	if !fdReqsNoRetry([]fdReq{retryable, noretry}) {
		t.Fatal("a tail containing a NoRetry command was not detected — FD would replay a non-retryable command")
	}
}

// TestFullDuplexBlockingCmdDetection proves the divert predicate catches blocking
// commands — in particular a RAW XREAD whose BLOCK token is a []byte (the form a
// plain string type switch would miss, letting it ride the shared pipe). Pure.
func TestFullDuplexBlockingCmdDetection(t *testing.T) {
	ctx := context.Background()
	if raw := NewCmd(ctx, "xread", []byte("BLOCK"), int64(0), "streams", "s", "$"); !isBlockingCmd(raw) {
		t.Fatal("raw XREAD BLOCK ([]byte token) not detected as blocking — would ride the FD pipe")
	}
	if raw := NewCmd(ctx, "xreadgroup", "group", "g", "c", []byte("BLOCK"), int64(0), "streams", "s", ">"); !isBlockingCmd(raw) {
		t.Fatal("raw XREADGROUP BLOCK not detected as blocking")
	}
	if nb := NewCmd(ctx, "xread", "streams", "s", "$"); isBlockingCmd(nb) {
		t.Fatal("non-blocking XREAD wrongly diverted — loses batching for nothing")
	}
	if !isBlockingCmd(NewCmd(ctx, "blpop", "k", 0)) {
		t.Fatal("BLPOP not detected as blocking")
	}
}

// TestFullDuplexBlockingDivertsOffPipe proves a parked blocking command does NOT
// head-of-line-block the shared FD pipe: it is diverted to a separate pooled
// connection, so pipelined commands submitted while it is parked complete
// promptly. Note: the per-caller ordering guarantee is deliberately NOT claimed
// across this divert boundary (the blocking command runs on its own connection),
// so this test does not assert ordering between the BLPOP and the GETs.
func TestFullDuplexBlockingDivertsOffPipe(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	c.Del(ctx, "fd:block:list")
	// Park a BLPOP (empty list → blocks up to 3s) on its own goroutine.
	blDone := make(chan error, 1)
	go func() { _, e := ap.BLPop(ctx, 3*time.Second, "fd:block:list").Result(); blDone <- e }()
	time.Sleep(100 * time.Millisecond) // let the BLPOP be issued and park

	// While BLPOP is parked, 100 SET+GET round-trips on the SAME ap must complete
	// well under the 3s block — if they were queued behind BLPOP on one pipe this
	// would take ~3s.
	start := time.Now()
	for i := 0; i < 100; i++ {
		if err := ap.Set(ctx, "fd:block:k"+itoa(i), itoa(i), 0).Err(); err != nil {
			t.Fatalf("set %d during parked BLPOP: %v", i, err)
		}
	}
	for i := 0; i < 100; i++ {
		if v, err := ap.Get(ctx, "fd:block:k"+itoa(i)).Result(); err != nil || v != itoa(i) {
			t.Fatalf("get %d during parked BLPOP: v=%q err=%v", i, v, err)
		}
	}
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Fatalf("200 pipelined ops took %s while BLPOP parked — head-of-line blocked behind the blocking command", elapsed)
	}
	select {
	case e := <-blDone:
		t.Fatalf("BLPOP returned early (%v); expected it parked while the pipe served other work", e)
	default: // still parked, as expected
	}

	// Release the BLPOP and let its goroutine exit cleanly.
	c.LPush(ctx, "fd:block:list", "x")
	select {
	case <-blDone:
	case <-time.After(2 * time.Second):
		t.Fatal("BLPOP did not return after LPush — divert path stuck")
	}
}

// TestFullDuplexContextCancelStaysAligned proves that abandoning a command's
// wait (WaitContext with a cancelled context) mid-stream does NOT desync the
// reader: the reader still drains that command's reply in FIFO order, so every
// following command matches its own reply. Half the SET waits are abandoned; the
// SETs must still have executed correctly, proven by reading them all back.
func TestFullDuplexContextCancelStaysAligned(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	const n = 200
	cancelled, cancel := context.WithCancel(ctx)
	cancel() // already-done context: WaitContext returns immediately
	futures := make([]AutoFuture, n)
	for i := 0; i < n; i++ {
		futures[i] = ap.Submit(ctx, NewStatusCmd(ctx, "set", "fdctx:"+itoa(i), itoa(i)))
	}
	// Abandon every other wait with a cancelled context; the reader must still
	// drain those replies so the stream stays aligned.
	for i := 0; i < n; i += 2 {
		_ = futures[i].WaitContext(cancelled) // may return ctx.Err() or the result — we don't care
	}
	// Read them all back: if any abandoned reply had been skipped, the FIFO would
	// be off by one and these values would be wrong or errored.
	for i := 0; i < n; i++ {
		if v, err := ap.Get(ctx, "fdctx:"+itoa(i)).Result(); err != nil || v != itoa(i) {
			t.Fatalf("get %d after abandoned waits: v=%q err=%v (stream desynced?)", i, v, err)
		}
	}
}

// TestFullDuplexNoGoroutineLeakOnClose opens and closes the FD engine repeatedly
// (with in-flight, never-waited work at Close time) and asserts the writer/reader
// goroutines are reaped each time — no accumulation across cycles.
func TestFullDuplexNoGoroutineLeakOnClose(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	// With a hook installed, every submitted command also spawns a host goroutine
	// (hostHook) — so this asserts those are reaped too, not just writer/reader.
	c.AddHook(&fdProcessCounterHook{})
	// Warm one cycle so any one-time client goroutines exist before the baseline.
	if ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true}); err == nil {
		ap.Set(ctx, "fdleak:warm", "1", 0).Err()
		ap.Close()
	}
	time.Sleep(100 * time.Millisecond)
	base := runtime.NumGoroutine()

	for iter := 0; iter < 5; iter++ {
		ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
		if err != nil {
			t.Fatalf("AsyncAutoPipeline: %v", err)
		}
		if ap.fd == nil {
			t.Fatal("full-duplex engine not active")
		}
		// Fire in-flight work and Close WITHOUT waiting — exercises Close-mid-flight.
		for i := 0; i < 50; i++ {
			_ = ap.Set(ctx, "fdleak:"+itoa(i), itoa(i), 0)
		}
		if err := ap.Close(); err != nil {
			t.Fatalf("Close iter %d: %v", iter, err)
		}
	}

	var now int
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
		now = runtime.NumGoroutine()
		if now <= base+2 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if now > base+4 {
		t.Fatalf("goroutine leak after 5 FD open/close cycles: base=%d now=%d (writer/reader not reaped)", base, now)
	}
}

// TestFDFailReqsNoDeadlock covers the failReqs self-deadlock: failReqs runs on
// the engine goroutine and must finalize each command WITHOUT awaiting its batch
// (cmd.Err() blocks on the very done channel failReqs is about to close). Pure,
// no server.
func TestFDFailReqsNoDeadlock(t *testing.T) {
	ctx := context.Background()
	cmd := NewStatusCmd(ctx, "set", "k", "v")
	b := newAPBatch()
	cmd.setReady(b) // now cmd.Err()/await() would block until b closes

	fd := &fdEngine{}
	done := make(chan struct{})
	go func() {
		fd.failReqs([]fdReq{{cmd: cmd, batch: b}}, ErrClosed)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("failReqs deadlocked: it awaited the batch.done it is responsible for closing")
	}
	if err := cmd.rawErr(); !errors.Is(err, ErrClosed) {
		t.Fatalf("cmd err = %v, want ErrClosed", err)
	}
	select {
	case <-b.done:
	default:
		t.Fatal("failReqs did not close the batch")
	}
}

// TestFDInflightHardCloseTakesUnackedTail covers the double-ownership fix: an
// entry the reader has completed+advanced must NEVER also be returned by the
// recovery path. hardClose stops the reader; takeRemaining (called after the
// reader would have exited) returns exactly the un-advanced tail, in order — so
// completed commands are not replayed/failed a second time. Pure, no server.
func TestFDInflightHardCloseTakesUnackedTail(t *testing.T) {
	ctx := context.Background()
	f := newFDInflight()
	all := make([]fdReq, 10)
	for i := range all {
		all[i] = fdReq{cmd: NewStatusCmd(ctx, "set", itoa(i), "v"), batch: newAPBatch()}
	}
	f.pushBatch(all)

	// Reader snapshots the front and completes+advances the first 4.
	buf, ok := f.frontBatch(nil)
	if !ok || len(buf) != 10 {
		t.Fatalf("frontBatch ok=%v n=%d, want ok=true n=10", ok, len(buf))
	}
	f.advance(4)

	// Connection-error recovery: stop the reader, then take the tail.
	f.hardClose()
	if _, ok := f.frontBatch(nil); ok {
		t.Fatal("frontBatch returned ok after hardClose — the reader would not exit")
	}
	rem := f.takeRemaining()
	if len(rem) != 6 {
		t.Fatalf("takeRemaining n=%d, want 6 (advanced entries must not reappear)", len(rem))
	}
	for i := 0; i < 6; i++ {
		if rem[i].cmd != all[4+i].cmd {
			t.Fatalf("tail[%d] mismatch — recovery order/ownership broken", i)
		}
	}
	// Taking again yields nothing (queue cleared): no entry can be recovered twice.
	if again := f.takeRemaining(); len(again) != 0 {
		t.Fatalf("second takeRemaining n=%d, want 0", len(again))
	}
}

// TestFDInflightOwnershipPartition pins the deque contract the Blocker-2 fix
// relies on: with the correct usage (hardClose, then takeRemaining ONLY after
// the reader has exited), every entry is owned by EXACTLY ONE side — advanced by
// the reader OR returned by takeRemaining, never both and never neither. A
// reader goroutine drains+advances while a second goroutine races a hardClose;
// under -race this also proves advance/hardClose/takeRemaining are lock-clean.
// (The session-level regression — that session() must take the tail only after
// <-readerDone, not before as the old closeRecover did — is covered end-to-end
// by TestFullDuplexRecoversFromConnKill.)
func TestFDInflightOwnershipPartition(t *testing.T) {
	ctx := context.Background()
	for iter := 0; iter < 200; iter++ {
		f := newFDInflight()
		const n = 64
		all := make([]fdReq, n)
		for i := range all {
			all[i] = fdReq{cmd: NewStatusCmd(ctx, "set", itoa(i), "v"), batch: newAPBatch()}
		}
		f.pushBatch(all)

		advanced := make(map[Cmder]struct{}, n)
		readerDone := make(chan struct{})
		go func() {
			defer close(readerDone)
			var buf []fdReq
			for {
				var ok bool
				buf, ok = f.frontBatch(buf)
				if !ok {
					return // hardClose observed → reader exits (mirrors the real reader)
				}
				// Complete a slice of the snapshot, then advance exactly that many.
				take := len(buf)/2 + 1
				if take > len(buf) {
					take = len(buf)
				}
				for i := 0; i < take; i++ {
					advanced[buf[i].cmd] = struct{}{}
				}
				f.advance(take)
			}
		}()

		// Race the hard close against the reader's progress.
		f.hardClose()
		<-readerDone // MUST wait before taking — that ordering is the fix
		rem := f.takeRemaining()

		// Partition check: advanced ⊎ remaining == all, disjoint, complete.
		if len(advanced)+len(rem) != n {
			t.Fatalf("iter %d: advanced=%d + remaining=%d != %d (entry lost or double-owned)",
				iter, len(advanced), len(rem), n)
		}
		for _, r := range rem {
			if _, dup := advanced[r.cmd]; dup {
				t.Fatalf("iter %d: cmd both advanced AND in recovery tail — double-owned (would double-execute)", iter)
			}
		}
	}
}

// fdCountLimiter counts Allow/ReportResult to verify the full-duplex engine
// accounts the Limiter once per session (Allow on acquire, ReportResult before
// release), balanced 1:1.
type fdCountLimiter struct{ allow, report atomic.Int64 }

func (l *fdCountLimiter) Allow() error         { l.allow.Add(1); return nil }
func (l *fdCountLimiter) ReportResult(_ error) { l.report.Add(1) }

// TestFullDuplexLimiterPerSession verifies FullDuplex honors opt.Limiter with
// per-session accounting: every session's conn acquisition is bracketed by
// exactly one Allow and one ReportResult (before release). Previously the FD
// engine bypassed the Limiter entirely (#3964).
func TestFullDuplexLimiterPerSession(t *testing.T) {
	ctx := context.Background()
	lim := &fdCountLimiter{}
	c := NewClient(&Options{
		Addr:                    ":6379",
		Protocol:                3,
		PipelinePoolSize:        4,
		PipelineReadBufferSize:  64 * 1024,
		PipelineWriteBufferSize: 64 * 1024,
		PoolSize:                4,
		Limiter:                 lim,
	})
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}

	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}
	if err := ap.Set(ctx, "fd:lim:k", "v", 0).Err(); err != nil {
		t.Fatalf("set: %v", err)
	}
	// A tiny idle window so any late session settles before we read the counters.
	time.Sleep(20 * time.Millisecond)
	// Close waits for the engine (ap.wg), so every session's deferred
	// ReportResult has run by the time Close returns.
	if err := ap.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	a, r := lim.allow.Load(), lim.report.Load()
	if a < 1 {
		t.Fatalf("FullDuplex never called Limiter.Allow (allow=%d) — Limiter bypassed", a)
	}
	if a != r {
		t.Fatalf("FullDuplex Limiter unbalanced: Allow=%d ReportResult=%d (must be 1:1 per session)", a, r)
	}
}

// TestFullDuplexRetryDivertsToNormalConn verifies the mechanism the FD reader
// uses for a retryable Redis error (LOADING/READONLY/…) or a redirect (MOVED/ASK):
// the command is re-run on the client's normal path and the caller is settled with
// that result — not left with the FD error. It drives retryOnNormalConn directly
// (a deterministic stand-in for the reader's divert) since inducing a real LOADING
// on a live server is not reproducible.
func TestFullDuplexRetryDivertsToNormalConn(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	if err := c.Set(ctx, "fd:retry:k", "v", 0).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}

	// As the reader does on a retryable error: hand an FD request to the divert.
	cmd := NewStringCmd(ctx, "get", "fd:retry:k")
	b := newAPBatch()
	cmd.setReady(b)
	ap.fd.retryOnNormalConn(fdReq{cmd: cmd, batch: b})

	select {
	case <-b.done:
	case <-time.After(2 * time.Second):
		t.Fatal("retryOnNormalConn did not complete the diverted command")
	}
	if v, err := cmd.Result(); err != nil || v != "v" {
		t.Fatalf("diverted GET = %q err=%v, want \"v\" (re-run on the normal path)", v, err)
	}
}

// fdOtelRecorder counts RecordOperationDuration to prove the full-duplex reader
// emits the native per-command OTel metric (it previously bypassed process, so
// no metric was recorded — #3964). All other Recorder methods are no-ops.
type fdOtelRecorder struct{ opDurations atomic.Int64 }

func (r *fdOtelRecorder) RecordOperationDuration(context.Context, time.Duration, otel.Cmder, int, error, *pool.Conn, int) {
	r.opDurations.Add(1)
}
func (r *fdOtelRecorder) RecordPipelineOperationDuration(context.Context, time.Duration, string, int, int, error, *pool.Conn, int) {
}
func (r *fdOtelRecorder) RecordConnectionCreateTime(context.Context, time.Duration, *pool.Conn) {}
func (r *fdOtelRecorder) RecordConnectionRelaxedTimeout(context.Context, int, *pool.Conn, string, string) {
}
func (r *fdOtelRecorder) RecordConnectionHandoff(context.Context, *pool.Conn, string)         {}
func (r *fdOtelRecorder) RecordError(context.Context, string, *pool.Conn, string, bool, int)  {}
func (r *fdOtelRecorder) RecordMaintenanceNotification(context.Context, *pool.Conn, string)   {}
func (r *fdOtelRecorder) RecordConnectionWaitTime(context.Context, time.Duration, *pool.Conn) {}
func (r *fdOtelRecorder) RecordConnectionClosed(context.Context, *pool.Conn, string, error)   {}
func (r *fdOtelRecorder) RecordPubSubMessage(context.Context, *pool.Conn, string, string, bool) {
}
func (r *fdOtelRecorder) RecordStreamLag(context.Context, time.Duration, *pool.Conn, string, string, string) {
}
func (r *fdOtelRecorder) RecordConnectionCount(context.Context, int, *pool.Conn, string, bool) {}
func (r *fdOtelRecorder) RecordPendingRequests(context.Context, int, *pool.Conn, string)       {}

// TestFullDuplexRecordsOTelOperationDuration verifies the FD reader records the
// native per-command OTel duration metric (redisotel-native), which it did not
// before — the reader completes commands without going through process().
func TestFullDuplexRecordsOTelOperationDuration(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	rec := &fdOtelRecorder{}
	otel.SetGlobalRecorder(rec)
	defer otel.SetGlobalRecorder(nil)

	// Runs through the FD pipe (writer -> reader), which is where the metric is
	// emitted; Result() blocks until the reader completed it (emit is before
	// complete()).
	if err := ap.Set(ctx, "fd:otel:k", "v", 0).Err(); err != nil {
		t.Fatalf("set: %v", err)
	}
	if n := rec.opDurations.Load(); n < 1 {
		t.Fatalf("FullDuplex recorded no OTel RecordOperationDuration (n=%d) — native metric bypassed", n)
	}
}

// TestFullDuplexDivertsHImportOffPipe verifies the full-duplex engine routes a
// managed HIMPORT command off the shared pipe to the normal Process path (#3964).
// The FD writer streams raw commands and never injects the registered HIMPORT
// PREPARE, so an HIMPORT SET riding the pipe can fail "no such fieldset"; diverting
// it through Process lets _process inject the PREPARE from the registry.
//
// The assertion is routing, not end-to-end HIMPORT (which needs Redis 8.10+): a
// diverted command executes on the MAIN pool via process(), while an FD-pipe
// command uses the dedicated pipeline pool and never touches the main pool. So a
// main-pool Hits/Misses bump after a lone HImportSet proves it diverted. The
// command's own result is irrelevant here — it may error on an old server.
func TestFullDuplexDivertsHImportOffPipe(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	// Warm the main pool so a later reuse registers as a Hit, then snapshot. The
	// FD engine uses the pipeline pool, so nothing but the diverted HImportSet
	// touches the main pool between the two snapshots.
	if err := c.Ping(ctx).Err(); err != nil {
		t.Fatalf("warm ping: %v", err)
	}
	before := c.PoolStats()

	// Err() on the async face blocks until the diverted command has executed, so
	// the "after" snapshot reflects its pool use. Ignore the result: it may be
	// "no such fieldset" (registry empty) or "unknown command" (Redis < 8.10) —
	// either way it ran on the main pool, which is what we assert.
	_ = ap.HImportSet(ctx, "fd:himport:k", "fd:himport:fs", "v").Err()

	after := c.PoolStats()
	beforeN := before.Hits + before.Misses
	afterN := after.Hits + after.Misses
	if afterN <= beforeN {
		t.Fatalf("HImportSet did not use the main pool (before=%d after=%d) — it rode the FD pipe instead of diverting to the normal path", beforeN, afterN)
	}
}

// fdCloseHook is a ProcessHook that does work AFTER next() returns, to prove the
// full-duplex engine tracks its hook-host goroutines so AutoPipeliner.Close waits
// for post-next hook work before returning (#3964). It signals once when it has
// entered the post-next phase, then holds briefly and records completion.
type fdCloseHook struct {
	entered   chan struct{}
	once      sync.Once
	finished  atomic.Bool
	holdFor   time.Duration
	watchName string
}

func (h *fdCloseHook) DialHook(next DialHook) DialHook { return next }

func (h *fdCloseHook) ProcessHook(next ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error {
		err := next(ctx, cmd)
		if cmd.Name() != h.watchName {
			return err
		}
		h.once.Do(func() { close(h.entered) })
		// Post-next work: if Close does not wait for this host goroutine, Close
		// returns while we are still sleeping and finished is still false.
		time.Sleep(h.holdFor)
		h.finished.Store(true)
		return err
	}
}

func (h *fdCloseHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}

// TestFullDuplexCloseWaitsForHookHosts verifies AutoPipeliner.Close does not
// return until a full-duplex command's post-next ProcessHook has finished. The FD
// hook host is the only goroutine that closes such a command's batch; before the
// fix it was untracked, so Close could return while accepted commands were still
// blocked behind post-reply hooks (drain-before-return contract violated).
func TestFullDuplexCloseWaitsForHookHosts(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}

	hook := &fdCloseHook{entered: make(chan struct{}), holdFor: 120 * time.Millisecond, watchName: "set"}
	c.AddHook(hook)

	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	// Fire the command on the async face and do NOT read its result (reading would
	// itself block on the host closing the batch, hiding the bug). Wait until the
	// host is in its post-next phase, then Close must wait for it to finish.
	ap.Set(ctx, "fd:close:k", "v", 0)
	select {
	case <-hook.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("post-next hook never ran — command did not reach the FD reader")
	}

	if err := ap.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if !hook.finished.Load() {
		t.Fatal("Close returned before the post-next ProcessHook finished — FD hook host not waited (drain-before-return violated)")
	}
}

// fdSelfReadHook calls next and then reads the command's OWN result
// (cmd.Err()), the documented pattern also exercised by the async autopipeline
// hook tests. On the full-duplex path the host goroutine is the only code that
// closes the command's batch, so without the executor guard this read blocks on
// batch.done forever (#3964, P1). It records the error it observed.
type fdSelfReadHook struct {
	watchName string
	observed  chan error
}

func (h *fdSelfReadHook) DialHook(next DialHook) DialHook { return next }

func (h *fdSelfReadHook) ProcessHook(next ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error {
		err := next(ctx, cmd)
		if cmd.Name() != h.watchName {
			return err
		}
		// Read own result after next: must return the just-executed view, not
		// block on the batch this very goroutine is responsible for closing.
		got := cmd.Err()
		select {
		case h.observed <- got:
		default:
		}
		return err
	}
}

func (h *fdSelfReadHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}

// TestFullDuplexHookReadingOwnResultDoesNotDeadlock verifies a ProcessHook that
// reads its command's result after next() completes instead of hanging, on the
// full-duplex path. Before the fix the FD host goroutine did not mark itself as
// the batch executor, so cmd.Err() inside the hook blocked on batch.done (which
// only that goroutine closes) — a hard deadlock.
func TestFullDuplexHookReadingOwnResultDoesNotDeadlock(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}

	hook := &fdSelfReadHook{watchName: "set", observed: make(chan error, 1)}
	c.AddHook(hook)

	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	// Guard the whole command with a timeout: a deadlock leaves the hook (and so
	// the caller awaiting the batch) blocked forever.
	var callerErr atomic.Value
	done := make(chan struct{})
	go func() {
		if e := ap.Set(ctx, "fd:selfread:k", "v", 0).Err(); e != nil {
			callerErr.Store(e)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("deadlock: hook read its command result after next and blocked on its own FD batch")
	}
	if v := callerErr.Load(); v != nil {
		t.Fatalf("set: %v", v)
	}
	select {
	case got := <-hook.observed:
		if got != nil {
			t.Fatalf("hook observed err=%v after next, want nil (just-executed view)", got)
		}
	default:
		t.Fatal("hook did not run on the FD path")
	}
}
