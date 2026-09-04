package redis

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/otel"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/push"
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

// TestFullDuplexReturnRunsPoolHooks verifies the held FD connection passes back
// through the pipeline pool's PoolHook Get/Put path on lease/return — that path
// is what gives per-conn hooks (maintnotifications, streaming-creds re-auth) a
// chance to run.
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

	// The engine leases lazily — on the first command, not at startup (an unused FD
	// autopipeliner must stay idle rather than dial in the background, #3964). Submit
	// one command to trigger the lease; after serving it the engine idle-returns the
	// conn, so a Get and a Put are both observed by the pool hook.
	if err := ap.Set(ctx, "fd:hook:k", "v", 0).Err(); err != nil {
		t.Fatalf("set: %v", err)
	}
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
	if err := ap.Set(ctx, "fd:hook:k2", "v", 0).Err(); err != nil {
		t.Fatalf("post-return set: %v", err)
	}
	if v, err := ap.Get(ctx, "fd:hook:k2").Result(); err != nil || v != "v" {
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

// TestFullDuplexStaysAlignedUnderConcurrentMutation verifies the ordered
// full-duplex reader keeps command↔reply FIFO alignment while a SECOND client
// mutates the key concurrently: every GET after the mutation returns the new
// value and never errors. It does NOT cover the reader's RESP3 push-drain — no
// invalidation push can reach the FD conn (pipeline-pool connections are excluded
// from CLIENT TRACKING), so push demux is covered by the maintnotifications e2e
// suite instead.
func TestFullDuplexStaysAlignedUnderConcurrentMutation(t *testing.T) {
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

	key := "fd:align:key"
	if err := c.Set(ctx, key, "v0", 0).Err(); err != nil {
		t.Fatalf("seed set: %v", err)
	}
	if v, err := ap.Get(ctx, key).Result(); err != nil || v != "v0" {
		t.Fatalf("prime GET: v=%q err=%v", v, err)
	}

	other := NewClient(&Options{Addr: addr})
	defer other.Close()
	if err := other.Set(ctx, key, "v1", 0).Err(); err != nil {
		t.Fatalf("concurrent set: %v", err)
	}

	for i := 0; i < 100; i++ {
		v, err := ap.Get(ctx, key).Result()
		if err != nil {
			t.Fatalf("GET %d after concurrent mutation: %v (FIFO misalignment?)", i, err)
		}
		if v != "v1" {
			t.Fatalf("GET %d: got %q want %q (reply/command misaligned)", i, v, "v1")
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
	// The submit queue is capped (min(window, 4096)): a buffered channel
	// allocates its full capacity eagerly, so a window-sized queue would cost
	// several MiB per engine up front; backpressure comes from the in-flight
	// deque, which grows only with actual in-flight.
	if want := 4096; cap(ap.fd.ch) != want {
		t.Fatalf("queue capacity %d, want %d (capped; window %d bounds in-flight, not the queue)",
			cap(ap.fd.ch), want, fdDefaultWindow)
	}
	if ap.fd.window != fdDefaultWindow {
		t.Fatalf("window %d, want default %d", ap.fd.window, fdDefaultWindow)
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

// TestFullDuplexHookShortCircuit exercises the short-circuit path, where host and
// reader both finalize the command: it must be race-free (-race), the caller must
// see the hook's error, and the command must still execute on the wire (FD cannot
// un-send an already-queued command). Guards the race where the host releases the
// caller before the reader has finished writing into the command.
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

// TestFDShutdownFlushCompletesBetweenSessionsBacklog pins the Close contract for
// work accepted while NO session holds a connection: a command sitting in fd.ch
// (or an unacked carry) when Close wins the between-sessions race must be
// EXECUTED via the normal pipeline path, not failed ErrClosed. Drives
// shutdownFlush directly, which is what run()'s two shutdown sites call.
func TestFDShutdownFlushCompletesBetweenSessionsBacklog(t *testing.T) {
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
	fd := ap.fd
	if fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	// Simulate the between-sessions Close: backlog queued in fd.ch (bypassing
	// submit — run() must not consume it, so this test does not race the engine's
	// own loop; the queue is drained below by takeQueue inside shutdownFlush).
	const n = 5
	reqs := make([]fdReq, n)
	for i := 0; i < n; i++ {
		cmd := NewStatusCmd(ctx, "set", fmt.Sprintf("fdsf:%d", i), fmt.Sprintf("v%d", i))
		reqs[i] = fdReq{cmd: cmd, batch: newAPBatch()}
		fd.ch <- reqs[i]
	}
	// carry: an unacked tail from a failed session that was never re-leased.
	carryCmd := NewStatusCmd(ctx, "set", "fdsf:carry", "vc")
	carry := []fdReq{{cmd: carryCmd, batch: newAPBatch()}}

	fd.shutdownFlush(context.Background(), carry)

	// Every batch settles and every command EXECUTED (not ErrClosed).
	for i := 0; i < n; i++ {
		select {
		case <-reqs[i].batch.done:
		case <-time.After(5 * time.Second):
			t.Fatalf("backlog cmd %d never completed after shutdownFlush", i)
		}
		if err := reqs[i].cmd.rawErr(); err != nil {
			t.Fatalf("backlog cmd %d err = %v, want executed (nil)", i, err)
		}
	}
	if err := carryCmd.rawErr(); err != nil {
		t.Fatalf("carry cmd err = %v, want executed (nil)", err)
	}
	verify := NewClient(&Options{Addr: ":6379"})
	defer verify.Close()
	for i := 0; i < n; i++ {
		if v, err := verify.Get(ctx, fmt.Sprintf("fdsf:%d", i)).Result(); err != nil || v != fmt.Sprintf("v%d", i) {
			t.Fatalf("fdsf:%d = %q, %v — accepted command was not executed on Close", i, v, err)
		}
	}
	if v, err := verify.Get(ctx, "fdsf:carry").Result(); err != nil || v != "vc" {
		t.Fatalf("fdsf:carry = %q, %v — carry was not executed on Close", v, err)
	}
	for i := 0; i < n; i++ {
		verify.Del(ctx, fmt.Sprintf("fdsf:%d", i))
	}
	verify.Del(ctx, "fdsf:carry")
}

// failWriteNetConn is a net.Conn whose Write always fails, so a buffered flush
// (the end of WithWriter) returns an error while the command bytes were already
// buffered — used to drive a write failure in writeCarryChunked deterministically.
type failWriteNetConn struct{ mockNetConn }

func (c *failWriteNetConn) Write(b []byte) (int, error) {
	return 0, errors.New("write boom")
}

// TestWriteCarryChunkedRecoversSuffixOnWriteError pins deque recovery of a
// partially written carry: writeBatch pushes each chunk into the in-flight deque
// BEFORE writing it, so when a multi-chunk carry write fails the un-written
// suffix must be pushed too — otherwise those accepted commands are in neither
// fd.ch nor the deque and their callers hang. Deterministic and dial-free.
// handoffSimHook stands in for the maintnotifications OnPut hook in tests that do
// not wire up the full manager: a connection marked for handoff is taken out of
// rotation on Put, so the FD writer's clean handoff recycle (which Puts the conn)
// makes progress. The real hook instead clears the mark (MarkQueuedForHandoff) and
// reconnects the conn seamlessly; either way a moving conn is never handed straight
// back to the writer still marked, which is what this hook guarantees for the test.
type handoffSimHook struct{}

func (handoffSimHook) OnGet(context.Context, *pool.Conn, bool) (bool, error) { return true, nil }

func (handoffSimHook) OnPut(_ context.Context, cn *pool.Conn) (shouldPool, shouldRemove bool, err error) {
	if cn.ShouldHandoff() {
		return false, true, nil // moving conn: take it out of rotation (fresh dial next lease)
	}
	return true, false, nil
}

func (handoffSimHook) OnRemove(context.Context, *pool.Conn, error) {}

func TestWriteCarryChunkedRecoversSuffixOnWriteError(t *testing.T) {
	cn := pool.NewConn(&failWriteNetConn{})
	fd := &fdEngine{
		ap:       &AutoPipeliner{config: &AutoPipelineOptions{}}, // MaxBatchBytes 0 = disabled
		client:   &Client{baseClient: &baseClient{opt: &Options{WriteTimeout: time.Second}}},
		maxBatch: 2, // 5 commands -> chunks [0:2] [2:4] [4:5]
	}
	inflight := newFDInflight()

	const n = 5
	carry := make([]fdReq, n)
	for i := range carry {
		carry[i] = fdReq{cmd: NewStatusCmd(context.Background(), "set", fmt.Sprintf("k%d", i), "v")}
	}

	if _, err := fd.writeCarryChunked(context.Background(), cn, inflight, carry, nil); err == nil {
		t.Fatal("writeCarryChunked returned nil error despite a failing write")
	}
	// The failing chunk was pushed by writeBatch; the suffix must be pushed too, so
	// the whole carry is recoverable by takeRemaining (without the fix only the
	// first chunk is present and the rest are lost).
	if got := inflight.len(); got != n {
		t.Fatalf("in-flight deque holds %d of %d carry commands after a write error — the unwritten suffix was dropped (callers would hang)", got, n)
	}
}

// TestWriteCarryChunkedStopsOnHandoff pins the clean-recycle contract (#3964
// review): a connection marked for handoff (MOVING/FAILING_OVER) while a recovered
// carry is being replayed must stop promptly and return the UNWRITTEN suffix
// out-of-band (errFDConnMoving) WITHOUT pushing it into the in-flight deque — so
// the caller drains the already-written prefix and replays only the never-sent
// suffix, never re-executing an already-sent command on the still-alive node.
func TestWriteCarryChunkedStopsOnHandoff(t *testing.T) {
	// Marked for handoff up front: the guard is at the TOP of the loop, before any
	// write, so the whole carry is the unwritten suffix and nothing is sent. A
	// failing-write conn is fine — writeBatch is never reached.
	cn := pool.NewConn(&failWriteNetConn{})
	if err := cn.MarkForHandoff(":6379", 1); err != nil {
		t.Fatalf("MarkForHandoff: %v", err)
	}
	fd := &fdEngine{
		ap:       &AutoPipeliner{config: &AutoPipelineOptions{}}, // MaxBatchBytes 0 = disabled
		client:   &Client{baseClient: &baseClient{opt: &Options{WriteTimeout: time.Second}}},
		maxBatch: 2,
	}
	inflight := newFDInflight()

	const n = 5
	carry := make([]fdReq, n)
	for i := range carry {
		carry[i] = fdReq{cmd: NewStatusCmd(context.Background(), "set", fmt.Sprintf("k%d", i), "v")}
	}

	// readerDone open (not closed) so the handoff branch fires, not the reader-gone one.
	readerDone := make(chan struct{})
	suffix, err := fd.writeCarryChunked(context.Background(), cn, inflight, carry, readerDone)
	if !errors.Is(err, errFDConnMoving) {
		t.Fatalf("writeCarryChunked returned %v, want errFDConnMoving after the conn was marked for handoff", err)
	}
	// The whole (unwritten) carry is returned as the suffix to replay on the next lease.
	if len(suffix) != n {
		t.Fatalf("returned suffix has %d of %d carry commands — the tail was dropped (callers would hang)", len(suffix), n)
	}
	// Critically, the suffix must NOT be in the in-flight deque: a clean drain of the
	// (empty) written prefix must not wait on replies for commands that were never sent.
	if got := inflight.len(); got != 0 {
		t.Fatalf("in-flight deque holds %d entries after a clean handoff recycle — the unwritten suffix must stay out-of-band, not be pushed", got)
	}
}

// TestFullDuplexCloseCompletesBacklogAfterConnKill is a broad bounded-hang guard:
// killing the connection mid-flight and then closing the engine must settle every
// accepted command (via the connection-error recovery + drainQueue path) rather
// than hang a caller. End-to-end only; the chunked-carry suffix case is pinned
// deterministically by TestWriteCarryChunkedRecoversSuffixOnWriteError.
func TestFullDuplexCloseCompletesBacklogAfterConnKill(t *testing.T) {
	ctx := context.Background()
	if err := NewClient(&Options{Addr: ":6379"}).Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	const delay = 40 * time.Millisecond
	paddr, stop := delayReplyProxy(t, "127.0.0.1:6379", delay)
	defer stop()

	c := fdTestClient(paddr)
	defer c.Close()
	const window, maxBatch = 8, 4 // small so the backlog spans multiple write chunks
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex:       true,
		FullDuplexWindow: window,
		MaxBatchSize:     maxBatch,
	})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	// Burst far more than the window so a backlog builds behind the lagging reader
	// (in fd.ch and the in-flight deque). Submit off-goroutine: backpressure blocks
	// ap.Set once full, and Close (ap.ctx cancel) releases it.
	const N = 200
	cmds := make([]*StatusCmd, N)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < N; i++ {
			cmds[i] = ap.Set(ctx, fmt.Sprintf("fdkill:%d", i), "v", 0)
		}
	}()

	// Let a backlog accumulate, then kill the connection mid-flight and close the
	// engine: the Close flush hits a dead connection with a multi-chunk backlog.
	time.Sleep(delay / 2)
	stop() // kill live conns so the next write/read fails

	closed := make(chan struct{})
	go func() { _ = ap.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("ap.Close() hung after conn kill — the Close flush blocked the reader on replies that never arrive")
	}
	wg.Wait() // every ap.Set returned (accepted, or released by Close)

	// Every command's future MUST settle — a dropped/unrecovered command would
	// block forever here. Bound it so a regression fails loudly instead of
	// deadlocking the whole test binary. Values or errors are both fine; only a
	// hang is a failure.
	done := make(chan struct{})
	go func() {
		for i := 0; i < N; i++ {
			if cmds[i] != nil {
				_ = cmds[i].Err()
			}
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("some accepted commands never completed after conn kill + Close — chunked backlog/carry dropped (callers would hang)")
	}
}

// TestFullDuplexBlockingFace pins full-duplex on the BLOCKING AutoPipeline face:
// the engine activates, a single caller gets normal synchronous semantics (result
// already on the returned cmd, errors surface on the call), per-goroutine
// ordering holds by construction (each call waits), many concurrent blocking
// callers all complete correctly, and Close is clean.
func TestFullDuplexBlockingFace(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := c.AutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AutoPipelineWithOptions: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active on the blocking face")
	}
	if !ap.blocking {
		t.Fatal("expected the blocking face")
	}

	// Single caller: the call itself blocks until executed — the returned cmd
	// already holds its result, no accessor gating involved.
	if err := ap.Set(ctx, "fdblk:k", "v1", 0).Err(); err != nil {
		t.Fatalf("set: %v", err)
	}
	if v, err := ap.Get(ctx, "fdblk:k").Result(); err != nil || v != "v1" {
		t.Fatalf("get = %q, %v; want v1 (synchronous read-your-write)", v, err)
	}

	// A per-command Redis error surfaces on the call, synchronously.
	if err := ap.LPush(ctx, "fdblk:k", "x").Err(); err == nil ||
		!strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("LPush on a string = %v; want WRONGTYPE surfaced on the blocking call", err)
	}
	// And the stream is not desynced by it.
	if v, err := ap.Get(ctx, "fdblk:k").Result(); err != nil || v != "v1" {
		t.Fatalf("get after WRONGTYPE = %q, %v; want v1", v, err)
	}

	// Per-goroutine ordering by construction: INCR sequence observed in order.
	ap.Del(ctx, "fdblk:ctr")
	for i := 1; i <= 20; i++ {
		n, err := ap.Incr(ctx, "fdblk:ctr").Result()
		if err != nil || n != int64(i) {
			t.Fatalf("incr %d = %d, %v; want %d (per-goroutine order)", i, n, err, i)
		}
	}

	// Many concurrent blocking callers: all complete with their own results.
	const G, M = 16, 25
	var wg sync.WaitGroup
	errs := make(chan error, G)
	for g := 0; g < G; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < M; i++ {
				key := fmt.Sprintf("fdblk:g%d:%d", g, i)
				if err := ap.Set(ctx, key, key, 0).Err(); err != nil {
					errs <- fmt.Errorf("g%d set %d: %w", g, i, err)
					return
				}
				if v, err := ap.Get(ctx, key).Result(); err != nil || v != key {
					errs <- fmt.Errorf("g%d get %d = %q, %v", g, i, v, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	select {
	case err := <-errs:
		t.Fatal(err)
	default:
	}
	// Cleanup.
	for g := 0; g < G; g++ {
		for i := 0; i < M; i++ {
			ap.Del(ctx, fmt.Sprintf("fdblk:g%d:%d", g, i))
		}
	}
	ap.Del(ctx, "fdblk:k", "fdblk:ctr")
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

// TestFullDuplexFastSubmitCloseRace exercises the FullDuplexFastSubmit fast path
// under a concurrent Close. The non-blocking fast send runs under the SAME submit
// RLock as the blocking send, so a send can never win the race with the shutdown
// drain (WLock + closed + drain), and the pooled blocking batch is recycled on
// every reject path. Every caller must settle (no hang, no panic) with either a
// served result or a shutdown/ctx error. Run under -race to catch any data race
// on the fast path.
func TestFullDuplexFastSubmitCloseRace(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	// Window 4096 -> chCap 4096 -> gate fires while len(ch) < 410. With G blocking
	// submitters (each at most one outstanding) len(ch) <= G << 410, so the fast
	// path is live for the whole run and genuinely races Close.
	ap, err := c.AutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex: true, FullDuplexFastSubmit: true, FullDuplexWindow: 4096, MaxBatchSize: 8,
	})
	if err != nil {
		t.Fatalf("AutoPipelineWithOptions: %v", err)
	}
	if ap.fd == nil || !ap.fd.fastSubmit {
		t.Fatal("fast-submit full-duplex engine not active")
	}

	const G, K = 16, 2000
	var settled int64
	var wg sync.WaitGroup
	for g := 0; g < G; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for k := 0; k < K; k++ {
				// nil (served) or a shutdown/ctx error are both fine; the point is
				// that EVERY call RETURNS — none hangs on a req stranded in the
				// channel past the drain, and none panics.
				_ = ap.Set(ctx, fmt.Sprintf("fsc:%d:%d", g, k), "v", 0).Err()
				atomic.AddInt64(&settled, 1)
			}
		}(g)
	}
	// Close while submitters are mid-flight so the fast send interleaves with the
	// shutdown drain.
	time.Sleep(20 * time.Millisecond)
	if err := ap.Close(); err != nil {
		t.Fatalf("Close during fast-submit: %v", err)
	}
	doneCh := make(chan struct{})
	go func() { wg.Wait(); close(doneCh) }()
	select {
	case <-doneCh:
	case <-time.After(15 * time.Second):
		t.Fatalf("callers hung after Close: settled %d/%d", atomic.LoadInt64(&settled), G*K)
	}
	if got := atomic.LoadInt64(&settled); got != int64(G*K) {
		t.Fatalf("not all callers settled: %d/%d", got, G*K)
	}
	// Prove the fast path was actually exercised — otherwise this test would pass
	// on the pre-existing blocking path alone and cover nothing.
	if took := ap.fd.fastSubmitTake.Load(); took == 0 {
		t.Fatal("fast-submit path was never taken — config did not exercise it")
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
// promptly. Ordering across the divert boundary is deliberately not asserted.
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

// TestFullDuplexEngineReapedOnClonePoolClose pins that closing a WithTimeout clone
// (which shares the pools but not the original wrapper's cached autopipeliner) still
// stops the ORIGINAL engine's goroutines. The clone's Close only sets the shared
// apClosed flag and runs the shared onClose hooks; without the hook that cancels the
// cached ap's context, run() would park on fd.ch forever (new submits are rejected,
// so nothing wakes it). The existing rejection test does not catch this because a
// parked engine still refuses submits.
func TestFullDuplexEngineReapedOnClonePoolClose(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	// Warm one cycle so one-time client goroutines exist before the baseline.
	if ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true}); err == nil {
		_ = ap.Set(ctx, "clonereap:warm", "1", 0).Err()
		ap.Close()
	}
	time.Sleep(100 * time.Millisecond)
	base := runtime.NumGoroutine()

	// Create the engine on the ORIGINAL client and leave it running (do NOT Close it).
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}
	for i := 0; i < 50; i++ {
		_ = ap.Set(ctx, "clonereap:"+itoa(i), itoa(i), 0)
	}

	// Close a WithTimeout clone: shares the pools + onClose registry, never touches
	// the original's cached ap. It must still reap the original engine.
	clone := c.WithTimeout(5 * time.Second)
	if err := clone.Close(); err != nil {
		t.Fatalf("clone close: %v", err)
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
		t.Fatalf("FD engine leaked after WithTimeout-clone Close: base=%d now=%d (shared close did not cancel the original ap)", base, now)
	}
}

// TestFullDuplexEngineReapedForClientAndClone pins the per-engine close-hook id: a
// client AND a WithTimeout clone can each cache the same face, sharing the onClose
// registry. With a per-slot CONSTANT hook id the clone's registration overwrote the
// client's, so closing one sharer cancelled only one engine and leaked the other.
// A unique-per-engine id keeps both callbacks, so closing the shared pools reaps
// both engines.
func TestFullDuplexEngineReapedForClientAndClone(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true}); err == nil {
		_ = ap.Set(ctx, "cloneboth:warm", "1", 0).Err()
		ap.Close()
	}
	time.Sleep(100 * time.Millisecond)
	base := runtime.NumGoroutine()

	// The client caches its own engine...
	apC, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil || apC.fd == nil {
		t.Fatalf("client AsyncAutoPipeline: ap=%v err=%v", apC, err)
	}
	for i := 0; i < 25; i++ {
		_ = apC.Set(ctx, "cloneboth:c:"+itoa(i), itoa(i), 0)
	}
	// ...and a WithTimeout clone caches its OWN engine of the same face, on the
	// shared onClose registry.
	clone := c.WithTimeout(5 * time.Second)
	apK, err := clone.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil || apK.fd == nil {
		t.Fatalf("clone AsyncAutoPipeline: ap=%v err=%v", apK, err)
	}
	for i := 0; i < 25; i++ {
		_ = apK.Set(ctx, "cloneboth:k:"+itoa(i), itoa(i), 0)
	}

	// Closing the clone closes the shared pools; BOTH engines must be reaped.
	if err := clone.Close(); err != nil {
		t.Fatalf("clone close: %v", err)
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
		t.Fatalf("engine leaked after clone Close with two cached engines: base=%d now=%d (a per-slot constant hook id overwrote one registration)", base, now)
	}
}

// TestFullDuplexSkipsShardQueuePrealloc pins that full-duplex mode does not
// preallocate the (unused) per-stripe shard queues to MaxBatchSize: submissions go
// to the FD engine, no shard flusher runs, so a large MaxBatchSize with a small
// window must not allocate MaxBatchSize slots per stripe up front (OOM risk).
func TestFullDuplexSkipsShardQueuePrealloc(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex:       true,
		MaxBatchSize:     1_000_000,
		FullDuplexWindow: 8,
	})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}
	for i, s := range ap.shards {
		for j := range s.stripes {
			if got := cap(s.stripes[j].queue); got != 0 {
				t.Fatalf("shard %d stripe %d queue cap = %d, want 0 (FD must not preallocate unused shard buffers)", i, j, got)
			}
		}
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

// TestFDInflightHardCloseTakesUnackedTail pins deque ownership: an entry the
// reader has completed+advanced must NEVER also be returned by the recovery path.
// hardClose stops the reader; takeRemaining (called after the reader would have
// exited) returns exactly the un-advanced tail, in order, so completed commands
// are never replayed or failed a second time. Pure, no server.
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

// TestFDInflightOwnershipPartition pins the deque partition invariant: with the
// correct usage (hardClose, then takeRemaining ONLY after the reader has exited)
// every entry is owned by EXACTLY ONE side — advanced by the reader OR returned
// by takeRemaining, never both and never neither. A reader drains+advances while
// a second goroutine races a hardClose, so -race also proves advance/hardClose/
// takeRemaining are lock-clean. The session-level path (take the tail only after
// <-readerDone) is covered end-to-end by TestFullDuplexRecoversFromConnKill.
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
// accounts the Limiter once per written batch (Allow before the flush,
// ReportResult on the reply side once the chunk's replies land), balanced 1:1.
type fdCountLimiter struct{ allow, report atomic.Int64 }

func (l *fdCountLimiter) Allow() error         { l.allow.Add(1); return nil }
func (l *fdCountLimiter) ReportResult(_ error) { l.report.Add(1) }

// TestFullDuplexLimiterPairing verifies FullDuplex honors opt.Limiter with
// per-batch accounting: every written chunk is bracketed by exactly one Allow
// and one ReportResult (strict pairing), and the Limiter is never bypassed.
func TestFullDuplexLimiterPairing(t *testing.T) {
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
	// A tiny idle window so any late write settles before we read the counters.
	time.Sleep(20 * time.Millisecond)
	// Close waits for the engine (ap.wg), so every batch's deferred
	// ReportResult has run by the time Close returns.
	if err := ap.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	a, r := lim.allow.Load(), lim.report.Load()
	if a < 1 {
		t.Fatalf("FullDuplex never called Limiter.Allow (allow=%d) — Limiter bypassed", a)
	}
	if a != r {
		t.Fatalf("FullDuplex Limiter unbalanced: Allow=%d ReportResult=%d (must be 1:1 per written batch)", a, r)
	}
}

// TestFullDuplexRetryDivertsToNormalConn verifies the mechanism the FD reader
// uses for a retryable Redis error (LOADING/READONLY/…): the command is re-run on
// the client's normal path and the caller is settled with that result — not left
// with the FD error. (Redirects no longer divert on the standalone FD path — they
// settle inline, since the normal path cannot route a MOVED/ASK anyway.) It drives
// retryOnNormalConn directly
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
	// startAttempt 1 mirrors the retryable-reply path (the FD socket attempt is
	// already spent), so the normal-path loop stays within the retry budget.
	cmd := NewStringCmd(ctx, "get", "fd:retry:k")
	b := newAPBatch()
	cmd.setReady(b)
	ap.fd.retryOnNormalConn(fdReq{cmd: cmd, batch: b}, 1)

	select {
	case <-b.done:
	case <-time.After(2 * time.Second):
		t.Fatal("retryOnNormalConn did not complete the diverted command")
	}
	if v, err := cmd.Result(); err != nil || v != "v" {
		t.Fatalf("diverted GET = %q err=%v, want \"v\" (re-run on the normal path)", v, err)
	}
}

// TestRetryStartAttempt pins the retry-budget accounting for FD diverts (#3964
// review): a MOVING/ASK redirect did not execute on the FD socket, so it gets the
// full budget (startAttempt 0); a retryable reply (LOADING/READONLY/TRYAGAIN)
// already spent the initial attempt on the FD socket, so the normal-path loop
// starts at attempt 1 — otherwise the command runs MaxRetries+2 times, one over
// budget.
func TestRetryStartAttempt(t *testing.T) {
	cases := []struct {
		name        string
		moved, ask  bool
		wantAttempt int
	}{
		{"moved", true, false, 0},
		{"ask", false, true, 0},
		{"retryable", false, false, 1},
	}
	for _, tc := range cases {
		if got := retryStartAttempt(tc.moved, tc.ask); got != tc.wantAttempt {
			t.Errorf("retryStartAttempt(moved=%v, ask=%v) = %d, want %d", tc.moved, tc.ask, got, tc.wantAttempt)
		}
	}
}

// fdOtelRecorder counts RecordOperationDuration to prove the full-duplex reader
// emits the native per-command OTel metric itself (it bypasses process, which
// would otherwise emit it). All other Recorder methods are no-ops.
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
// native per-command OTel duration metric (redisotel-native) itself, which it
// must because it completes commands without going through process().
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

// TestFullDuplexDivertsHImportOffPipe verifies the FD engine routes a managed
// HIMPORT command off the shared pipe to the normal Process path: the FD writer
// never injects the registered PREPARE, so an HIMPORT SET riding the pipe can
// fail "no such fieldset". The assertion is routing, not end-to-end HIMPORT
// (which needs Redis 8.10+): a diverted command runs on the MAIN pool while an
// FD-pipe command never touches it, so a main-pool Hits/Misses bump after a lone
// HImportSet proves the divert. Its own result is irrelevant — it may error on an
// old server.
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
// for post-next hook work before returning. It signals once when it has entered
// the post-next phase, then holds briefly and records completion.
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
// hook host is the only goroutine that closes such a command's batch, so an
// untracked host would let Close return with accepted commands still blocked
// behind post-reply hooks — violating drain-before-return.
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
// batch.done forever. It records the error it observed.
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
// full-duplex path: unless the FD host marks itself as the batch executor,
// cmd.Err() inside the hook blocks on batch.done — which only that same goroutine
// closes, a hard deadlock.
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

// fdPrePanicHook panics BEFORE calling next, to prove the FD host waits for the
// reader to finish writing the command before releasing the caller: the command
// is already streamed, so closing the batch immediately would race the reader's
// write into cmd against the caller's read of it.
type fdPrePanicHook struct{ watchName string }

func (h *fdPrePanicHook) DialHook(next DialHook) DialHook { return next }
func (h *fdPrePanicHook) ProcessHook(next ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error {
		if cmd.Name() == h.watchName {
			panic("boom: hook panic before next")
		}
		return next(ctx, cmd)
	}
}

func (h *fdPrePanicHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}

// TestFullDuplexPreNextHookPanicSettlesWithoutRace verifies a ProcessHook that
// panics before next() surfaces an error to the caller, does not hang, and does
// not race the reader's write into the command (run with -race): a recover that
// closed the batch without first awaiting hookDone would let the reader write cmd
// after the caller had already observed the failure.
func TestFullDuplexPreNextHookPanicSettlesWithoutRace(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	c.AddHook(&fdPrePanicHook{watchName: "set"})
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}
	done := make(chan error, 1)
	go func() { done <- ap.Set(ctx, "fd:prepanic:k", "v", 0).Err() }()
	select {
	case e := <-done:
		if e == nil {
			t.Fatal("expected the hook panic to surface as an error")
		}
	case <-time.After(3 * time.Second):
		t.Fatal("caller hung after a pre-next hook panic")
	}
}

// TestFDFirstNoRetry verifies the tail-split index that lets the FD retry path
// replay the retryable PREFIX of an unacked tail while never re-sending a SENT
// NoRetry command (or anything ordered after it). The gate is sent-aware: a
// NoRetry command that never reached the wire does not split the tail —
// re-issuing it is its first send (see also TestFDFirstNoRetrySentGate).
func TestFDFirstNoRetry(t *testing.T) {
	ctx := context.Background()
	retry := fdReq{cmd: NewStringCmd(ctx, "get", "k"), sent: true}  // NoRetry() == false
	nore := fdReq{cmd: NewRawWriteToCmd(ctx, nil, "x"), sent: true} // NoRetry() == true, sent
	noreUnsent := fdReq{cmd: NewRawWriteToCmd(ctx, nil, "x")}       // NoRetry() == true, never sent
	cases := []struct {
		name string
		in   []fdReq
		want int
	}{
		{"empty", nil, 0},
		{"all-retryable", []fdReq{retry, retry}, 2},
		{"leading-noretry", []fdReq{nore, retry}, 0},
		{"noretry-in-middle", []fdReq{retry, retry, nore, retry}, 2},
		{"trailing-noretry", []fdReq{retry, nore}, 1},
		{"unsent-noretry-replayable", []fdReq{retry, noreUnsent, retry}, 3},
	}
	for _, tc := range cases {
		if got := fdFirstNoRetry(tc.in); got != tc.want {
			t.Fatalf("%s: fdFirstNoRetry = %d, want %d", tc.name, got, tc.want)
		}
	}
}

// TestFDBatchEnd verifies the replay chunk boundaries: the recovered tail is
// re-issued in the same MaxBatchSize/MaxBatchBytes-capped chunks as freshly
// drained work, so a large recovered window is not flushed in one oversized write.
func TestFDBatchEnd(t *testing.T) {
	ctx := context.Background()
	mk := func(val string) fdReq { return fdReq{cmd: NewStatusCmd(ctx, "set", "k", val)} }

	small := make([]fdReq, 7)
	for i := range small {
		small[i] = mk("v")
	}
	// maxBatch cap, byteLimit disabled: chunks of maxBatch, last chunk the remainder.
	if got := fdBatchEnd(small, 0, 3, 0); got != 3 {
		t.Fatalf("maxBatch: end=%d want 3", got)
	}
	if got := fdBatchEnd(small, 6, 3, 0); got != 7 {
		t.Fatalf("tail remainder: end=%d want 7", got)
	}
	if got := fdBatchEnd(small[:1], 0, 3, 0); got != 1 {
		t.Fatalf("single element: end=%d want 1", got)
	}

	// byteLimit cap. Each command is ~1052 bytes (cmdApproxBytes: per-arg len + 16).
	big := make([]fdReq, 4)
	for i := range big {
		big[i] = mk(strings.Repeat("x", 1000))
	}
	// Limit below one command's size: the lone oversized command still goes (chunk 1).
	if got := fdBatchEnd(big, 0, 100, 500); got != 1 {
		t.Fatalf("byteLimit lone oversized: end=%d want 1", got)
	}
	// Limit spanning ~two commands: first always in, stop once the payload reaches it.
	if got := fdBatchEnd(big, 0, 100, 1500); got != 2 {
		t.Fatalf("byteLimit two: end=%d want 2", got)
	}
}

// fdPanicWriter panics from Write, simulating a user io.Writer that panics while
// a RawWriteToCmd's readReply streams the raw reply on the FD reader goroutine.
type fdPanicWriter struct{}

func (fdPanicWriter) Write(p []byte) (int, error) { panic("boom: reply decoder panic") }

// TestFullDuplexReaderPanicRecovers verifies a panic in reply decoding on the FD
// reader goroutine is recovered (not a process crash): the command is settled
// with an error and the engine keeps serving on a fresh session. A raw
// RawWriteToCmd rides the FD pipe and its writer panics while the reader streams
// the reply.
func TestFullDuplexReaderPanicRecovers(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if err := c.Set(ctx, "fd:rpanic:k", "hello", 0).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	cmd := NewRawWriteToCmd(ctx, fdPanicWriter{}, "get", "fd:rpanic:k")
	f := ap.Submit(ctx, cmd)
	done := make(chan struct{})
	go func() { _ = f.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("command never settled after a reader-decoder panic (reader did not recover)")
	}
	if cmd.Err() == nil {
		t.Fatal("expected an error after the reader-decoder panic, got nil")
	}
	// The engine must survive: a subsequent command works on a fresh session.
	if err := ap.Set(ctx, "fd:rpanic:after", "v", 0).Err(); err != nil {
		t.Fatalf("engine did not recover after the reader panic: %v", err)
	}
}

// fdNoopHook is a passthrough ProcessHook whose only effect is to make the FD
// engine host each command (hookCount > 0), so every completed command has a
// hookDone channel — a double-complete would then double-close it (panic).
type fdNoopHook struct{}

func (fdNoopHook) DialHook(next DialHook) DialHook                                  { return next }
func (fdNoopHook) ProcessHook(next ProcessHook) ProcessHook                         { return next }
func (fdNoopHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook { return next }

// TestFullDuplexReaderPanicMidBatchNoDoubleComplete guards the reader-panic
// recover path: when the panic hits a command sharing a frontBatch snapshot with
// EARLIER, already-completed commands, the recover must advance those out of the
// deque — otherwise recovery re-owns and re-completes them, double-closing their
// hookDone (a second panic that crashes the process). A no-op hook makes every
// command hooked, with completed GETs ahead of the panicking one in a batch.
func TestFullDuplexReaderPanicMidBatchNoDoubleComplete(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	c.AddHook(fdNoopHook{})
	if err := c.Set(ctx, "fd:rpanic2:k", "hello", 0).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	// Fire several hooked GETs immediately followed by a decoder-panicking command,
	// none awaited, so they land in one writer batch / reader snapshot: the GETs
	// complete (done > 0) before the panic.
	for i := 0; i < 8; i++ {
		ap.Get(ctx, "fd:rpanic2:k")
	}
	panicCmd := NewRawWriteToCmd(ctx, fdPanicWriter{}, "get", "fd:rpanic2:k")
	f := ap.Submit(ctx, panicCmd)
	done := make(chan struct{})
	go func() { _ = f.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("panicking command never settled")
	}
	// If the completed GETs were re-completed, their hookDone double-close would
	// have crashed the process. Reaching here on a working engine proves it did not.
	if err := ap.Set(ctx, "fd:rpanic2:after", "v", 0).Err(); err != nil {
		t.Fatalf("engine did not recover after the mid-batch reader panic: %v", err)
	}
}

var errFDLimiterOpen = errors.New("fd test: limiter breaker open")

// fdRejectLimiter denies every Allow(), simulating an open circuit breaker.
type fdRejectLimiter struct{ allow atomic.Int64 }

func (l *fdRejectLimiter) Allow() error         { l.allow.Add(1); return errFDLimiterOpen }
func (l *fdRejectLimiter) ReportResult(_ error) {}

// TestFullDuplexLimiterRejectFailsQueuedWork verifies that when the Limiter
// denies chunk admission at write time (writeBatch), accepted commands
// fail-fast with the limiter error instead of hanging in fd.ch until the
// breaker closes.
func TestFullDuplexLimiterRejectFailsQueuedWork(t *testing.T) {
	ctx := context.Background()
	probe := NewClient(&Options{Addr: ":6379"})
	defer probe.Close()
	if err := probe.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}

	c := NewClient(&Options{
		Addr:                    ":6379",
		Protocol:                3,
		PipelinePoolSize:        4,
		PipelineReadBufferSize:  64 * 1024,
		PipelineWriteBufferSize: 64 * 1024,
		PoolSize:                4,
		Limiter:                 &fdRejectLimiter{},
		MaxRetries:              2,
	})
	defer c.Close()
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	done := make(chan error, 1)
	go func() { done <- ap.Set(ctx, "fd:limreject:k", "v", 0).Err() }()
	select {
	case e := <-done:
		if !errors.Is(e, errFDLimiterOpen) {
			t.Fatalf("got %v, want the limiter error (fail-fast, not hang)", e)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("command hung on limiter rejection instead of failing fast")
	}
}

// TestFullDuplexProcessReportsSubmitRejection verifies raw Process(ctx,cmd)
// surfaces an FD submit-time rejection instead of returning nil: every submit
// reject path (closed / caller-ctx cancel / engine ctx) returns the shared
// completedBatch sentinel, which processAsync checks to report cmd.rawErr(). The
// closed path is the one exercised here; the others use the identical return.
func TestFullDuplexProcessReportsSubmitRejection(t *testing.T) {
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
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}
	if err := ap.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	// Raw Process on a closed FD autopipeliner must report ErrClosed, not nil.
	if err := ap.Process(ctx, NewStatusCmd(ctx, "ping")); !errors.Is(err, ErrClosed) {
		t.Fatalf("Process after Close = %v, want ErrClosed (submit rejection not surfaced)", err)
	}
}

// TestFullDuplexCloseFlushesBacklog verifies graceful Close executes the
// accepted-but-unwritten fd.ch commands instead of failing them ErrClosed
// ("accepted ⇒ completes"). A burst is submitted and Close called immediately, so
// some commands are still in the backlog when Close runs; none may be ErrClosed.
func TestFullDuplexCloseFlushesBacklog(t *testing.T) {
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
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	const n = 300
	cmds := make([]*StatusCmd, n)
	for i := 0; i < n; i++ {
		cmds[i] = ap.Set(ctx, "fd:closeflush:"+itoa(i), "v", 0)
	}
	if err := ap.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	for i, cmd := range cmds {
		if errors.Is(cmd.Err(), ErrClosed) {
			t.Fatalf("cmd %d came back ErrClosed — Close did not flush the accepted fd.ch backlog", i)
		}
	}
}

// TestFullDuplexLeaseFailureFailsBacklog verifies that when the engine cannot
// lease a connection for a new session (fdLeaseErr, server down), accepted
// commands fail-fast once the lease retries are exhausted instead of hanging in
// fd.ch. Uses a dead address, so it needs no live server.
func TestFullDuplexLeaseFailureFailsBacklog(t *testing.T) {
	ctx := context.Background()
	c := NewClient(&Options{
		Addr:                    "127.0.0.1:1", // nothing listening: dial refused
		Protocol:                3,
		PipelinePoolSize:        2,
		PipelineReadBufferSize:  64 * 1024,
		PipelineWriteBufferSize: 64 * 1024,
		PoolSize:                2,
		MaxRetries:              2,
		DialTimeout:             150 * time.Millisecond,
		MinRetryBackoff:         time.Millisecond,
		MaxRetryBackoff:         5 * time.Millisecond,
	})
	defer c.Close()
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	done := make(chan error, 1)
	go func() { done <- ap.Set(ctx, "fd:leasefail:k", "v", 0).Err() }()
	select {
	case e := <-done:
		if e == nil {
			t.Fatal("expected a connection error on a persistent lease failure, got nil")
		}
	case <-time.After(6 * time.Second):
		t.Fatal("command hung on a persistent lease failure instead of failing after retries")
	}
}

// TestFullDuplexMaxHoldIdleDoesNotChurn verifies that with FullDuplexMaxHold set
// shorter than FullDuplexIdleTimeout a quiet engine does NOT Get/Put-recycle
// every max-hold interval: the max-hold branch returns fdIdle when the pipe is
// drained, so run() blocks for the next command instead of re-leasing.
func TestFullDuplexMaxHoldIdleDoesNotChurn(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex:            true,
		FullDuplexMaxHold:     40 * time.Millisecond,
		FullDuplexIdleTimeout: 3 * time.Second,
	})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	// No work for several max-hold intervals. A drained engine must idle, not churn.
	time.Sleep(300 * time.Millisecond)
	if n := ap.fd.recycles.Load(); n > 2 {
		t.Fatalf("idle engine recycled %d times in 300ms (max-hold ~40ms) — it churns Get/Put when idle", n)
	}
}

// TestFullDuplexHandoffRecyclesPromptly verifies the writer observes a
// connection marked for maintenance handoff and ends the session with a clean
// recycle well before FullDuplexMaxHold, so the pool can queue the handoff at
// Put instead of the conn continuing to take writes to a moving node until
// max-hold. (Review thread #3789192590.)
func TestFullDuplexHandoffRecyclesPromptly(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	// Long max-hold and idle so ONLY a handoff can cause a prompt recycle.
	ap, err := c.AutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex:            true,
		Unordered:             false,
		MaxConcurrentBatches:  1,
		FullDuplexMaxHold:     30 * time.Second,
		FullDuplexIdleTimeout: 30 * time.Second,
	})
	if err != nil {
		t.Fatalf("AutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	// The FD handoff recycle Puts the moving conn so the maintnotifications OnPut
	// hook can hand it off. This test does not wire up that manager, so install a
	// stand-in hook that takes a marked conn out of rotation on Put — otherwise the
	// marked conn would be handed straight back and the recycle would livelock.
	if pp := c.getPipelinePool(); pp != nil {
		pp.AddPoolHook(handoffSimHook{})
	}

	// Start a session and wait until the engine holds a connection.
	if err := ap.Set(ctx, "fd:handoff:k", "v", 0).Err(); err != nil {
		t.Fatalf("initial set: %v", err)
	}
	cn := ap.fd.curConn.Load()
	for deadline := time.Now().Add(2 * time.Second); cn == nil && time.Now().Before(deadline); {
		time.Sleep(5 * time.Millisecond)
		cn = ap.fd.curConn.Load()
	}
	if cn == nil {
		t.Fatal("engine never exposed a held connection")
	}

	before := ap.fd.recycles.Load()
	// Mark the held conn for handoff, as a MOVING push handler would.
	if err := cn.MarkForHandoff(":6379", 1); err != nil {
		t.Fatalf("MarkForHandoff: %v", err)
	}
	// Keep issuing commands so the writer loops and observes ShouldHandoff; assert
	// a recycle happens far faster than the 30s max-hold.
	start := time.Now()
	recycled := false
	for time.Since(start) < 5*time.Second {
		_ = ap.Get(ctx, "fd:handoff:k").Err() // errors are fine; drives the loop
		if ap.fd.recycles.Load() > before {
			recycled = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !recycled {
		t.Fatalf("session did not recycle after handoff mark within 5s (max-hold is 30s) — writer is not observing ShouldHandoff")
	}
	if el := time.Since(start); el > 10*time.Second {
		t.Fatalf("recycle took %v, far above expectation", el)
	}
}

// fdEncoderPanicArg is a command argument whose BinaryMarshaler panics, so
// writeCmd (proto WriteArgs) panics mid-serialization - simulating a user
// BinaryMarshaler that blows up on the FD writer goroutine.
type fdEncoderPanicArg struct{}

func (fdEncoderPanicArg) MarshalBinary() ([]byte, error) { panic("boom: encoder panic") }

// TestFDWriteBatchStampsSentPerCommand pins finding eTWPc: writeBatch must stamp
// sent/writtenAt per-command INSIDE the serialize loop, so a command the
// serializer never reaches (behind an earlier encoder panic) keeps sent=false and
// its optimistic attempt refunded - otherwise a never-executed NoRetry behind the
// panic is failed unsent (acute at MaxRetries==0).
func TestFDWriteBatchStampsSentPerCommand(t *testing.T) {
	ctx := context.Background()
	cn := pool.NewConn(&mockNetConn{})
	fd := &fdEngine{
		ap:       &AutoPipeliner{config: &AutoPipelineOptions{}},
		client:   &Client{baseClient: &baseClient{opt: &Options{WriteTimeout: time.Second}}},
		maxBatch: 8,
	}
	inflight := newFDInflight()

	// index 1's encoder panics; indices 2 and 3 must never be serialized.
	reqs := []fdReq{
		{cmd: NewStatusCmd(ctx, "set", "k0", "v"), attempts: 1},
		{cmd: NewStatusCmd(ctx, "set", "k1", fdEncoderPanicArg{}), attempts: 1},
		{cmd: NewRawWriteToCmd(ctx, nil, "get", "k2"), attempts: 1}, // NoRetry() == true
		{cmd: NewStatusCmd(ctx, "set", "k3", "v"), attempts: 1},
	}

	err := fd.writeBatch(ctx, cn, inflight, reqs)
	if err == nil || !errors.Is(err, errFDPanicRecovered) {
		t.Fatalf("writeBatch err = %v, want errFDPanicRecovered", err)
	}

	if !reqs[0].sent {
		t.Error("reqs[0] not marked sent (it was serialized)")
	}
	if !reqs[1].sent {
		t.Error("reqs[1] not marked sent (the serializer reached it; it panicked)")
	}
	if reqs[2].sent {
		t.Error("reqs[2] marked sent but was never serialized (behind the panic)")
	}
	if reqs[3].sent {
		t.Error("reqs[3] marked sent but was never serialized (behind the panic)")
	}

	if reqs[0].attempts != 1 || reqs[1].attempts != 1 {
		t.Errorf("serialized-prefix attempts = %d,%d, want 1,1", reqs[0].attempts, reqs[1].attempts)
	}
	if reqs[2].attempts != 0 || reqs[3].attempts != 0 {
		t.Errorf("never-serialized-suffix attempts = %d,%d, want 0,0 (refunded)", reqs[2].attempts, reqs[3].attempts)
	}

	if inflight.len() != 4 {
		t.Fatalf("inflight.len() = %d, want 4 (whole batch recovered for settlement)", inflight.len())
	}

	// The never-serialized NoRetry (reqs[2]) must REPLAY, not fail: sent=false so it
	// is not a split point, and refunded so it is not budget-exhausted at MaxRetries=0.
	if n := fdFirstNoRetry(reqs[2:]); n != len(reqs[2:]) {
		t.Errorf("fdFirstNoRetry flagged a never-sent NoRetry at %d - it would be failed unsent", n)
	}
	kept, exhausted := fdPartitionByBudget(reqs[2:], 0)
	if len(kept) != 2 || len(exhausted) != 0 {
		t.Errorf("fdPartitionByBudget(suffix, 0) kept=%d exhausted=%d, want 2,0 - never-serialized suffix would be failed at MaxRetries=0", len(kept), len(exhausted))
	}
}

// fdFakeRedisErr implements the redis.Error interface (a custom
// PushNotificationProcessor could return one).
type fdFakeRedisErr struct{}

func (fdFakeRedisErr) Error() string { return "FAKE custom redis error" }
func (fdFakeRedisErr) RedisError()   {}

// TestFDPipelineErrConnUnusableStampsCmds pins finding eR1Nh: when a push-drain
// returns errConnUnusable WRAPPING a redis.Error, isRedisError classifies it as a
// reply, so the old guard skipped setCmdsErr and every command kept Err()==nil
// while Exec returned an error (the FD shutdown flush could then complete them as
// successes). The errConnUnusable marker must take precedence and stamp all cmds.
func TestFDPipelineErrConnUnusableStampsCmds(t *testing.T) {
	ctx := context.Background()
	c := NewClient(&Options{Addr: ":6379"})
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}

	wrapped := fmt.Errorf("%w: pipeline push drain: %w", errConnUnusable, fdFakeRedisErr{})
	// Preconditions that make the bug possible: isRedisError sees through the wrap,
	// yet the marker is present.
	if !isRedisError(wrapped) {
		t.Fatal("precondition: isRedisError(wrapped) should be true (it unwraps to a redis.Error)")
	}
	if !errors.Is(wrapped, errConnUnusable) {
		t.Fatal("precondition: errors.Is(wrapped, errConnUnusable) should be true")
	}

	cmds := []Cmder{
		NewStatusCmd(ctx, "ping"),
		NewStatusCmd(ctx, "ping"),
	}
	stub := func(context.Context, *pool.Conn, []Cmder) (bool, error) {
		return false, wrapped // no retry; behaves like a fatal push-drain error
	}

	err := c.generalProcessPipeline(ctx, cmds, stub, "test", 0)
	if !errors.Is(err, errConnUnusable) {
		t.Fatalf("generalProcessPipeline err = %v, want errConnUnusable-wrapped", err)
	}
	for i, cmd := range cmds {
		if cmd.Err() == nil {
			t.Errorf("cmd[%d].Err() == nil - a transport-desync error was left unstamped", i)
		}
	}
}

// fdCountingHook counts ProcessHook invocations.
type fdCountingHook struct{ n *int32 }

func (h fdCountingHook) DialHook(next DialHook) DialHook { return next }
func (h fdCountingHook) ProcessHook(next ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error {
		atomic.AddInt32(h.n, 1)
		return next(ctx, cmd)
	}
}

func (h fdCountingHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}

// TestFDLiveHookChain checks that the FD host runs the client's live process-hook
// chain. Hooks are added at construction (before traffic), so the FD host loads live
// hook state like the synchronous path — no submit-time snapshot.
func TestFDLiveHookChain(t *testing.T) {
	ctx := context.Background()
	c := NewClient(&Options{Addr: ":6379"}) // no dial: only hook methods are exercised
	defer c.Close()

	var h1 int32
	c.AddHook(fdCountingHook{&h1})

	cmd := NewStatusCmd(ctx, "ping")
	base := func(context.Context, Cmder) error { return nil }

	if err := c.withProcessHook(ctx, cmd, base); err != nil {
		t.Fatalf("withProcessHook: %v", err)
	}
	if got := atomic.LoadInt32(&h1); got != 1 {
		t.Fatalf("live hook ran %d times, want 1", got)
	}
}

// TestFDWriteBatchRefundsReplaySuffixByReached pins finding F1: writeBatch's
// recovery-push must refund the optimistic attempt by how far the serialize loop
// REACHED this call (index >= written), NOT by the lifetime `sent` flag. `sent` is
// sticky across replays, so on a SECOND-session replay every command already carries
// sent==true; keying the refund on !sent then skips the suffix that this call never
// serialized (an earlier command's encoder panicked), leaving it over-charged and
// budget-exhausted one replay early. At MaxRetries==1 that FAILS a command that was
// never actually re-issued.
func TestFDWriteBatchRefundsReplaySuffixByReached(t *testing.T) {
	ctx := context.Background()
	cn := pool.NewConn(&mockNetConn{})
	fd := &fdEngine{
		ap:       &AutoPipeliner{config: &AutoPipelineOptions{}},
		client:   &Client{baseClient: &baseClient{opt: &Options{WriteTimeout: time.Second}}},
		maxBatch: 8,
	}
	inflight := newFDInflight()

	// SECOND-session replay: every command was already sent in session 1 (sent==true,
	// sticky) and re-charged for this replay (attempts==2: submit + one replay bump,
	// like run()'s carry[i].attempts++). index 1's encoder panics, so indices 2 and 3
	// are never reached this call.
	reqs := []fdReq{
		{cmd: NewStatusCmd(ctx, "set", "k0", "v"), attempts: 2, sent: true},
		{cmd: NewStatusCmd(ctx, "set", "k1", fdEncoderPanicArg{}), attempts: 2, sent: true},
		{cmd: NewStatusCmd(ctx, "set", "k2", "v"), attempts: 2, sent: true},
		{cmd: NewStatusCmd(ctx, "set", "k3", "v"), attempts: 2, sent: true},
	}

	err := fd.writeBatch(ctx, cn, inflight, reqs)
	if err == nil || !errors.Is(err, errFDPanicRecovered) {
		t.Fatalf("writeBatch err = %v, want errFDPanicRecovered", err)
	}

	// The prefix REACHED this call keeps its charge: reqs[0] serialized cleanly,
	// reqs[1] panicked mid-serialize (its bytes may have reached the wire).
	if reqs[0].attempts != 2 || reqs[1].attempts != 2 {
		t.Errorf("reached-prefix attempts = %d,%d, want 2,2 (kept)", reqs[0].attempts, reqs[1].attempts)
	}
	// The suffix NOT reached this call must be refunded DESPITE sent==true — this is
	// the fix. A refund keyed on !sent would skip these (they carry the sticky flag).
	if reqs[2].attempts != 1 || reqs[3].attempts != 1 {
		t.Errorf("unreached-suffix attempts = %d,%d, want 1,1 (refunded despite sent==true)", reqs[2].attempts, reqs[3].attempts)
	}
	// Consequence: at MaxRetries==1 the refunded suffix stays eligible for one more
	// replay; over-charged (attempts 2 > 1) it would be declared budget-exhausted and
	// FAILED without ever being re-issued.
	kept, exhausted := fdPartitionByBudget(reqs[2:], 1)
	if len(kept) != 2 || len(exhausted) != 0 {
		t.Errorf("fdPartitionByBudget(suffix, 1) kept=%d exhausted=%d, want 2,0 (suffix loses its retry without the reached-based refund)", len(kept), len(exhausted))
	}
	if inflight.len() != 4 {
		t.Fatalf("inflight.len() = %d, want 4 (whole batch recovered for settlement)", inflight.len())
	}
}

// fdPanicAllowLimiter is a user Limiter whose Allow panics (user code that blows up
// on the engine's writer goroutine). ReportResult counts calls so the test can pin
// that a panicking Allow — like a deny — grants no permit and so reports nothing.
type fdPanicAllowLimiter struct{ reports atomic.Int64 }

func (l *fdPanicAllowLimiter) Allow() error         { panic("boom: limiter Allow panic") }
func (l *fdPanicAllowLimiter) ReportResult(_ error) { l.reports.Add(1) }

// TestFDWriteBatchRecoversPanickingLimiterAllow pins finding F4: Limiter.Allow runs
// on the FD writer goroutine BEFORE writeBatch arms its serialize-panic defer, so a
// panicking Allow (user code) would escape fd.run and crash the process with the
// accepted chunk unsettled. writeBatch must recover it, fail the chunk (deny
// semantics), and — because no permit was granted — never call ReportResult.
func TestFDWriteBatchRecoversPanickingLimiterAllow(t *testing.T) {
	ctx := context.Background()
	cn := pool.NewConn(&mockNetConn{})
	lim := &fdPanicAllowLimiter{}
	fd := &fdEngine{
		ap:       &AutoPipeliner{config: &AutoPipelineOptions{}},
		client:   &Client{baseClient: &baseClient{opt: &Options{WriteTimeout: time.Second, Limiter: lim}}},
		maxBatch: 8,
	}
	inflight := newFDInflight()

	reqs := []fdReq{
		{cmd: NewStatusCmd(ctx, "set", "k0", "v"), batch: newAPBatch(), attempts: 1},
		{cmd: NewStatusCmd(ctx, "set", "k1", "v"), batch: newAPBatch(), attempts: 1},
	}

	// The panic must be recovered inside writeBatch, not propagated to the caller.
	err := func() (e error) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("writeBatch propagated a limiter Allow panic instead of recovering it: %v", r)
			}
		}()
		return fd.writeBatch(ctx, cn, inflight, reqs)
	}()
	// Deny semantics: writeBatch returns nil (only THIS chunk failed), each command
	// carries the wrapped panic error, and the connection is untouched.
	if err != nil {
		t.Fatalf("writeBatch err = %v, want nil (a deny fails the chunk, not writeBatch)", err)
	}
	for i := range reqs {
		got := reqs[i].cmd.rawErr()
		if got == nil {
			t.Errorf("reqs[%d].Err() == nil, want the recovered panic error", i)
		} else if !errors.Is(got, errFDPanicRecovered) {
			t.Errorf("reqs[%d].Err() = %v, want errFDPanicRecovered", i, got)
		}
	}
	if reqs[0].sent || reqs[1].sent {
		t.Errorf("a command was marked sent after a denied (panicking) Allow — the connection must be untouched")
	}
	if inflight.len() != 0 {
		t.Errorf("inflight.len() = %d, want 0 — a denied chunk must never enter the in-flight deque", inflight.len())
	}
	// Strict pairing: a panicking Allow grants no permit, so there is NO ReportResult.
	if n := lim.reports.Load(); n != 0 {
		t.Errorf("ReportResult called %d times after a panicking Allow, want 0 (no permit -> no report)", n)
	}
}

// TestAutoPipelineConcurrentSharedDrainRunsOnce pins finding F3: the two close entry
// points for one engine — the explicit AutoPipeliner.Close and the shared-pool close
// hook (which calls cancelAndDrain WITHOUT setting ap.closed) — can run concurrently
// when a pool-sharing clone closes the shared pools while the owner calls Close. The
// drain body must run exactly ONCE (two concurrent drains would interleave drainAll's
// flushers->sweep->batchWg.Wait ordering and panic "WaitGroup misuse: Add called
// concurrently with Wait"), and both callers must get the SAME close error.
//
// No -race red-check is available: there is no shared-memory data race here (the
// hazard is a WaitGroup-misuse runtime panic, and each caller returns its own value).
// The red-check is the drainRuns counter: reverting cancelAndDrain to call drainBody
// directly (no drainOnce) makes both callers run the body, so drainRuns == 2.
func TestAutoPipelineConcurrentSharedDrainRunsOnce(t *testing.T) {
	c := NewClient(&Options{Addr: ":6379"}) // no dial: nothing is dispatched
	defer c.Close()
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{Unordered: true, NumShards: 4})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	errs := make([]error, 2)
	start := make(chan struct{})
	// Path A: the explicit Close (CAS on ap.closed, unregister hook, then drain).
	go func() { defer wg.Done(); <-start; errs[0] = ap.Close() }()
	// Path B: the shared-pool close hook path (cancelAndDrain, ap.closed left false).
	go func() { defer wg.Done(); <-start; errs[1] = ap.cancelAndDrain() }()
	close(start)
	wg.Wait()

	if got := ap.drainRuns.Load(); got != 1 {
		t.Fatalf("drain body ran %d times, want 1 — concurrent closers double-drained the engine", got)
	}
	if errs[0] != errs[1] {
		t.Fatalf("concurrent close paths returned different results: Close=%v hook=%v", errs[0], errs[1])
	}
	if errs[0] != nil {
		t.Fatalf("healthy concurrent close returned %v, want nil", errs[0])
	}
}

// fdPanicReportLimiter is a user Limiter whose ReportResult panics — the reply-side
// obligation report (fdLimiterReport.settle) runs on FD background goroutines: the
// reader's success path and the write-failure/settleTail paths. reports counts calls
// (incremented BEFORE the panic) so pairing stays assertable; panicOnce restricts the
// panic to the FIRST report for the manual red-check, so a removed recover exhibits
// the replay of an already-consumed reply rather than a cascade of crashes.
type fdPanicReportLimiter struct {
	reports   atomic.Int64
	panicOnce bool
}

func (l *fdPanicReportLimiter) Allow() error { return nil }

func (l *fdPanicReportLimiter) ReportResult(_ error) {
	n := l.reports.Add(1)
	if !l.panicOnce || n == 1 {
		panic("boom: limiter ReportResult panic")
	}
}

// fdCannedReplyConn is a net.Conn whose Write succeeds (a peer that accepts the
// batch) and whose Read serves preloaded RESP replies, so the FD reader decodes a
// real reply for every command. After the canned bytes drain it blocks until Close,
// so a stray read past the last reply never returns a spurious EOF mid-parse.
type fdCannedReplyConn struct {
	mockNetConn
	mu     sync.Mutex
	buf    *bytes.Reader
	closed chan struct{}
}

func newFDCannedReplyConn(reply []byte) *fdCannedReplyConn {
	return &fdCannedReplyConn{buf: bytes.NewReader(reply), closed: make(chan struct{})}
}

func (c *fdCannedReplyConn) Read(b []byte) (int, error) {
	c.mu.Lock()
	if c.buf.Len() > 0 {
		n, err := c.buf.Read(b)
		c.mu.Unlock()
		return n, err
	}
	c.mu.Unlock()
	<-c.closed
	return 0, io.EOF
}

func (c *fdCannedReplyConn) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	return nil
}

// TestFDLimiterReportPanicOnReplyDoesNotReplay pins the reply-side half of the
// "limiter report panic kills engine" finding: ReportResult runs on the reader's
// success path (fdLimiterReport.settle) BEFORE the reader advances past the command.
// An unguarded panic there hits the reader's session-failure recovery, which recovers
// the still-unadvanced req as an unacked tail and replays it — a command whose reply
// was ALREADY consumed executes twice (the INCR-twice bug). settle must recover the
// panic so the command completes with its real reply, is never replayed, and the
// engine keeps serving.
//
// Deterministic and dial-free: the conn accepts every write and serves one +OK per
// command, so the reader decodes a real reply for each; the session ends via ctx
// cancel (fdGraceful) once every command has completed.
//
// Red-check: remove the recover in fdLimiterReport.settle AND set panicOnce=true
// below — the first settle(nil) then panics into the reader recovery, session returns
// fdConnErr with a non-empty tail, and no command completes (the replay), so the
// hookDone wait below times out and the test fails.
func TestFDLimiterReportPanicOnReplyDoesNotReplay(t *testing.T) {
	const n = 4
	lim := &fdPanicReportLimiter{} // panicOnce=false: EVERY report panics -> the guard must hold repeatedly
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fd := &fdEngine{
		ap:       &AutoPipeliner{config: &AutoPipelineOptions{}, ctx: ctx},
		client:   &Client{baseClient: &baseClient{opt: &Options{WriteTimeout: time.Second, ReadTimeout: time.Second, Limiter: lim}}},
		maxBatch: 1,   // one chunk per command -> every req carries a limReport, so every reply settles
		window:   100, // >= n, so the writer never window-gates the carry
	}

	var reply bytes.Buffer
	cmds := make([]*StatusCmd, n)
	dones := make([]chan struct{}, n)
	carry := make([]fdReq, n)
	for i := range carry {
		reply.WriteString("+OK\r\n")
		cmds[i] = NewStatusCmd(ctx, "set", fmt.Sprintf("k%d", i), "v")
		dones[i] = make(chan struct{})
		carry[i] = fdReq{cmd: cmds[i], hookDone: dones[i], attempts: 1}
	}
	cn := pool.NewConn(newFDCannedReplyConn(reply.Bytes()))

	type sess struct {
		reqs   []fdReq
		result fdResult
		err    error
	}
	done := make(chan sess, 1)
	go func() {
		r, res, e := fd.session(context.Background(), cn, carry)
		done <- sess{r, res, e}
	}()

	// Every command must complete (its reply consumed and delivered) despite each
	// report panicking. A hung wait here is the replay/loss bug: the reader died on
	// the first panic and the req was recovered for replay instead of completed.
	for i := range dones {
		select {
		case <-dones[i]:
		case <-time.After(5 * time.Second):
			t.Fatalf("command %d never completed — a ReportResult panic killed the reader (replay/loss)", i)
		}
	}
	// Completed with the REAL reply, not merely woken with an error: proves the
	// consumed reply survived the panic (no double execution).
	for i := range cmds {
		if err := cmds[i].Err(); err != nil {
			t.Errorf("command %d: Err() = %v, want nil (completed with its reply)", i, err)
		}
		if v := cmds[i].Val(); v != "OK" {
			t.Errorf("command %d: Val() = %q, want OK", i, v)
		}
	}

	// The engine survived: end the session cleanly and confirm no tail was recovered
	// for replay.
	cancel()
	var got sess
	select {
	case got = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("session() hung after ctx cancel — the engine did not converge")
	}
	if got.result != fdGraceful {
		t.Fatalf("session result = %v, want fdGraceful (a report panic must not look like a transport failure)", got.result)
	}
	if len(got.reqs) != 0 {
		t.Fatalf("session returned %d reqs for replay, want 0 (nothing may be re-executed)", len(got.reqs))
	}
	if got.err != nil {
		t.Fatalf("session err = %v, want nil", got.err)
	}
	// Strict pairing survives: every Allow's obligation still reported exactly once
	// (the panic was swallowed but the Once fired).
	if r := lim.reports.Load(); r != n {
		t.Fatalf("ReportResult fired %d times, want %d", r, n)
	}
}

// fdRecordPool is a minimal pool.Pooler that hands out one preset conn and records
// Put/Remove, so a test can assert an init-panicked conn is Removed and never Put.
type fdRecordPool struct {
	pool.Pooler
	conn    *pool.Conn
	gets    atomic.Int64
	puts    atomic.Int64
	removes atomic.Int64
}

func (p *fdRecordPool) Get(context.Context) (*pool.Conn, error) { p.gets.Add(1); return p.conn, nil }
func (p *fdRecordPool) Put(context.Context, *pool.Conn)         { p.puts.Add(1) }
func (p *fdRecordPool) Remove(context.Context, *pool.Conn, error) {
	p.removes.Add(1)
}

// fdPanicInitConn is a net.Conn whose I/O panics, so the init handshake in
// initPooledConn panics rather than returning an error.
type fdPanicInitConn struct{ mockNetConn }

func (fdPanicInitConn) Write([]byte) (int, error) { panic("test: net.Conn Write panic during init") }
func (fdPanicInitConn) Read([]byte) (int, error)  { panic("test: net.Conn Read panic during init") }

// TestFDAttemptInitPanicRetiresConnFailsLease pins the 1218 fix: a panic during the
// acquisition/initialization phase (initPooledConn -> OnConnect/handshake) must be
// recovered in attempt, the leased conn RETIRED (Removed, never Put back
// half-initialized), and the carry returned as fdLeaseErr — the same disposition an
// init error gets — instead of crashing the sole engine goroutine.
//
// Red-check: remove attempt's recover defer — the panic escapes fd.run (process crash)
// and the release defer, seeing the zero-value result, Puts the poisoned conn.
func TestFDAttemptInitPanicRetiresConnFailsLease(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cn := pool.NewConn(&fdPanicInitConn{})
	p := &fdRecordPool{conn: cn}
	fd := &fdEngine{
		ap:       &AutoPipeliner{config: &AutoPipelineOptions{}, ctx: ctx},
		client:   &Client{baseClient: &baseClient{opt: &Options{DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second}}},
		pool:     p,
		maxBatch: 1,
		window:   100,
	}
	carry := []fdReq{{cmd: NewStatusCmd(ctx, "get", "k"), attempts: 1}}

	unacked, result, aerr := fd.attempt(context.Background(), carry)

	if result != fdLeaseErr {
		t.Fatalf("result = %v; want fdLeaseErr (init panic disposed like an init error)", result)
	}
	if !errors.Is(aerr, errFDPanicRecovered) {
		t.Fatalf("aerr = %v; want it to wrap errFDPanicRecovered (a recover fired)", aerr)
	}
	// Prove it was ATTEMPT's boundary (init phase), not session's writer backstop:
	// attempt wraps with "full-duplex attempt:", session with "full-duplex session:".
	if !strings.Contains(aerr.Error(), "full-duplex attempt:") {
		t.Fatalf("aerr = %v; want attempt's recover (prefix 'full-duplex attempt:')", aerr)
	}
	if p.removes.Load() != 1 {
		t.Fatalf("removes = %d; want 1 (the half-initialized conn must be Removed)", p.removes.Load())
	}
	if p.puts.Load() != 0 {
		t.Fatalf("puts = %d; want 0 (a panicked-init conn must NEVER be Put back)", p.puts.Load())
	}
	if len(unacked) != len(carry) {
		t.Fatalf("unacked = %d; want %d (carry returned for the lease-retry budget)", len(unacked), len(carry))
	}
}

// TestFDReaderNoRetryPanicSurfacesInlineNoReplay pins the 1356 fix: the reply-path
// NoRetry() consult runs AFTER the reply has already landed. A panicking custom
// NoRetry() there must be recovered (fdNoRetrySafe treats it as non-retryable) so the
// reply is surfaced INLINE — never diverted/replayed, which would re-run an
// already-answered (possibly mutating) command.
//
// Red-check: revert line ~1356 to `!req.cmd.NoRetry()` — the panic reaches the reader's
// session recover, the command is recovered as an unacked tail, and either the command
// never completes (hangs below) or session returns it for replay (reqs != 0).
func TestFDReaderNoRetryPanicSurfacesInlineNoReplay(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fd := &fdEngine{
		ap:       &AutoPipeliner{config: &AutoPipelineOptions{}, ctx: ctx},
		client:   &Client{baseClient: &baseClient{opt: &Options{WriteTimeout: time.Second, ReadTimeout: time.Second, MaxRetries: 3}}},
		maxBatch: 1,
		window:   100,
	}
	done := make(chan struct{})
	cmd := panicNoRetryCmd{NewStatusCmd(ctx, "get", "k")}
	carry := []fdReq{{cmd: cmd, hookDone: done, attempts: 1}}
	// A RETRYABLE reply error (a normal command would divert to the client's normal
	// path with MaxRetries>0). The panicking NoRetry() must be contained, so the reply
	// is surfaced inline instead.
	cn := pool.NewConn(newFDCannedReplyConn([]byte("-LOADING Redis is loading the dataset in memory\r\n")))

	type sess struct {
		reqs   []fdReq
		result fdResult
		err    error
	}
	res := make(chan sess, 1)
	go func() { r, rs, e := fd.session(context.Background(), cn, carry); res <- sess{r, rs, e} }()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("command never completed — the NoRetry() panic killed the reader (replay/loss)")
	}
	if cmd.Err() == nil {
		t.Fatal("command Err() = nil; want the LOADING reply surfaced inline")
	}
	cancel()
	var got sess
	select {
	case got = <-res:
	case <-time.After(5 * time.Second):
		t.Fatal("session hung after cancel — the engine did not converge")
	}
	if len(got.reqs) != 0 {
		t.Fatalf("session returned %d reqs for replay, want 0 (an answered command must not be re-executed)", len(got.reqs))
	}
}

// TestFDCarryArgsPanicDropsCommandKeepsSession pins the 2003 fix: a carry command whose
// Args() panics during sizing (the session-start command / Close backlog never passed
// the serve loop's admission) is failed+dropped without tearing the session down — the
// other carried commands still complete on the HEALTHY connection.
//
// Red-check: replace fdBatchEndSafe with the unguarded fdBatchEnd — the panic escapes
// the engine goroutine and every command below hangs (the process would crash in prod).
func TestFDCarryArgsPanicDropsCommandKeepsSession(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fd := &fdEngine{
		ap:       &AutoPipeliner{config: &AutoPipelineOptions{}, ctx: ctx},
		client:   &Client{baseClient: &baseClient{opt: &Options{WriteTimeout: time.Second, ReadTimeout: time.Second}}},
		maxBatch: 1,
		window:   100,
	}
	g0 := NewStatusCmd(ctx, "get", "k0")
	g2 := NewStatusCmd(ctx, "get", "k2")
	bad := panicArgsCmd{NewStatusCmd(ctx, "get", "bad")}
	d0, d1, d2 := make(chan struct{}), make(chan struct{}), make(chan struct{})
	carry := []fdReq{
		{cmd: g0, hookDone: d0, attempts: 1},
		{cmd: bad, hookDone: d1, attempts: 1},
		{cmd: g2, hookDone: d2, attempts: 1},
	}
	// Only the two GOOD commands are written and get replies; the middle command's
	// Args() panics during sizing and is failed+dropped.
	cn := pool.NewConn(newFDCannedReplyConn([]byte("+OK\r\n+OK\r\n")))

	type sess struct {
		reqs   []fdReq
		result fdResult
		err    error
	}
	res := make(chan sess, 1)
	go func() { r, rs, e := fd.session(context.Background(), cn, carry); res <- sess{r, rs, e} }()

	for i, d := range []chan struct{}{d0, d1, d2} {
		select {
		case <-d:
		case <-time.After(5 * time.Second):
			t.Fatalf("carry command %d never completed — a carry Args() panic stranded the session", i)
		}
	}
	if g0.Err() != nil || g0.Val() != "OK" {
		t.Errorf("g0: err=%v val=%q; want OK", g0.Err(), g0.Val())
	}
	if g2.Err() != nil || g2.Val() != "OK" {
		t.Errorf("g2: err=%v val=%q; want OK", g2.Err(), g2.Val())
	}
	if err := bad.Err(); err == nil || !errors.Is(err, errFDPanicRecovered) {
		t.Errorf("bad: err=%v; want it to wrap errFDPanicRecovered (failed+dropped)", err)
	}
	cancel()
	var got sess
	select {
	case got = <-res:
	case <-time.After(5 * time.Second):
		t.Fatal("session hung after cancel — the engine did not converge")
	}
	if got.result != fdGraceful {
		t.Fatalf("session result = %v; want fdGraceful (the conn stayed healthy — a carry Args panic must not desync it)", got.result)
	}
	if len(got.reqs) != 0 {
		t.Fatalf("session returned %d reqs; want 0", len(got.reqs))
	}
}

// TestFDLimiterReportPanicOnWriteFailureNoCrash pins the write-side half of the
// finding: writeBatch's report defer settles the chunk's obligation with the write
// error via fdLimiterReport.settle. A panicking ReportResult there must be contained
// so it neither escapes writeBatch (on the real writer goroutine it would crash the
// process) nor replaces the real write error; the chunk still lands in the in-flight
// deque for normal conn-error recovery.
//
// Red-check: remove the recover in fdLimiterReport.settle — the report panic then
// escapes writeBatch and the recover wrapper below fires t.Fatalf.
func TestFDLimiterReportPanicOnWriteFailureNoCrash(t *testing.T) {
	ctx := context.Background()
	lim := &fdPanicReportLimiter{}
	cn := pool.NewConn(&failWriteNetConn{})
	fd := &fdEngine{
		ap:       &AutoPipeliner{config: &AutoPipelineOptions{}},
		client:   &Client{baseClient: &baseClient{opt: &Options{WriteTimeout: time.Second, Limiter: lim}}},
		maxBatch: 8,
	}
	inflight := newFDInflight()

	reqs := []fdReq{
		{cmd: NewStatusCmd(ctx, "set", "k0", "v"), attempts: 1},
		{cmd: NewStatusCmd(ctx, "set", "k1", "v"), attempts: 1},
	}

	err := func() (e error) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("writeBatch propagated a ReportResult panic instead of recovering it: %v", r)
			}
		}()
		return fd.writeBatch(ctx, cn, inflight, reqs)
	}()
	if err == nil {
		t.Fatal("writeBatch err = nil, want the write error (a report panic must not swallow the failure)")
	}
	if inflight.len() != len(reqs) {
		t.Fatalf("inflight.len() = %d, want %d (a write-failed chunk still lands for conn-error recovery)", inflight.len(), len(reqs))
	}
	// The chunk's single write-failure obligation reported exactly once (panic
	// swallowed, Once fired).
	if r := lim.reports.Load(); r != 1 {
		t.Fatalf("ReportResult fired %d times, want 1", r)
	}
}

// TestFDReportReplyMetricsRecoversCallbackPanic pins that a panicking user metric
// callback (OTel duration or the native error callback) cannot escape the reader:
// reportReplyMetrics runs BEFORE the reply is advanced out of the in-flight deque,
// and the reader's broad recovery would re-own an unadvanced req and replay an
// already-consumed reply (a mutating command twice). It must recover the panic so
// the caller completes normally.
func TestFDReportReplyMetricsRecoversCallbackPanic(t *testing.T) {
	var calls atomic.Int64
	pool.SetAllMetricCallbacks(&pool.MetricCallbacks{
		Error: func(_ context.Context, _ string, _ *pool.Conn, _ string, _ bool, _ int) {
			calls.Add(1)
			panic("boom from a user metric callback")
		},
	})
	defer pool.SetAllMetricCallbacks(nil)

	fd := &fdEngine{client: &Client{baseClient: &baseClient{opt: &Options{}}}}
	req := fdReq{cmd: NewStatusCmd(context.Background(), "get", "k"), attempts: 1}

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("reportReplyMetrics propagated a callback panic: %v", r)
			}
		}()
		// A non-nil error routes to the panicking error callback.
		fd.reportReplyMetrics(context.Background(), req, errors.New("WRONGTYPE Operation"), nil)
	}()

	if calls.Load() != 1 {
		t.Fatalf("error callback invoked %d times, want 1 (it must be reached, then its panic recovered)", calls.Load())
	}
}

// TestFDFailReqsRecoversMetricCallbackPanic pins that a panicking user MetricError
// callback on the engine-goroutine failure path (failReqs, reached on lease
// failure / retry exhaustion / Close) cannot abort settlement: every req must get
// its error set and its batch closed (caller woken), and the panic must not
// escape into fd.run. The guard is PER-req, not around the loop — three reqs with
// a callback that always panics prove reqs after the first still settle. Pure, no
// server.
func TestFDFailReqsRecoversMetricCallbackPanic(t *testing.T) {
	var calls atomic.Int64
	pool.SetAllMetricCallbacks(&pool.MetricCallbacks{
		Error: func(context.Context, string, *pool.Conn, string, bool, int) {
			calls.Add(1)
			panic("boom from a user metric callback")
		},
	})
	defer pool.SetAllMetricCallbacks(nil)

	ctx := context.Background()
	fd := &fdEngine{client: &Client{baseClient: &baseClient{opt: &Options{}}}}

	const n = 3
	reqs := make([]fdReq, n)
	batches := make([]*apBatch, n)
	for i := range reqs {
		cmd := NewStatusCmd(ctx, "set", "k", "v")
		b := newAPBatch()
		cmd.setReady(b) // cmd.Err()/await() would now block until b closes
		batches[i] = b
		reqs[i] = fdReq{cmd: cmd, batch: b, ctx: ctx, attempts: 1}
	}

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("failReqs propagated a callback panic: %v", r)
			}
		}()
		fd.failReqs(reqs, ErrClosed)
	}()

	if got := calls.Load(); got != n {
		t.Fatalf("error callback invoked %d times, want %d (every req must reach it)", got, n)
	}
	for i := range reqs {
		if err := reqs[i].cmd.rawErr(); !errors.Is(err, ErrClosed) {
			t.Fatalf("req %d cmd err = %v, want ErrClosed", i, err)
		}
		select {
		case <-batches[i].done:
		default:
			t.Fatalf("req %d batch not closed: a panicking callback left the caller wedged", i)
		}
	}
}

// TestFDFailQueueRecoversMetricCallbackPanic is the failQueue twin of the above:
// draining the accepted backlog on an fdLeaseErr must settle every buffered req
// even when the MetricError callback panics, and must not propagate. Pure, no
// server.
func TestFDFailQueueRecoversMetricCallbackPanic(t *testing.T) {
	var calls atomic.Int64
	pool.SetAllMetricCallbacks(&pool.MetricCallbacks{
		Error: func(context.Context, string, *pool.Conn, string, bool, int) {
			calls.Add(1)
			panic("boom from a user metric callback")
		},
	})
	defer pool.SetAllMetricCallbacks(nil)

	ctx := context.Background()
	const n = 3
	fd := &fdEngine{client: &Client{baseClient: &baseClient{opt: &Options{}}}, ch: make(chan fdReq, n)}
	batches := make([]*apBatch, n)
	for i := 0; i < n; i++ {
		cmd := NewStatusCmd(ctx, "set", "k", "v")
		b := newAPBatch()
		cmd.setReady(b)
		batches[i] = b
		fd.ch <- fdReq{cmd: cmd, batch: b, ctx: ctx, attempts: 1}
	}

	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("failQueue propagated a callback panic: %v", r)
			}
		}()
		fd.failQueue(ErrClosed)
	}()

	if got := calls.Load(); got != n {
		t.Fatalf("error callback invoked %d times, want %d (every buffered req must reach it)", got, n)
	}
	for i := range batches {
		select {
		case <-batches[i].done:
		default:
			t.Fatalf("req %d batch not closed: a panicking callback left the caller wedged", i)
		}
	}
}

// fdBlockingDurationRecorder is a custom OTel recorder whose duration callback
// reads its OWN async command (cmd.Err()/cmd.String()). Both accessors await the
// command's batch.done, which the FD reader closes only AFTER the duration
// callback returns — so without the executor-guard escape in reportReplyMetrics
// this call blocks forever and wedges the reader. Embeds fdOtelRecorder for the
// no-op remainder of the Recorder interface.
type fdBlockingDurationRecorder struct {
	fdOtelRecorder
	called atomic.Int64
	gotErr atomic.Value // string: cmd.Err() text ("" for nil)
	gotStr atomic.Value // string: cmd.String()
}

func (r *fdBlockingDurationRecorder) RecordOperationDuration(_ context.Context, _ time.Duration, cmd otel.Cmder, _ int, _ error, _ *pool.Conn, _ int) {
	r.called.Add(1)
	acc, ok := cmd.(interface {
		Err() error
		String() string
	})
	if !ok {
		return
	}
	if e := acc.Err(); e != nil {
		r.gotErr.Store(e.Error())
	} else {
		r.gotErr.Store("")
	}
	r.gotStr.Store(acc.String())
}

// TestFDReportReplyMetricsDurationCallbackNoDeadlock pins F3: a duration callback
// that reads its own command must not wedge the reader. reportReplyMetrics runs
// BEFORE req.complete() and the reader is not otherwise the batch's executor, so a
// naive cmd.Err()/cmd.String() would block on the batch.done the reader has not
// closed yet. The fix registers the reader as the batch's executor for the call,
// so the accessor guard returns the just-set view without blocking. Asserting the
// returned value (not just "did not hang") separates a working guard from one that
// short-circuits to a garbage view. Pure, no server.
func TestFDReportReplyMetricsDurationCallbackNoDeadlock(t *testing.T) {
	rec := &fdBlockingDurationRecorder{}
	otel.SetGlobalRecorder(rec)
	defer otel.SetGlobalRecorder(nil)

	ctx := context.Background()
	cmd := NewStatusCmd(ctx, "get", "k")
	cmd.SetVal("OK") // the "final result" the reader set before reporting
	b := newAPBatch()
	cmd.setReady(b) // cmd.Err()/cmd.String() now await b.done until it closes
	req := fdReq{cmd: cmd, batch: b, attempts: 1, writtenAt: time.Now()}

	fd := &fdEngine{client: &Client{baseClient: &baseClient{opt: &Options{}}}}

	done := make(chan struct{})
	go func() {
		defer close(done)
		// e == nil exercises the duration callback (the blocking-view path). The
		// batch is deliberately left open, matching the real order where
		// reportReplyMetrics runs before req.complete().
		fd.reportReplyMetrics(ctx, req, nil, nil)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("reportReplyMetrics wedged: a duration callback read its own command and blocked on batch.done the reader has not yet closed")
	}

	if got := rec.called.Load(); got != 1 {
		t.Fatalf("duration callback invoked %d times, want 1", got)
	}
	if v, _ := rec.gotErr.Load().(string); v != "" {
		t.Fatalf("callback saw cmd.Err() = %q, want \"\" (the just-set view)", v)
	}
	if v, _ := rec.gotStr.Load().(string); !strings.Contains(v, "OK") {
		t.Fatalf("callback saw cmd.String() = %q, want it to contain the set value OK", v)
	}
	// The guard must not have completed the batch; that stays req.complete()'s job.
	select {
	case <-b.done:
		t.Fatal("reportReplyMetrics closed the batch; completion is req.complete()'s job")
	default:
	}
}

// fdPanicNoRetryCmd is a Cmder whose NoRetry() panics. flushReqs reads
// reqs[i].cmd.NoRetry() at the top of each chunk (before any I/O), so this
// triggers the shutdown-flush recover defer at the exact `i` the defer expects,
// without needing a live server.
type fdPanicNoRetryCmd struct{ *StatusCmd }

func (fdPanicNoRetryCmd) NoRetry() bool { panic("boom: NoRetry panic in shutdown flush") }

// TestFDShutdownFlushAbortsAfterRecoveredPanic pins finding F1: flushReqs
// recovers an encoder/serialize panic in a carried-tail flush and fails that
// group, but with an UNNAMED return it returned nil after recovery, so
// flushCarryBudgeted treated the failed group as success and ran later
// attempt-count groups (and, via shutdownFlush, the fresh queue) even though an
// earlier ordered command never completed. The named return must propagate the
// failure so the ordered flush aborts like a transport failure.
//
// Red-check: revert flushReqs to an unnamed/nil return -> flushCarryBudgeted
// runs the later group (runPipeline call count becomes 1).
func TestFDShutdownFlushAbortsAfterRecoveredPanic(t *testing.T) {
	ctx := context.Background()
	var runCalls atomic.Int32
	fd := &fdEngine{
		ap:       &AutoPipeliner{config: &AutoPipelineOptions{}},
		client:   &Client{baseClient: &baseClient{opt: &Options{MaxRetries: 5}}},
		maxBatch: 8,
		runPipeline: func(_ context.Context, _ []Cmder, _ int) error {
			runCalls.Add(1)
			return nil
		},
	}

	// carry is attempts-descending, so [attempts=2] and [attempts=1] are two
	// contiguous groups. group1's first command panics in NoRetry(); group2 is a
	// normal command that must NOT run once group1 aborts. MaxRetries=5 keeps both
	// groups' budgets positive (rem = MaxRetries+1-attempts), so neither is
	// dropped as budget-exhausted before it is flushed.
	g1 := fdPanicNoRetryCmd{NewStatusCmd(ctx, "set", "k0", "v")}
	g2 := NewStatusCmd(ctx, "get", "k1")
	carry := []fdReq{
		{cmd: g1, batch: newAPBatch(), attempts: 2},
		{cmd: g2, batch: newAPBatch(), attempts: 1},
	}

	err := fd.flushCarryBudgeted(ctx, carry)
	if err == nil || !errors.Is(err, errFDPanicRecovered) {
		t.Fatalf("flushCarryBudgeted err = %v, want errFDPanicRecovered", err)
	}
	if got := runCalls.Load(); got != 0 {
		t.Fatalf("runPipeline called %d times, want 0 - a later group ran after a recovered shutdown-flush panic", got)
	}
	// Both the panicking group and the aborted later group must be failed (not left
	// hanging) with the panic error.
	if e := g1.rawErr(); e == nil || !errors.Is(e, errFDPanicRecovered) {
		t.Fatalf("group1 cmd Err() = %v, want errFDPanicRecovered", e)
	}
	if e := g2.rawErr(); e == nil || !errors.Is(e, errFDPanicRecovered) {
		t.Fatalf("group2 cmd Err() = %v, want errFDPanicRecovered (failed by the abort)", e)
	}
}

// TestFDFlushReqsAbortsChunksOnErrConnUnusable pins finding F2: when a
// shutdown-flush chunk returns an errConnUnusable wrapper (e.g. a custom push
// processor returned a redis.Error during the close-time drain, so the chunk was
// never written), the chunk loop classified it as an ordinary Redis reply
// (isRedisError unwraps the marker) and CONTINUED to later chunks - flushing an
// ordered shutdown out of order. The errConnUnusable precedence
// (pipelineErrShouldStamp) must abort and fail the remaining reqs.
//
// Red-check: revert the guard to `!isRedisError(err)` -> the loop continues and
// runPipeline is called for every chunk (call count becomes 3).
func TestFDFlushReqsAbortsChunksOnErrConnUnusable(t *testing.T) {
	ctx := context.Background()

	// The exact shape withPipelineConn produces on a drain failure: errConnUnusable
	// wrapping a redis.Error. isRedisError sees through the wrap, yet the marker is
	// present - the bug's precondition.
	wrapped := fmt.Errorf("%w: pipeline push drain: %w", errConnUnusable, fdFakeRedisErr{})
	if !isRedisError(wrapped) {
		t.Fatal("precondition: isRedisError(wrapped) should be true (it unwraps to a redis.Error)")
	}
	if !errors.Is(wrapped, errConnUnusable) {
		t.Fatal("precondition: errors.Is(wrapped, errConnUnusable) should be true")
	}

	var calls atomic.Int32
	fd := &fdEngine{
		ap:       &AutoPipeliner{config: &AutoPipelineOptions{}},
		client:   &Client{baseClient: &baseClient{opt: &Options{}}},
		maxBatch: 1, // one command per chunk, so the three reqs span three chunks
		runPipeline: func(_ context.Context, _ []Cmder, _ int) error {
			if calls.Add(1) == 1 {
				return wrapped // chunk 1: desynced, never written
			}
			return nil
		},
	}
	reqs := []fdReq{
		{cmd: NewStatusCmd(ctx, "ping"), batch: newAPBatch(), attempts: 1},
		{cmd: NewStatusCmd(ctx, "ping"), batch: newAPBatch(), attempts: 1},
		{cmd: NewStatusCmd(ctx, "ping"), batch: newAPBatch(), attempts: 1},
	}

	err := fd.flushReqs(ctx, reqs, 0)
	if !errors.Is(err, errConnUnusable) {
		t.Fatalf("flushReqs err = %v, want errConnUnusable-wrapped", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("runPipeline called %d times, want 1 - later chunks ran after an errConnUnusable chunk", got)
	}
	// The un-run chunks must be failed with the desync error, not completed as
	// successes.
	for i := 1; i < len(reqs); i++ {
		if e := reqs[i].cmd.rawErr(); e == nil || !errors.Is(e, errConnUnusable) {
			t.Errorf("reqs[%d].Err() = %v, want errConnUnusable (failed by the abort)", i, e)
		}
	}
}

// fdDrainErrProcessor is a custom (non-*push.Processor) PushNotificationProcessor
// whose drain returns a caller-supplied error.
type fdDrainErrProcessor struct{ err error }

func (fdDrainErrProcessor) GetHandler(string) push.NotificationHandler { return nil }
func (fdDrainErrProcessor) RegisterHandler(string, push.NotificationHandler, bool) error {
	return nil
}
func (fdDrainErrProcessor) UnregisterHandler(string) error { return nil }
func (p fdDrainErrProcessor) ProcessPendingNotifications(context.Context, push.NotificationHandlerContext, *proto.Reader) error {
	return p.err
}

// TestReleaseConnRemovesConnAfterCustomDrainError pins finding F3: on the release
// path, a custom PushNotificationProcessor that returns a redis.Error during the
// release-time push drain left the possibly-partially-drained conn returning to
// the pool - releaseConnToPool only Removed it when isBadConn recognized the
// error, and isBadConn returns false for a (non-readonly, non-moved) redis.Error.
// A drain error must make the conn unusable so it is Removed regardless.
//
// Red-check: restore the `if isBadConn(err, ...) { Remove; return }` guard -> the
// redis.Error drain error is not isBadConn, so the conn is Put (puts=1, removes=0).
func TestReleaseConnRemovesConnAfterCustomDrainError(t *testing.T) {
	server, client := newIdleTCPConnPair(t)
	defer server.Close()
	defer client.Close()

	cn := pool.NewConn(client)
	// A push frame so MaybeHasData() and PeekReplyType() see a RespPush, routing to
	// the custom processor (redis.go peeks the type, then calls it).
	if _, err := server.Write([]byte(">1\r\n$3\r\nfoo\r\n")); err != nil {
		t.Fatalf("write push frame: %v", err)
	}
	deadline := time.Now().Add(time.Second)
	for !cn.MaybeHasData() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !cn.MaybeHasData() {
		t.Fatal("push frame never became readable")
	}

	cp := &releaseRecordingPool{}
	c := &baseClient{
		opt:           &Options{Addr: "127.0.0.1:6379", Protocol: 3},
		connPool:      cp,
		pushProcessor: fdDrainErrProcessor{err: fdFakeRedisErr{}},
	}
	// err=nil so the first guard (isBadConn/errConnUnusable on the op error) does
	// not fire; the drain error alone must drive removal.
	c.releaseConn(context.Background(), cn, nil)

	if cp.removes != 1 || cp.puts != 0 {
		t.Fatalf("a custom release-drain error must remove, not re-pool, the conn: removes=%d puts=%d",
			cp.removes, cp.puts)
	}
}

// TestFDConfigReportsResolvedWindow pins finding F4: newFDEngine resolves a zero
// FullDuplexWindow to fdDefaultWindow in a local, so Config() (documented to
// report the effective config) reported 0. The resolved window must be written
// back so Config() reports the value actually enforced.
//
// Red-check: drop `ap.config.FullDuplexWindow = w` in newFDEngine -> Config()
// reports 0.
func TestFDConfigReportsResolvedWindow(t *testing.T) {
	c := NewClient(&Options{
		Addr:                    internalTestRedisAddr(),
		Protocol:                3,
		PipelineReadBufferSize:  64 * 1024,
		PipelineWriteBufferSize: 64 * 1024,
	})
	defer c.Close()

	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex:       true,
		FullDuplexWindow: 0, // 0 => default
	})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active; cannot exercise the window write-back")
	}

	if got := ap.Config().FullDuplexWindow; got != fdDefaultWindow {
		t.Fatalf("Config().FullDuplexWindow = %d, want %d (resolved default not published)", got, fdDefaultWindow)
	}
}

// TestFDBlockingBatchPoolReuse exercises the pooled-batch lifecycle directly:
// get → signal (as the reader would, via close) → wait → recycle, thousands of
// times with concurrent get/put churn. Run under -race: a mis-delivered wakeup
// from a recycled channel, a missed reset, or a double-signal surfaces as a race
// or a hang here without needing a live server.
func TestFDBlockingBatchPoolReuse(t *testing.T) {
	const workers = 32
	const iters = 5000
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				b := getFDBlockingBatch()
				if !b.pooled {
					t.Errorf("pooled batch lost its pooled flag")
					return
				}
				if b.closed.Load() {
					t.Errorf("recycled batch came back already closed")
					return
				}
				// A completer (the reader) signals on its own goroutine while the
				// "caller" waits — the real submit/complete/Wait shape.
				done := make(chan struct{})
				go func() {
					b.close() // buffered send for pooled batches
					close(done)
				}()
				<-b.done // the caller's Wait drains the signal
				<-done
				// close is idempotent: a second completer (e.g. a racing failReqs)
				// must not send a second value or panic.
				b.close()
				select {
				case <-b.done:
					t.Errorf("second close signalled a recycled channel")
					return
				default:
				}
				putFDBlockingBatch(b)
			}
		}()
	}
	wg.Wait()
}

// TestFDBlockingBatchPoolResets guards the individual-field reset: a batch put
// back dirty must come out clean, or the next caller's completion is a no-op.
func TestFDBlockingBatchPoolResets(t *testing.T) {
	b := getFDBlockingBatch()
	// Dirty every field the reset must clear.
	b.closed.Store(true)
	b.dispGid.Store(999)
	b.nodeCount.Store(3)
	b.nodeGids = []int64{1, 2, 3}
	// Leave a stray signal in the buffered channel.
	select {
	case b.done <- struct{}{}:
	default:
	}
	putFDBlockingBatch(b)

	// The pool may hand back a different object, so loop until we observe ours
	// (or give up after a bounded number of gets) — either way every returned
	// batch must be clean.
	for i := 0; i < 64; i++ {
		g := getFDBlockingBatch()
		if g.closed.Load() || g.dispGid.Load() != 0 || g.nodeCount.Load() != 0 || g.nodeGids != nil {
			t.Fatalf("getFDBlockingBatch returned a dirty batch")
		}
		select {
		case <-g.done:
			t.Fatalf("getFDBlockingBatch returned a batch with a stray signal")
		default:
		}
		putFDBlockingBatch(g)
	}
}

// mkReq returns an fdReq tagged with a unique *apBatch pointer used as identity
// in the model comparison.
func mkReq() fdReq { return fdReq{batch: newAPBatch()} }

// TestFDInflightRingModel compares the ring buffer against a reference []fdReq
// model over a long randomized sequence of push / advance / frontBatch /
// takeRemaining, deliberately driving the head around the array so growth and
// snapshots straddle the wrap seam — the one place a ring breaks and that no
// existing FD test targets.
func TestFDInflightRingModel(t *testing.T) {
	rng := rand.New(rand.NewSource(12345))
	for trial := 0; trial < 200; trial++ {
		f := newFDInflightCap(rng.Intn(8)) // mix of presized and grow-from-zero
		var model []fdReq
		var scratch []fdReq

		for step := 0; step < 400; step++ {
			switch rng.Intn(3) {
			case 0: // push a batch
				k := 1 + rng.Intn(20)
				reqs := make([]fdReq, k)
				for i := range reqs {
					reqs[i] = mkReq()
				}
				f.pushBatch(reqs)
				model = append(model, reqs...)
			case 1: // advance (pop front)
				if len(model) == 0 {
					continue
				}
				n := rng.Intn(len(model) + 1)
				f.advance(n)
				model = model[n:]
			case 2: // frontBatch snapshot must equal model front prefix, in order
				if len(model) == 0 {
					continue // frontBatch blocks by contract on an empty, open ring
				}
				scratch, _ = f.frontBatch(scratch)
				want := len(model)
				if want > fdReadBatch {
					want = fdReadBatch
				}
				if len(scratch) != want {
					t.Fatalf("trial %d step %d: frontBatch len=%d want=%d", trial, step, len(scratch), want)
				}
				for i := 0; i < want; i++ {
					if scratch[i].batch != model[i].batch {
						t.Fatalf("trial %d step %d: frontBatch[%d] mismatch", trial, step, i)
					}
				}
			}
			if f.len() != len(model) {
				t.Fatalf("trial %d step %d: len=%d want=%d", trial, step, f.len(), len(model))
			}
			if f.empty() != (len(model) == 0) {
				t.Fatalf("trial %d step %d: empty=%v want=%v", trial, step, f.empty(), len(model) == 0)
			}
		}

		// takeRemaining must return exactly the model tail, in order.
		rem := f.takeRemaining()
		if len(rem) != len(model) {
			t.Fatalf("trial %d: takeRemaining len=%d want=%d", trial, len(rem), len(model))
		}
		for i := range rem {
			if rem[i].batch != model[i].batch {
				t.Fatalf("trial %d: takeRemaining[%d] mismatch", trial, i)
			}
		}
	}
}

// TestFDInflightRingGrowWhileWrapped forces the specific hazard: grow the ring
// while the live window straddles the end of the backing array (head > 0 and
// the tail has wrapped to the front), then verify order is preserved end to end.
func TestFDInflightRingGrowWhileWrapped(t *testing.T) {
	f := newFDInflightCap(8)
	var model []fdReq

	push := func(k int) {
		reqs := make([]fdReq, k)
		for i := range reqs {
			reqs[i] = mkReq()
		}
		f.pushBatch(reqs)
		model = append(model, reqs...)
	}
	adv := func(n int) { f.advance(n); model = model[n:] }

	push(8)  // fill: head=0 count=8 cap=8
	adv(5)   // head=5 count=3
	push(4)  // tail wraps past end: entries at 5,6,7,0,... head=5 count=7 cap=8 (still fits)
	adv(2)   // head=7 count=5
	push(10) // count 15 > cap 8 -> grow WHILE wrapped (head=7)
	// Advance across what was the old wrap seam and verify order throughout.
	for f.len() > 0 {
		var snap []fdReq
		snap, _ = f.frontBatch(snap)
		if snap[0].batch != model[0].batch {
			t.Fatalf("front mismatch after wrapped grow")
		}
		adv(1)
	}
	if len(model) != 0 {
		t.Fatalf("model not drained: %d left", len(model))
	}
	if rem := f.takeRemaining(); rem != nil {
		t.Fatalf("takeRemaining after drain should be nil, got %d", len(rem))
	}
}

// TestFDInflightRingAdvanceEmpty guards against a divide-by-zero in advance on a
// never-grown ring (nil backing buffer): n>0 clamps to 0, and the modulo update
// must not run. Regression for the copilot review on #3970.
func TestFDInflightRingAdvanceEmpty(t *testing.T) {
	f := newFDInflight() // zero-cap: buf is nil until first push
	f.advance(5)         // must be a no-op, not a panic
	if f.len() != 0 {
		t.Fatalf("len=%d after advance on empty ring, want 0", f.len())
	}
	// Also after draining a grown ring back to empty.
	f.pushBatch([]fdReq{mkReq(), mkReq()})
	f.advance(2)
	f.advance(3) // over-advance on an empty (but grown) ring: no-op, no panic
	if f.len() != 0 {
		t.Fatalf("len=%d after over-advance, want 0", f.len())
	}
}

// TestFDInflightRingZeroesConsumed guards the load-bearing zeroing in advance:
// popped slots must be cleared so drained entries don't pin cmd/ctx/batch for
// the life of the session.
func TestFDInflightRingZeroesConsumed(t *testing.T) {
	f := newFDInflightCap(4)
	reqs := []fdReq{mkReq(), mkReq(), mkReq(), mkReq()}
	f.pushBatch(reqs)
	f.advance(4)
	for i := range f.buf {
		if f.buf[i].batch != nil {
			t.Fatalf("buf[%d] not zeroed after advance: %#v", i, f.buf[i])
		}
	}
}

// TestFullDuplexSparseTrafficPooledBatch drives the blocking full-duplex face
// with intentionally sparse traffic: each caller pauses longer than the idle
// timeout between commands, so the engine returns its connection to the pool and
// re-leases on the next command — every command runs in its own single-entry
// batch, repeatedly. This is the path where the pooled completion batch (buffered
// done, recycled after Wait) and the idle-return/re-lease machinery meet, so it
// guards against a mis-delivered wakeup from a recycled batch under intermittent
// execution. Correctness checks: ECHO round-trips its own token (no
// misattribution) and per-caller INCR is strictly sequential (no lost or
// duplicated completion); nothing hangs.
func TestFullDuplexSparseTrafficPooledBatch(t *testing.T) {
	ctx := context.Background()

	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := c.AutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex:            true,
		Unordered:             false,
		MaxConcurrentBatches:  1,
		FullDuplexIdleTimeout: 40 * time.Millisecond,
		FullDuplexMaxHold:     10 * time.Second,
	})
	if err != nil {
		t.Fatalf("AutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	const workers = 4
	const opsPerWorker = 12 // ~12 * 70ms ~= 0.85s per worker, run in parallel
	for i := 0; i < workers; i++ {
		if err := ap.Del(ctx, fmt.Sprintf("fd:sparse:%d", i)).Err(); err != nil {
			t.Fatalf("del: %v", err)
		}
	}

	done := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("fd:sparse:%d", id)
			for op := 0; op < opsPerWorker; op++ {
				// Pause past the 40ms idle timeout so the conn is returned and
				// re-leased for this command (the sparse / intermittent case).
				time.Sleep(70 * time.Millisecond)

				token := fmt.Sprintf("w%d-op%d", id, op)
				if got, err := ap.Echo(ctx, token).Result(); err != nil {
					done <- fmt.Errorf("worker %d op %d: echo: %w", id, op, err)
					return
				} else if got != token {
					done <- fmt.Errorf("worker %d op %d: ECHO misattribution: sent %q got %q", id, op, token, got)
					return
				}

				want := int64(op + 1)
				if got, err := ap.Incr(ctx, key).Result(); err != nil {
					done <- fmt.Errorf("worker %d op %d: incr: %w", id, op, err)
					return
				} else if got != want {
					done <- fmt.Errorf("worker %d op %d: INCR out of order: got %d want %d", id, op, got, want)
					return
				}
			}
			done <- nil
		}(i)
	}

	// Watchdog: sparse traffic must still complete promptly (each op is ~1 RTT +
	// the 70ms pause). A recycled-batch mis-wakeup that dropped a completion would
	// hang a caller here.
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(30 * time.Second):
		t.Fatal("sparse-traffic workers did not finish (possible dropped completion)")
	}

	for i := 0; i < workers; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
}

// TestFullDuplexSpillsToMainPoolWhenPipelinePoolFull pins that an FD lease falls back
// to the main pool when the dedicated pipeline pool is saturated, mirroring
// withPipelineConn — otherwise a full pipeline pool fails already-accepted commands
// even though the main pool has idle capacity.
func TestFullDuplexSpillsToMainPoolWhenPipelinePoolFull(t *testing.T) {
	ctx := context.Background()

	// PipelinePoolSize:1 so holding its one conn saturates it; the main pool has room.
	c := NewClient(&Options{
		Addr:                    ":6379",
		Protocol:                3,
		PipelinePoolSize:        1,
		PoolSize:                8,
		PipelineReadBufferSize:  64 * 1024,
		PipelineWriteBufferSize: 64 * 1024,
	})
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}

	// Detect the spill: a Get on the MAIN pool by the FD lease.
	mainHook := &fdCountHook{}
	c.connPool.AddPoolHook(mainHook)

	// Saturate the pipeline pool by holding its only turn.
	pp := c.getPipelinePool()
	held, err := pp.Get(ctx)
	if err != nil {
		t.Fatalf("hold pipeline conn: %v", err)
	}
	defer pp.Put(ctx, held)

	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()

	// The engine leases lazily on the first command; the pipeline pool is full, so it
	// must spill to the main pool and still succeed.
	if err := ap.Set(ctx, "fd:spill:k", "v", 0).Err(); err != nil {
		t.Fatalf("FD command failed instead of spilling to the main pool: %v", err)
	}
	if v, err := ap.Get(ctx, "fd:spill:k").Result(); err != nil || v != "v" {
		t.Fatalf("FD get after spill: v=%q err=%v", v, err)
	}
	if g := mainHook.gets.Load(); g < 1 {
		t.Fatalf("FD lease did not spill to the main pool (main-pool gets=%d)", g)
	}
}
