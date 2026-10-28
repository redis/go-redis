package redis

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"runtime"
	"strconv"
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
	// yet elapsed). This is the baseline that makes the return below meaningful
	// (it would not hold if lease/return were removed — the conn would stay held).
	// Checked on the engine, not the pool: the pool also keeps the engine's warm
	// spare idle, and its refill dial is asynchronous.
	if ap.fd.curConn.Load() == nil {
		t.Fatal("expected the FD conn held right after a command")
	}

	// Returned: the engine holds nothing and the pool has the returned conn plus
	// the warm spare idle.
	returned := false
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		if ap.fd.curConn.Load() == nil && pp.IdleLen() >= 2 {
			returned = true
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !returned {
		t.Fatalf("pipeline conn not returned to the pool after idle (held=%v, IdleLen=%d)",
			ap.fd.curConn.Load() != nil, pp.IdleLen())
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
	// The submit queue is bounded by the WINDOW, not by a separate 4096 cap.
	//
	// That cap existed because the submit path was a buffered channel, which
	// allocates its whole capacity eagerly. The slice queue starts at 64 entries
	// and grows to the live depth, so a window-sized bound costs nothing up
	// front — and the cap was bounding ADMISSION rather than memory: callers
	// whose batch did not fit parked on a cap-1 room signal and were woken one
	// at a time, which collapsed pipelined throughput at high caller counts.
	if want := fdDefaultWindow; ap.fd.q.capacity() != want {
		t.Fatalf("queue capacity %d, want %d (the window bounds the queue as well as in-flight)",
			ap.fd.q.capacity(), want)
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
// work accepted while NO session holds a connection: a command sitting in the submit queue
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

	// Simulate the between-sessions Close: backlog queued in the submit queue (bypassing
	// submit — run() must not consume it, so this test does not race the engine's
	// own loop; the queue is drained below by takeQueue inside shutdownFlush).
	const n = 5
	reqs := make([]fdReq, n)
	for i := 0; i < n; i++ {
		cmd := NewStatusCmd(ctx, "set", fmt.Sprintf("fdsf:%d", i), fmt.Sprintf("v%d", i))
		reqs[i] = fdReq{cmd: cmd, batch: newAPBatch()}
		fd.q.push(reqs[i])
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
// the submit queue nor the deque and their callers hang. Deterministic and dial-free.
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

	if _, err := fd.writeCarryChunked(context.Background(), cn, inflight, carry, nil, nil); err == nil {
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
	suffix, err := fd.writeCarryChunked(context.Background(), cn, inflight, carry, readerDone, nil)
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

// TestWriteCarryChunkedStopsOnMaxHold pins r3937925412: a LIVE connection held past
// FullDuplexMaxHold while replaying a recovered carry must stop between chunks and
// return the UNWRITTEN suffix out-of-band (errFDMaxHold) WITHOUT pushing it into the
// in-flight deque — so the caller clean-recycles (drains the written prefix, Puts the
// conn so the hold ends) and replays only the never-sent suffix on the next lease.
// Carry replay runs BEFORE the serve loop, so without this poll a long or
// backpressured replay never reaches the serve loop's max-hold select and pins the conn
// past FullDuplexMaxHold.
//
// Red-check: remove the maxC poll in writeCarryChunked — err is nil (the whole carry is
// written) and the max-hold bound is never observed during replay.
func TestWriteCarryChunkedStopsOnMaxHold(t *testing.T) {
	// A failing-write conn is fine: the max-hold poll is at the TOP of the loop, before
	// any write, so with maxC already fired the whole carry is the unwritten suffix and
	// nothing is sent.
	cn := pool.NewConn(&failWriteNetConn{})
	fd := &fdEngine{
		ap:       &AutoPipeliner{ctx: context.Background(), config: &AutoPipelineOptions{}}, // ctx not cancelled: LIVE path
		client:   &Client{baseClient: &baseClient{opt: &Options{WriteTimeout: time.Second}}},
		maxBatch: 2,
		window:   16,
	}
	inflight := newFDInflight()

	const n = 5
	carry := make([]fdReq, n)
	for i := range carry {
		carry[i] = fdReq{cmd: NewStatusCmd(context.Background(), "set", fmt.Sprintf("k%d", i), "v")}
	}

	// maxC already fired: the top-of-loop poll trips on the first chunk (LIVE path).
	maxC := make(chan time.Time, 1)
	maxC <- time.Now()
	// readerDone open (not closed) so the handoff / reader-gone branches do not fire.
	readerDone := make(chan struct{})
	suffix, err := fd.writeCarryChunked(context.Background(), cn, inflight, carry, readerDone, maxC)
	if !errors.Is(err, errFDMaxHold) {
		t.Fatalf("writeCarryChunked returned %v, want errFDMaxHold after the conn was held past max-hold", err)
	}
	// The whole (unwritten) carry is returned as the suffix to replay on the next lease.
	if len(suffix) != n {
		t.Fatalf("returned suffix has %d of %d carry commands — the tail was dropped (callers would hang)", len(suffix), n)
	}
	// Critically, the suffix must NOT be in the in-flight deque: a clean drain of the
	// (empty) written prefix must not wait on replies for commands that were never sent.
	if got := inflight.len(); got != 0 {
		t.Fatalf("in-flight deque holds %d entries after a max-hold recycle — the unwritten suffix must stay out-of-band, not be pushed", got)
	}
}

// TestWriteCarryChunkedStopsOnMaxHoldUnderBackpressure pins the STUCK-writer arm of
// r3937925412: a writer blocked in the between-chunk window gate (waiting on
// inflight.room while the in-flight deque is at the window) cannot observe max-hold from
// the non-blocking between-chunk poll. The gate select must itself watch maxC so a
// backpressured replay still recycles the conn instead of pinning it past
// FullDuplexMaxHold. This is the exact scenario the finding calls out ("writer stuck in
// writeCarryChunked can't observe maxC").
//
// Red-check: remove ONLY the `case <-maxC:` from the LIVE window-gate select (leave the
// between-chunk poll) — the gate blocks forever and the test times out.
func TestWriteCarryChunkedStopsOnMaxHoldUnderBackpressure(t *testing.T) {
	cn := pool.NewConn(&failWriteNetConn{})
	fd := &fdEngine{
		ap:       &AutoPipeliner{ctx: context.Background(), config: &AutoPipelineOptions{}}, // LIVE path
		client:   &Client{baseClient: &baseClient{opt: &Options{WriteTimeout: time.Second}}},
		maxBatch: 4,
		window:   1, // in-flight at the window forces the writer into the gate wait
	}
	inflight := newFDInflight()
	// Pre-fill the deque to the window so the FIRST chunk blocks in the gate before any
	// write. Nothing pops it (no reader), so the gate only exits on maxC.
	inflight.pushBatch([]fdReq{{cmd: NewStatusCmd(context.Background(), "get", "filler")}})

	const n = 5
	carry := make([]fdReq, n)
	for i := range carry {
		carry[i] = fdReq{cmd: NewStatusCmd(context.Background(), "set", fmt.Sprintf("k%d", i), "v")}
	}

	// Empty at entry so the between-chunk poll passes (default), then fired shortly after
	// so the ONLY exit is the gate's maxC case.
	maxC := make(chan time.Time)
	go func() {
		time.Sleep(50 * time.Millisecond)
		maxC <- time.Now()
	}()
	readerDone := make(chan struct{})

	suffix, err := fd.writeCarryChunked(context.Background(), cn, inflight, carry, readerDone, maxC)
	if !errors.Is(err, errFDMaxHold) {
		t.Fatalf("writeCarryChunked returned %v, want errFDMaxHold after the stuck writer hit max-hold", err)
	}
	if len(suffix) != n {
		t.Fatalf("returned suffix has %d of %d carry commands — the tail was dropped (callers would hang)", len(suffix), n)
	}
	// The carry suffix stays out-of-band; only the pre-filled marker remains in the deque.
	if got := inflight.len(); got != 1 {
		t.Fatalf("in-flight deque holds %d entries; want 1 (the pre-filled marker; the carry suffix must stay out-of-band)", got)
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
	// (in the submit queue and the in-flight deque). Submit off-goroutine: backpressure blocks
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

// TestFullDuplexSubmitCloseRace races concurrent submits against Close. The
// queue push runs under the SAME submit RLock as the rest of the submit path, so
// a push can never win the race with the shutdown drain (WLock + closed +
// drain), and the pooled blocking batch is recycled on every reject path. Every
// caller must settle (no hang, no panic) with either a served result or a
// shutdown/ctx error. Run under -race to catch any data race on the submit path.
func TestFullDuplexSubmitCloseRace(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := c.AutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex: true, FullDuplexWindow: 4096, MaxBatchSize: 8,
	})
	if err != nil {
		t.Fatalf("AutoPipelineWithOptions: %v", err)
	}
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
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
	// Close while submitters are mid-flight so the push interleaves with the
	// shutdown drain.
	time.Sleep(20 * time.Millisecond)
	if err := ap.Close(); err != nil {
		t.Fatalf("Close during concurrent submit: %v", err)
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
// cached ap's context, run() would park on the submit queue forever (new submits are rejected,
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

// TestFDRetryOnNormalConnCloseCancelPanicSafeSetErr pins that both
// retrySem-saturated arms in retryOnNormalConn — reached when Close cancels
// fd.ap.ctx while retrySem is full — go through fdSetErrSafe. Both run
// synchronously on the READER goroutine, before retryOnNormalConn's own
// recover (which only wraps the retry goroutine spawned further down) is in
// play: a panicking custom Cmder there previously escaped to the reader's
// session-failure recovery, tearing down the whole session and replaying the
// unacked tail — including requests already written, executing them twice
// (cursor bugbot on #4002). Pure, no server.
func TestFDRetryOnNormalConnCloseCancelPanicSafeSetErr(t *testing.T) {
	ctx := context.Background()

	t.Run("ctx already done before the first select", func(t *testing.T) {
		apCtx, cancel := context.WithCancel(context.Background())
		cancel()
		fd := &fdEngine{ap: &AutoPipeliner{ctx: apCtx}, retrySem: make(chan struct{}, 1)}
		fd.retrySem <- struct{}{} // saturate: no free slot

		cmd := &fdSetErrPanicCmd{StatusCmd: NewStatusCmd(ctx, "ping")}
		b := newAPBatch()
		done := make(chan struct{})
		go func() {
			fd.retryOnNormalConn(fdReq{cmd: cmd, batch: b, ctx: ctx}, 0)
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("retryOnNormalConn hung on a Close-time SetErr panic (first arm)")
		}
		select {
		case <-b.done:
		default:
			t.Fatal("batch was not completed")
		}
	})

	t.Run("ctx cancelled while blocked waiting for a slot", func(t *testing.T) {
		apCtx, cancel := context.WithCancel(context.Background())
		defer cancel()
		fd := &fdEngine{ap: &AutoPipeliner{ctx: apCtx}, retrySem: make(chan struct{}, 1)}
		fd.retrySem <- struct{}{} // saturate: no free slot, ctx still live

		cmd := &fdSetErrPanicCmd{StatusCmd: NewStatusCmd(ctx, "ping")}
		b := newAPBatch()
		done := make(chan struct{})
		go func() {
			fd.retryOnNormalConn(fdReq{cmd: cmd, batch: b, ctx: ctx}, 0)
			close(done)
		}()
		time.Sleep(20 * time.Millisecond) // let it park in the blocking inner select
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("retryOnNormalConn hung on a Close-time SetErr panic (second arm)")
		}
		select {
		case <-b.done:
		default:
			t.Fatal("batch was not completed")
		}
	})
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

// fdSetErrPanicCmd models a user-defined Cmder with a broken invariant check
// in SetErr: it always panics instead of recording the error.
type fdSetErrPanicCmd struct {
	*StatusCmd
}

func (c *fdSetErrPanicCmd) SetErr(error) { panic("boom: SetErr panics") }

// TestFullDuplexHookHostSetErrPanicRecovers pins that hostHook's normal
// result assignment AND its own recovery-defer reassignment both go through
// fdSetErrSafe. Before that fix, a custom Cmder whose SetErr always panics
// would panic on the normal assignment, enter the recovery defer, panic AGAIN
// on that defer's own SetErr call, and escape unrecovered — killing the
// hook-host goroutine (and the process) before b.close() ever woke the
// caller (cursor bugbot on #4002).
func TestFullDuplexHookHostSetErrPanicRecovers(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	c.AddHook(fdNoopHook{})
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	cmd := &fdSetErrPanicCmd{StatusCmd: NewStatusCmd(ctx, "ping")}
	f := ap.Submit(ctx, cmd)
	done := make(chan struct{})
	go func() {
		f.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("caller hung after a panicking SetErr in the hook host")
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

// fdRedisErrWriter returns a redis-typed error from Write, simulating a user
// io.Writer that fails (not panics) while a RawWriteToCmd streams the raw reply
// on the FD reader goroutine. isRedisError is true for this error, so the reader
// must classify it as session-fatal by the command TYPE — the payload is unread
// and the socket desynced — rather than mistaking it for a benign server reply.
type fdRedisErrWriter struct{}

func (fdRedisErrWriter) Write(p []byte) (int, error) {
	return 0, proto.RedisError("ERR sink boom")
}

// TestFullDuplexReaderRawSinkErrorRecovers is the non-panic sibling of
// TestFullDuplexReaderPanicRecovers: a RawWriteToCmd whose writer RETURNS a
// redis-typed error mid-stream must fail the session (the raw reply is
// half-streamed, so the socket is desynced) instead of settling inline and
// advancing the reader over leftover bytes. The value is large so the reply spans
// multiple read/write cycles and the first failed Write leaves payload unread. The
// engine must recover on a fresh session and read correct replies afterward.
func TestFullDuplexReaderRawSinkErrorRecovers(t *testing.T) {
	ctx := context.Background()
	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if err := c.Set(ctx, "fd:rsink:k", strings.Repeat("x", 1<<20), 0).Err(); err != nil {
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

	cmd := NewRawWriteToCmd(ctx, fdRedisErrWriter{}, "get", "fd:rsink:k")
	f := ap.Submit(ctx, cmd)
	done := make(chan struct{})
	go func() { _ = f.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("raw command never settled after a sink write error")
	}
	if cmd.Err() == nil {
		t.Fatal("expected an error after the sink write failed, got nil")
	}
	// The engine must recover AND read correct replies on the fresh session: a
	// subsequent GET must return exactly what was stored, proving no leftover bytes
	// from the abandoned raw reply were misread as a later command's reply.
	if err := ap.Set(ctx, "fd:rsink:after", "sentinel-value", 0).Err(); err != nil {
		t.Fatalf("engine did not recover after the sink error: %v", err)
	}
	if got, err := ap.Get(ctx, "fd:rsink:after").Result(); err != nil || got != "sentinel-value" {
		t.Fatalf("post-recovery GET = %q, err=%v; want %q (reply desync?)", got, err, "sentinel-value")
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
// fail-fast with the limiter error instead of hanging in the submit queue until the
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
// accepted-but-unwritten queued commands instead of failing them ErrClosed
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
			t.Fatalf("cmd %d came back ErrClosed — Close did not flush the accepted queue backlog", i)
		}
	}
}

// TestFullDuplexLeaseFailureFailsBacklog verifies that when the engine cannot
// lease a connection for a new session (fdLeaseErr, server down), accepted
// commands fail-fast once the lease retries are exhausted instead of hanging in
// the submit queue. Uses a dead address, so it needs no live server.
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

// fdPanicErrorStr is an error whose Error() panics — models an error returned by a
// custom Limiter whose stringification is buggy. classifyCommandError calls Error(), so
// this exercises the classify-before-complete panic path.
type fdPanicErrorStr struct{}

func (fdPanicErrorStr) Error() string { panic("boom: error Error() panic") }

// TestFDFailReqsClassifyPanicStillCompletes pins r3937450285: failReqs classifies the
// error (err.Error(), user-reachable) BEFORE completing the chunk, which is not in the
// in-flight queue, so a panic there would block every caller forever. The guarded
// classification must contain it and still complete the requests.
//
// Red-check: revert classifyCommandErrorGuarded back to classifyCommandError — the panic
// escapes failReqs and the command below never completes (test times out).
func TestFDFailReqsClassifyPanicStillCompletes(t *testing.T) {
	pool.SetAllMetricCallbacks(&pool.MetricCallbacks{
		Error: func(context.Context, string, *pool.Conn, string, bool, int) {},
	})
	defer pool.SetAllMetricCallbacks(nil)

	fd := &fdEngine{client: &Client{baseClient: &baseClient{opt: &Options{}}}}
	done := make(chan struct{})
	req := fdReq{cmd: NewStatusCmd(context.Background(), "get", "k"), hookDone: done, attempts: 1}

	fin := make(chan struct{})
	go func() {
		defer close(fin)
		fd.failReqs([]fdReq{req}, fdPanicErrorStr{})
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("failReqs did not complete the request — a panicking err.Error() in classification wedged it")
	}
	select {
	case <-fin:
	case <-time.After(2 * time.Second):
		t.Fatal("failReqs did not return")
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

// fdPutPanicPool is a pool.Pooler whose Put panics — models the release path blowing up
// (a custom pool, or the push drain in releaseConnToPool running a custom
// PushNotificationProcessor). It records Remove so a test can assert attempt's
// release-defer recover retires the conn instead of leaking it.
type fdPutPanicPool struct {
	pool.Pooler
	conn    *pool.Conn
	removes atomic.Int64
	puts    atomic.Int64
}

func (p *fdPutPanicPool) Get(context.Context) (*pool.Conn, error) { return p.conn, nil }
func (p *fdPutPanicPool) Put(context.Context, *pool.Conn) {
	p.puts.Add(1)
	panic("test: pool.Put panic on the full-duplex release path")
}
func (p *fdPutPanicPool) Remove(context.Context, *pool.Conn, error) { p.removes.Add(1) }

// TestFDReleasePanicRetiresConn pins r3937925393: attempt's release defer runs LAST
// (LIFO — the init recover is registered after it and runs FIRST), so a panic while
// releasing a CLEAN-ended conn (releaseConnToPool drains pushes via a custom processor,
// then Puts) had no outer boundary: it escaped the sole fd.run goroutine (process crash)
// and leaked the leased conn. The release defer now recovers the panic and Removes the
// conn — its drain/Put state is unknown, so Put would poison the pool.
//
// Red-check: remove the nested recover in attempt's release else-branch — the Put panic
// escapes attempt() and crashes the test process.
func TestFDReleasePanicRetiresConn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	const n = 3
	var reply bytes.Buffer
	dones := make([]chan struct{}, n)
	carry := make([]fdReq, n)
	for i := range carry {
		reply.WriteString("+OK\r\n")
		dones[i] = make(chan struct{})
		carry[i] = fdReq{cmd: NewStatusCmd(ctx, "set", fmt.Sprintf("k%d", i), "v"), hookDone: dones[i], attempts: 1}
	}
	cn := pool.NewConn(newFDCannedReplyConn(reply.Bytes()))
	cn.SetUsable(true) // IsInited() -> initPooledConn is a no-op, so attempt reaches session + the release defer
	p := &fdPutPanicPool{conn: cn}
	fd := &fdEngine{
		ap: &AutoPipeliner{config: &AutoPipelineOptions{}, ctx: ctx},
		// Protocol != 3: processPushNotifications is a no-op, so releaseConnToPool reaches Put.
		client:   &Client{baseClient: &baseClient{opt: &Options{WriteTimeout: time.Second, ReadTimeout: time.Second}}},
		pool:     p,
		maxBatch: 1,
		window:   100,
	}

	type res struct {
		result fdResult
		err    error
	}
	done := make(chan res, 1)
	go func() {
		_, result, e := fd.attempt(context.Background(), carry)
		done <- res{result, e}
	}()

	// Every command completes on the healthy session first.
	for i := range dones {
		select {
		case <-dones[i]:
		case <-time.After(5 * time.Second):
			t.Fatalf("command %d never completed", i)
		}
	}
	// End the session cleanly (fdGraceful): attempt then runs the release defer, where Put panics.
	cancel()

	var got res
	select {
	case got = <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("attempt() hung after ctx cancel — a release panic wedged the engine")
	}
	if got.result != fdGraceful {
		t.Fatalf("attempt result = %v, want fdGraceful (a release panic must not change the session outcome)", got.result)
	}
	if p.puts.Load() != 1 {
		t.Fatalf("puts = %d, want 1 (release attempted a Put, which panicked)", p.puts.Load())
	}
	if p.removes.Load() != 1 {
		t.Fatalf("removes = %d, want 1 (the release-panic recover must retire the conn)", p.removes.Load())
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

// TestFDReportReplyMetricsNilReplyType pins that the reader's per-reply error
// callback reports a Nil reply as NIL, not as an UNKNOWN internal error, so the
// recorder can drop it. Pure, no server.
func TestFDReportReplyMetricsNilReplyType(t *testing.T) {
	var gotType atomic.Value
	var gotInternal atomic.Bool
	pool.SetAllMetricCallbacks(&pool.MetricCallbacks{
		Error: func(_ context.Context, errorType string, _ *pool.Conn, _ string, isInternal bool, _ int) {
			gotType.Store(errorType)
			gotInternal.Store(isInternal)
		},
	})
	defer pool.SetAllMetricCallbacks(nil)

	fd := &fdEngine{client: &Client{baseClient: &baseClient{opt: &Options{}}}}
	req := fdReq{cmd: NewStringCmd(context.Background(), "get", "k"), attempts: 1}

	fd.reportReplyMetrics(context.Background(), req, Nil, nil)
	if got := gotType.Load(); got != "NIL" || gotInternal.Load() {
		t.Fatalf("errorType = %v, internal = %v, want NIL, false", got, gotInternal.Load())
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
	fd := &fdEngine{client: &Client{baseClient: &baseClient{opt: &Options{}}}, q: newFDQueue(n)}
	batches := make([]*apBatch, n)
	for i := 0; i < n; i++ {
		cmd := NewStatusCmd(ctx, "set", "k", "v")
		b := newAPBatch()
		cmd.setReady(b)
		batches[i] = b
		fd.q.push(fdReq{cmd: cmd, batch: b, ctx: ctx, attempts: 1})
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
	// writtenOff is an offset from the engine epoch, so the engine below must
	// carry one for the duration to come out positive.
	req := fdReq{cmd: cmd, batch: b, attempts: 1, writtenOff: int64(time.Millisecond)}

	fd := &fdEngine{
		client: &Client{baseClient: &baseClient{opt: &Options{}}},
		epoch:  time.Now().Add(-time.Second),
	}

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
type fdDrainErrProcessor struct {
	err error
	// calls, when non-nil, counts ProcessPendingNotifications invocations so a
	// test can assert whether the release drain handed the processor any work.
	// The release drain runs synchronously on the releasing goroutine, so a
	// plain counter is race-free here.
	calls *int
}

func (fdDrainErrProcessor) GetHandler(string) push.NotificationHandler { return nil }
func (fdDrainErrProcessor) RegisterHandler(string, push.NotificationHandler, bool) error {
	return nil
}
func (fdDrainErrProcessor) UnregisterHandler(string) error { return nil }
func (p fdDrainErrProcessor) ProcessPendingNotifications(context.Context, push.NotificationHandlerContext, *proto.Reader) error {
	if p.calls != nil {
		*p.calls++
	}
	return p.err
}

// TestReleaseConnRemovesConnAfterCustomDrainError pins finding F3: on the release
// path, a custom PushNotificationProcessor that returns a redis.Error during the
// release-time push drain left the possibly-partially-drained conn returning to
// the pool - releaseConnToPool only Removed it when isBadConn recognized the
// error, and isBadConn returns false for a (non-readonly, non-moved) redis.Error.
// A drain error must make the conn unusable so it is Removed regardless.
//
// The push frame is pulled into the reader buffer first: the release drain hands
// a custom processor work only when real bytes are BUFFERED (see
// peekAndProcessPushNotifications and .claude/specs/push.md) — bare socket
// readability can be a zero-RESP-byte TLS control record, and a custom
// processor's blocking loop handed that times out and retires a healthy conn.
// With the frame buffered the processor runs, its error propagates, and the
// Remove-on-any-drain-error rule is what this test pins.
// TestReleaseConnKeepsCustomProcessorConnOnBareReadiness covers the unbuffered
// side of that gate.
//
// Red-check: restore the `if isBadConn(err, ...) { Remove; return }` guard -> the
// redis.Error drain error is not isBadConn, so the conn is Put (puts=1, removes=0).
func TestReleaseConnRemovesConnAfterCustomDrainError(t *testing.T) {
	server, client := newIdleTCPConnPair(t)
	defer server.Close()
	defer client.Close()

	cn := pool.NewConn(client)
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
	// Buffer the frame without consuming it: PeekReplyType reads the type byte
	// into the bufio buffer and leaves it there.
	if err := cn.WithReader(context.Background(), time.Second, func(rd *proto.Reader) error {
		_, err := rd.PeekReplyType()
		return err
	}); err != nil {
		t.Fatalf("buffering push frame: %v", err)
	}
	if !cn.HasBufferedData() {
		t.Fatal("push frame not buffered after peek")
	}

	calls := 0
	cp := &releaseRecordingPool{}
	c := &baseClient{
		opt:           &Options{Addr: "127.0.0.1:6379", Protocol: 3},
		connPool:      cp,
		pushProcessor: fdDrainErrProcessor{err: fdFakeRedisErr{}, calls: &calls},
	}
	// err=nil so the first guard (isBadConn/errConnUnusable on the op error) does
	// not fire; the drain error alone must drive removal.
	c.releaseConn(context.Background(), cn, nil)

	if calls != 1 {
		t.Fatalf("custom processor invoked %d times; want 1 (a buffered push must reach it)", calls)
	}
	if cp.removes != 1 || cp.puts != 0 {
		t.Fatalf("a custom release-drain error must remove, not re-pool, the conn: removes=%d puts=%d",
			cp.removes, cp.puts)
	}
}

// TestReleaseConnKeepsCustomProcessorConnOnBareReadiness pins the other side of
// the release drain's custom-processor gate (#3989, .claude/specs/push.md): when
// the socket is readable but NOTHING is buffered, a custom processor is not
// invoked and the conn is re-pooled intact. Readability alone can be a
// zero-RESP-byte TLS control record; a custom processor's blocking loop handed
// that would time out on an empty read, and the caller would retire a healthy
// connection. The frame stays in the socket for the next reply read, the
// session recycle, or the MaxStaleness backstop. Only the built-in processor,
// which treats an empty probe as benign, drains on bare readiness.
func TestReleaseConnKeepsCustomProcessorConnOnBareReadiness(t *testing.T) {
	server, client := newIdleTCPConnPair(t)
	defer server.Close()
	defer client.Close()

	cn := pool.NewConn(client)
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
	if cn.HasBufferedData() {
		t.Fatal("precondition: nothing may be buffered yet")
	}

	calls := 0
	cp := &releaseRecordingPool{}
	c := &baseClient{
		opt:           &Options{Addr: "127.0.0.1:6379", Protocol: 3},
		connPool:      cp,
		pushProcessor: fdDrainErrProcessor{err: fdFakeRedisErr{}, calls: &calls},
	}
	c.releaseConn(context.Background(), cn, nil)

	if calls != 0 {
		t.Fatalf("custom processor invoked %d times on bare readiness; want 0", calls)
	}
	if cp.puts != 1 || cp.removes != 0 {
		t.Fatalf("bare readiness with a custom processor must re-pool the conn intact: puts=%d removes=%d",
			cp.puts, cp.removes)
	}
	if cn.HasBufferedData() {
		t.Fatal("nothing may have been consumed from the socket")
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

// TestFullDuplexSpillsToMainPoolOnPipelineDialError pins the spill-on-dial-error
// fix (codex on #4002): the FD lease acquires from the pipeline pool with TryGet,
// which DIALS when the pool has no idle conn. A transient dial failure there is not
// saturation (ErrPoolTryFull/ErrPoolExhausted), so the previous allow-list surfaced
// it as a lease error and failed the accepted backlog even with a healthy main pool.
// The spill is now a deny-list: everything except a cancelled ctx or a closed pool
// spills. Here the pipeline pool's dialer always errors while the main pool is
// healthy, so the accepted commands must spill and complete.
//
// Red-check: restore the allow-list (spill only on ErrPoolTryFull/ErrPoolExhausted)
// -> TryGet's dial error returns fdLeaseErr and Set below fails instead of spilling.
func TestFullDuplexSpillsToMainPoolOnPipelineDialError(t *testing.T) {
	ctx := context.Background()

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

	// Swap the pipeline pool for one whose dialer always fails, BEFORE the engine
	// leases (the engine reads loadPipelinePool live on first command). A raw pool
	// with a plain failing dialer does a single dial attempt — no root-level retry
	// wrapper — so TryGet returns the dial error at once. The main pool keeps its
	// real dialer.
	orig := c.pipelinePool
	failPool := pool.NewConnPool(&pool.Options{
		Dialer:             func(context.Context) (net.Conn, error) { return nil, errors.New("pipeline dial boom") },
		PoolSize:           1,
		MaxConcurrentDials: 1,
		MinIdleConns:       0,
		PoolTimeout:        100 * time.Millisecond,
		DialTimeout:        time.Second,
		ConnMaxIdleTime:    -1,
	})
	c.pipelinePool = &pipelinePoolRef{pool: failPool, name: "fd-faildial-pipe"}
	t.Cleanup(func() {
		_ = failPool.Close()
		if orig != nil {
			_ = orig.pool.Close()
		}
	})

	// Detect the spill: a Get on the MAIN pool by the FD lease.
	mainHook := &fdCountHook{}
	c.connPool.AddPoolHook(mainHook)

	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()

	// The engine leases lazily on the first command; the pipeline pool's dial fails,
	// so the lease must spill to the healthy main pool and the command must succeed,
	// not fail with the dial error.
	if err := ap.Set(ctx, "fd:faildial:k", "v", 0).Err(); err != nil {
		t.Fatalf("FD command failed instead of spilling on a pipeline dial error: %v", err)
	}
	if v, err := ap.Get(ctx, "fd:faildial:k").Result(); err != nil || v != "v" {
		t.Fatalf("FD get after dial-error spill: v=%q err=%v", v, err)
	}
	if g := mainHook.gets.Load(); g < 1 {
		t.Fatalf("FD lease did not spill to the main pool on a pipeline dial error (main-pool gets=%d)", g)
	}
}

// --- MaxFlushDelay on the full-duplex writer (the fdAccumMinFor gate) -------

// TestFDAccumMinFor pins the in-flight threshold the writer uses to decide
// whether waiting MaxFlushDelay is worthwhile. Pure function, no server, so
// both sides of the gate are covered deterministically.
func TestFDAccumMinFor(t *testing.T) {
	for _, tc := range []struct {
		name   string
		window int
		want   int
	}{
		// Below 64*512 the shift underflows the floor, so the floor wins: a
		// caller who shrinks the window must not end up with a threshold so low
		// that any trickle of traffic trips it.
		{"zero", 0, 64},
		{"tiny", 1, 64},
		{"at-floor-boundary", 64 * 512, 64},
		// Above it the threshold tracks the window, so enlarging the pipeline
		// depth raises the bar in proportion.
		{"default-window", fdDefaultWindow, fdDefaultWindow / 512},
		{"double-default", 2 * fdDefaultWindow, 2 * fdDefaultWindow / 512},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := fdAccumMinFor(tc.window); got != tc.want {
				t.Fatalf("fdAccumMinFor(%d) = %d, want %d", tc.window, got, tc.want)
			}
		})
	}
	// The default must land at 128: that is the value every queue-path
	// measurement in the fdAccumMinFor doc comment was taken at, so changing it
	// invalidates them. On the channel submit path the same 128 measured -11.1%
	// at 128 callers, which is why #4014 uses a different shift — the threshold
	// is mechanism-specific, and this pins THIS mechanism's.
	if got := fdAccumMinFor(fdDefaultWindow); got != 128 {
		t.Fatalf("default threshold = %d, want 128", got)
	}
}

// TestFullDuplexMaxFlushDelayNoWaitAtLowConcurrency is the load-gate contract,
// and the regression test for the gate itself: with a single caller in flight
// the writer must NOT wait MaxFlushDelay, so a lone command still costs ~1 RTT.
// The delay is set far above any plausible round trip, so if the gate is ever
// removed or its threshold drops to ~0, the command inherits the delay and the
// deadline below fails. (Unguarded, this configuration measured 64 callers
// dropping from 130k ops/s at p50 0.48ms to 41k at p50 1.59ms.)
func TestFullDuplexMaxFlushDelayNoWaitAtLowConcurrency(t *testing.T) {
	ctx := context.Background()

	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}

	const delay = 400 * time.Millisecond
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex:    true,
		MaxFlushDelay: delay,
	})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()

	if err := ap.Set(ctx, "fd:accum:one", "v", 0).Err(); err != nil {
		t.Fatalf("warm Set: %v", err)
	}

	// Several attempts rather than one: a single sample could be unlucky (GC,
	// scheduler), but the gate either waits on every batch or on none of them.
	const attempts = 5
	var fast int
	for i := 0; i < attempts; i++ {
		start := time.Now()
		if err := ap.Get(ctx, "fd:accum:one").Err(); err != nil {
			t.Fatalf("Get: %v", err)
		}
		if time.Since(start) < delay/4 {
			fast++
		}
	}
	if fast <= attempts/2 {
		t.Fatalf("only %d/%d single-caller commands beat %v; the in-flight gate "+
			"is not keeping low concurrency off the accumulation wait",
			fast, attempts, delay/4)
	}
}

// TestFullDuplexMaxFlushDelayCorrectUnderLoad drives enough sustained
// concurrency to push in-flight past fdAccumMinFor, so the accumulation wait is
// genuinely armed and both of its arms -- a command arriving, and the timer
// expiring -- are exercised. Instrumenting the drain loop while developing this
// test recorded 260 arms across 702 queue-empty hits for this configuration, so
// the path is reached rather than merely reachable.
//
// It asserts correctness through that path (every reply is the value that was
// written, no errors, no stall) rather than an achieved batch size: commands
// per flush is not observable from outside the engine without adding
// instrumentation. The batching effect itself is quantified in the benchmark
// numbers in the PR description.
func TestFullDuplexMaxFlushDelayCorrectUnderLoad(t *testing.T) {
	ctx := context.Background()

	c := fdTestClient(":6379")
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}

	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex: true,
		// 1024 keeps the threshold at the 64 floor while leaving the window well
		// above it, so the gate trips on load rather than on the window
		// backpressure gate.
		FullDuplexWindow: 1024,
		MaxFlushDelay:    2 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()

	const (
		workers = 128
		perWkr  = 40
	)
	for i := 0; i < workers; i++ {
		if err := ap.Set(ctx, fmt.Sprintf("fd:accum:load:%d", i), fmt.Sprint(i), 0).Err(); err != nil {
			t.Fatalf("prefill: %v", err)
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			key, want := fmt.Sprintf("fd:accum:load:%d", id), fmt.Sprint(id)
			for j := 0; j < perWkr; j++ {
				got, err := ap.Get(ctx, key).Result()
				if err != nil {
					errs <- fmt.Errorf("worker %d: %w", id, err)
					return
				}
				if got != want {
					errs <- fmt.Errorf("worker %d: got %q want %q", id, got, want)
					return
				}
			}
		}(i)
	}

	waited := make(chan struct{})
	go func() { wg.Wait(); close(waited) }()
	select {
	case <-waited:
	case <-time.After(30 * time.Second):
		t.Fatal("load phase did not finish in 30s: the writer is stuck in the " +
			"accumulation wait")
	}
	close(errs)
	for err := range errs {
		t.Fatalf("%v", err)
	}
}

// ===== from autopipeline_cluster_fd_internal_test.go =====

// clusterFDTestAddrs is the local 3-master cluster the functional test targets.
// Override with GOREDIS_CLUSTER_FD_ADDRS (comma-separated) to point at another
// cluster (e.g. a real RE endpoint). The test self-skips when the cluster is not
// reachable, so it is safe to run in any environment.
var clusterFDTestAddrs = clusterFDTestAddrsFromEnv()

func clusterFDTestAddrsFromEnv() []string {
	if v := os.Getenv("GOREDIS_CLUSTER_FD_ADDRS"); v != "" {
		return strings.Split(v, ",")
	}
	return []string{"127.0.0.1:16600", "127.0.0.1:16601", "127.0.0.1:16602"}
}

func dialClusterFDTest(t *testing.T) *ClusterClient {
	t.Helper()
	cc := NewClusterClient(&ClusterOptions{Addrs: clusterFDTestAddrs})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cc.Ping(ctx).Err(); err != nil {
		cc.Close()
		t.Skipf("local cluster not reachable at %v: %v", clusterFDTestAddrs, err)
	}
	// Force a topology load so state.Masters is populated for the assertions.
	if _, err := cc.state.ReloadOrGet(ctx); err != nil {
		cc.Close()
		t.Skipf("cluster state not loadable: %v", err)
	}
	return cc
}

// TestClusterFullDuplexEngagesPerNode is the correctness demo for native
// full-duplex on a ClusterClient: FD is reported active, one FD child engine
// runs per master, a mixed workload across all slots executes with zero errors,
// diverted commands (fan-out, blocking) still work, and Close returns cleanly.
func TestClusterFullDuplexEngagesPerNode(t *testing.T) {
	cc := dialClusterFDTest(t)
	defer cc.Close()

	ctx := context.Background()

	ap, err := cc.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipelineWithOptions: %v", err)
	}
	defer ap.Close()

	// (a) Config() reports FD active — the exact bit that was always false on a
	// ClusterClient before this change.
	if !ap.Config().FullDuplex {
		t.Fatalf("Config().FullDuplex = false, want true (cluster FD should be active)")
	}
	if ap.clusterFD == nil {
		t.Fatalf("ap.clusterFD is nil, want a router")
	}
	if got := ap.Config().NumShards; got != 1 {
		t.Fatalf("Config().NumShards = %d, want 1 (cluster FD forces a single flusherless shard)", got)
	}

	// (c) Mixed GET/SET across many keys → keys spread over all three masters'
	// slot ranges. Fire concurrently so the FD engines actually batch. Zero errors
	// and correct read-back proves per-node routing lands each key on its owner.
	const nKeys = 300
	var wg sync.WaitGroup
	errCh := make(chan error, nKeys)
	for i := 0; i < nKeys; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("cfd:{%d}:k", i)
			val := fmt.Sprintf("v%d", i)
			if err := ap.Set(ctx, key, val, 0).Err(); err != nil {
				errCh <- fmt.Errorf("SET %s: %w", key, err)
				return
			}
			got, err := ap.Get(ctx, key).Result()
			if err != nil {
				errCh <- fmt.Errorf("GET %s: %w", key, err)
				return
			}
			if got != val {
				errCh <- fmt.Errorf("GET %s = %q, want %q", key, got, val)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("workload error: %v", err)
	}

	// (b) One FD child engine per master, and every child actually engaged FD.
	state, err := cc.state.Get(ctx)
	if err != nil {
		t.Fatalf("state.Get: %v", err)
	}
	nMasters := len(state.Masters)
	ap.clusterFD.mu.RLock()
	nChildren := len(ap.clusterFD.children)
	var notFD, badBudget int
	for _, ch := range ap.clusterFD.children {
		if ch.fd == nil {
			notFD++
			continue
		}
		// Recovery budget must be the cluster's MaxRedirects, not the node client's
		// MaxRetries (-1) — otherwise carry-replay is disabled on this master.
		if ch.fd.retryBudget() != cc.opt.MaxRedirects {
			badBudget++
		}
	}
	ap.clusterFD.mu.RUnlock()
	if nChildren != nMasters {
		t.Errorf("clusterFD has %d children, want %d (one per master)", nChildren, nMasters)
	}
	if notFD != 0 {
		t.Errorf("%d/%d children did not engage FD (child.fd == nil)", notFD, nChildren)
	}
	if badBudget != 0 {
		t.Errorf("%d/%d children have retryBudget != cc.MaxRedirects (%d) — carry-replay would be misbudgeted",
			badBudget, nChildren, cc.opt.MaxRedirects)
	}
	t.Logf("cluster FD: %d masters, %d FD children engaged", nMasters, nChildren-notFD)

	// (d) Diversion still works: a fan-out command (DBSIZE) and a blocking command
	// (BLPOP with a short timeout) must not ride the FD pipe.
	if err := cc.DBSize(ctx).Err(); err != nil {
		t.Errorf("DBSize (fan-out) failed: %v", err)
	}
	blKey := "cfd:{bl}:list"
	if _, err := ap.BLPop(ctx, 100*time.Millisecond, blKey).Result(); err != nil && err != Nil {
		t.Errorf("BLPop (diverted) unexpected error: %v", err)
	}

	// (e) Close returns without hanging.
	done := make(chan error, 1)
	go func() { done <- ap.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("ap.Close returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("ap.Close did not return within 10s")
	}
}

// TestClusterFullDuplexReuseAfterClose covers the lifecycle the single-AP test
// cannot: closing the cluster autopipeliner closes the per-node FD children,
// which stay cached on each node.Client. A fresh autopipeliner on the SAME
// (still-open) ClusterClient must NOT hand back a closed child and fail every
// command with ErrClosed — the node getter rebuilds a live child and the router
// re-engages FD.
func TestClusterFullDuplexReuseAfterClose(t *testing.T) {
	cc := dialClusterFDTest(t)
	defer cc.Close()
	ctx := context.Background()

	ap1, err := cc.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("first AsyncAutoPipelineWithOptions: %v", err)
	}
	if err := ap1.Set(ctx, "cfd:{reuse}:k", "1", 0).Err(); err != nil {
		t.Fatalf("first SET: %v", err)
	}
	if err := ap1.Close(); err != nil {
		t.Fatalf("ap1.Close: %v", err)
	}

	// getOrCreateAutoPipeliner rebuilds on a closed cached instance, so this
	// returns a NEW parent (the ClusterClient itself was never closed).
	ap2, err := cc.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("second AsyncAutoPipelineWithOptions: %v", err)
	}
	defer ap2.Close()
	if ap2 == ap1 {
		t.Fatalf("expected a fresh autopipeliner after Close, got the closed one")
	}
	if !ap2.Config().FullDuplex {
		t.Fatalf("reused AP Config().FullDuplex = false, want true")
	}

	// The command that would have failed ErrClosed if a closed child were cached.
	got, err := func() (string, error) {
		if err := ap2.Set(ctx, "cfd:{reuse}:k", "2", 0).Err(); err != nil {
			return "", err
		}
		return ap2.Get(ctx, "cfd:{reuse}:k").Result()
	}()
	if err != nil {
		t.Fatalf("SET/GET on reused AP: %v", err)
	}
	if got != "2" {
		t.Fatalf("GET after reuse = %q, want %q", got, "2")
	}

	// FD re-engaged: a fresh live child, not the closed one.
	ap2.clusterFD.mu.RLock()
	var closedChildren int
	for _, ch := range ap2.clusterFD.children {
		if ch.fd == nil || ch.IsClosed() {
			closedChildren++
		}
	}
	nChildren := len(ap2.clusterFD.children)
	ap2.clusterFD.mu.RUnlock()
	if nChildren == 0 {
		t.Fatalf("reused AP has no FD children after traffic")
	}
	if closedChildren != 0 {
		t.Errorf("reused AP cached %d closed/non-FD children, want 0", closedChildren)
	}
}

// TestClusterFullDuplexChildCloseSelfHeals covers the case ReuseAfterClose does
// not: the ROUTER's parent AutoPipeliner stays open, but ONE node child gets
// closed underneath it — the shape of a cluster topology GC closing a node's
// client (and its cached FD child) while the rest of the cluster, and this
// router, are still very much in use. A command on that node must self-heal
// (rebuild the child, or divert to Process for a node that is truly gone)
// rather than surface the closed child's ErrClosed to the caller.
//
// This pins the observable CONTRACT, not the specific race a bot review
// flagged. Closing the child here happens strictly BEFORE the next submit, so
// getOrCreateChild's own pre-existing cached-but-closed rebuild
// (autopipeline_cluster_fd.go childFor) already satisfies this test on its
// own — the harder case (a submit that is already parked inside
// fdEngine.submit's backpressure select when the child closes, per
// autopipeline_fullduplex.go) is a true TOCTOU race with no deterministic
// repro; clusterFDRouter.submit's post-dispatch retry (same file) narrows
// that window but is not exercised as a distinct case here.
func TestClusterFullDuplexChildCloseSelfHeals(t *testing.T) {
	cc := dialClusterFDTest(t)
	defer cc.Close()
	ctx := context.Background()

	ap, err := cc.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipelineWithOptions: %v", err)
	}
	defer ap.Close()

	const key = "cfd:{childclose}:k"
	if err := ap.Set(ctx, key, "1", 0).Err(); err != nil {
		t.Fatalf("initial SET: %v", err)
	}

	// Grab the child that owns key's slot and close it DIRECTLY — simulating
	// topology GC tearing down a node child without touching the ClusterClient
	// or the router (unlike ReuseAfterClose, which closes the whole parent).
	ap.clusterFD.mu.RLock()
	var closedChild *AutoPipeliner
	for _, ch := range ap.clusterFD.children {
		closedChild = ch
		break
	}
	ap.clusterFD.mu.RUnlock()
	if closedChild == nil {
		t.Fatalf("no FD child cached after initial SET")
	}
	if err := closedChild.Close(); err != nil {
		t.Fatalf("closedChild.Close: %v", err)
	}

	// A command on the SAME key must still succeed — self-healed, not ErrClosed.
	if err := ap.Set(ctx, key, "2", 0).Err(); err != nil {
		t.Fatalf("SET after child close: %v (want self-heal, not ErrClosed)", err)
	}
	got, err := ap.Get(ctx, key).Result()
	if err != nil {
		t.Fatalf("GET after child close: %v", err)
	}
	if got != "2" {
		t.Fatalf("GET after child close = %q, want %q", got, "2")
	}

	// The router now caches a fresh, live child for that node — not the closed one.
	ap.clusterFD.mu.RLock()
	var sawFresh bool
	for _, ch := range ap.clusterFD.children {
		if ch != closedChild && !ch.IsClosed() {
			sawFresh = true
		}
	}
	ap.clusterFD.mu.RUnlock()
	if !sawFresh {
		t.Errorf("router did not rebuild a fresh child after the cached one closed")
	}
}

// nodeClientFor reports the *Client that owns key's slot in the current
// topology, without creating or touching any FD child (unlike childFor).
// Used by eviction tests to find a key that routes to a node different from
// one already cached.
func nodeClientFor(t *testing.T, ctx context.Context, r *clusterFDRouter, key string) (*Client, bool) {
	t.Helper()
	slot := r.cc.cmdSlot(NewStatusCmd(ctx, "get", key), -1)
	if slot < 0 {
		return nil, false
	}
	state, err := r.cc.state.Get(ctx)
	if err != nil {
		return nil, false
	}
	node, err := state.slotMasterNode(slot)
	if err != nil || node == nil {
		return nil, false
	}
	return node.Client, true
}

// TestClusterFullDuplexEvictsStaleChildOnRebuild pins that a closed node
// child for a DIFFERENT node than the one currently being (re)built gets
// swept out of clusterFDRouter.children. Topology GC can close a node client
// (and its cached child) while the router stays up; once the cluster stops
// routing to that *Client, getOrCreateChild is never called with it again, so
// nothing else would notice it went stale — without the sweep it (and its FD
// engine, submit channel, held connection) would sit retained in the map for
// the router's whole life (cursor bugbot on #4002).
func TestClusterFullDuplexEvictsStaleChildOnRebuild(t *testing.T) {
	cc := dialClusterFDTest(t)
	defer cc.Close()
	ctx := context.Background()

	ap, err := cc.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipelineWithOptions: %v", err)
	}
	defer ap.Close()

	// Seed one key to populate its node's child, then hunt for a second tag
	// that lands on a DIFFERENT node — a first-time build for that second
	// node is what forces getOrCreateChild's write-lock (sweep) path without
	// needing to touch the second node's own cache state.
	const seedKey = "cfd:{evictseed}:k"
	if err := ap.Set(ctx, seedKey, "1", 0).Err(); err != nil {
		t.Fatalf("seed SET: %v", err)
	}
	ap.clusterFD.mu.RLock()
	var staleClient *Client
	var staleChild *AutoPipeliner
	for c, ch := range ap.clusterFD.children {
		staleClient, staleChild = c, ch
	}
	ap.clusterFD.mu.RUnlock()
	if staleClient == nil {
		t.Fatal("no FD child cached after seed SET")
	}

	var secondKey string
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("cfd:{evicttarget%d}:k", i)
		nc, ok := nodeClientFor(t, ctx, ap.clusterFD, k)
		if ok && nc != staleClient {
			secondKey = k
			break
		}
	}
	if secondKey == "" {
		t.Skip("could not find a tag routing to a different master after 50 tries")
	}

	// Simulate topology GC: close the seed node's child directly, but leave
	// its (now stale) entry in the map exactly as a real close hook would.
	if err := staleChild.Close(); err != nil {
		t.Fatalf("staleChild.Close: %v", err)
	}

	// First-time build for the second node: write-lock path, sweep runs.
	if err := ap.Set(ctx, secondKey, "1", 0).Err(); err != nil {
		t.Fatalf("SET on second node: %v", err)
	}

	ap.clusterFD.mu.RLock()
	_, stillPresent := ap.clusterFD.children[staleClient]
	n := len(ap.clusterFD.children)
	ap.clusterFD.mu.RUnlock()
	if stillPresent {
		t.Error("stale closed child was not evicted from clusterFDRouter.children")
	}
	if n != 1 {
		t.Errorf("clusterFDRouter.children has %d entries, want 1 (only the second node's fresh child)", n)
	}
}

// TestClusterFullDuplexEvictsChildOnNodeClose pins that a node's entry is
// pruned from clusterFDRouter.children the moment the node's OWN *Client
// closes — with NO further submit or rebuild needed. The sweep in
// getOrCreateChild only runs when some OTHER node's cache miss takes the
// write-lock path; a cluster whose live traffic only ever hits
// already-cached nodes would never trigger it, so a node removed by
// topology GC would sit retained forever (cursor bugbot on #4002, a gap in
// the sweep-only fix).
func TestClusterFullDuplexEvictsChildOnNodeClose(t *testing.T) {
	cc := dialClusterFDTest(t)
	defer cc.Close()
	ctx := context.Background()

	ap, err := cc.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipelineWithOptions: %v", err)
	}
	defer ap.Close()

	if err := ap.Set(ctx, "cfd:{nodeclose}:k", "1", 0).Err(); err != nil {
		t.Fatalf("seed SET: %v", err)
	}

	ap.clusterFD.mu.RLock()
	var nc *Client
	for c := range ap.clusterFD.children {
		nc = c
	}
	ap.clusterFD.mu.RUnlock()
	if nc == nil {
		t.Fatal("no FD child cached after seed SET")
	}

	// Close the NODE CLIENT itself (not its cached child) — this is exactly
	// what clusterNodes.GC does to a removed node (osscluster.go).
	if err := nc.Close(); err != nil {
		t.Fatalf("nc.Close: %v", err)
	}

	ap.clusterFD.mu.RLock()
	_, stillPresent := ap.clusterFD.children[nc]
	ap.clusterFD.mu.RUnlock()
	if stillPresent {
		t.Error("node client's entry was not evicted immediately on its own Close")
	}
}

// TestClusterFullDuplexGatedOffForReplicaRouting asserts the construction gate:
// a ClusterClient configured for replica routing (ReadOnly / RouteByLatency /
// RouteRandomly) must NOT engage cluster FD, because the router only routes to
// the slot master (slotMasterNode) and would silently ignore those options,
// pinning reads to masters. It must fall back to the half-duplex flushers (which
// honor the ShardPicker) and report Config().FullDuplex == false — the honest
// effective state. Pure construction check: no live cluster required (the gate
// reads cc.opt synchronously, and the half-duplex AP dials nothing until a
// command is submitted).
// TestClusterFDRouterCloseUnregistersEvictHooks pins that closing a cluster FD
// router detaches the per-node evict hooks getOrCreateChild registered on each
// node client's onClose registry — for every node it ever registered on, not
// only those still in children (the stale-child sweep and the hook itself drop
// children entries while the registration stays live). Left registered, each
// hook kept the closed router, and through parent the AutoPipeliner and its
// clusterReprocess closure, reachable until the node client itself closed,
// accumulating one per autopipeliner create/close cycle per node (codex +
// cursor bugbot on #4002). No server: the registry and router state are driven
// directly, the way getOrCreateChild's publish step leaves them.
func TestClusterFDRouterCloseUnregistersEvictHooks(t *testing.T) {
	nodeA := NewClient(&Options{Addr: "localhost:1"}) // never dialed
	nodeB := NewClient(&Options{Addr: "localhost:1"})
	t.Cleanup(func() { _ = nodeA.Close(); _ = nodeB.Close() })

	r := &clusterFDRouter{children: make(map[*Client]*AutoPipeliner)}
	for _, nc := range []*Client{nodeA, nodeB} {
		nc := nc
		if !nc.onClose.register(clusterFDRouterEvictID(r, nc), func() error {
			r.mu.Lock()
			delete(r.children, nc)
			delete(r.evictHooks, nc)
			r.mu.Unlock()
			return nil
		}) {
			t.Fatal("register on a live node client must succeed")
		}
		if r.evictHooks == nil {
			r.evictHooks = make(map[*Client]struct{})
		}
		r.evictHooks[nc] = struct{}{}
	}
	// Neither node has a children entry (as after the sweep dropped them); the
	// registrations must be detached regardless.
	hasHook := func(nc *Client) bool {
		nc.onClose.mu.Lock()
		defer nc.onClose.mu.Unlock()
		_, ok := nc.onClose.hooks[clusterFDRouterEvictID(r, nc)]
		return ok
	}
	if !hasHook(nodeA) || !hasHook(nodeB) {
		t.Fatal("precondition: both node clients must hold this router's evict hook")
	}

	if err := r.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if hasHook(nodeA) || hasHook(nodeB) {
		t.Fatal("close must unregister the router's evict hook from every node client it registered on")
	}
	r.mu.Lock()
	tracked := len(r.evictHooks)
	r.mu.Unlock()
	if tracked != 0 {
		t.Fatalf("evictHooks still tracks %d node clients after close", tracked)
	}
}

func TestClusterFullDuplexGatedOffForReplicaRouting(t *testing.T) {
	addrs := []string{"127.0.0.1:16600"}
	cases := []struct {
		name string
		opt  *ClusterOptions
	}{
		{"ReadOnly", &ClusterOptions{Addrs: addrs, ReadOnly: true}},
		{"RouteByLatency", &ClusterOptions{Addrs: addrs, RouteByLatency: true}},
		{"RouteRandomly", &ClusterOptions{Addrs: addrs, RouteRandomly: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cc := NewClusterClient(tc.opt)
			defer cc.Close()
			ap, err := cc.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
			if err != nil {
				t.Fatalf("AsyncAutoPipelineWithOptions: %v", err)
			}
			defer ap.Close()
			if ap.Config().FullDuplex {
				t.Errorf("Config().FullDuplex = true, want false (replica routing must disable cluster FD)")
			}
			if ap.clusterFD != nil {
				t.Errorf("ap.clusterFD is non-nil, want nil (cluster FD must not engage under replica routing)")
			}
		})
	}
}

// TestClusterFullDuplexResolvesDefaultsOnParent asserts the effective-defaults
// contract: a default-constructed cluster-FD autopipeliner must report the
// resolved FD tuning defaults from Config(), not the raw zeros the user passed —
// the same guarantee the standalone FD path gives. The per-node children resolve
// these in newFDEngine; the parent must resolve them too so Config() is honest.
// Pure construction check: cluster FD engages on PipelinePoolSize>=0 (the default)
// without loading topology, so no live cluster is required.
func TestClusterFullDuplexResolvesDefaultsOnParent(t *testing.T) {
	cc := NewClusterClient(&ClusterOptions{Addrs: []string{"127.0.0.1:16600"}})
	defer cc.Close()

	ap, err := cc.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipelineWithOptions: %v", err)
	}
	defer ap.Close()

	cfg := ap.Config()
	if !cfg.FullDuplex || ap.clusterFD == nil {
		t.Fatalf("cluster FD did not engage (FullDuplex=%v, clusterFD==nil:%v); cannot check defaults",
			cfg.FullDuplex, ap.clusterFD == nil)
	}
	if cfg.FullDuplexWindow != fdDefaultWindow {
		t.Errorf("Config().FullDuplexWindow = %d, want %d (effective default)", cfg.FullDuplexWindow, fdDefaultWindow)
	}
	if cfg.FullDuplexIdleTimeout != fdDefaultIdle {
		t.Errorf("Config().FullDuplexIdleTimeout = %v, want %v (effective default)", cfg.FullDuplexIdleTimeout, fdDefaultIdle)
	}
	if cfg.FullDuplexMaxHold != fdDefaultMaxHold {
		t.Errorf("Config().FullDuplexMaxHold = %v, want %v (effective default)", cfg.FullDuplexMaxHold, fdDefaultMaxHold)
	}
}

// TestClusterFullDuplexRecoveryBudgetFromMaxRedirects asserts the connection-
// failure recovery budget wiring: a cluster node.Client normalizes MaxRetries to
// -1 (cluster retries live in MaxRedirects), which the FD carry-replay would read
// as "budget already spent" and fail every in-flight command on the first socket
// error. The router must seed each child with the ClusterClient's MaxRedirects
// instead. Pure construction check: the router is built at construction, so no
// live cluster is needed to verify the plumbed budget.
func TestClusterFullDuplexRecoveryBudgetFromMaxRedirects(t *testing.T) {
	cc := NewClusterClient(&ClusterOptions{Addrs: []string{"127.0.0.1:16600"}, MaxRedirects: 5})
	defer cc.Close()

	ap, err := cc.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipelineWithOptions: %v", err)
	}
	defer ap.Close()
	if ap.clusterFD == nil {
		t.Fatalf("cluster FD did not engage; cannot check the recovery budget")
	}
	if got := ap.clusterFD.childCfg.clusterRetryBudget; got != 5 {
		t.Errorf("child clusterRetryBudget = %d, want 5 (cc.opt.MaxRedirects); the node client's MaxRetries=-1 would disable carry-replay", got)
	}
}

// ===== from autopipeline_fd_accum_reader_exit_test.go =====

// A batch taken off the queue while the writer waits in the MaxFlushDelay
// accumulation must not be written once the reader has exited. Writing it put
// a command the reader could never acknowledge on a connection the server
// still reads (a read timeout leaves the socket open), so the server ran it,
// and recovery then replayed it as unacknowledged: one SET, two executions.
// The batch is now kept unsent and recovered as never sent, so it runs once.
func TestFullDuplexAccumulationDoesNotWriteAfterReaderExit(t *testing.T) {
	const held = 70 // above the accumulation gate (64 at this window)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	var conns, sets atomic.Int64
	// heldAt / setAt: when the first connection received the last held GET
	// and the SET (unix nanos, 0 = never).
	var heldAt, setAt atomic.Int64
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			first := conns.Add(1) == 1
			go func(c net.Conn) {
				defer c.Close()
				rd := bufio.NewReader(c)
				gets := 0
				for {
					args, err := readRESPCommand(rd)
					if err != nil {
						return
					}
					name := ""
					if len(args) > 0 {
						name = strings.ToLower(args[0])
					}
					switch {
					case name == "hello":
						_, _ = io.WriteString(c, "*4\r\n$6\r\nserver\r\n$5\r\nredis\r\n$5\r\nproto\r\n:2\r\n")
					case name == "set":
						sets.Add(1)
						if first {
							setAt.Store(time.Now().UnixNano())
						}
						if !first {
							_, _ = io.WriteString(c, "+OK\r\n")
						}
					case name == "get":
						if !first {
							_, _ = io.WriteString(c, "$-1\r\n")
							continue
						}
						// Hold every GET on the first connection: the reader times
						// out while the writer is parked in its accumulation wait.
						// The connection stays open and read, so a late write
						// would still run.
						if gets++; gets == held {
							heldAt.Store(time.Now().UnixNano())
						}
					default:
						_, _ = io.WriteString(c, "+OK\r\n")
					}
				}
			}(c)
		}
	}()

	c := NewClient(&Options{
		Addr:             ln.Addr().String(),
		Protocol:         2,
		DisableIdentity:  true,
		PipelinePoolSize: 2,
		PoolSize:         2,
		ReadTimeout:      300 * time.Millisecond,
		MaxRetries:       3,
	})
	defer c.Close()
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex:       true,
		FullDuplexWindow: 4096,            // accumulation gate at its 64 floor
		MaxFlushDelay:    2 * time.Second, // far longer than the test: the reader dies mid-wait
	})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	ctx := context.Background()
	gets := make([]*StringCmd, held)
	for i := range gets {
		gets[i] = ap.Get(ctx, "g")
	}
	time.Sleep(50 * time.Millisecond) // the GETs are written and held
	set := ap.Set(ctx, "x", "v", 0)
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	// The case under test is a SET the writer was HOLDING in its wait when the
	// reader timed out. A writer whose wait ended earlier (a short grace with
	// nothing else arriving) sent the SET before the timeout, and replaying a
	// sent command after a timeout is ordinary at-least-once recovery.
	if at := setAt.Load(); at != 0 && time.Duration(at-heldAt.Load()) < 200*time.Millisecond {
		t.Skip("the writer sent the SET before the reader timed out; it was not held in a wait")
	}
	if n := sets.Load(); n != 1 {
		t.Fatalf("SET executed %d times, want 1 (written after the reader exited, then replayed?)", n)
	}
	if conns.Load() < 2 {
		t.Fatal("the session never recovered on a new connection; the test did not exercise the reader exit")
	}
}

// ===== from autopipeline_fd_batch_backpressure_test.go =====

// fdBackpressureEngine is an engine with no writer and a submit queue of
// capacity 10 holding 9 commands, so a 5-command batch cannot be admitted.
func fdBackpressureEngine(t *testing.T) *fdEngine {
	t.Helper()
	apCtx, apCancel := context.WithCancel(context.Background())
	t.Cleanup(apCancel)
	fd := &fdEngine{
		ap:     &AutoPipeliner{config: &AutoPipelineOptions{}, ctx: apCtx},
		client: &Client{baseClient: &baseClient{opt: &Options{}}},
		q:      newFDQueue(10),
	}
	for i := 0; i < 9; i++ {
		fd.q.push(fdReq{cmd: NewStatusCmd(context.Background(), "set", "k", "v"), batch: newAPBatch()})
	}
	return fd
}

// startBlockedBatch submits a 5-command batch that must wait for room and
// returns a func that cancels it and waits for it to return.
func startBlockedBatch(t *testing.T, fd *fdEngine) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cmds := make([]Cmder, 5)
	for i := range cmds {
		cmds[i] = NewStatusCmd(ctx, "set", "k", "v")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if b, err := fd.submitBatch(ctx, cmds); b != nil || err != nil {
			t.Errorf("submitBatch = (%v, %v); want a ctx rejection", b, err)
		}
	}()
	time.Sleep(50 * time.Millisecond) // let it park on the room signal
	return func() {
		cancel()
		<-done
	}
}

// submitBatchParked reports whether the goroutine running submitBatch is
// parked in its select. A spinning loop never parks: it finds the room signal
// it just put back and runs on.
func submitBatchParked(t *testing.T) bool {
	t.Helper()
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(g, "(*fdEngine).submitBatch(") {
			header, _, _ := strings.Cut(g, "\n")
			return strings.Contains(header, "[select")
		}
	}
	t.Fatal("no goroutine is running submitBatch")
	return false
}

// TestFDSubmitBatchDoesNotSpinWhileBlocked pins that a batch waiting for more
// room than one wake frees does not spin. It used to put the room signal back
// whenever any slot was free, take it straight back, fail to fit, and loop,
// burning a core for as long as the writer was blocked.
func TestFDSubmitBatchDoesNotSpinWhileBlocked(t *testing.T) {
	fd := fdBackpressureEngine(t)
	stop := startBlockedBatch(t, fd)
	defer stop()

	wakeHead(t, fd.q) // one slot free, the batch needs five
	time.Sleep(20 * time.Millisecond)
	running := 0
	const samples = 20
	for i := 0; i < samples; i++ {
		if !submitBatchParked(t) {
			running++
		}
		time.Sleep(5 * time.Millisecond)
	}
	if running > 1 {
		t.Fatalf("the waiting batch was running in %d of %d samples: it is spinning", running, samples)
	}
}

// wakeHead signals the head of the holder line the way a take would.
func wakeHead(t *testing.T, q *fdQueue) {
	t.Helper()
	q.mu.Lock()
	head := q.headWakeLocked()
	q.mu.Unlock()
	if head == nil {
		t.Fatal("no holder is waiting")
	}
	signalHolder(head)
}

// holderCount reports how many batches are waiting with a reservation.
func holderCount(q *fdQueue) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.holders)
}

// waitHolders blocks until n batches hold a reservation.
func waitHolders(t *testing.T, q *fdQueue, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for holderCount(q) != n {
		if time.Now().After(deadline) {
			t.Fatalf("holders = %d, want %d", holderCount(q), n)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestFDSubmitBatchWakesOnlyTheHead pins the direct wake: a take signals the
// head of the holder line on its own channel, and a holder behind it is not
// woken at all, so no wake is shared or passed between holders.
func TestFDSubmitBatchWakesOnlyTheHead(t *testing.T) {
	fd := fdBackpressureEngine(t)
	headID, headWake := fd.q.hold(1) // an earlier holder, standing in for a head
	stop := startBlockedBatch(t, fd) // the batch lines up behind it
	defer stop()
	waitHolders(t, fd.q, 2)

	fd.q.takeInto(nil, 1)
	select {
	case <-headWake:
	case <-time.After(time.Second):
		t.Fatal("a take did not wake the head of the line")
	}
	time.Sleep(20 * time.Millisecond)
	if !submitBatchParked(t) {
		t.Fatal("the batch behind the head was woken by a take meant for the head")
	}

	// The head leaves: the batch becomes the head and is woken to try.
	fd.q.unhold(headID)
	time.Sleep(20 * time.Millisecond)
	if n := holderCount(fd.q); n != 1 {
		t.Fatalf("holders after unhold = %d, want 1", n)
	}
}

// TestFDSubmitBatchHoldersAreFIFO pins that holders are admitted in arrival
// order: small batches that arrive behind a large one cannot take the room
// the writer frees for the large one, even though each alone would fit, and
// each is admitted in turn once it reaches the head of the line.
func TestFDSubmitBatchHoldersAreFIFO(t *testing.T) {
	apCtx, apCancel := context.WithCancel(context.Background())
	defer apCancel()
	fd := &fdEngine{
		ap:     &AutoPipeliner{config: &AutoPipelineOptions{}, ctx: apCtx},
		client: &Client{baseClient: &baseClient{opt: &Options{}}},
		q:      newFDQueue(10),
	}
	for i := 0; i < 10; i++ {
		fd.q.push(fdReq{cmd: NewStatusCmd(context.Background(), "set", "k", "v"), batch: newAPBatch()})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mk := func(n int) []Cmder {
		cmds := make([]Cmder, n)
		for i := range cmds {
			cmds[i] = NewStatusCmd(ctx, "set", "k", "v")
		}
		return cmds
	}
	order := make(chan string, 3)
	submit := func(name string, cmds []Cmder) {
		b, err := fd.submitBatch(ctx, cmds)
		if err != nil || len(b) != len(cmds) {
			t.Errorf("%s: submitBatch = (%d, %v)", name, len(b), err)
		}
		order <- name
	}
	go submit("big", mk(10))
	waitHolders(t, fd.q, 1)
	go submit("small1", mk(1))
	waitHolders(t, fd.q, 2)
	go submit("small2", mk(1))
	waitHolders(t, fd.q, 3)

	// Drain one at a time, as a writer with a one-command wave would. After
	// each take a small batch alone would fit; neither may be admitted.
	for i := 0; i < 10; i++ {
		fd.q.takeInto(nil, 1)
		time.Sleep(5 * time.Millisecond)
		if i < 9 {
			select {
			case name := <-order:
				t.Fatalf("%q admitted after %d takes, ahead of the head of the line", name, i+1)
			default:
			}
		}
	}
	next := func(want string) {
		t.Helper()
		select {
		case got := <-order:
			if got != want {
				t.Fatalf("admitted %q, want %q", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("%q was not admitted", want)
		}
	}
	next("big") // 10 live again
	fd.q.takeInto(nil, 1)
	next("small1")
	fd.q.takeInto(nil, 1)
	next("small2")
}

// TestFDSubmitBatchCloseReleasesAllHolders pins that closing the queue wakes
// every holder, not just the head: a holder behind the head has no other
// room signal, and Close must not leave it waiting for its ctx.
func TestFDSubmitBatchCloseReleasesAllHolders(t *testing.T) {
	fd := fdBackpressureEngine(t)
	ctx := context.Background()
	done := make(chan error, 2)
	for _, n := range []int{5, 5} {
		cmds := make([]Cmder, n)
		for i := range cmds {
			cmds[i] = NewStatusCmd(ctx, "set", "k", "v")
		}
		go func() {
			b, err := fd.submitBatch(ctx, cmds)
			if b != nil || err != nil {
				t.Errorf("submitBatch = (%v, %v); want a submit-time rejection", b, err)
			}
			done <- cmds[0].Err()
		}()
	}
	waitHolders(t, fd.q, 2)

	fd.q.closeQueue()
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err != ErrClosed {
				t.Fatalf("holder error = %v, want ErrClosed", err)
			}
		case <-time.After(time.Second):
			t.Fatal("a holder was not released by closeQueue")
		}
	}
}

// TestFDSubmitBatchReservesRoom pins that a refused batch holds its slots: the
// room the writer frees accumulates for it instead of going to single
// submitters, and the singles resume once the batch is in.
func TestFDSubmitBatchReservesRoom(t *testing.T) {
	fd := fdBackpressureEngine(t) // capacity 10, 9 queued
	ctx := context.Background()
	cmds := make([]Cmder, 5)
	for i := range cmds {
		cmds[i] = NewStatusCmd(ctx, "set", "k", "v")
	}
	type result struct {
		batches []*apBatch
		err     error
	}
	done := make(chan result, 1)
	go func() {
		b, err := fd.submitBatch(ctx, cmds)
		done <- result{b, err}
	}()
	time.Sleep(50 * time.Millisecond)

	// One slot is free, but the batch has reserved five: a single is refused.
	single := fdReq{cmd: NewStatusCmd(ctx, "set", "k", "v"), batch: newAPBatch()}
	if res := fd.q.push(single); res != fdPushFull {
		t.Fatalf("push with a batch waiting = %v; want fdPushFull (slots reserved)", res)
	}

	// The writer takes four. The batch now fits (5 live + 5), and the take
	// wakes it through batchRoom.
	if got := len(fd.q.takeInto(nil, 4)); got != 4 {
		t.Fatalf("takeInto = %d, want 4", got)
	}
	select {
	case r := <-done:
		if r.err != nil || len(r.batches) != 5 {
			t.Fatalf("submitBatch = (%d batches, %v); want 5 batches, nil", len(r.batches), r.err)
		}
	case <-time.After(time.Second):
		t.Fatal("the batch was not admitted after the writer freed its room")
	}
	if d := fd.q.depth(); d != 10 {
		t.Fatalf("depth after admission = %d, want 10", d)
	}
	if fd.q.roomFor(1) {
		t.Fatal("roomFor(1) with a full queue and no reservation = true")
	}

	// The reservation is consumed: once the writer frees a slot, a single fits.
	fd.q.takeInto(nil, 1)
	if res := fd.q.push(single); res != fdPushOK {
		t.Fatalf("push after admission = %v; want fdPushOK (reservation released)", res)
	}
}

// TestFDSubmitBatchReleasesReservationOnCancel pins that a batch that gives up
// gives its slots back and wakes a single its reservation was holding back.
func TestFDSubmitBatchReleasesReservationOnCancel(t *testing.T) {
	fd := fdBackpressureEngine(t)
	stop := startBlockedBatch(t, fd)

	single := fdReq{cmd: NewStatusCmd(context.Background(), "set", "k", "v"), batch: newAPBatch()}
	if res := fd.q.push(single); res != fdPushFull {
		t.Fatalf("push with a batch waiting = %v; want fdPushFull", res)
	}
	stop() // ctx cancelled: the batch is rejected and must unhold

	select {
	case <-fd.q.roomCh():
	case <-time.After(time.Second):
		t.Fatal("unhold did not wake the single the reservation held back")
	}
	if res := fd.q.push(single); res != fdPushOK {
		t.Fatalf("push after the batch gave up = %v; want fdPushOK", res)
	}
}

// TestFDSubmitBatchNotStarvedBySingles is the end-to-end shape of the fix: a
// writer that frees two slots per take, singles that refill every free slot
// as fast as they can, and a batch longer than a take. Without a reservation
// the batch never sees its whole length free at once and waits until its ctx
// expires.
func TestFDSubmitBatchNotStarvedBySingles(t *testing.T) {
	apCtx, apCancel := context.WithCancel(context.Background())
	defer apCancel()
	fd := &fdEngine{
		ap:     &AutoPipeliner{config: &AutoPipelineOptions{}, ctx: apCtx},
		client: &Client{baseClient: &baseClient{opt: &Options{}}},
		q:      newFDQueue(10),
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// The writer: two slots per take, continuously.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			fd.q.takeInto(nil, 2)
			time.Sleep(200 * time.Microsecond)
		}
	}()
	// The singles: refill as submit() does, waiting on room when refused.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := fdReq{cmd: NewStatusCmd(context.Background(), "set", "k", "v"), batch: newAPBatch()}
			for {
				select {
				case <-stop:
					return
				default:
				}
				if fd.q.push(req) == fdPushOK {
					continue
				}
				select {
				case <-fd.q.roomCh():
					if fd.q.roomFor(1) {
						fd.q.signalRoom()
					}
				case <-stop:
					return
				}
			}
		}()
	}
	time.Sleep(20 * time.Millisecond) // let the singles saturate the queue

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cmds := make([]Cmder, 8) // four times a take
	for i := range cmds {
		cmds[i] = NewStatusCmd(ctx, "set", "k", "v")
	}
	batches, err := fd.submitBatch(ctx, cmds)
	close(stop)
	wg.Wait()
	if err != nil || len(batches) != 8 {
		t.Fatalf("submitBatch under single-command load = (%d batches, %v, cmd err %v); want admission",
			len(batches), err, cmds[0].Err())
	}
}

// ===== from autopipeline_fd_failqueue_metrics_test.go =====

// TestFDFailQueueSkipsPipelinedErrorMetric pins that failQueue, like
// failReqs, reports no per-command error for a pipelined command.
// fdPipelineExec reports the batch once, as an ordinary pipeline does, so a
// per-command report here would count the same failure twice. A plain
// command in the same backlog is still reported.
func TestFDFailQueueSkipsPipelinedErrorMetric(t *testing.T) {
	var calls atomic.Int64
	pool.SetAllMetricCallbacks(&pool.MetricCallbacks{
		Error: func(context.Context, string, *pool.Conn, string, bool, int) {
			calls.Add(1)
		},
	})
	defer pool.SetAllMetricCallbacks(nil)

	ctx := context.Background()
	fd := &fdEngine{client: &Client{baseClient: &baseClient{opt: &Options{}}}, q: newFDQueue(4)}
	group := newAPBatch()
	group.fdGroup = group
	var batches []*apBatch
	for i := 0; i < 3; i++ {
		b := group
		if i > 0 {
			b = newAPBatch()
			b.fdGroup = group
		}
		batches = append(batches, b)
		fd.q.push(fdReq{cmd: NewStatusCmd(ctx, "set", "k", "v"), batch: b, ctx: ctx, attempts: 1, pipelined: true})
	}
	plain := newAPBatch()
	batches = append(batches, plain)
	fd.q.push(fdReq{cmd: NewStatusCmd(ctx, "set", "k", "v"), batch: plain, ctx: ctx, attempts: 1})

	fd.failQueue(ErrClosed)

	if got := calls.Load(); got != 1 {
		t.Fatalf("error callback invoked %d times, want 1 (the plain command only)", got)
	}
	for i, b := range batches {
		select {
		case <-b.done:
		default:
			t.Fatalf("req %d not completed", i)
		}
	}
}

// ===== from autopipeline_fd_fatal_read_test.go =====

// fdPoisonServer is a minimal RESP2 server for the batched-reader tests. GET
// replies with its own key, HELLO with a minimal map, anything else with +OK. The FIRST connection that
// sends a GET is poisoned: its replies are held until `hold` GETs arrived, then
// written in ONE write with reply number `bad` replaced by an unparseable
// line. Holding the replies puts the fault in the middle of a reader snapshot
// with more replies already buffered behind it. Later connections (the replay
// after the session fails) answer each command at once.
type fdPoisonServer struct {
	ln       net.Listener
	hold     int
	bad      int
	poisoned atomic.Bool
	wg       sync.WaitGroup
	mu       sync.Mutex
	conns    []net.Conn
}

func newFDPoisonServer(t *testing.T, hold, bad int) *fdPoisonServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &fdPoisonServer{ln: ln, hold: hold, bad: bad}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.conns = append(s.conns, c)
			s.mu.Unlock()
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.serve(c)
			}()
		}
	}()
	return s
}

func (s *fdPoisonServer) addr() string { return s.ln.Addr().String() }

func (s *fdPoisonServer) close() {
	_ = s.ln.Close()
	s.mu.Lock()
	for _, c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *fdPoisonServer) serve(c net.Conn) {
	defer c.Close()
	rd := bufio.NewReader(c)
	poison := false
	var held []byte
	gets := 0
	for {
		args, err := readRESPCommand(rd)
		if err != nil {
			return
		}
		if len(args) == 2 && (args[0] == "get" || args[0] == "GET") {
			if gets == 0 && !s.poisoned.Load() {
				poison = s.poisoned.CompareAndSwap(false, true)
			}
			gets++
			reply := fmt.Sprintf("$%d\r\n%s\r\n", len(args[1]), args[1])
			if !poison {
				if _, err := io.WriteString(c, reply); err != nil {
					return
				}
				continue
			}
			if gets == s.bad {
				reply = "?not-resp\r\n" // unknown type byte: a protocol fault
			}
			held = append(held, reply...)
			if gets == s.hold {
				if _, err := c.Write(held); err != nil {
					return
				}
				held = nil
			}
			continue
		}
		if len(args) > 0 && (args[0] == "hello" || args[0] == "HELLO") {
			// RESP2 HELLO reply: a flat key/value array.
			if _, err := io.WriteString(c, "*4\r\n$6\r\nserver\r\n$5\r\nredis\r\n$5\r\nproto\r\n:2\r\n"); err != nil {
				return
			}
			continue
		}
		if _, err := io.WriteString(c, "+OK\r\n"); err != nil {
			return
		}
	}
}

// TestFullDuplexFatalReplyStopsTheSnapshot covers a protocol fault in the
// middle of a reader snapshot. The reader must stop at the fault: the failed
// command and everything after it stay in the deque and are replayed on a new
// connection. Before the fix the reader kept reading the rest of the snapshot
// from the desynced stream, then advanced past the failed command without
// completing it, so that caller never returned.
func TestFullDuplexFatalReplyStopsTheSnapshot(t *testing.T) {
	const n, bad = 20, 10
	srv := newFDPoisonServer(t, n, bad)
	defer srv.close()

	c := NewClient(&Options{
		Addr:             srv.addr(),
		Protocol:         2,
		DisableIdentity:  true,
		PipelinePoolSize: 2,
		PoolSize:         2,
		MaxRetries:       3,
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

	ctx := context.Background()
	cmds := make([]*StringCmd, n)
	for i := range cmds {
		cmds[i] = ap.Get(ctx, "k"+strconv.Itoa(i))
	}

	for i, cmd := range cmds {
		done := make(chan struct{})
		var v string
		var gerr error
		go func() {
			v, gerr = cmd.Result()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("command %d never completed: the reader advanced past it without completing it", i)
		}
		// Replies before the fault are correct. The faulted command and the
		// unread tail settle with the protocol error: a protocol fault is not
		// retried, since those commands may already have run. None of them may
		// receive a value read from the desynced stream.
		if i < bad-1 {
			if want := "k" + strconv.Itoa(i); gerr != nil || v != want {
				t.Fatalf("command %d: v=%q err=%v, want %q", i, v, gerr, want)
			}
		} else if gerr == nil || v != "" {
			t.Fatalf("command %d at/after the fault: v=%q err=%v, want the protocol error and no value", i, v, gerr)
		}
	}
	if !srv.poisoned.Load() {
		t.Fatal("the poisoned connection was never used; the test did not exercise the fault")
	}
}

// TestFullDuplexGroupedReadRearmsDeadline covers a reply that is only partly
// buffered when the group reaches it. k1 and k2 share one read group. k1
// arrives slowly, using most of the read timeout, and its last write also
// carries the first bytes of k2, so k2 needs another socket read. Before the
// fix that read ran under the deadline armed for k1 and timed out, although k2
// arrived well within its own ReadTimeout.
func TestFullDuplexGroupedReadRearmsDeadline(t *testing.T) {
	const timeout = time.Second
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	var once atomic.Bool
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				rd := bufio.NewReader(c)
				slow := false
				gets := 0
				for {
					args, err := readRESPCommand(rd)
					if err != nil {
						return
					}
					switch {
					case len(args) > 0 && (args[0] == "hello" || args[0] == "HELLO"):
						_, _ = io.WriteString(c, "*4\r\n$6\r\nserver\r\n$5\r\nredis\r\n$5\r\nproto\r\n:2\r\n")
					case len(args) == 2 && (args[0] == "get" || args[0] == "GET"):
						if gets == 0 {
							slow = once.CompareAndSwap(false, true)
						}
						gets++
						if !slow {
							_, _ = io.WriteString(c, "$2\r\n"+args[1]+"\r\n")
							continue
						}
						switch gets {
						case 1:
							// Delay k0 so k1 and k2 are both in flight when the
							// reader takes its next snapshot: they must share a group.
							time.Sleep(100 * time.Millisecond)
							_, _ = io.WriteString(c, "$2\r\nk0\r\n")
						case 3:
							// k1 finishes at 0.6*timeout together with the first
							// bytes of k2, which finishes at 1.2*timeout.
							_, _ = io.WriteString(c, "$2\r\nk")
							time.Sleep(timeout * 6 / 10)
							_, _ = io.WriteString(c, "1\r\n$2\r\nk")
							time.Sleep(timeout * 6 / 10)
							_, _ = io.WriteString(c, "2\r\n")
						}
					default:
						_, _ = io.WriteString(c, "+OK\r\n")
					}
				}
			}(c)
		}
	}()

	c := NewClient(&Options{
		Addr:             ln.Addr().String(),
		Protocol:         2,
		DisableIdentity:  true,
		PipelinePoolSize: 2,
		PoolSize:         2,
		ReadTimeout:      timeout,
		MaxRetries:       -1, // a replay would hide the spurious timeout
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

	ctx := context.Background()
	cmds := []*StringCmd{ap.Get(ctx, "k0"), ap.Get(ctx, "k1"), ap.Get(ctx, "k2")}
	for i, cmd := range cmds {
		if v, err := cmd.Result(); err != nil || v != "k"+strconv.Itoa(i) {
			t.Fatalf("reply %d: v=%q err=%v (k2 read under k1's deadline?)", i, v, err)
		}
	}
	if !once.Load() {
		t.Fatal("the slow connection was never used; the test did not exercise the partial frame")
	}
}

// ===== from autopipeline_fd_flush_budget_test.go =====

// fdBudgetProbe is a stub engine whose Close flush records the retry bound of
// each pipeline it runs.
func fdBudgetProbe() (*fdEngine, *[]int) {
	var bounds []int
	fd := &fdEngine{
		ap:       &AutoPipeliner{config: &AutoPipelineOptions{}},
		client:   &Client{baseClient: &baseClient{opt: &Options{MaxRetries: 3}}},
		maxBatch: 8,
		runPipeline: func(_ context.Context, _ []Cmder, maxRetries int) error {
			bounds = append(bounds, maxRetries)
			return nil
		},
	}
	return fd, &bounds
}

// One FD pipeline flushed on Close can carry commands with different attempt
// counts. It runs as one pipeline, so it gets the smallest remaining budget
// of its commands: the budget of its first command let a later command with
// more attempts run past its MaxRetries+1 executions.
func TestFDCloseFlushUsesSmallestBudgetOfAPipeline(t *testing.T) {
	ctx := context.Background()
	fd, bounds := fdBudgetProbe()
	carry := fdPipeGroupReqs(ctx, 1, 3) // budgets 3 and 1 with MaxRetries 3
	if err := fd.flushCarryBudgeted(ctx, carry); err != nil {
		t.Fatalf("flushCarryBudgeted: %v", err)
	}
	if got := *bounds; len(got) != 1 || got[0] != 1 {
		t.Fatalf("pipeline retry bounds %v, want [1] (the smallest budget)", got)
	}
}

// When any command of the pipeline has spent its budget, the pipeline does not
// run: running it would execute that command again.
func TestFDCloseFlushFailsPipelineWithSpentBudget(t *testing.T) {
	ctx := context.Background()
	fd, bounds := fdBudgetProbe()
	carry := fdPipeGroupReqs(ctx, 1, 5) // the second has run past MaxRetries+1
	if err := fd.flushCarryBudgeted(ctx, carry); err != nil {
		t.Fatalf("flushCarryBudgeted: %v", err)
	}
	if got := *bounds; len(got) != 0 {
		t.Fatalf("the pipeline ran with bounds %v; a command in it had spent its budget", got)
	}
	for i, r := range carry {
		if !errors.Is(r.cmd.Err(), errFDRetryBudgetExhausted) {
			t.Fatalf("command %d err=%v, want errFDRetryBudgetExhausted", i, r.cmd.Err())
		}
	}
}

// A command in front of an FD pipeline is not part of that pipeline's budget
// group. With the same attempt count as the pipeline's first command it used
// to join the group, so a later pipeline command that had spent its budget
// failed the unrelated command too.
func TestFDCloseFlushKeepsOtherCommandsOutOfAPipelineBudget(t *testing.T) {
	ctx := context.Background()
	fd, bounds := fdBudgetProbe()
	other := fdReq{cmd: NewStatusCmd(ctx, "set", "o", "v"), batch: newAPBatch(), attempts: 2}
	carry := append([]fdReq{other}, fdPipeGroupReqs(ctx, 2, 5)...) // the pipeline's 2nd command has no budget left
	if err := fd.flushCarryBudgeted(ctx, carry); err != nil {
		t.Fatalf("flushCarryBudgeted: %v", err)
	}
	if got := *bounds; len(got) != 1 || got[0] != 2 {
		t.Fatalf("retry bounds %v, want [2]: the other command runs alone with its own budget", got)
	}
	if err := carry[0].cmd.Err(); err != nil {
		t.Fatalf("other command err=%v, want nil", err)
	}
	for i, r := range carry[1:] {
		if !errors.Is(r.cmd.Err(), errFDRetryBudgetExhausted) {
			t.Fatalf("pipeline command %d err=%v, want errFDRetryBudgetExhausted", i, r.cmd.Err())
		}
	}
}

// ===== from autopipeline_fd_group_deadline_test.go =====

// TestFullDuplexGroupedReadFullDeadlinePerSocketRead covers a partly buffered
// reply reached EARLY in a read group: k1 completes less than half the
// timeout after the group armed its deadline, together with the first bytes
// of k2, and k2 needs another 0.9*timeout. Stopping the group only after half
// the timeout still read k2 under that deadline, which expired before k2
// arrived, although k2 arrived within its own ReadTimeout. A reply that needs
// socket data must start a new group with a full deadline.
func TestFullDuplexGroupedReadFullDeadlinePerSocketRead(t *testing.T) {
	const timeout = time.Second
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	var once atomic.Bool
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				rd := bufio.NewReader(c)
				slow := false
				gets := 0
				for {
					args, err := readRESPCommand(rd)
					if err != nil {
						return
					}
					switch {
					case len(args) > 0 && (args[0] == "hello" || args[0] == "HELLO"):
						_, _ = io.WriteString(c, "*4\r\n$6\r\nserver\r\n$5\r\nredis\r\n$5\r\nproto\r\n:2\r\n")
					case len(args) == 2 && (args[0] == "get" || args[0] == "GET"):
						if gets == 0 {
							slow = once.CompareAndSwap(false, true)
						}
						gets++
						if !slow {
							_, _ = io.WriteString(c, "$2\r\n"+args[1]+"\r\n")
							continue
						}
						switch gets {
						case 1:
							// Delay k0 so k1 and k2 are in flight and read in the
							// same group as the reply before them.
							time.Sleep(100 * time.Millisecond)
							_, _ = io.WriteString(c, "$2\r\nk0\r\n")
						case 3:
							_, _ = io.WriteString(c, "$2\r\nk")
							time.Sleep(timeout * 3 / 10)
							_, _ = io.WriteString(c, "1\r\n$2\r\nk")
							time.Sleep(timeout * 9 / 10)
							_, _ = io.WriteString(c, "2\r\n")
						}
					default:
						_, _ = io.WriteString(c, "+OK\r\n")
					}
				}
			}(c)
		}
	}()

	c := NewClient(&Options{
		Addr:             ln.Addr().String(),
		Protocol:         2,
		DisableIdentity:  true,
		PipelinePoolSize: 2,
		PoolSize:         2,
		ReadTimeout:      timeout,
		MaxRetries:       -1, // a replay would hide the spurious timeout
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

	ctx := context.Background()
	cmds := []*StringCmd{ap.Get(ctx, "k0"), ap.Get(ctx, "k1"), ap.Get(ctx, "k2")}
	for i, cmd := range cmds {
		if v, err := cmd.Result(); err != nil || v != "k"+strconv.Itoa(i) {
			t.Fatalf("reply %d: v=%q err=%v (k2 read under k1's deadline?)", i, v, err)
		}
	}
	if !once.Load() {
		t.Fatal("the slow connection was never used; the test did not exercise the partial frame")
	}
}

// ===== from autopipeline_fd_group_panic_test.go =====

// fdPanicReplyCmd is a GET whose reply decoder panics, like a RawWriteToCmd
// whose user io.Writer panics while the reply streams into it.
type fdPanicReplyCmd struct{ *StringCmd }

func (fdPanicReplyCmd) readReply(*proto.Reader) error { panic("boom: reply decoder") }

// A decoder panic in the middle of a read group must not replay the replies
// already read in that group. The first reply is delayed so SET a, GET b and
// the panicking command share one group. Before the fix the panic escaped to
// the reader's recover, which never counted the group's completed replies, so
// recovery replayed SET a and GET b on a new connection.
func TestFullDuplexGroupPanicDoesNotReplayReadReplies(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	var sets atomic.Int64
	var first atomic.Bool
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				rd := bufio.NewReader(c)
				scripted := false
				var held []string
				n := 0
				for {
					args, err := readRESPCommand(rd)
					if err != nil {
						return
					}
					name := strings.ToLower(args[0])
					var reply string
					switch {
					case name == "hello":
						reply = "*4\r\n$6\r\nserver\r\n$5\r\nredis\r\n$5\r\nproto\r\n:2\r\n"
					case name == "set":
						sets.Add(1)
						reply = "+OK\r\n"
					case name == "get":
						reply = "$" + itoa(len(args[1])) + "\r\n" + args[1] + "\r\n"
					default:
						reply = "+OK\r\n"
					}
					if name != "get" && name != "set" {
						_, _ = io.WriteString(c, reply)
						continue
					}
					if n == 0 {
						scripted = first.CompareAndSwap(false, true)
					}
					n++
					if !scripted {
						_, _ = io.WriteString(c, reply)
						continue
					}
					if n == 1 {
						time.Sleep(100 * time.Millisecond) // let the next three queue up
						_, _ = io.WriteString(c, reply)
						continue
					}
					held = append(held, reply)
					if len(held) == 3 {
						_, _ = io.WriteString(c, strings.Join(held, "")) // one group
						held = nil
					}
				}
			}(c)
		}
	}()

	cl := NewClient(&Options{Addr: ln.Addr().String(), Protocol: 2, DisableIdentity: true,
		PipelinePoolSize: 2, MaxRetries: 1,
		MinRetryBackoff: time.Millisecond, MaxRetryBackoff: time.Millisecond})
	defer cl.Close()
	ap, err := cl.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	ctx := context.Background()
	g0 := ap.Get(ctx, "k0")
	set := ap.Set(ctx, "a", "v", 0)
	getB := ap.Get(ctx, "b")
	boom := ap.Submit(ctx, fdPanicReplyCmd{NewStringCmd(ctx, "get", "c")})

	if v, err := g0.Result(); err != nil || v != "k0" {
		t.Fatalf("g0: v=%q err=%v", v, err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET a: %v", err)
	}
	if v, err := getB.Result(); err != nil || v != "b" {
		t.Fatalf("GET b: v=%q err=%v", v, err)
	}
	if err := boom.Wait(); err == nil {
		t.Fatal("the panicking command succeeded")
	}
	if n := sets.Load(); n != 1 {
		t.Fatalf("SET a executed %d times: replies already read in the group were replayed", n)
	}
}

// Once a burst has drained, the queue must not keep the burst's backing array;
// a queue that stays small keeps its buffer, so ordinary traffic does not pay a
// reallocation per wave.
func TestFDQueueReleasesBurstBuffer(t *testing.T) {
	q := newFDQueue(1 << 16)
	for i := 0; i < 10000; i++ {
		q.push(fdQueueTag(i))
	}
	for q.depth() > 0 {
		q.takeInto(nil, 200)
	}
	if c := cap(q.buf); c > fdQueueRetainCap {
		t.Fatalf("drained queue kept a %d-slot buffer, want <= %d", c, fdQueueRetainCap)
	}

	small := newFDQueue(1 << 16)
	for i := 0; i < 1000; i++ {
		small.push(fdQueueTag(i))
	}
	before := cap(small.buf)
	small.takeInto(nil, 1000)
	if cap(small.buf) != before {
		t.Fatalf("a small queue dropped its buffer (%d -> %d); it should reuse it", before, cap(small.buf))
	}
}

// ===== from autopipeline_fd_push_panic_test.go =====

type fdPanickingPushHandler struct{}

func (fdPanickingPushHandler) HandlePushNotification(context.Context, push.NotificationHandlerContext, []interface{}) error {
	panic("boom from a push handler")
}

// A push handler that panics while the grouped reader drains a push between
// two replies must not make the session replay the replies the group already
// read. The panic escaped the group's WithReader (only the reply decoder was
// guarded), so the read SET reply was never completed and recovery ran the
// SET again: one write, two executions. The drain now turns the panic into a
// fatal drain error at that position, like a decoder panic.
func TestFullDuplexPushHandlerPanicDoesNotReplayReadReplies(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	var sets atomic.Int64
	var first atomic.Bool
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			poisoned := first.CompareAndSwap(false, true)
			go func(c net.Conn) {
				defer c.Close()
				rd := bufio.NewReader(c)
				for {
					args, err := readRESPCommand(rd)
					if err != nil {
						return
					}
					name := ""
					if len(args) > 0 {
						name = strings.ToLower(args[0])
					}
					switch {
					case name == "hello":
						_, _ = io.WriteString(c, "%1\r\n$5\r\nproto\r\n:3\r\n")
					case name == "set":
						sets.Add(1)
						if !poisoned {
							_, _ = io.WriteString(c, "+OK\r\n")
							continue
						}
						// Answer SET and the GET behind it in ONE write, with a
						// push between the replies, so both are in one read group.
						if _, err := readRESPCommand(rd); err != nil {
							return
						}
						_, _ = io.WriteString(c, "+OK\r\n>2\r\n$4\r\nboom\r\n$1\r\nx\r\n$1\r\nv\r\n")
					case name == "get" && len(args) == 2 && args[1] == "slow":
						// Keep the reader busy so SET and the GET behind it are
						// both in flight when it takes its next snapshot.
						time.Sleep(100 * time.Millisecond)
						_, _ = io.WriteString(c, "$-1\r\n")
					case name == "get":
						_, _ = io.WriteString(c, "$1\r\nv\r\n")
					default:
						_, _ = io.WriteString(c, "+OK\r\n")
					}
				}
			}(c)
		}
	}()

	c := NewClient(&Options{
		Addr:             ln.Addr().String(),
		Protocol:         3,
		DisableIdentity:  true,
		PipelinePoolSize: 2,
		PoolSize:         2,
		ReadTimeout:      2 * time.Second,
		MaxRetries:       3,
	})
	defer c.Close()
	if err := c.RegisterPushNotificationHandler("boom", fdPanickingPushHandler{}, false); err != nil {
		t.Fatalf("RegisterPushNotificationHandler: %v", err)
	}
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	ctx := context.Background()
	slow := ap.Get(ctx, "slow")
	time.Sleep(20 * time.Millisecond) // slow is on the wire, the reader waits on it
	set := ap.Set(ctx, "k", "v", 0)
	get := ap.Get(ctx, "x")
	_ = get.Err()
	_ = slow.Err()
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if !first.Load() {
		t.Fatal("the poisoned connection was never used")
	}
	if n := sets.Load(); n != 1 {
		t.Fatalf("SET executed %d times, want 1 (its read reply was replayed after the push panic)", n)
	}
}

// ===== from autopipeline_fd_shards_internal_test.go =====

// The same key must pick the same engine whatever Go type carries it. A key
// given as []byte or *string used to look keyless and round-robin, so a
// same-key write and read could land on two engines with no order between
// them. An empty-string key used to collide with the keyless sentinel.
func TestFDForRoutesKeyFormsTogether(t *testing.T) {
	ctx := context.Background()
	cl := NewClient(&Options{Addr: internalTestRedisAddr()})
	defer cl.Close()
	if err := cl.Ping(ctx).Err(); err != nil {
		t.Skipf("no local redis: %v", err)
	}
	ap, err := cl.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex:           true,
		NumShards:            8,
		MaxConcurrentBatches: 1,
	})
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	defer ap.Close()
	if len(ap.fds) != 8 {
		t.Fatalf("want 8 engines, got %d", len(ap.fds))
	}

	for i := 0; i < 200; i++ {
		k := "fdkey:" + itoa(i)
		s := k
		want := ap.fdFor(NewCmd(ctx, "get", k))
		for _, arg := range []interface{}{[]byte(k), &s} {
			if got := ap.fdFor(NewCmd(ctx, "get", arg)); got != want {
				t.Fatalf("key %q as %T picked a different engine than as string", k, arg)
			}
		}
	}

	// A raw FCALL routes by its declared key, not by the function name, so it
	// shares an engine with a plain read of that key.
	for i := 0; i < 200; i++ {
		k := "fdfcall:" + itoa(i)
		if ap.fdFor(NewCmd(ctx, "fcall", "myfn", "1", k)) != ap.fdFor(NewCmd(ctx, "get", k)) {
			t.Fatalf("raw FCALL on %q picked a different engine than GET %q", k, k)
		}
	}

	first := ap.fdFor(NewCmd(ctx, "get", ""))
	for i := 0; i < 16; i++ {
		if ap.fdFor(NewCmd(ctx, "set", "", "v")) != first {
			t.Fatal("empty-string key round-robined instead of routing by hash")
		}
	}
}

// ===== from autopipeline_fdqueue_test.go =====

// fdQueueTag builds a request whose attempts field carries a sequence number,
// so FIFO order can be checked without a real command.
func fdQueueTag(n int) fdReq { return fdReq{attempts: n} }

// TestFDQueueFIFOModel drives the queue with random pushes, batch pushes,
// partial takes and give-backs (pushFront), and checks every take against a
// plain slice model. It pins the queue's order contract across the head
// offset, the reset-on-empty and the compaction.
func TestFDQueueFIFOModel(t *testing.T) {
	for seed := int64(1); seed <= 20; seed++ {
		rng := rand.New(rand.NewSource(seed))
		q := newFDQueue(512)
		var model []int
		next := 0
		for op := 0; op < 5000; op++ {
			switch r := rng.Intn(10); {
			case r < 4: // push
				if q.push(fdQueueTag(next)) == fdPushOK {
					model = append(model, next)
					next++
				} else if len(model) < 512 {
					t.Fatalf("seed %d: push refused at depth %d", seed, len(model))
				}
			case r < 6: // pushBatch
				k := 1 + rng.Intn(40)
				batch := make([]fdReq, k)
				for i := range batch {
					batch[i] = fdQueueTag(next + i)
				}
				if q.pushBatch(batch) == fdPushOK {
					for i := 0; i < k; i++ {
						model = append(model, next+i)
					}
					next += k
				} else if len(model)+k <= 512 {
					t.Fatalf("seed %d: pushBatch(%d) refused at depth %d", seed, k, len(model))
				}
			default: // take, maybe give some back
				got := q.takeInto(nil, 1+rng.Intn(64))
				for i, r := range got {
					if r.attempts != model[i] {
						t.Fatalf("seed %d op %d: took %d at %d, want %d", seed, op, r.attempts, i, model[i])
					}
				}
				model = model[len(got):]
				if g := rng.Intn(len(got) + 1); g > 0 && rng.Intn(3) == 0 {
					back := got[len(got)-g:]
					q.pushFront(back)
					head := make([]int, 0, g+len(model))
					for _, r := range back {
						head = append(head, r.attempts)
					}
					model = append(head, model...)
				}
			}
			if d := q.depth(); d != len(model) {
				t.Fatalf("seed %d op %d: depth %d, model %d", seed, op, d, len(model))
			}
		}
		got := q.drainAll(nil)
		if len(got) != len(model) {
			t.Fatalf("seed %d: drained %d, model %d", seed, len(got), len(model))
		}
		for i, r := range got {
			if r.attempts != model[i] {
				t.Fatalf("seed %d: drained %d at %d, want %d", seed, r.attempts, i, model[i])
			}
		}
	}
}

// BenchmarkFDQueueDeepDrain is the writer draining a full default-size queue
// in 200-command waves while submitters refill it. Before the head offset,
// every take shifted the whole remaining queue under the lock.
func BenchmarkFDQueueDeepDrain(b *testing.B) {
	const depth, wave = 65536, 200
	q := newFDQueue(depth)
	for i := 0; i < depth; i++ {
		q.push(fdQueueTag(i))
	}
	refill := make([]fdReq, wave)
	dst := make([]fdReq, 0, wave)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		dst = q.takeInto(dst[:0], wave)
		q.pushBatch(refill)
	}
}

// rerouteTo returns an fdReroute that runs the commands on target and counts
// them.
func rerouteTo(target *Client, n *atomic.Int64) func(context.Context, []Cmder, int) error {
	return func(ctx context.Context, cmds []Cmder, maxRetries int) error {
		n.Add(int64(len(cmds)))
		return target.processPipelineRetries(ctx, cmds, maxRetries)
	}
}

// fdRerouteTestEngine is an engine with only what the flush paths need, and
// a reroute that records each call's retry bound and serves the commands with
// serve.
func fdRerouteTestEngine(budget int, serve func(cmds []Cmder) error) (*fdEngine, *[]int) {
	var bounds []int
	fd := &fdEngine{
		ap:                 &AutoPipeliner{config: &AutoPipelineOptions{}},
		client:             &Client{baseClient: &baseClient{opt: &Options{}}},
		epoch:              time.Now(),
		maxBatch:           8,
		q:                  newFDQueue(8),
		redirectAware:      true,
		clusterRetryBudget: budget,
	}
	fd.reroute = func(_ context.Context, cmds []Cmder, maxRetries int) error {
		bounds = append(bounds, maxRetries)
		return serve(cmds)
	}
	return fd, &bounds
}

func fdServeOK(cmds []Cmder) error {
	for _, c := range cmds {
		c.SetErr(nil)
	}
	return nil
}

// A rerouted command keeps its remaining retry budget: the owner gets the
// budget left after the attempts already made on the FD connection, not a
// fresh one (codex P1 on #4111). Red before: the bound was dropped, and the
// cluster owner ran its full MaxRedirects loop.
func TestFDRerouteKeepsRemainingRetryBudget(t *testing.T) {
	ctx := context.Background()
	fd, bounds := fdRerouteTestEngine(3, fdServeOK)
	// POST-BUMP carry: attempts=2 is one execution done on the FD connection.
	// attempts=1 is a queued command never sent.
	reqs := []fdReq{
		{cmd: NewStatusCmd(ctx, "set", "a", "1"), batch: newAPBatch(), attempts: 2},
		{cmd: NewStatusCmd(ctx, "set", "b", "1"), batch: newAPBatch(), attempts: 2},
		{cmd: NewStatusCmd(ctx, "set", "c", "1"), batch: newAPBatch(), attempts: 1},
	}
	fd.rerouteReqs(ctx, reqs)
	if got, want := fmt.Sprint(*bounds), "[2 3]"; got != want {
		t.Fatalf("reroute retry bounds = %s, want %s (budget 3 minus executions done)", got, want)
	}
}

// The Close-time flush of an engine with an owner goes through the owner, not
// through the engine's own client (codex P2 on #4111: a child closed by
// topology GC flushed through its dropped node).
func TestFDShutdownFlushReroutes(t *testing.T) {
	ctx := context.Background()
	fd, bounds := fdRerouteTestEngine(3, fdServeOK)
	// Nothing listens here: a flush through the engine's own client fails.
	fd.client = NewClient(&Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: 50 * time.Millisecond})
	defer fd.client.Close()

	carry := NewStatusCmd(ctx, "set", "a", "1")
	queued := NewStatusCmd(ctx, "set", "b", "1")
	if fd.q.push(fdReq{cmd: queued, batch: newAPBatch(), attempts: 1}) != fdPushOK {
		t.Fatal("queue push failed")
	}
	fd.shutdownFlush(ctx, []fdReq{{cmd: carry, batch: newAPBatch(), attempts: 2}})
	if got, want := fmt.Sprint(*bounds), "[2 3]"; got != want {
		t.Fatalf("reroute retry bounds = %s, want %s", got, want)
	}
	for _, c := range []*StatusCmd{carry, queued} {
		if err := c.rawErr(); err != nil {
			t.Fatalf("%v: err = %v, want it served through the reroute", c.Args(), err)
		}
	}
}

// An owner pipeline that returns an earlier Redis error (redis.Nil) while a
// later command failed in transport still aborts the rest of the flush
// (codex P2 on #4111).
func TestFDRerouteDetectsMaskedTransportError(t *testing.T) {
	ctx := context.Background()
	calls := 0
	fd, _ := fdRerouteTestEngine(3, func(cmds []Cmder) error {
		calls++
		cmds[0].SetErr(Nil)
		for _, c := range cmds[1:] {
			c.SetErr(io.EOF)
		}
		return Nil // first command error, as ClusterClient.processPipeline returns
	})
	fd.maxBatch = 2 // two chunks
	reqs := make([]fdReq, 4)
	for i := range reqs {
		reqs[i] = fdReq{cmd: NewStringCmd(ctx, "get", "k"+itoa(i)), batch: newAPBatch(), attempts: 1}
	}
	fd.rerouteReqs(ctx, reqs)
	if calls != 1 {
		t.Fatalf("owner ran %d chunks, want 1: the second ran after a transport failure", calls)
	}
	for i := 2; i < len(reqs); i++ {
		if err := reqs[i].cmd.rawErr(); !errors.Is(err, io.EOF) {
			t.Fatalf("reqs[%d] err = %v, want io.EOF (failed by the abort)", i, err)
		}
	}
}

// Rerouted commands report the per-command duration metric the reader would
// have reported (codex P2 on #4111).
func TestFDRerouteRecordsCommandMetrics(t *testing.T) {
	rec := &fdOtelRecorder{}
	otel.SetGlobalRecorder(rec)
	defer otel.SetGlobalRecorder(nil)

	ctx := context.Background()
	fd, _ := fdRerouteTestEngine(3, fdServeOK)
	reqs := []fdReq{
		{cmd: NewStatusCmd(ctx, "set", "a", "1"), batch: newAPBatch(), attempts: 2},
		{cmd: NewStatusCmd(ctx, "set", "b", "1"), batch: newAPBatch(), attempts: 1},
	}
	fd.rerouteReqs(ctx, reqs)
	if n := rec.opDurations.Load(); n != int64(len(reqs)) {
		t.Fatalf("operation durations recorded = %d, want %d", n, len(reqs))
	}
}

// With an owner reroute, a persistent lease failure runs the accepted commands
// through the owner instead of failing them.
func TestFDLeaseFailureReroutesBacklog(t *testing.T) {
	ctx := context.Background()
	target := NewClient(&Options{Addr: ":6379"})
	defer target.Close()
	if err := target.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}

	c := NewClient(&Options{
		Addr:             "127.0.0.1:1", // nothing listening: every lease fails
		Protocol:         3,
		PipelinePoolSize: 2,
		PoolSize:         2,
		MaxRetries:       2,
		DialTimeout:      150 * time.Millisecond,
		MinRetryBackoff:  time.Millisecond,
		MaxRetryBackoff:  5 * time.Millisecond,
	})
	defer c.Close()
	var rerouted atomic.Int64
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex: true,
		fdReroute:  rerouteTo(target, &rerouted),
	})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil || ap.fd.reroute == nil {
		t.Fatal("full-duplex engine with a reroute not active")
	}

	key := "fd:reroute:lease"
	done := make(chan error, 1)
	go func() { done <- ap.Set(ctx, key, "v", 0).Err() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Set = %v, want it served through the reroute", err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("command hung on a persistent lease failure")
	}
	if rerouted.Load() == 0 {
		t.Fatal("reroute not used")
	}
	if got, err := target.Get(ctx, key).Result(); err != nil || got != "v" {
		t.Fatalf("GET on the reroute target = %q, %v", got, err)
	}
}

// With an owner reroute, the replayable tail after a connection error runs
// through the owner, in order: no worker sees a failed or misaligned reply.
func TestFDConnErrReroutesTail(t *testing.T) {
	ctx := context.Background()
	target := NewClient(&Options{Addr: ":6379"})
	defer target.Close()
	if err := target.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	c := fdTestClient(":6379")
	defer c.Close()
	var rerouted atomic.Int64
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex: true,
		fdReroute:  rerouteTo(target, &rerouted),
	})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()

	killer := NewClient(&Options{Addr: ":6379"})
	defer killer.Close()

	const workers = 16
	var stop atomic.Bool
	done := make(chan error, workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			key := "fd:reroute:kill:" + itoa(w)
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
					done <- fmt.Errorf("worker %d get #%d: got %q want %q", w, n, got, val)
					return
				}
			}
			done <- nil
		}(w)
	}

	time.Sleep(120 * time.Millisecond)
	for i := 0; i < 5; i++ {
		_ = killer.Do(ctx, "CLIENT", "KILL", "TYPE", "normal", "SKIPME", "yes").Err()
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
			t.Fatal("timeout: a worker hung after a connection kill")
		}
	}
	if rerouted.Load() == 0 {
		t.Fatal("no command went through the reroute across 5 connection kills")
	}
}

// The cluster full-duplex router gives its node children a reroute through
// the ClusterClient, and Config never hands it back.
func TestClusterFDRouterChildReroutes(t *testing.T) {
	cc := NewClusterClient(&ClusterOptions{Addrs: []string{"127.0.0.1:1"}})
	defer cc.Close()
	r := newClusterFDRouter(nil, cc, &AutoPipelineOptions{FullDuplex: true}, true)
	if r.childCfg.fdReroute == nil {
		t.Fatal("cluster FD child config has no reroute")
	}

	ap := &AutoPipeliner{config: r.childCfg}
	if ap.Config().fdReroute != nil {
		t.Fatal("Config() exposes the internal reroute")
	}
}

// Cluster full-duplex under connection kills on every master: each node child's
// replayable tail reroutes through the ClusterClient, in order, so no worker
// sees a failed or misaligned reply.
func TestClusterFDConnKillReroutesTail(t *testing.T) {
	cc := dialClusterFDTest(t)
	defer cc.Close()
	ctx := context.Background()

	ap, err := cc.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipelineWithOptions: %v", err)
	}
	defer ap.Close()
	if ap.clusterFD == nil {
		t.Fatal("cluster full-duplex router not active")
	}

	const workers = 24
	var stop atomic.Bool
	done := make(chan error, workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			key := fmt.Sprintf("cfd:reroute:{%d}", w) // spread over all masters
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
					done <- fmt.Errorf("worker %d get #%d: got %q want %q", w, n, got, val)
					return
				}
			}
			done <- nil
		}(w)
	}

	time.Sleep(150 * time.Millisecond)
	for i := 0; i < 4; i++ {
		_ = cc.ForEachMaster(ctx, func(ctx context.Context, m *Client) error {
			return m.Do(ctx, "CLIENT", "KILL", "TYPE", "normal", "SKIPME", "yes").Err()
		})
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(150 * time.Millisecond)
	stop.Store(true)

	deadline := time.After(20 * time.Second)
	for w := 0; w < workers; w++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-deadline:
			t.Fatal("timeout: a worker hung after a connection kill")
		}
	}
}

// FullDuplexPipes chooses the number of full-duplex pipes (engines). NumShards>1
// is still honored as the 9.23.0 spelling when FullDuplexPipes is 0, and
// Config() reports the effective pipe count either way. Construction only:
// engines lease their connection lazily, so no server is needed.
func TestFullDuplexPipesEngineCount(t *testing.T) {
	cases := []struct {
		name string
		cfg  AutoPipelineOptions
		want int
	}{
		{"default", AutoPipelineOptions{FullDuplex: true}, 1},
		{"pipes", AutoPipelineOptions{FullDuplex: true, FullDuplexPipes: 3}, 3},
		{"numshards fallback", AutoPipelineOptions{FullDuplex: true, NumShards: 3}, 3},
		{"both equal", AutoPipelineOptions{FullDuplex: true, FullDuplexPipes: 3, NumShards: 3}, 3},
		{"pipes with numshards 1", AutoPipelineOptions{FullDuplex: true, FullDuplexPipes: 2, NumShards: 1}, 2},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := NewClient(&Options{Addr: internalTestRedisAddr()})
			defer c.Close()
			cfg := tc.cfg
			ap, err := c.AsyncAutoPipelineWithOptions(&cfg)
			if err != nil {
				t.Fatalf("construct: %v", err)
			}
			defer ap.Close()
			if got := len(ap.fds); got != tc.want {
				t.Fatalf("engines = %d, want %d", got, tc.want)
			}
			got := ap.Config()
			if !got.FullDuplex {
				t.Fatal("Config().FullDuplex = false, want true")
			}
			if got.FullDuplexPipes != tc.want {
				t.Fatalf("Config().FullDuplexPipes = %d, want %d", got.FullDuplexPipes, tc.want)
			}
		})
	}
}

func TestFullDuplexPipesValidation(t *testing.T) {
	cases := []struct {
		name    string
		cfg     AutoPipelineOptions
		wantErr string
	}{
		{"negative", AutoPipelineOptions{FullDuplex: true, FullDuplexPipes: -1}, "FullDuplexPipes=-1"},
		{"disagrees with numshards", AutoPipelineOptions{FullDuplex: true, FullDuplexPipes: 4, NumShards: 2}, "disagree"},
		// Like the other full-duplex fields, it is ignored without FullDuplex.
		{"ignored without full duplex", AutoPipelineOptions{FullDuplexPipes: -1}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate: %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Validate: got %v, want an error containing %q", err, tc.wantErr)
			}
		})
	}
}

// The pool-size error names the field the caller set.
func TestFullDuplexPipesNeedsPipelinePool(t *testing.T) {
	c := NewClient(&Options{Addr: internalTestRedisAddr(), PipelinePoolSize: 2})
	defer c.Close()
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true, FullDuplexPipes: 4})
	if err == nil {
		ap.Close()
		t.Fatal("FullDuplexPipes=4 on a pipeline pool of 2 was accepted")
	}
	if !strings.Contains(err.Error(), "FullDuplexPipes=4") || !strings.Contains(err.Error(), "PipelinePoolSize gives 2") {
		t.Fatalf("error should name FullDuplexPipes and the pool size, got: %v", err)
	}
}

// When full duplex cannot engage (no pipeline pool), FullDuplexPipes has no
// effect. Unlike NumShards>1, it must not turn into round-robin half-duplex
// shards that then demand Unordered on the ordered async face.
func TestFullDuplexPipesNoEffectWithoutPipelinePool(t *testing.T) {
	c := NewClient(&Options{Addr: internalTestRedisAddr(), PipelinePoolSize: -1})
	defer c.Close()
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true, FullDuplexPipes: 4})
	if err != nil {
		t.Fatalf("construct: %v (FullDuplexPipes must not require Unordered when full duplex is off)", err)
	}
	defer ap.Close()
	if ap.Config().FullDuplex {
		t.Fatal("Config().FullDuplex = true without a pipeline pool")
	}
	if got := len(ap.shards); got != 1 {
		t.Fatalf("half-duplex shards = %d, want 1 (FullDuplexPipes must not shard the half-duplex queue)", got)
	}
}

// Cluster full duplex runs one pipe per master. The per-node child config is a
// copy of the parent's, so a parent FullDuplexPipes must be forced to 1 there;
// otherwise every master would get that many engines.
func TestFullDuplexPipesClusterChildForcedToOne(t *testing.T) {
	cc := NewClusterClient(&ClusterOptions{Addrs: []string{internalTestRedisAddr()}})
	defer cc.Close()
	ap, err := cc.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true, FullDuplexPipes: 4})
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	defer ap.Close()
	if ap.clusterFD == nil {
		t.Fatal("cluster full duplex did not engage")
	}
	if got := ap.clusterFD.childCfg.FullDuplexPipes; got != 1 {
		t.Fatalf("child FullDuplexPipes = %d, want 1", got)
	}
	if got := ap.Config().FullDuplexPipes; got != 1 {
		t.Fatalf("Config().FullDuplexPipes = %d, want 1 (one pipe per master)", got)
	}
}

// warmSpareClient returns a client whose dialer counts dials and, once slow is
// set, takes slowDial per dial.
func warmSpareClient(t *testing.T, minIdle int, slow *atomic.Bool) *Client {
	t.Helper()
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	client := NewClient(&Options{
		Addr:                 internalTestRedisAddr(),
		PipelineMinIdleConns: minIdle,
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if slow.Load() {
				time.Sleep(slowDial)
			}
			var d net.Dialer
			return d.DialContext(ctx, network, addr)
		},
	})
	t.Cleanup(func() { _ = client.Close() })
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	return client
}

const slowDial = 500 * time.Millisecond

// waitFDSession waits until the engine holds a session connection and the
// pipeline pool has want idle connections whose dials have finished. IdleLen
// counts a min-idle connection as soon as its dial starts; Len only once it is
// in the pool, so Len must also reach the held connection plus the idle ones.
func waitFDSession(t *testing.T, ap *AutoPipeliner, client *Client, want int) {
	t.Helper()
	pp := client.loadPipelinePool().pool
	deadline := time.Now().Add(5 * time.Second)
	for ap.fd.curConn.Load() == nil || pp.IdleLen() != want || pp.Len() != want+1 {
		if time.Now().After(deadline) {
			t.Fatalf("session held=%v, pipeline Len=%d IdleLen=%d, want held with %d idle",
				ap.fd.curConn.Load() != nil, pp.Len(), pp.IdleLen(), want)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestFDWarmSpareReplacesWithoutDial checks the full-duplex engine keeps an
// idle spare in the pipeline pool, so after its connection fails the
// replacement does not wait for a dial.
func TestFDWarmSpareReplacesWithoutDial(t *testing.T) {
	ctx := context.Background()
	var slow atomic.Bool
	client := warmSpareClient(t, 0, &slow)

	ap, err := newAutoPipeliner(client, &AutoPipelineOptions{FullDuplex: true}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Skip("full-duplex engine not enabled for this client")
	}
	if err := ap.Set(ctx, "fdspare:k", "1", 0).Err(); err != nil {
		t.Fatal(err)
	}
	waitFDSession(t, ap, client, 1)

	// From now on a dial is slow. Break the held connection: the next command
	// fails its write and is replayed on the replacement.
	slow.Store(true)
	_ = ap.fd.curConn.Load().Close()

	start := time.Now()
	if err := ap.Set(ctx, "fdspare:k", "2", 0).Err(); err != nil {
		t.Fatalf("command after connection failure: %v", err)
	}
	if d := time.Since(start); d >= slowDial/2 {
		t.Fatalf("recovery took %v; the replacement dialed instead of leasing the warm spare", d)
	}
	if v, err := client.Get(ctx, "fdspare:k").Result(); err != nil || v != "2" {
		t.Fatalf("get = %q, %v; want \"2\"", v, err)
	}
}

// TestFDWarmSpareOptOut checks a negative PipelineMinIdleConns keeps the
// pipeline pool at zero idle connections under full-duplex.
func TestFDWarmSpareOptOut(t *testing.T) {
	ctx := context.Background()
	var slow atomic.Bool
	client := warmSpareClient(t, -1, &slow)

	ap, err := newAutoPipeliner(client, &AutoPipelineOptions{FullDuplex: true}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Skip("full-duplex engine not enabled for this client")
	}
	if err := ap.Set(ctx, "fdspare:o", "1", 0).Err(); err != nil {
		t.Fatal(err)
	}
	waitFDSession(t, ap, client, 0)
	time.Sleep(50 * time.Millisecond)
	if n := client.loadPipelinePool().pool.IdleLen(); n != 0 {
		t.Fatalf("pipeline IdleLen = %d with PipelineMinIdleConns -1, want 0", n)
	}
}

// TestFDSpareFits checks the spare is only added when it does not take a slot
// an engine needs and the idle limit can hold it next to a returned session
// connection (Codex on #4101).
func TestFDSpareFits(t *testing.T) {
	for _, tc := range []struct {
		poolSize, maxIdle, engines int
		want                       bool
	}{
		{10, 0, 1, true},  // defaults
		{1, 0, 1, false},  // the only slot belongs to the engine
		{2, 0, 2, false},  // NumShards == PipelinePoolSize
		{3, 0, 2, true},   // room for two engines and a spare
		{10, 1, 1, false}, // spare would fill the only idle slot
		{10, 2, 1, true},  // spare + returned session conn
		{10, 2, 2, false}, // two engines need both idle slots
	} {
		if got := fdSpareFits(tc.poolSize, tc.maxIdle, tc.engines); got != tc.want {
			t.Errorf("fdSpareFits(pool=%d, maxIdle=%d, engines=%d) = %v, want %v",
				tc.poolSize, tc.maxIdle, tc.engines, got, tc.want)
		}
	}
}

// TestFDNoSpareWhenItDoesNotFit checks a client whose pipeline pool cannot fit
// a spare gets none, and that the full-duplex session then stays on the
// pipeline pool instead of spilling to the main pool.
func TestFDNoSpareWhenItDoesNotFit(t *testing.T) {
	for _, tc := range []struct {
		name string
		opt  func(*Options)
	}{
		{"PipelinePoolSize 1", func(o *Options) { o.PipelinePoolSize = 1 }},
		{"MaxIdleConns 1", func(o *Options) { o.MaxIdleConns = 1 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			if err := probeRedis(internalTestRedisAddr()); err != nil {
				t.Skipf("no redis: %v", err)
			}
			opt := &Options{Addr: internalTestRedisAddr()}
			tc.opt(opt)
			client := NewClient(opt)
			defer client.Close()
			if err := client.Ping(ctx).Err(); err != nil {
				t.Skipf("no redis: %v", err)
			}
			ap, err := newAutoPipeliner(client, &AutoPipelineOptions{FullDuplex: true}, false)
			if err != nil {
				t.Fatal(err)
			}
			defer ap.Close()
			if ap.fd == nil {
				t.Skip("full-duplex engine not enabled for this client")
			}
			if err := ap.Set(ctx, "fdspare:fit", "1", 0).Err(); err != nil {
				t.Fatal(err)
			}
			waitFDSession(t, ap, client, 0)
			time.Sleep(50 * time.Millisecond)
			pp := client.loadPipelinePool().pool
			if n := pp.IdleLen(); n != 0 {
				t.Fatalf("pipeline IdleLen = %d, want 0 (no spare)", n)
			}
			if ap.fd.curConnSpilled.Load() {
				t.Fatal("full-duplex session spilled to the main pool")
			}
		})
	}
}

// TestPipelineMinIdleConnsWithoutFD checks PipelineMinIdleConns alone keeps
// idle connections in the pipeline pool, and the default keeps none.
func TestPipelineMinIdleConnsWithoutFD(t *testing.T) {
	var slow atomic.Bool
	client := warmSpareClient(t, 2, &slow)
	pp := client.loadPipelinePool().pool
	deadline := time.Now().Add(5 * time.Second)
	for pp.IdleLen() != 2 || pp.Len() != 2 {
		if time.Now().After(deadline) {
			t.Fatalf("pipeline Len=%d IdleLen=%d, want 2/2", pp.Len(), pp.IdleLen())
		}
		time.Sleep(time.Millisecond)
	}

	def := warmSpareClient(t, 0, &slow)
	time.Sleep(50 * time.Millisecond)
	if n := def.loadPipelinePool().pool.IdleLen(); n != 0 {
		t.Fatalf("default pipeline IdleLen = %d, want 0", n)
	}
}
