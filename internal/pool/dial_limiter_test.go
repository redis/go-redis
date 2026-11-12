package pool_test

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

// Real-world impact of DialRateLimit, measured against a live Redis (RESP3,
// localhost, Apple M4 Max): 200 goroutines x 100 PINGs arriving as a single
// burst, PoolSize=50, PoolTimeout=3s — sustained saturation, the worst case
// for the limiter since idle connections are never sufficient for everyone.
//
//	config     | conns | throttled | timeouts | ops/s  | avg    | p50    | p99    | max
//	disabled   |  50   |     0     |    0     | 51,267 |  3.9ms |  3.8ms |  5.6ms |  13ms
//	rate=50/s  |  50   |     0     |    0     | 45,570 |  4.4ms |  4.2ms |  8.6ms |  19ms
//	rate=20/s  |  33   |   8,147   |    0     | 30,616 |  6.4ms |  6.2ms | 12.8ms |  24ms
//	rate=5/s   |  13   |   5,923   |    0     | 12,386 | 15.8ms | 13.9ms | 53.6ms | 260ms
//
// Reading it: disabled dials the pool straight to 50 (the dial storm of issue
// #3890); rate >= burst demand behaves like disabled; lower rates trade
// throughput and tail latency for fewer connections (rate=5/s: 74% fewer
// conns), with zero pool timeouts and zero errors — every request is served
// via reuse or the PoolTimeout escape hatch. Under a short burst (the target
// scenario) the latency cost disappears with the burst, while the pool avoids
// inflating to PoolSize; idle-hit fast paths never consult the limiter, and
// with the feature disabled hot-path benchmarks match master (interleaved
// benchstat: no significant delta, allocations byte-identical).

// countingDialer returns a dialer that increments dials on every call.
func countingDialer(dials *atomic.Int32) func(context.Context) (net.Conn, error) {
	return func(context.Context) (net.Conn, error) {
		dials.Add(1)
		return newDummyConn(), nil
	}
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("condition not met within %v", timeout)
}

// TestDialRateLimitReuseShortCircuit is the core behavior: a Get() that is
// throttled by the dial rate limiter must be satisfied by a connection returned
// to the idle pool while it waits, instead of dialing a new one.
func TestDialRateLimitReuseShortCircuit(t *testing.T) {
	var dials atomic.Int32
	p := pool.NewConnPool(&pool.Options{
		Dialer:             countingDialer(&dials),
		PoolSize:           10,
		MaxConcurrentDials: 10,
		DialRateLimit:      1,
		DialRateBurst:      1,
		PoolTimeout:        2 * time.Second,
	})
	defer p.Close()
	ctx := context.Background()

	// First Get consumes the single burst token and dials c1.
	c1, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("first Get failed: %v", err)
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("expected exactly 1 dial, got %d", got)
	}

	// Second Get is throttled (no token, no idle connection) and parks.
	type result struct {
		cn  *pool.Conn
		err error
	}
	done := make(chan result, 1)
	go func() {
		cn, err := p.Get(ctx)
		done <- result{cn, err}
	}()

	// Wait until the second Get has parked in the dial wait queue.
	waitFor(t, time.Second, func() bool { return p.DialWaitQueueLen() == 1 })

	// Return c1 to the idle pool: the parked Get should wake, re-pop it, and
	// reuse it without dialing a new connection.
	p.Put(ctx, c1)

	r := <-done
	if r.err != nil {
		t.Fatalf("throttled Get failed: %v", r.err)
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("throttled Get should reuse the idle conn, but dials=%d", got)
	}
	if r.cn != c1 {
		t.Fatal("throttled Get should have returned the reused connection c1")
	}
	if st := p.Stats(); st.RateLimitedDials != 1 {
		t.Fatalf("RateLimitedDials=%d, want 1", st.RateLimitedDials)
	}
	p.Put(ctx, r.cn)
}

// TestDialRateLimitTryGetDoesNotPark verifies that a throttled TryGet keeps its
// non-blocking contract: it returns ErrPoolTryFull at once (so the caller can
// spill) instead of parking for a dial token.
func TestDialRateLimitTryGetDoesNotPark(t *testing.T) {
	var dials atomic.Int32
	p := pool.NewConnPool(&pool.Options{
		Dialer:             countingDialer(&dials),
		PoolSize:           10,
		MaxConcurrentDials: 10,
		DialRateLimit:      1,
		DialRateBurst:      1,
		PoolTimeout:        2 * time.Second,
	})
	defer p.Close()
	ctx := context.Background()

	c1, err := p.TryGet(ctx)
	if err != nil {
		t.Fatalf("first TryGet failed: %v", err)
	}
	defer p.Put(ctx, c1)

	start := time.Now()
	_, err = p.TryGet(ctx)
	if err != pool.ErrPoolTryFull {
		t.Fatalf("throttled TryGet err=%v, want ErrPoolTryFull", err)
	}
	if d := time.Since(start); d > 200*time.Millisecond {
		t.Fatalf("throttled TryGet blocked for %v", d)
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("dials=%d, want 1", got)
	}
	st := p.Stats()
	if st.RateLimitedDials != 1 {
		t.Fatalf("RateLimitedDials=%d, want 1", st.RateLimitedDials)
	}
	if st.Timeouts != 0 {
		t.Fatalf("Timeouts=%d, want 0", st.Timeouts)
	}
}

// TestDialRateLimitEscapeHatch verifies that when no connection is returned and
// no token frees up within PoolTimeout, the throttled Get() creates a new
// connection anyway rather than erroring.
func TestDialRateLimitEscapeHatch(t *testing.T) {
	var dials atomic.Int32
	p := pool.NewConnPool(&pool.Options{
		Dialer:             countingDialer(&dials),
		PoolSize:           10,
		MaxConcurrentDials: 10,
		DialRateLimit:      1,
		DialRateBurst:      1,
		PoolTimeout:        50 * time.Millisecond,
	})
	defer p.Close()
	ctx := context.Background()

	c1, err := p.Get(ctx) // dial #1, consumes the token
	if err != nil {
		t.Fatalf("first Get failed: %v", err)
	}

	start := time.Now()
	c2, err := p.Get(ctx) // throttled, no idle -> park until PoolTimeout -> create
	if err != nil {
		t.Fatalf("throttled Get should escape and succeed, got err: %v", err)
	}
	elapsed := time.Since(start)

	if got := dials.Load(); got != 2 {
		t.Fatalf("escape hatch should dial a 2nd connection, dials=%d", got)
	}
	if elapsed < 40*time.Millisecond {
		t.Fatalf("escape hatch should wait ~PoolTimeout before dialing, waited only %v", elapsed)
	}
	if st := p.Stats(); st.RateLimitedDials != 1 {
		t.Fatalf("RateLimitedDials=%d, want 1", st.RateLimitedDials)
	}
	p.Put(ctx, c1)
	p.Put(ctx, c2)
}

// TestDialRateLimitContextCancel verifies that a throttled Get() honors context
// cancellation, does not dial, and frees its turn (no leak).
func TestDialRateLimitContextCancel(t *testing.T) {
	var dials atomic.Int32
	p := pool.NewConnPool(&pool.Options{
		Dialer:             countingDialer(&dials),
		PoolSize:           10,
		MaxConcurrentDials: 10,
		DialRateLimit:      1,
		DialRateBurst:      1,
		PoolTimeout:        5 * time.Second,
	})
	defer p.Close()

	c1, err := p.Get(context.Background()) // dial #1, consumes the token
	if err != nil {
		t.Fatalf("first Get failed: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := p.Get(ctx); err == nil {
		t.Fatal("throttled Get should fail when context is cancelled")
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("cancelled Get must not dial, dials=%d", got)
	}
	waitFor(t, time.Second, func() bool { return p.DialWaitQueueLen() == 0 })

	// The cancelled Get must have freed its turn: returning c1 and getting a
	// connection again must still work (reusing the idle c1).
	p.Put(context.Background(), c1)
	c2, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("Get after cancel failed (turn leaked?): %v", err)
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("Get after cancel should reuse c1, dials=%d", got)
	}
	p.Put(context.Background(), c2)
}

// TestDialRateLimitConcurrentReuse is the concurrency-level proof of the core
// benefit: many throttled Get() callers contend for a single connection that is
// handed around via the idle pool. With one token already spent and none
// refilling within the test window, every caller must be satisfied by reuse and
// no additional connection may be dialed. A lost wakeup would hang here (caught
// by the watchdog) rather than silently pass.
func TestDialRateLimitConcurrentReuse(t *testing.T) {
	var dials atomic.Int32
	p := pool.NewConnPool(&pool.Options{
		Dialer:             countingDialer(&dials),
		PoolSize:           64,
		MaxConcurrentDials: 64,
		DialRateLimit:      1,
		DialRateBurst:      1,
		PoolTimeout:        5 * time.Second,
	})
	defer p.Close()
	ctx := context.Background()

	// Consume the only token by creating the sole connection.
	c1, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("seed Get failed: %v", err)
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("seed should dial exactly once, got %d", got)
	}

	const n = 10
	var wg sync.WaitGroup
	var ok atomic.Int32
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			cn, err := p.Get(ctx)
			if err != nil {
				return
			}
			ok.Add(1)
			p.Put(ctx, cn)
		}()
	}

	// Release c1 into the idle pool to start the hand-off chain.
	p.Put(ctx, c1)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("throttled Gets deadlocked (lost wakeup?)")
	}

	if got := ok.Load(); got != n {
		t.Fatalf("only %d/%d throttled Gets succeeded", got, n)
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("all throttled Gets should reuse the single connection, but dials=%d", got)
	}
}

// TestDialRateLimitConcurrentEscape verifies that a batch of concurrently
// throttled Get() callers with no reusable connections does not deadlock: each
// parks until PoolTimeout and then creates its own connection. Proves the
// escape hatch is safe under contention and the batch is bounded.
func TestDialRateLimitConcurrentEscape(t *testing.T) {
	var dials atomic.Int32
	p := pool.NewConnPool(&pool.Options{
		Dialer:             countingDialer(&dials),
		PoolSize:           64,
		MaxConcurrentDials: 64,
		DialRateLimit:      1,
		DialRateBurst:      1,
		PoolTimeout:        50 * time.Millisecond,
	})
	defer p.Close()
	ctx := context.Background()

	// Consume the only token; nothing is ever returned to the idle pool.
	c0, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("seed Get failed: %v", err)
	}

	const n = 10
	var wg sync.WaitGroup
	var ok atomic.Int32
	conns := make([]*pool.Conn, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			cn, err := p.Get(ctx) // throttled, never fed -> escape and dial
			if err != nil {
				return
			}
			conns[i] = cn
			ok.Add(1)
		}(i)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent escape deadlocked")
	}

	if got := ok.Load(); got != n {
		t.Fatalf("only %d/%d throttled Gets escaped and dialed", got, n)
	}
	// One seed dial + one escape dial per throttled Get.
	if got := dials.Load(); got != n+1 {
		t.Fatalf("expected %d dials (seed + %d escapes), got %d", n+1, n, got)
	}

	for _, cn := range conns {
		if cn != nil {
			p.Put(ctx, cn)
		}
	}
	p.Put(ctx, c0)
}

// TestDialRateLimitMinIdleConnsPaced verifies the min-idle refill path respects
// the dial rate limit: reconnecting after mass connection loss must not storm
// the server with MinIdleConns concurrent dials.
func TestDialRateLimitMinIdleConnsPaced(t *testing.T) {
	var dials atomic.Int32
	p := pool.NewConnPool(&pool.Options{
		Dialer:             countingDialer(&dials),
		PoolSize:           10,
		MaxConcurrentDials: 10,
		MinIdleConns:       5,
		DialRateLimit:      1,
		DialRateBurst:      1,
		PoolTimeout:        time.Second,
	})
	defer p.Close()

	// The refill wants 5 connections but the bucket holds a single token
	// (rate 1/s): within 300ms at most the burst token plus one refill can
	// pass. Unpaced, all 5 dials fire immediately.
	time.Sleep(300 * time.Millisecond)
	if got := dials.Load(); got > 2 {
		t.Fatalf("min-idle refill not paced: %d dials within 300ms at rate 1/s", got)
	}
}

// TestDialRateLimitCloseWakesWaiters verifies pool Close() immediately wakes
// Get() callers parked by the dial limiter instead of letting them sleep out
// their park timer.
func TestDialRateLimitCloseWakesWaiters(t *testing.T) {
	var dials atomic.Int32
	p := pool.NewConnPool(&pool.Options{
		Dialer:             countingDialer(&dials),
		PoolSize:           10,
		MaxConcurrentDials: 10,
		DialRateLimit:      1,
		DialRateBurst:      1,
		PoolTimeout:        10 * time.Second, // park timer far in the future
	})
	ctx := context.Background()

	if _, err := p.Get(ctx); err != nil { // consume the only token
		t.Fatalf("seed Get failed: %v", err)
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := p.Get(ctx) // throttled -> parks
		errCh <- err
	}()
	waitFor(t, time.Second, func() bool { return p.DialWaitQueueLen() == 1 })

	start := time.Now()
	_ = p.Close()

	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("parked Get should fail after pool Close")
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Fatalf("parked Get took %v to observe Close (should be immediate)", elapsed)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("parked Get still blocked after Close (waiter not woken)")
	}
}

// TestDialRateLimitDeadlineIncludesTurnWait verifies the throttle deadline is
// anchored at Get() entry, so time already spent waiting for a pool turn counts
// against PoolTimeout and a throttled Get never waits ~2x PoolTimeout total.
func TestDialRateLimitDeadlineIncludesTurnWait(t *testing.T) {
	var dials atomic.Int32
	p := pool.NewConnPool(&pool.Options{
		Dialer:             countingDialer(&dials),
		PoolSize:           1, // single turn: second Get blocks in waitTurn
		MaxConcurrentDials: 1,
		DialRateLimit:      1,
		DialRateBurst:      1,
		PoolTimeout:        800 * time.Millisecond,
	})
	defer p.Close()
	ctx := context.Background()

	c1, err := p.Get(ctx) // dial #1: consumes the token and the only turn
	if err != nil {
		t.Fatalf("seed Get failed: %v", err)
	}

	// Free the turn (without returning an idle conn) halfway through the
	// timeout budget: the second Get spends ~400ms in waitTurn, then parks
	// only until the original deadline (~400ms more) before escaping.
	go func() {
		time.Sleep(400 * time.Millisecond)
		p.Remove(ctx, c1, nil)
	}()

	start := time.Now()
	c2, err := p.Get(ctx)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("throttled Get should escape and succeed, got: %v", err)
	}
	p.Put(ctx, c2)

	if got := dials.Load(); got != 2 {
		t.Fatalf("expected 2 dials (seed + escape), got %d", got)
	}
	// With the deadline anchored at Get() entry, total wait ≈ PoolTimeout
	// (800ms). The old bug re-anchored it after waitTurn: ~400ms + 800ms =
	// ~1200ms. The 1100ms bound leaves 300ms of scheduling slack for loaded
	// CI while still failing if the deadline stops crediting waitTurn time.
	if elapsed > 1100*time.Millisecond {
		t.Fatalf("throttled Get waited %v; deadline not crediting waitTurn time (~2x PoolTimeout bug)", elapsed)
	}
}

// TestDialRateLimitRefillDoesNotStarveGets reproduces the review finding that
// a min-idle refill worker must acquire its dial token BEFORE taking a pool
// turn: a worker sleeping for a token while sitting on a turn starves
// foreground Get() calls of turns and they fail with ErrPoolTimeout even
// though the pool has capacity.
func TestDialRateLimitRefillDoesNotStarveGets(t *testing.T) {
	var dials atomic.Int32
	p := pool.NewConnPool(&pool.Options{
		Dialer:             countingDialer(&dials),
		PoolSize:           1, // single turn: a turn-holding refill blocks all Gets
		MaxConcurrentDials: 1,
		MinIdleConns:       1,
		DialRateLimit:      1, // 1 token/s: refill after the burst token waits ~1s
		DialRateBurst:      1,
		PoolTimeout:        300 * time.Millisecond,
	})
	defer p.Close()
	ctx := context.Background()

	// Initial refill consumes the burst token to create the idle conn.
	waitFor(t, 2*time.Second, func() bool { return p.IdleLen() == 1 })

	// Take the conn and Remove it: frees the turn, drops below MinIdleConns,
	// and triggers a refill with an empty token bucket — that refill worker
	// now waits ~1s for the next token.
	cn, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	p.Remove(ctx, cn, nil)

	// A foreground Get must not be starved of the turn by the waiting refill:
	// it parks (no token), escapes at PoolTimeout, and dials.
	start := time.Now()
	cn2, err := p.Get(ctx)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("foreground Get starved by token-waiting refill worker: %v (after %v)", err, elapsed)
	}
	p.Put(ctx, cn2)
	if elapsed > 600*time.Millisecond {
		t.Fatalf("foreground Get took %v; expected ~PoolTimeout escape (300ms)", elapsed)
	}
}

// TestDialRateLimitRefillDoesNotInflateCapacity pins the MaxActiveConns
// interaction: refill workers parked on a rate-limit token must not count as
// live capacity. Before the fix checkMinIdleConns bumped poolSize once per
// spawned worker, so with MinIdleConns == MaxActiveConns the counter hit the
// cap while zero connections existed and the first Get() failed with
// ErrPoolExhausted.
func TestDialRateLimitRefillDoesNotInflateCapacity(t *testing.T) {
	var dials atomic.Int32
	slowDialer := func(context.Context) (net.Conn, error) {
		dials.Add(1)
		time.Sleep(20 * time.Millisecond) // keep the first refill dial in flight
		return newDummyConn(), nil
	}
	p := pool.NewConnPool(&pool.Options{
		Dialer:             slowDialer,
		PoolSize:           10,
		MaxConcurrentDials: 10,
		MinIdleConns:       5,
		MaxActiveConns:     5,
		DialRateLimit:      1, // refill workers 2..5 park on the token bucket
		DialRateBurst:      1,
		PoolTimeout:        3 * time.Second,
	})
	defer p.Close()

	// At this point at most one refill dial is underway; the other four
	// workers wait for tokens and must not be counted as active conns.
	cn, err := p.Get(context.Background())
	if err != nil {
		t.Fatalf("Get with token-waiting refill workers failed: %v", err)
	}
	p.Put(context.Background(), cn)
}

// TestDialRateLimitDisabled verifies zero behavioral change when the limiter is
// not configured.
func TestDialRateLimitDisabled(t *testing.T) {
	var dials atomic.Int32
	p := pool.NewConnPool(&pool.Options{
		Dialer:             countingDialer(&dials),
		PoolSize:           10,
		MaxConcurrentDials: 10,
		PoolTimeout:        time.Second,
		// DialRateLimit unset -> rate limiting disabled.
	})
	defer p.Close()
	ctx := context.Background()

	const n = 8
	conns := make([]*pool.Conn, n)
	for i := 0; i < n; i++ {
		cn, err := p.Get(ctx)
		if err != nil {
			t.Fatalf("Get #%d failed: %v", i, err)
		}
		conns[i] = cn
	}
	for _, cn := range conns {
		p.Put(ctx, cn)
	}

	if got := dials.Load(); got != n {
		t.Fatalf("disabled limiter should dial freely: dials=%d, want %d", got, n)
	}
	if st := p.Stats(); st.RateLimitedDials != 0 {
		t.Fatalf("RateLimitedDials must be 0 when disabled, got %d", st.RateLimitedDials)
	}
	if p.DialWaitQueueLen() != 0 {
		t.Fatal("no waiters expected when limiter disabled")
	}
}

// rejectIdleHook rejects every idle connection on Get (accept=false, no
// error), so the pool puts it back. New connections are accepted.
type rejectIdleHook struct {
	rejected atomic.Int32
}

func (h *rejectIdleHook) OnGet(_ context.Context, _ *pool.Conn, isNewConn bool) (bool, error) {
	if isNewConn {
		return true, nil
	}
	h.rejected.Add(1)
	return false, nil
}

func (h *rejectIdleHook) OnPut(context.Context, *pool.Conn) (bool, bool, error) {
	return true, false, nil
}

func (h *rejectIdleHook) OnRemove(context.Context, *pool.Conn, error) {}

// TestDialRateLimitHookRejectedIdleDoesNotSpin: an idle connection that a hook
// keeps rejecting stays acquirable. A throttled Get() must park and then dial,
// not loop on the idle re-check until a token frees up.
func TestDialRateLimitHookRejectedIdleDoesNotSpin(t *testing.T) {
	var dials atomic.Int32
	p := pool.NewConnPool(&pool.Options{
		Dialer:             countingDialer(&dials),
		PoolSize:           10,
		MaxConcurrentDials: 10,
		DialRateLimit:      1,
		DialRateBurst:      1,
		PoolTimeout:        200 * time.Millisecond,
	})
	defer p.Close()
	ctx := context.Background()

	c1, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("first Get failed: %v", err)
	}
	p.Put(ctx, c1)

	hook := &rejectIdleHook{}
	p.AddPoolHook(hook)

	c2, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("throttled Get failed: %v", err)
	}
	defer p.Put(ctx, c2)
	if c2 == c1 {
		t.Fatal("throttled Get returned the rejected idle conn")
	}
	if got := hook.rejected.Load(); got > 50 {
		t.Fatalf("idle conn rejected %d times: throttled Get spun on the idle re-check", got)
	}
}

// TestDialRateLimitRefillRechecksAfterTokenWait: a refill worker that waited
// for a dial token must not dial when the idle floor was met meanwhile.
func TestDialRateLimitRefillRechecksAfterTokenWait(t *testing.T) {
	var dials atomic.Int32
	p := pool.NewConnPool(&pool.Options{
		Dialer:             countingDialer(&dials),
		PoolSize:           10,
		MaxConcurrentDials: 10,
		MinIdleConns:       2,
		DialRateLimit:      1,
		DialRateBurst:      1,
		PoolTimeout:        200 * time.Millisecond,
	})
	defer p.Close()
	ctx := context.Background()

	// One refill worker takes the burst token; the other waits for a token.
	waitFor(t, time.Second, func() bool { return p.IdleLen() == 1 })

	// Meet the floor from the foreground: take the prewarmed conn, then a
	// throttled Get escapes after PoolTimeout and dials; return both.
	c1, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("Get 1 failed: %v", err)
	}
	c2, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("Get 2 failed: %v", err)
	}
	p.Put(ctx, c1)
	p.Put(ctx, c2)
	if got := p.IdleLen(); got != 2 {
		t.Fatalf("IdleLen=%d after Put, want 2", got)
	}

	// Let the waiting refill workers get their tokens.
	time.Sleep(2500 * time.Millisecond)
	if got := p.IdleLen(); got != 2 {
		t.Fatalf("IdleLen=%d, want 2 (MinIdleConns): refill dialed past the floor", got)
	}
	if got := dials.Load(); got != 2 {
		t.Fatalf("dials=%d, want 2", got)
	}
}

// TestDialRateLimitRefundsTokenWhenNoDial: a Get or TryGet that takes a dial
// token but never starts a dial (no dial permit) must return the token.
func TestDialRateLimitRefundsTokenWhenNoDial(t *testing.T) {
	var dials atomic.Int32
	gate := make(chan struct{})
	p := pool.NewConnPool(&pool.Options{
		Dialer: func(context.Context) (net.Conn, error) {
			dials.Add(1)
			<-gate
			return newDummyConn(), nil
		},
		PoolSize:           10,
		MaxConcurrentDials: 1,
		DialRateLimit:      1,
		DialRateBurst:      3,
		PoolTimeout:        100 * time.Millisecond,
	})
	defer p.Close()
	ctx := context.Background()

	// Get 1 takes a token and holds the only dial permit in a blocked dial.
	got1 := make(chan *pool.Conn, 1)
	go func() {
		cn, err := p.Get(ctx)
		if err != nil {
			t.Errorf("Get 1 failed: %v", err)
		}
		got1 <- cn
	}()
	waitFor(t, time.Second, func() bool { return dials.Load() == 1 })

	// TryGet takes a token, then finds no free dial permit.
	if _, err := p.TryGet(ctx); err != pool.ErrPoolTryFull {
		t.Fatalf("TryGet err=%v, want ErrPoolTryFull", err)
	}
	// Get takes a token, then times out waiting for a dial permit.
	cctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	_, err := p.Get(cctx)
	cancel()
	if err != context.DeadlineExceeded {
		t.Fatalf("Get err=%v, want context.DeadlineExceeded", err)
	}

	close(gate)
	c1 := <-got1
	defer p.Put(ctx, c1)

	// Both unused tokens are back: two more dials need no throttling.
	c2, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("Get 2 failed: %v", err)
	}
	defer p.Put(ctx, c2)
	c3, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("Get 3 failed: %v", err)
	}
	defer p.Put(ctx, c3)
	if st := p.Stats(); st.RateLimitedDials != 0 {
		t.Fatalf("RateLimitedDials=%d, want 0: unused tokens were not refunded", st.RateLimitedDials)
	}
}

// cancelOnIdleGetHook cancels the Get's context and rejects the idle conn.
type cancelOnIdleGetHook struct {
	cancel atomic.Pointer[context.CancelFunc]
}

func (h *cancelOnIdleGetHook) OnGet(_ context.Context, _ *pool.Conn, isNewConn bool) (bool, error) {
	if isNewConn {
		return true, nil
	}
	if c := h.cancel.Load(); c != nil {
		(*c)()
	}
	return false, nil
}

func (h *cancelOnIdleGetHook) OnPut(context.Context, *pool.Conn) (bool, bool, error) {
	return true, false, nil
}

func (h *cancelOnIdleGetHook) OnRemove(context.Context, *pool.Conn, error) {}

// TestDialRateLimitCanceledGetDoesNotDial: a Get canceled while throttled must
// return the context error, even when the throttle returns without parking
// (here the PoolTimeout deadline is already due), and must not dial.
func TestDialRateLimitCanceledGetDoesNotDial(t *testing.T) {
	var dials atomic.Int32
	p := pool.NewConnPool(&pool.Options{
		Dialer:             countingDialer(&dials),
		PoolSize:           10,
		MaxConcurrentDials: 10,
		DialRateLimit:      1,
		DialRateBurst:      1,
		PoolTimeout:        time.Nanosecond,
	})
	defer p.Close()
	ctx := context.Background()

	c1, err := p.Get(ctx)
	if err != nil {
		t.Fatalf("first Get failed: %v", err)
	}
	p.Put(ctx, c1)

	hook := &cancelOnIdleGetHook{}
	p.AddPoolHook(hook)

	for i := 0; i < 20; i++ {
		cctx, cancel := context.WithCancel(ctx)
		hook.cancel.Store(&cancel)
		cn, err := p.Get(cctx)
		cancel()
		if err == nil {
			p.Put(ctx, cn)
		}
		if err != context.Canceled {
			t.Fatalf("iteration %d: Get err=%v, want context.Canceled", i, err)
		}
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("dials=%d, want 1: a canceled Get dialed", got)
	}
}

// TestDialRateLimitRefundWakesParkedWaiter: a token refunded because a dial
// never started must wake a parked Get() so it can use the token, instead of
// sleeping until the next refill.
func TestDialRateLimitRefundWakesParkedWaiter(t *testing.T) {
	var dials atomic.Int32
	gate := make(chan struct{})
	p := pool.NewConnPool(&pool.Options{
		Dialer: func(context.Context) (net.Conn, error) {
			dials.Add(1)
			<-gate
			return newDummyConn(), nil
		},
		PoolSize:           10,
		MaxConcurrentDials: 1,
		DialRateLimit:      1,
		DialRateBurst:      2,
		PoolTimeout:        3 * time.Second,
	})
	defer p.Close()
	ctx := context.Background()

	type result struct {
		cn  *pool.Conn
		err error
	}
	// Get 1 takes token 1 and holds the only dial permit in a blocked dial.
	r1 := make(chan result, 1)
	go func() {
		cn, err := p.Get(ctx)
		r1 <- result{cn, err}
	}()
	waitFor(t, time.Second, func() bool { return dials.Load() == 1 })

	// Get 2 takes token 2 and waits for a dial permit until its ctx expires.
	r2 := make(chan error, 1)
	go func() {
		cctx, cancel := context.WithTimeout(ctx, 300*time.Millisecond)
		defer cancel()
		_, err := p.Get(cctx)
		r2 <- err
	}()
	waitFor(t, time.Second, func() bool { return p.DialTokens() < 1 })

	// Get 3 finds no token and parks until the next refill (~1s).
	r3 := make(chan result, 1)
	go func() {
		cn, err := p.Get(ctx)
		r3 <- result{cn, err}
	}()
	waitFor(t, time.Second, func() bool { return p.DialWaitQueueLen() == 1 })

	// Get 2 gives up and refunds its token: Get 3 must wake at once.
	if err := <-r2; err != context.DeadlineExceeded {
		t.Fatalf("Get 2 err=%v, want context.DeadlineExceeded", err)
	}
	waitFor(t, 200*time.Millisecond, func() bool { return p.DialWaitQueueLen() == 0 })

	close(gate)
	for _, ch := range []chan result{r1, r3} {
		r := <-ch
		if r.err != nil {
			t.Fatalf("Get failed: %v", r.err)
		}
		p.Put(ctx, r.cn)
	}
}

// TestDialRateLimitRefillRefundsOnPreDialFailure: a refill worker whose
// addIdleConn fails before dialing (MaxActiveConns reached) returns its token.
func TestDialRateLimitRefillRefundsOnPreDialFailure(t *testing.T) {
	var dials atomic.Int32
	p := pool.NewConnPool(&pool.Options{
		Dialer:             countingDialer(&dials),
		PoolSize:           10,
		MaxConcurrentDials: 10,
		MaxActiveConns:     1,
		DialRateLimit:      1,
		DialRateBurst:      2,
		PoolTimeout:        time.Second,
	})
	defer p.Close()
	ctx := context.Background()

	// A dedicated conn (not paced) fills MaxActiveConns.
	dedicated, err := p.NewConn(ctx)
	if err != nil {
		t.Fatalf("NewConn failed: %v", err)
	}
	defer p.RemoveWithoutTurn(ctx, dedicated, nil)

	// The refill worker takes a token, then reserveConn fails.
	p.RaiseMinIdleConns(1)
	time.Sleep(100 * time.Millisecond)
	if got := p.DialTokens(); got < 1.9 {
		t.Fatalf("DialTokens=%.2f, want 2: pre-dial refill failure kept its token", got)
	}
	if got := dials.Load(); got != 1 {
		t.Fatalf("dials=%d, want 1", got)
	}
}

// TestDialRateLimitRefundsOnDialFastFail: a Get whose dial fails fast because
// dial errors are saturated (no dial attempt) returns its token.
func TestDialRateLimitRefundsOnDialFastFail(t *testing.T) {
	var dials atomic.Int32
	p := pool.NewConnPool(&pool.Options{
		Dialer: func(context.Context) (net.Conn, error) {
			dials.Add(1)
			return nil, errors.New("dial failed")
		},
		PoolSize:           1,
		MaxConcurrentDials: 1,
		DialerRetries:      1,
		DialRateLimit:      1,
		DialRateBurst:      3,
		PoolTimeout:        time.Second,
	})
	defer p.Close()
	ctx := context.Background()

	// Get 1 takes a token and dials; the failure saturates dial errors.
	if _, err := p.Get(ctx); err == nil {
		t.Fatal("Get 1: want dial error")
	}
	// Get 2 takes a token but fails fast without dialing.
	if _, err := p.Get(ctx); err == nil {
		t.Fatal("Get 2: want dial error")
	}
	if got := p.DialTokens(); got < 1.9 {
		t.Fatalf("DialTokens=%.2f, want 2: fast-failed Get kept its token", got)
	}
}

// TestDialRateLimitHasFreeCapacityNeedsToken: with no idle conn and no dial
// token, a Get would park, so HasFreeCapacity must report no capacity.
func TestDialRateLimitHasFreeCapacityNeedsToken(t *testing.T) {
	p := pool.NewConnPool(&pool.Options{
		Dialer:             func(context.Context) (net.Conn, error) { return newDummyConn(), nil },
		PoolSize:           10,
		MaxConcurrentDials: 10,
		DialRateLimit:      1,
		DialRateBurst:      1,
		PoolTimeout:        3 * time.Second,
	})
	defer p.Close()
	if !p.HasFreeCapacity() {
		t.Fatal("HasFreeCapacity=false with a dial token available")
	}
	cn, err := p.Get(context.Background()) // takes the only token
	if err != nil {
		t.Fatal(err)
	}
	defer p.Put(context.Background(), cn)
	if p.HasFreeCapacity() {
		t.Fatal("HasFreeCapacity=true with no idle conn and no dial token: a Get would park")
	}
}
