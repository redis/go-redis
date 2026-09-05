package redis

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

// TestFullDuplexIdleInvalidation pins the feature-complete full-duplex engine's
// idle push drain, WITH a negative control.
//
// The engine holds one connection out of the pool, so the background CSC drainer
// never visits it; the session's reader must drain server-initiated pushes even
// when no reply is pending, or an invalidation arriving while the coalescer is
// idle is never processed and the entry serves stale until TTL.
//
//   - idle_drain_on_converges: with the drain at its default 5ms cadence, a key
//     mutated from a second client while the coalescer is idle must converge —
//     only possible if the idle reader processed the invalidation (a cache hit is
//     served locally without touching the socket).
//   - idle_drain_off_stays_stale: the NEGATIVE CONTROL. With the idle drain
//     effectively disabled (probe set to ~1h), the same sequence must NOT
//     converge. If it does, some other path drains the held connection and the
//     positive case would prove nothing.
func TestFullDuplexIdleInvalidation(t *testing.T) {
	probe := NewClient(&Options{Addr: internalTestRedisAddr(), Protocol: 3})
	if err := probe.Ping(context.Background()).Err(); err != nil {
		probe.Close()
		t.Skipf("no redis: %v", err)
	}
	probe.Close()

	t.Run("idle_drain_on_converges", func(t *testing.T) {
		if !fdIdleConverges(t, "fdidle:on") {
			t.Fatal("with the idle drain ON, an invalidation on the held connection " +
				"was never processed: the entry served stale")
		}
	})

	t.Run("idle_drain_off_stays_stale", func(t *testing.T) {
		old := cscFullDuplexIdleProbe
		cscFullDuplexIdleProbe = time.Hour // idle drain effectively disabled
		t.Cleanup(func() { cscFullDuplexIdleProbe = old })
		if fdIdleConverges(t, "fdidle:off") {
			t.Fatal("NEGATIVE CONTROL FAILED: with the idle drain disabled the key still " +
				"converged, so something other than the idle reader drains the held " +
				"connection — the positive case does not prove the idle-drain fix")
		}
	})
}

// fdIdleConverges warms a key through the full-duplex coalescer, lets the
// coalescer go idle, mutates the key from a second client, and reports whether
// the caching client converges to the new value within ~0.8s.
func fdIdleConverges(t *testing.T, key string) bool {
	t.Helper()
	ctx := context.Background()

	cached := NewClient(&Options{
		Addr:                          internalTestRedisAddr(),
		Protocol:                      3,
		PoolSize:                      4,
		ClientSideCacheConfig:         &ClientSideCacheConfig{MaxEntries: 1000},
		ClientSideCacheCoalesceMisses: true,
	})
	seeder := NewClient(&Options{Addr: internalTestRedisAddr(), Protocol: 3})
	defer cached.Close()
	defer seeder.Close()
	if err := cached.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if cached.cscMissCoalescer.Load() == nil {
		t.Fatal("coalescer not active (config not honored?)")
	}
	t.Cleanup(func() { seeder.Del(context.Background(), key) })

	if err := seeder.Set(ctx, key, "v1", 0).Err(); err != nil {
		t.Fatal(err)
	}
	// Warm through the coalescer: the miss is fetched on the held connection and
	// publishes the entry tracked on that connection.
	if got, err := cached.Get(ctx, key).Result(); err != nil || got != "v1" {
		t.Fatalf("warm read = %q, %v; want v1", got, err)
	}
	if got, err := cached.Get(ctx, key).Result(); err != nil || got != "v1" {
		t.Fatalf("second read = %q, %v; want v1 (should be a local hit)", got, err)
	}
	if mcv := cached.cscMissCoalescer.Load(); mcv == nil || mcv.batches.Load() == 0 {
		t.Fatal("coalescer never batched; the warm read did not route through it")
	}

	// Go idle, then invalidate from the second client. The push arrives on the
	// held connection; only the session's reader can process it.
	time.Sleep(60 * time.Millisecond)
	if err := seeder.Set(ctx, key, "v2", 0).Err(); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(800 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got, _ := cached.Get(ctx, key).Result(); got == "v2" {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

// TestFullDuplexSessionReleasesIdleConn pins #3965: a full-duplex session must
// return its held connection after the idle grace instead of sitting on it until
// the 30s recycle age — at PoolSize:1 a non-cacheable command (PING) would
// otherwise block behind the idle session until PoolTimeout and fail.
func TestFullDuplexSessionReleasesIdleConn(t *testing.T) {
	ctx := context.Background()
	probe := NewClient(&Options{Addr: internalTestRedisAddr(), Protocol: 3})
	if err := probe.Ping(ctx).Err(); err != nil {
		probe.Close()
		t.Skipf("no redis: %v", err)
	}
	probe.Close()

	old := cscFullDuplexSessionIdle
	cscFullDuplexSessionIdle = 50 * time.Millisecond
	t.Cleanup(func() { cscFullDuplexSessionIdle = old })

	cached := NewClient(&Options{
		Addr:                          internalTestRedisAddr(),
		Protocol:                      3,
		PoolSize:                      1, // the FD session's held conn is the ONLY conn
		PoolTimeout:                   2 * time.Second,
		ClientSideCacheConfig:         &ClientSideCacheConfig{MaxEntries: 100},
		ClientSideCacheCoalesceMisses: true,
	})
	seeder := NewClient(&Options{Addr: internalTestRedisAddr(), Protocol: 3})
	defer cached.Close()
	defer seeder.Close()

	key := "fdidlerel:k"
	if err := seeder.Set(ctx, key, "v", 0).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { seeder.Del(context.Background(), key) })
	// Route a miss through the FD session so it acquires the pool's only conn.
	if got, err := cached.Get(ctx, key).Result(); err != nil || got != "v" {
		t.Fatalf("warm read = %q, %v", got, err)
	}

	// Past the grace the session must have released the conn; a non-cacheable
	// PING then completes promptly. Without the release it blocks the full
	// PoolTimeout and fails (the session held the conn toward the 30s recycle).
	time.Sleep(200 * time.Millisecond)
	start := time.Now()
	if err := cached.Ping(ctx).Err(); err != nil {
		t.Fatalf("PING after idle grace failed: %v — the idle FD session is still holding the pool's only conn", err)
	}
	if d := time.Since(start); d > time.Second {
		t.Fatalf("PING took %v; want prompt (idle session should have released the conn)", d)
	}
}

// TestClassifyCachedReply pins the abandoned-path reply classifier: it must agree
// with isCacheableReplyResult on the error-vs-value axis (value and nil are
// cacheable, a top-level RESP error is not) WITHOUT a caller command to populate.
// This is what lets applyAndSettle still publish an abandoned request's reply to
// the shared cache while skipping the caller's now-returned Cmder (#3965 :57).
func TestClassifyCachedReply(t *testing.T) {
	cases := []struct {
		name      string
		raw       string
		cacheable bool
	}{
		{"bulk string", "$2\r\nOK\r\n", true},
		{"integer", ":5\r\n", true},
		{"resp2 nil", "$-1\r\n", true},
		{"resp3 null", "_\r\n", true},
		{"array", "*2\r\n$1\r\na\r\n$1\r\nb\r\n", true},
		{"redis error", "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := isCacheableReplyResult(classifyCachedReply([]byte(tc.raw)))
			if got != tc.cacheable {
				t.Fatalf("classifyCachedReply(%q) cacheable = %v; want %v", tc.raw, got, tc.cacheable)
			}
		})
	}
}

// TestCSCMissReqClaimInterlock pins the cmd-ownership CAS used by the abandoned
// path (#3965 :57): exactly one of the caller (claimAbandon) and the
// worker/reader (claimApply) may win from the pending state, so the worker never
// writes the caller's Cmder after the caller has returned and may be reusing it.
func TestCSCMissReqClaimInterlock(t *testing.T) {
	// Caller wins: a later apply claim must lose, so the worker skips the write.
	r := &cscMissReq{}
	if !r.claimAbandon() {
		t.Fatal("first claimAbandon must win from pending")
	}
	if r.claimApply() {
		t.Fatal("claimApply must lose after the caller abandoned")
	}
	if r.claimAbandon() {
		t.Fatal("claimAbandon must be idempotent-losing on repeat")
	}

	// Worker wins: a later abandon claim must lose, so the caller waits.
	w := &cscMissReq{}
	if !w.claimApply() {
		t.Fatal("first claimApply must win from pending")
	}
	if w.claimAbandon() {
		t.Fatal("claimAbandon must lose after the worker started applying")
	}

	// Concurrent: exactly one side wins.
	for i := 0; i < 2000; i++ {
		rr := &cscMissReq{}
		var abandon, apply bool
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); abandon = rr.claimAbandon() }()
		go func() { defer wg.Done(); apply = rr.claimApply() }()
		wg.Wait()
		if abandon == apply {
			t.Fatalf("iter %d: exactly one claim must win, got abandon=%v apply=%v", i, abandon, apply)
		}
	}
}

// TestCSCMissWireSnapshotImmuneToArgMutation pins the fix for the abandoned-
// args race: the wire form is snapshotted at enqueue while the caller owns
// cmd, so a caller that abandons and then mutates a []byte arg can neither
// change what goes on the wire nor cause a reply for the mutated key to be
// published under the original cache key.
func TestCSCMissWireSnapshotImmuneToArgMutation(t *testing.T) {
	key := []byte("orig-key")
	cmd := NewStringCmd(context.Background(), "get", key)

	var wireBuf bytes.Buffer
	if err := writeCmd(proto.NewWriter(&wireBuf), cmd); err != nil {
		t.Fatalf("snapshot encode: %v", err)
	}
	wire := wireBuf.Bytes()
	if !bytes.Contains(wire, []byte("orig-key")) {
		t.Fatalf("snapshot must contain the original key, got %q", wire)
	}

	copy(key, "MUTATED!") // caller reuses its buffer after abandoning

	if !bytes.Contains(wire, []byte("orig-key")) || bytes.Contains(wire, []byte("MUTATED!")) {
		t.Fatalf("wire snapshot changed after caller mutated its arg buffer: %q", wire)
	}
}

// TestCSCMissCoalesceAbandonedFetchNoRace is a -race guard for #3965 :57: a caller
// whose context cancels mid-fetch returns and reads its Cmder while the coalescer
// may still be settling the reply. The claim interlock must keep the worker from
// writing the caller's Cmder after it returned. Probabilistic (timing-dependent),
// so its value is under `go test -race`; a regression that drops the interlock
// trips the detector.
func TestCSCMissCoalesceAbandonedFetchNoRace(t *testing.T) {
	probe := NewClient(&Options{Addr: internalTestRedisAddr(), Protocol: 3})
	if err := probe.Ping(context.Background()).Err(); err != nil {
		probe.Close()
		t.Skipf("no redis: %v", err)
	}
	probe.Close()

	cached := NewClient(&Options{
		Addr:                          internalTestRedisAddr(),
		Protocol:                      3,
		PoolSize:                      4,
		ClientSideCacheConfig:         &ClientSideCacheConfig{MaxEntries: 100000},
		ClientSideCacheCoalesceMisses: true,
	})
	seeder := NewClient(&Options{Addr: internalTestRedisAddr(), Protocol: 3})
	defer cached.Close()
	defer seeder.Close()

	const N = 400
	keys := make([]string, N)
	for i := range keys {
		keys[i] = "cscabandon:" + strconv.Itoa(i)
		if err := seeder.Set(context.Background(), keys[i], "v", 0).Err(); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	t.Cleanup(func() {
		for _, k := range keys {
			seeder.Del(context.Background(), k)
		}
	})

	var wg sync.WaitGroup
	for i, k := range keys {
		wg.Add(1)
		go func(k string, i int) {
			defer wg.Done()
			// Non-zero so ctx.Err() is nil at entry (the request reaches the
			// coalescer) but small enough that many cancel while the fetch is in
			// flight — the window the interlock guards.
			d := time.Duration(40+(i%24)*20) * time.Microsecond
			ctx, cancel := context.WithTimeout(context.Background(), d)
			defer cancel()
			cmd := cached.Get(ctx, k)
			// Touch the cmd right after return; a worker writing it concurrently
			// would be a data race.
			_, _ = cmd.Result()
		}(k, i)
	}
	wg.Wait()

	// Guard against a silently-empty run: if no fetch was abandoned mid-flight the
	// interlock window was never exercised, so a green -race proves nothing. Skip
	// (don't pass) so the gap is visible. When it does land, this is the neutralize
	// anchor — reverting claimApply to an unconditional write makes -race fire here.
	if n := cached.cscMissCoalescer.Load().abandonedApplies.Load(); n == 0 {
		t.Skip("timing window never landed: no fetch was abandoned mid-flight; " +
			"interlock not exercised this run")
	}
}

// TestFullDuplexDisabledMidMissRetriesUncached pins #3965 :188: when CSC serving
// is disabled after a miss is reserved (a RESP3 downgrade / CLIENT TRACKING
// rejection), the full-duplex session must NOT surface pool.ErrClosed for a valid
// cacheable read. It settles the reserved miss with the retry-uncached sentinel
// (so processCached re-runs it on the normal path) and releases the reservation.
func TestFullDuplexDisabledMidMissRetriesUncached(t *testing.T) {
	ctx := context.Background()
	probe := NewClient(&Options{Addr: internalTestRedisAddr(), Protocol: 3})
	if err := probe.Ping(ctx).Err(); err != nil {
		probe.Close()
		t.Skipf("no redis: %v", err)
	}
	probe.Close()

	cached := NewClient(&Options{
		Addr:                          internalTestRedisAddr(),
		Protocol:                      3,
		PoolSize:                      4,
		ClientSideCacheConfig:         &ClientSideCacheConfig{MaxEntries: 1000},
		ClientSideCacheCoalesceMisses: true,
	})
	seeder := NewClient(&Options{Addr: internalTestRedisAddr(), Protocol: 3})
	defer cached.Close()
	defer seeder.Close()
	if err := cached.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if cached.cscMissCoalescer.Load() == nil {
		t.Fatal("coalescer not active")
	}

	key := "fdmiss:disabled"
	if err := seeder.Set(ctx, key, "hello", 0).Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { seeder.Del(context.Background(), key) })

	// Reserve the miss exactly as processCached would (namespaced), then disable CSC
	// serving before handing it to the session — so the session's pre-start check
	// sees serving off.
	nsKey := cscNamespacedKey(cached.cscKeyPrefix, key)
	token, shouldFetch := cached.csc.Reserve(nsKey, []string{nsKey})
	if !shouldFetch {
		t.Fatal("expected to own the reservation")
	}
	// Capture the coalescer BEFORE disabling: disableCSCServing signals the drainer,
	// whose teardown asynchronously swaps cscMissCoalescer to nil. A scheduler delay
	// between the two would make a live cscMissCoalescer.Load() below return nil and
	// nil-deref. The captured pointer still honors the serving-off flag fetch checks.
	mc := cached.cscMissCoalescer.Load()
	if mc == nil {
		t.Fatal("coalescer not active")
	}
	cached.disableCSCServing(ctx, "test: force retry-uncached path")

	cmd := NewStringCmd(ctx, "get", key)
	if _, err := mc.fetch(ctx, cmd, nsKey, token); err != errCSCRetryUncached {
		t.Fatalf("fetch after CSC disabled = %v; want errCSCRetryUncached (should not surface ErrClosed)", err)
	}
	// The reservation must be released, or later readers block IN_PROGRESS until
	// StaleTimeout: a fresh Reserve must again own the fetch.
	if _, again := cached.csc.Reserve(nsKey, []string{nsKey}); !again {
		t.Fatal("reservation left IN_PROGRESS after retry-uncached settle")
	}
}

// TestGrabIntoBoundsBatchToWriteBuffer pins the write-buffer bound on coalesced
// batches (Ofek review #3989). A request that would push the batch past
// maxBatchBytes is returned as the carry, not packed on. So bufio does not flush
// in the middle of a batch while the reader waits, which could deadlock the write
// and the read on large payloads. The first request always goes. The carry stays
// in the writer's own state, not on mc.ch, so a shutdown cannot strand it.
func TestGrabIntoBoundsBatchToWriteBuffer(t *testing.T) {
	mc := &cscMissCoalescer{
		ch:            make(chan *cscMissReq, 8),
		maxBatchBytes: 100,
	}
	first := &cscMissReq{wire: make([]byte, 40)}
	small := &cscMissReq{wire: make([]byte, 40)} // 40+40=80 <= 100: fits
	big := &cscMissReq{wire: make([]byte, 80)}   // 80 more would overshoot 100: carried
	mc.ch <- small
	mc.ch <- big

	batch, carry := mc.grabInto(nil, first)
	if len(batch) != 2 || batch[0] != first || batch[1] != small {
		t.Fatalf("batch = %d reqs, want [first, small] within the byte cap", len(batch))
	}
	if carry != big {
		t.Fatalf("carry = %p, want the overshooting request %p", carry, big)
	}
	// The carry must not be put back on mc.ch.
	select {
	case <-mc.ch:
		t.Fatal("overshooting request was put back on mc.ch instead of carried")
	default:
	}
}

// TestSettleErrTagsTransportFailureForRetry checks the retry path (Ofek review
// #3989). settleErr tags a coalescer session or transport failure as
// cscSessionError, so processCached re-runs it on the normal path with MaxRetries
// instead of a raw io.EOF. The retry-uncached sentinel stays matchable by
// identity. A command reply stays untagged and is returned as-is.
func TestSettleErrTagsTransportFailureForRetry(t *testing.T) {
	mc := &cscMissCoalescer{c: &baseClient{csc: NewLocalCache(CacheConfig{MaxEntries: 4})}}

	// A transport failure is tagged and still unwraps to the cause.
	req := &cscMissReq{cacheKey: "k", done: make(chan error, 1)}
	mc.settleErr(req, io.EOF)
	got := <-req.done
	var se cscSessionError
	if !errors.As(got, &se) {
		t.Fatalf("settleErr(io.EOF) = %T, want cscSessionError so the caller re-runs", got)
	}
	if !errors.Is(got, io.EOF) {
		t.Fatal("cscSessionError must unwrap to the underlying cause")
	}

	// The retry-uncached sentinel is matched by identity, not tagged.
	req2 := &cscMissReq{cacheKey: "k", done: make(chan error, 1)}
	mc.settleErr(req2, errCSCRetryUncached)
	if got := <-req2.done; got != errCSCRetryUncached {
		t.Fatalf("settleErr(sentinel) = %v, want errCSCRetryUncached unwrapped", got)
	}

	// A command reply (redis.Nil) applied by applyAndSettle is NOT a session error:
	// processCached returns it as-is, keeping the no-per-command-retry tradeoff.
	if errors.As(error(Nil), &se) {
		t.Fatal("redis.Nil must not classify as a session error")
	}
}

// TestGrabIntoAlwaysIncludesFirst: a lone first larger than the cap still goes
// (latency-first), and anything queued behind it is carried.
func TestGrabIntoAlwaysIncludesFirst(t *testing.T) {
	mc := &cscMissCoalescer{
		ch:            make(chan *cscMissReq, 8),
		maxBatchBytes: 100,
	}
	first := &cscMissReq{wire: make([]byte, 200)} // alone exceeds the cap
	more := &cscMissReq{wire: make([]byte, 10)}
	mc.ch <- more

	batch, carry := mc.grabInto(nil, first)
	if len(batch) != 1 || batch[0] != first {
		t.Fatalf("batch = %d reqs, want just the oversized first", len(batch))
	}
	if carry != more {
		t.Fatalf("carry = %p, want the deferred request %p", carry, more)
	}
}

// recordCloseConn records whether Close was called (drainBackstopRun force-closes the
// conn when the stopping-drain budget is spent).
type recordCloseConn struct {
	mockNetConn
	closed atomic.Bool
}

func (c *recordCloseConn) Close() error { c.closed.Store(true); return nil }

// TestCSCMissStopDrainCapsTotalOnTrickle pins the P1 (csc_miss_coalesce_modes.go): on the
// stopping (Close) path, drainBackstopRun must not renew the drain forever when the server
// trickles one reply per interval. Without a total cap the per-interval progress check
// renews indefinitely — with up to cscFullDuplexDepth in-flight, Close blocks in wg.Wait
// for hours. With the cap it force-closes the conn and returns once the stopping budget is
// spent, EVEN WHILE readsDone keeps advancing.
//
// stopDrainBudget is set below one interval so the FIRST timer (interval is floored at
// batchBudget()+1s >= 6s) already sees the budget spent — exercising the cap in bounded
// time instead of the ~48s (8 × interval) production default.
//
// Red-check: remove ONLY the stopDeadline ARMING in drainBackstopRun (leave the comparison)
// — the trickle keeps progress alive, the drain never returns, and the watchdog fires.
func TestCSCMissStopDrainCapsTotalOnTrickle(t *testing.T) {
	nc := &recordCloseConn{mockNetConn: mockNetConn{addr: ":6379"}}
	cn := pool.NewConn(nc)
	mc := &cscMissCoalescer{
		c:               &baseClient{opt: &Options{}}, // Read/WriteTimeout 0 -> batchBudget floor 5s -> interval 6s
		stop:            make(chan struct{}),
		stopDrainBudget: 50 * time.Millisecond, // below one interval: the first timer sees it spent
	}

	var readsDone atomic.Uint64
	superDone := make(chan struct{}) // never closed: force the timer path, not a clean finish
	trickleStop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		tk := time.NewTicker(200 * time.Millisecond)
		defer tk.Stop()
		for {
			select {
			case <-trickleStop:
				return
			case <-tk.C:
				readsDone.Add(1) // steady progress that OUTLIVES the first interval
			}
		}
	}()

	done := make(chan struct{})
	go func() {
		mc.drainBackstopRun(cn, &readsDone, superDone, true)
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		close(trickleStop)
		wg.Wait()
		t.Fatal("drainBackstopRun did not return: the stopping drain renewed forever despite the total cap (arming missing?)")
	}
	close(trickleStop)
	wg.Wait()

	if !nc.closed.Load() {
		t.Fatal("connection was not force-closed when the stopping-drain budget was spent")
	}
	// Prove the CAP fired (progress branch), not the zero-progress branch: the trickle
	// bumped across the first ~6s interval, so readsDone advanced before the drain returned.
	// The zero-progress branch requires an interval with NO bump, which the trickle prevents.
	if got := readsDone.Load(); got < 2 {
		t.Fatalf("readsDone = %d; want >= 2 (the trickle must outlive the first interval so the CAP fires, not zero-progress)", got)
	}
}
