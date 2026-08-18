package redis

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

// errCSCRetryUncached settles a coalesced miss when the coalescer bows out for a
// reason unrelated to the command itself: CSC serving was disabled after the
// miss was queued (a RESP3 downgrade, or CLIENT TRACKING rejected during a
// connection re-init), so this session would fetch on an untracked conn or none
// at all. It never reaches the caller — processCached catches it and re-runs the
// command uncached, so a valid cacheable read is not failed with a spurious
// pool.ErrClosed. The reservation is always cancelled before this is settled, so
// no waiter is left IN_PROGRESS.
var errCSCRetryUncached = errors.New("redis: csc miss-coalescer disabled mid-fetch; retry uncached")

// Reader-miss coalescing, behind Options.ClientSideCacheCoalesceMisses.
//
// The two-client CSC shape serves cache HITS locally (no connection) but every
// MISS fetches on the caching client's MAIN pool, one command per connection.
// At a small pool that is the wall: N concurrent misses contend one GET at a
// time (measured: at CSC pool 8 the reader misses alone saturate the pool, p99
// ~420ms at 64 workers, and no refresh policy helps — the readers are the
// bottleneck).
//
// This streams each miss's ORIGINAL command onto a held tracked (main-pool)
// full-duplex connection (see csc_miss_coalesce_modes.go), reads the replies
// back in order, and applies each to its caller's command AND to the shared
// cache. Individual pipelined commands (not MGET), so it is cluster-safe and
// works for any read shape, not just string GET. Misses are CALLER-BLOCKING, so
// the engine is latency-first: a lone miss is written immediately (batching only
// packs what is already queued, never waits for more), and new misses stream out
// while earlier replies are still in flight (~1 RTT per miss, no batch
// phase-lock, no pool Get on the hot path).
//
// Being cache-aware is why it cannot just ride the ordinary autopipeliner: it
// must Reserve/fulfilCached each key so the entry is tracked and
// single-flighted. The caller Reserves in processCached (only shouldFetch==true
// misses reach here); the engine owns the token from that point and settles it —
// fulfilled or cancelled — before waking the caller, or a dangling IN_PROGRESS
// entry blocks every reader of that key for StaleTimeout.
//
// Tradeoff, same as the autopipeline path: a coalesced miss loses per-command
// MaxRetries/backoff/LOADING handling.

const (
	cscMissBatchMax   = 128
	cscMissQueueDepth = 4096
	// cscFullDuplexDepth caps in-flight commands on the single-connection
	// full-duplex engine (flow control for the writer).
	cscFullDuplexDepth = 4096
)

// cscCoalesceMissesEnabled is read once, at coalescer construction (per client).
func cscCoalesceMissesEnabled(opt *Options) bool {
	return opt != nil && opt.ClientSideCacheCoalesceMisses
}

// cscReq* are the states of cscMissReq.apply, the single-word interlock over who
// owns req.cmd: the fetching caller or the reader applying the reply. Exactly
// one side wins the pending->X CAS, so the reader never writes cmd after the
// caller has returned and may be reusing it.
const (
	cscReqPending   uint32 = iota // no one has claimed cmd yet
	cscReqAbandoned               // caller's ctx (or Close) fired first: cmd is the caller's again
	cscReqApplying                // reader claimed cmd first: it will write and settle
)

// cscMissReq is one caller waiting for its missed key. done carries the fetch
// result back (the reply is already applied to cmd by the time done fires).
type cscMissReq struct {
	cmd      Cmder
	cacheKey string
	token    uint64
	done     chan error
	// servedBy is the session connection that served (or failed) this request,
	// set by the engine BEFORE settling done (the channel receive is the
	// happens-before edge for the caller's read). Feeds the native OTel
	// recorder: processCached stamps it into processState so a coalesced miss
	// reports its serving connection and an attempt, like any other command.
	// Nil when the request never reached a session (enqueue reject, abandon).
	servedBy *pool.Conn
	// wire is cmd's RESP encoding, snapshotted at enqueue while the caller
	// still owned cmd. The session writer writes ONLY these engine-owned bytes
	// — it never reads cmd — so an abandoning caller may reuse mutable args
	// (e.g. a []byte key) the moment fetch returns, and a post-abandon
	// mutation can neither reach the wire nor publish under the original
	// cache key. Only the reply side touches cmd, gated by the apply interlock.
	wire []byte
	// apply interlocks ownership of cmd between the fetching caller and the
	// reader. The caller abandons (CAS pending->abandoned) if its context is
	// cancelled — or Close races its enqueue — before a reply is applied; the
	// reader claims (CAS pending->applying) before it writes the reply into cmd. A
	// plain flag would still race: the reader could read "not abandoned", begin
	// writing cmd, and the caller cancel mid-write. Either way the reader still
	// settles the token and publishes to the shared cache (the fetch is never
	// wasted) — only the cmd write is gated.
	apply atomic.Uint32
}

// claimAbandon is the caller's side of the cmd interlock: it succeeds only if no
// reply is being applied, in which case the reader skips the cmd write.
func (r *cscMissReq) claimAbandon() bool {
	return r.apply.CompareAndSwap(cscReqPending, cscReqAbandoned)
}

// claimApply is the reader's side: it succeeds only if the caller has not
// abandoned, in which case it is safe to write the caller's cmd.
func (r *cscMissReq) claimApply() bool {
	return r.apply.CompareAndSwap(cscReqPending, cscReqApplying)
}

type cscMissCoalescer struct {
	c        *baseClient
	ch       chan *cscMissReq
	stop     chan struct{}
	stopOnce sync.Once // guards close(stop): Close and the GC cleanup can both stop
	wg       sync.WaitGroup

	batched    atomic.Uint64 // reqs that went through a batch
	batches    atomic.Uint64 // batches flushed
	failed     atomic.Uint64 // reqs settled as errors (conn failure)
	maxBatchSz atomic.Uint64
	// abandonedApplies counts replies whose caller had already abandoned the req,
	// so the cmd write was skipped. Read only by the -race abandoned-path test, to
	// assert it hit the window it guards.
	abandonedApplies atomic.Uint64
}

// startCSCMissCoalescer launches the coalescer sessions. No-op unless
// Options.ClientSideCacheCoalesceMisses is set and CSC is active, and — see the
// option's GoDoc — unless the cache is the built-in *LocalCache: the publish path
// (fulfillCached capture, refresh integration, hot-entry collection) is
// LocalCache-specific, so a custom Cache falls back to per-caller fetches.
func (c *baseClient) startCSCMissCoalescer() {
	if !cscCoalesceMissesEnabled(c.opt) || c.cscMissCoalescer.Load() != nil {
		return
	}
	if _, ok := c.csc.(*LocalCache); !ok {
		return
	}
	mc := &cscMissCoalescer{
		c:    c,
		ch:   make(chan *cscMissReq, cscMissQueueDepth),
		stop: make(chan struct{}),
	}
	c.cscMissCoalescer.Store(mc)
	// N independent full-duplex sessions, each holding its own connection and
	// pulling misses from the shared queue. Order-free: coalesced misses are
	// standalone per-key fetches with no cross-request contract, and each
	// session's per-conn reader still matches replies to its own requests.
	for i := 0; i < cscFullDuplexConnsDefault; i++ {
		mc.wg.Add(1)
		go mc.fullDuplexLoop()
	}
}

func (c *baseClient) stopCSCMissCoalescer() {
	// Swap, not load-then-nil: exactly one caller wins even if Close races the
	// GC cleanup path, so mc.stop is closed once.
	mc := c.cscMissCoalescer.Swap(nil)
	if mc == nil {
		return
	}
	mc.stopWorkers()
	mc.wg.Wait()
	// Any request still queued after stop is drained and settled so no caller
	// hangs on its done channel. Retry-uncached, matching fetch's stop paths:
	// teardown deactivates serving before stopping the coalescer, so a caller
	// woken here re-runs its read on the (possibly still open) pool instead of
	// surfacing a spurious ErrClosed mid-window; on a truly closed client the
	// uncached re-run fails with the real error.
	for {
		select {
		case req := <-mc.ch:
			mc.c.csc.Cancel(req.cacheKey, req.token)
			req.done <- errCSCRetryUncached
		default:
			return
		}
	}
}

// stopWorkers signals every coalescer goroutine to exit; idempotent, does not
// wait. Called by stopCSCMissCoalescer (Close) and by the GC cleanup for a client
// dropped without Close — the sessions retain the base client (cache, pools), so
// leaving them running leaks all of it per forgotten client.
func (mc *cscMissCoalescer) stopWorkers() {
	mc.stopOnce.Do(func() { close(mc.stop) })
}

// fetch hands a reserved miss to the coalescer and waits for the result. The
// caller has already Reserved (shouldFetch==true); the coalescer owns the token
// from here. Honors the caller's context: if it cancels while waiting, the
// session still settles the token and populates the cache (the fetch is not
// wasted), the caller just returns early.
//
// served is the session connection that produced the settle (nil when the
// request never reached one); processCached stamps it into processState so the
// native OTel recorder attributes the miss like any other command.
func (mc *cscMissCoalescer) fetch(ctx context.Context, cmd Cmder, cacheKey string, token uint64) (served *pool.Conn, err error) {
	req := &cscMissReq{cmd: cmd, cacheKey: cacheKey, token: token, done: make(chan error, 1)}
	// Snapshot the wire form NOW, while the caller still owns cmd: the session
	// writer writes these engine-owned bytes and never reads cmd again, so an
	// abandoning caller can immediately reuse mutable args (e.g. a []byte key)
	// without racing arg serialization, a mutated key can never be sent or
	// published under the original cache key, and the fetch always completes —
	// the reservation always settles. The reply side still claims cmd (apply
	// interlock) before writing the result into it.
	var wireBuf bytes.Buffer
	if err := writeCmd(proto.NewWriter(&wireBuf), cmd); err != nil {
		mc.c.csc.Cancel(cacheKey, token)
		return nil, err
	}
	req.wire = wireBuf.Bytes()
	// Reject early if the coalescer is already stopping, so a send does not win the
	// select race against a closed mc.stop and land in mc.ch after the shutdown
	// drain, where nothing would pick it up. This narrows but cannot close that
	// race; the mc.stop case in the wait select below is what guarantees the caller
	// never hangs on a post-drain req.
	select {
	case <-mc.stop:
		// Retry-uncached, not ErrClosed: the coalescer stopping does not mean
		// the CLIENT is closed (teardown deactivates serving before stopping
		// the coalescer, and a clone can race that window) — the uncached
		// re-run either succeeds on the open pool or surfaces the real error.
		mc.c.csc.Cancel(cacheKey, token)
		return nil, errCSCRetryUncached
	default:
	}
	select {
	case mc.ch <- req:
	case <-mc.stop:
		mc.c.csc.Cancel(cacheKey, token)
		return nil, errCSCRetryUncached
	case <-ctx.Done():
		mc.c.csc.Cancel(cacheKey, token)
		return nil, ctx.Err()
	}
	select {
	case err := <-req.done:
		return req.servedBy, err
	case <-mc.stop:
		// Close raced our enqueue: the req may have landed in mc.ch after the
		// shutdown drain, so nothing will settle req.done. Winning the interlock
		// means no reply is being applied — cancel the reservation and return instead
		// of hanging (a duplicate Cancel, if the drain also got this req, is a no-op
		// on a settled token).
		if req.claimAbandon() {
			mc.c.csc.Cancel(cacheKey, token)
			// The req may be sitting in mc.ch AFTER the shutdown drain ran, where
			// no worker will ever dequeue it — a live WithTimeout clone's pointer
			// would then retain it (and its Cmder) indefinitely. Re-run the
			// non-blocking drain to empty the queue; settling our own abandoned
			// req is harmless (done is buffered, the duplicate Cancel is a no-op).
			mc.drainQueueErr(errCSCRetryUncached)
			return nil, errCSCRetryUncached
		}
		// The reader claimed cmd and is mid-apply: wait so we do not read/reuse
		// cmd concurrently (the receive is a happens-before edge), then return
		// the settle result itself — the reply may have been applied
		// successfully, and discarding it for a blanket ErrClosed would fail a
		// read that has its value.
		e := <-req.done
		return req.servedBy, e
	case <-ctx.Done():
		// Caller stopped waiting. If we win the interlock the reader skips the cmd
		// write but still settles the token and publishes to the cache; if it already
		// claimed cmd, wait rather than race by reusing cmd. Either way return the
		// context error, matching the non-coalesced path (processWithRetry), so a
		// cancelled Get never returns a value depending on who won the CAS.
		//
		// Report the cancellation metric here, like processWithRetry: the reader may
		// still complete the background fetch (a success via applyAndSettle, or its
		// own error via settleErr), so neither of those records THIS caller's
		// context cancellation — without this the cancellation rate is undercounted
		// whenever miss coalescing is enabled. Attribute against the caller's ctx.
		//
		// Pass a nil conn deliberately: req.servedBy is written by the session and
		// only safe to read AFTER the req.done receive (that receive is the
		// happens-before edge, see the field doc), which has NOT happened on this
		// branch — reading it here would race the engine's write and, on a
		// cancellation that beats the serve, be nil anyway. The metric recorder
		// treats a nil conn as "no peer attributes", matching processWithRetry when
		// it cancels before a connection is obtained.
		if errorCallback := pool.GetMetricErrorCallback(); errorCallback != nil {
			errorType, statusCode, isInternal := classifyCommandError(ctx.Err())
			errorCallback(ctx, errorType, nil, statusCode, isInternal, 0)
		}
		if !req.claimAbandon() {
			<-req.done
			return req.servedBy, ctx.Err()
		}
		return nil, ctx.Err()
	}
}

// applyAndSettle applies raw to the caller's command, publishes to the cache when
// cacheable (under the reading connection's tracking generation), and wakes the
// caller. Sends req.done exactly once.
//
// It claims the cmd interlock first: an abandoned caller (context cancelled, or
// Close raced its enqueue) has its Cmder back and may be reading or reusing it,
// so the reply must NOT be written there. The fetch is still not wasted — the
// reply is classified from a throwaway parse and, when cacheable, published for
// the next reader; only the caller's cmd write is skipped.
func (mc *cscMissCoalescer) applyAndSettle(req *cscMissReq, raw []byte, connID, capturedGen uint64) {
	c := mc.c
	var applyErr error
	if req.claimApply() {
		applyErr = applyCachedReply(req.cmd, raw)
	} else {
		// Abandoned: classify without touching the caller's cmd.
		applyErr = classifyCachedReply(raw)
		mc.abandonedApplies.Add(1)
	}
	if isCacheableReplyResult(applyErr) {
		fc := &cscFetchCapture{
			raw:     raw,
			connID:  connID,
			initGen: capturedGen,
			key:     req.cacheKey,
			token:   req.token,
		}
		c.fulfillCached(req.cacheKey, req.token, fc)
	} else {
		// WRONGTYPE / NOPERM / ...: returned to the caller, not cached.
		c.csc.Cancel(req.cacheKey, req.token)
	}
	// Native error-metric parity with processWithRetry, which emits for every
	// non-nil command error (redis.Nil included) on the uncoalesced path.
	if applyErr != nil {
		if errorCallback := pool.GetMetricErrorCallback(); errorCallback != nil {
			errorType, statusCode, isInternal := classifyCommandError(applyErr)
			errorCallback(context.Background(), errorType, req.servedBy, statusCode, isInternal, 0)
		}
	}
	req.done <- applyErr
}

// batchBudget bounds one coalesced batch's write + reads (connection acquisition
// is bounded separately, by acquireCtx). It follows the client's configured
// timeouts instead of a fixed cap: a client that deliberately sets
// ReadTimeout/WriteTimeout high for slow cacheable reads must not see only its
// coalesced misses cut off early (WithReader clamps the read to min(ctx deadline,
// ReadTimeout), so a shorter ctx would win). The 5s floor covers scheduling
// overhead when the configured timeouts are tiny or disabled.
func (mc *cscMissCoalescer) batchBudget() time.Duration {
	opt := mc.c.opt
	var budget time.Duration
	if opt.WriteTimeout > 0 {
		budget += opt.WriteTimeout
	}
	if opt.ReadTimeout > 0 {
		budget += opt.ReadTimeout
	}
	if budget < 5*time.Second {
		budget = 5 * time.Second
	}
	return budget
}

// countBatch records batch-size accounting. The max is a CAS loop: with
// concurrent sessions a load-then-store could commit a smaller value over a
// larger one that landed in between, permanently underreporting the maximum.
func (mc *cscMissCoalescer) countBatch(batch []*cscMissReq) {
	mc.batches.Add(1)
	mc.batched.Add(uint64(len(batch)))
	n := uint64(len(batch))
	for {
		cur := mc.maxBatchSz.Load()
		if n <= cur || mc.maxBatchSz.CompareAndSwap(cur, n) {
			return
		}
	}
}

// settleErr cancels the reservation and fails one waiting caller. It emits the
// native error metric (parity with processWithRetry and the FD autopipeline
// engine's failReqs) — except for the retry-uncached sentinel, whose command
// re-runs on the fully instrumented uncoalesced path and would double-count.
func (mc *cscMissCoalescer) settleErr(req *cscMissReq, err error) {
	mc.c.csc.Cancel(req.cacheKey, req.token)
	if err != errCSCRetryUncached {
		if errorCallback := pool.GetMetricErrorCallback(); errorCallback != nil {
			errorType, statusCode, isInternal := classifyCommandError(err)
			errorCallback(context.Background(), errorType, req.servedBy, statusCode, isInternal, 0)
		}
	}
	req.done <- err
	mc.failed.Add(1)
}

// settleAllErr fails every request in batch from index `from` onward.
func (mc *cscMissCoalescer) settleAllErr(batch []*cscMissReq, from int, err error) {
	for i := from; i < len(batch); i++ {
		mc.settleErr(batch[i], err)
	}
}

// drainQueueErr fails every request currently queued (non-blocking). Used when a
// connection cannot be acquired, so a caller on a context without a deadline is
// not blocked forever and no reservation is left IN_PROGRESS.
func (mc *cscMissCoalescer) drainQueueErr(err error) {
	for {
		select {
		case req := <-mc.ch:
			mc.settleErr(req, err)
		default:
			return
		}
	}
}

// grabInto appends first plus whatever else is already queued (non-blocking, up
// to cscMissBatchMax) into dst, reusing dst's backing array.
func (mc *cscMissCoalescer) grabInto(dst []*cscMissReq, first *cscMissReq) []*cscMissReq {
	dst = append(dst, first)
	for len(dst) < cscMissBatchMax {
		select {
		case more := <-mc.ch:
			dst = append(dst, more)
		default:
			return dst
		}
	}
	return dst
}

// Coalescer observability is the client's normal telemetry (the otel operation
// and error callbacks fire for coalesced misses like any other command path).
// The engine's internal counters (batched/batches/failed/maxBatchSz on
// cscMissCoalescer) exist for in-package tests, which read them directly off
// c.cscMissCoalescer — deliberately NOT exported as a stats API.
