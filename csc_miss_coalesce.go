package redis

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

// errCSCRetryUncached is settled to a coalesced miss when the coalescer bows out
// for a reason unrelated to the command itself: CSC serving was disabled after
// the miss was queued (e.g. a RESP3 downgrade, or CLIENT TRACKING rejected during
// a connection re-init), so this held-connection session would fetch on an
// untracked conn or none at all. It never reaches the caller — processCached
// catches it and re-runs the command uncached on the normal path, so a valid
// cacheable read is not failed with a spurious pool.ErrClosed. The reservation is
// always cancelled before this is settled, so no waiter is left IN_PROGRESS.
var errCSCRetryUncached = errors.New("redis: csc miss-coalescer disabled mid-fetch; retry uncached")

// Reader-miss coalescing (PROTOTYPE, env-gated).
//
// The two-client CSC shape serves cache HITS locally (no connection) but every
// cache MISS fetches on the caching client's MAIN pool, one command per
// connection. Under churn at a small pool that is the wall: N concurrent misses
// contend for pool connections one GET at a time (measured: at CSC pool 8 the
// reader misses alone saturate the pool, p99 ~420ms at 64 workers, and no
// refresh policy helps because the readers are the bottleneck).
//
// This coalesces concurrent misses the way refreshInvalidatedBatch coalesces
// invalidated keys: each miss's ORIGINAL command is pipelined onto ONE
// tracked (main-pool) connection, replies are read back in order, each is
// applied to its caller's command AND published to the shared cache. Individual
// pipelined commands (not MGET) so it is cluster-safe and works for any read
// shape, not just string GET.
//
// It is cache-aware, which is the whole point and the reason it cannot just ride
// the ordinary autopipeliner: it must Reserve/fulfilCached each key so the entry
// is tracked and single-flighted. Reserve is done by the caller in processCached
// (only shouldFetch==true misses reach here); this owns the token from that
// point and settles it — fulfilled or cancelled — before waking the caller, or a
// dangling IN_PROGRESS entry would block every reader of that key for
// StaleTimeout.
//
// Tradeoff, same as the autopipeline path: a coalesced miss loses per-command
// MaxRetries/backoff/LOADING handling. Behind Options.ClientSideCacheCoalesceMisses.

const (
	cscMissBatchMax   = 128
	cscMissQueueDepth = 4096
	// cscMissWorkersDefault batchers run concurrently in the default "workers"
	// mode, each holding one main-pool (tracked) connection while it writes+reads
	// its batch. Size it to the caching client's pool (env override): more workers
	// than pool connections just makes workers block in the pool Get.
	cscMissWorkersDefault = 8
	// cscFullDuplexDepth caps in-flight commands on the single-connection
	// full-duplex engine (flow control for the writer).
	cscFullDuplexDepth = 4096
)

// Engine selection + tuning come from the client's Options (ClientSideCacheCoalesce*)
// and are read once, at coalescer construction (per client).
func cscCoalesceMissesEnabled(opt *Options) bool {
	return opt != nil && opt.ClientSideCacheCoalesceMisses
}

func cscMissWorkersCount(opt *Options) int {
	if opt != nil && opt.ClientSideCacheCoalesceWorkers > 0 {
		return opt.ClientSideCacheCoalesceWorkers
	}
	return cscMissWorkersDefault
}

// cscForcePinned lets a benchmark opt into the "pinned" PROTOTYPE engine, which
// is otherwise NOT reachable from the public ClientSideCacheCoalesceMode option.
// Pinned holds one connection with no idle invalidation drain, so it can serve
// stale values after an invalidation (see #3965) and is unsafe for general use;
// it is kept only for engine benchmark comparison.
var cscForcePinned bool

// cscCoalesceMode selects the flush engine: "workers" (default, N pool conns,
// half-duplex batches) or "fullduplex" (1 held conn, concurrent writer+reader,
// deep pipeline). The "pinned" engine is a benchmark-only PROTOTYPE and is NOT
// selectable via the public option (it falls back to "workers"); it can only be
// forced internally via cscForcePinned.
func cscCoalesceMode(opt *Options) string {
	if opt == nil {
		return "workers"
	}
	switch opt.ClientSideCacheCoalesceMode {
	case "fullduplex":
		return "fullduplex"
	case "pinned":
		if cscForcePinned {
			return "pinned"
		}
		return "workers"
	default:
		return "workers"
	}
}

// cscReq* are the states of cscMissReq.apply, the single-word interlock that
// decides who owns req.cmd: the fetching caller or the worker/reader applying
// the reply. Exactly one side wins the pending->X CAS, so the worker never
// writes cmd after the caller has returned and may be reusing it.
const (
	cscReqPending   uint32 = iota // no one has claimed cmd yet
	cscReqAbandoned               // caller's ctx (or Close) fired first: cmd is the caller's again
	cscReqApplying                // worker/reader claimed cmd first: it will write and settle
)

// cscMissReq is one caller waiting for its missed key. done carries the fetch
// result back (the reply is already applied to cmd by the time done fires).
type cscMissReq struct {
	cmd      Cmder
	cacheKey string
	token    uint64
	done     chan error
	// apply interlocks ownership of cmd between the fetching caller and the
	// worker/reader. The caller abandons (CAS pending->abandoned) if its context
	// is cancelled — or Close races its enqueue — before a worker starts applying;
	// the worker claims (CAS pending->applying) before it writes the reply into
	// cmd. A plain flag would still race: a worker could read "not abandoned",
	// begin writing cmd, and the caller cancel mid-write. The CAS makes the two
	// decisions mutually exclusive. Either way the worker still settles the token
	// and publishes to the shared cache (the fetch is never wasted) — only the cmd
	// write is gated.
	apply atomic.Uint32
}

// claimAbandon is the caller's side of the cmd interlock: it succeeds only if no
// worker has started applying, in which case the worker will skip the cmd write.
func (r *cscMissReq) claimAbandon() bool {
	return r.apply.CompareAndSwap(cscReqPending, cscReqAbandoned)
}

// claimApply is the worker's side: it succeeds only if the caller has not
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
	mode     string // "workers" | "pinned" | "fullduplex"

	batched    atomic.Uint64 // reqs that went through a batch
	batches    atomic.Uint64 // batches flushed
	failed     atomic.Uint64 // reqs settled as errors (conn failure)
	maxBatchSz atomic.Uint64
	// abandonedApplies counts replies whose caller had already abandoned the req
	// (lost the cmd interlock), so the cmd write was skipped. Used only to assert
	// the -race abandoned-path test actually hit the window it guards.
	abandonedApplies atomic.Uint64
}

// startCSCMissCoalescer launches the batcher goroutines. No-op unless
// Options.ClientSideCacheCoalesceMisses is set and CSC is active, and — see the
// option's GoDoc — unless the cache is the built-in *LocalCache: the coalescer's
// publish path (fulfillCached capture, refresh integration, hot-entry
// collection) is LocalCache-specific, so a custom Cache implementation falls
// back to per-caller fetches.
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
		mode: cscCoalesceMode(c.opt),
	}
	c.cscMissCoalescer.Store(mc)
	switch mc.mode {
	case "pinned":
		mc.wg.Add(1)
		go mc.pinnedWorker()
	case "fullduplex":
		// N independent full-duplex sessions, each holding its own connection and
		// pulling independent misses from the shared queue. Order-free: coalesced
		// misses are standalone per-key fetches with no cross-request contract, and
		// each session's per-conn reader still matches replies to requests. Sharding
		// spreads the miss + invalidation-drain load off a single connection, which
		// is what cuts the low-concurrency p99 tail.
		for i := 0; i < cscFullDuplexConnsDefault; i++ {
			mc.wg.Add(1)
			go mc.fullDuplexLoop()
		}
	default:
		n := cscMissWorkersCount(c.opt)
		if n < 1 {
			n = 1
		}
		for i := 0; i < n; i++ {
			mc.wg.Add(1)
			go mc.worker()
		}
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
	// Any request still queued after stop is drained and failed so no caller
	// hangs on its done channel.
	for {
		select {
		case req := <-mc.ch:
			mc.c.csc.Cancel(req.cacheKey, req.token)
			req.done <- pool.ErrClosed
		default:
			return
		}
	}
}

// stopWorkers signals every coalescer goroutine to exit; idempotent, does not
// wait. Called by stopCSCMissCoalescer (Close) and by the GC cleanup for a
// client dropped without Close — the workers retain the base client (cache,
// pools), so leaving them running would leak all of it per forgotten client.
func (mc *cscMissCoalescer) stopWorkers() {
	mc.stopOnce.Do(func() { close(mc.stop) })
}

// fetch hands a reserved miss to the coalescer and waits for the result. The
// caller has already Reserved (shouldFetch==true) and this owns the token now.
// Honors the caller's context: if it cancels while waiting, the batch still
// settles the token and populates the cache (the fetch is not wasted), the
// caller just returns early.
func (mc *cscMissCoalescer) fetch(ctx context.Context, cmd Cmder, cacheKey string, token uint64) error {
	req := &cscMissReq{cmd: cmd, cacheKey: cacheKey, token: token, done: make(chan error, 1)}
	// Reject early if the coalescer is already shutting down, so a send does not
	// win the select race against a closed mc.stop and land in mc.ch after the
	// shutdown drain (where no worker would ever pick it up). This narrows — but
	// cannot fully close — that race; the mc.stop case in the wait select below is
	// what guarantees the caller never hangs on a post-drain req.
	select {
	case <-mc.stop:
		mc.c.csc.Cancel(cacheKey, token)
		return pool.ErrClosed
	default:
	}
	select {
	case mc.ch <- req:
	case <-mc.stop:
		mc.c.csc.Cancel(cacheKey, token)
		return pool.ErrClosed
	case <-ctx.Done():
		mc.c.csc.Cancel(cacheKey, token)
		return ctx.Err()
	}
	select {
	case err := <-req.done:
		return err
	case <-mc.stop:
		// Close raced our enqueue: the req may have landed in mc.ch after the
		// shutdown drain, so no worker will ever settle req.done. If we win the
		// interlock (no worker is applying), cancel the reservation and return
		// instead of hanging — a duplicate Cancel, if the drain also got this req,
		// is a no-op on a settled token. If a worker already claimed cmd, it owns
		// the write and will settle; wait for it so we do not touch cmd concurrently.
		if req.claimAbandon() {
			mc.c.csc.Cancel(cacheKey, token)
			return pool.ErrClosed
		}
		// A worker already claimed cmd and is mid-write: wait for it to finish so we
		// do not read/reuse cmd concurrently, then return the shutdown error (the
		// receive is a happens-before edge, so the later cmd.SetErr is race-free).
		<-req.done
		return pool.ErrClosed
	case <-ctx.Done():
		// Caller stopped waiting. If we win the interlock the worker skips the cmd
		// write but still settles the token and publishes to the cache (the fetch is
		// not wasted); we just return early. If a worker already claimed cmd, it is
		// mid-write — wait for it rather than race by reusing cmd. Either way return
		// the context error, matching the non-coalesced path (processWithRetry), so a
		// cancelled Get never returns a value depending on who won the CAS.
		if !req.claimAbandon() {
			<-req.done
		}
		return ctx.Err()
	}
}

func (mc *cscMissCoalescer) worker() {
	defer mc.wg.Done()
	batch := make([]*cscMissReq, 0, cscMissBatchMax)
	for {
		select {
		case <-mc.stop:
			return
		case req := <-mc.ch:
			batch = append(batch[:0], req)
			// Take whatever else is already waiting so one connection serves many
			// misses. A lone miss (nothing queued) flushes immediately — at
			// inflight 1 every caller is blocked on its own Get, so any
			// accumulation delay would be pure added latency.
		drain:
			for len(batch) < cscMissBatchMax {
				select {
				case more := <-mc.ch:
					batch = append(batch, more)
				default:
					break drain
				}
			}
			mc.flush(batch)
		}
	}
}

// flush (workers mode) acquires a fresh main-pool connection, pipelines the
// batch onto it half-duplex (write all, then read all), and releases it. Every
// req.done is sent exactly once and every token is settled.
//
// A published entry is only invalidatable if the connection that read it is
// running CLIENT TRACKING, so this path must land on a tracked connection.
// initConn issues CLIENT TRACKING ON for every pooled connection while CSC is
// active (it is pool-agnostic), so the main pool is tracked and correct. The
// main pool is used rather than the pipeline pool so the choice stays correct
// even if a later change makes a dedicated pipeline pool deliberately untracked
// (publishing a cache entry on an untracked connection would serve stale until
// TTL). No contention cost: with the coalescer in front, reader misses are
// handed to it, so the caching client's main pool has no other user.
func (mc *cscMissCoalescer) flush(batch []*cscMissReq) {
	c := mc.c
	mc.countBatch(batch)

	// Acquire under acquireCtx (PoolTimeout-bounded AND cancelled by stop, so
	// Close is not stalled behind a Get blocked on a saturated pool), then run
	// the batch under the full write+read budget.
	getCtx, getCancel := mc.acquireCtx()
	cn, err := c.getConn(getCtx)
	getCancel()
	if err != nil {
		mc.settleAllErr(batch, 0, err)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), mc.batchBudget())
	defer cancel()
	settled, ferr := mc.flushBatch(ctx, cn, batch)
	c.releaseConn(ctx, cn, ferr)
	if ferr != nil {
		// Write/read failure: settle every request not yet handled as an error so
		// no caller hangs and no token is left IN_PROGRESS.
		mc.settleAllErr(batch, settled, ferr)
	}
}

// flushBatch writes batch on cn, then reads and settles replies in order.
// Returns the number settled and any connection error; on error the caller
// settles the remaining requests. Used by both the workers and pinned engines.
func (mc *cscMissCoalescer) flushBatch(ctx context.Context, cn *pool.Conn, batch []*cscMissReq) (settled int, err error) {
	c := mc.c
	connID := cn.GetID()
	// Coverage generation captured BEFORE the reads, same discipline as
	// refreshInvalidatedBatch/fulfillCached: if this conn loses tracking
	// mid-batch every reply is rejected by fulfillCached rather than published
	// under lost coverage.
	capturedGen := c.cscConnInitGen(connID)

	// ctx is the ENGINE's own bounded batch budget, not a caller context, so it
	// must NOT go through c.context() — with ContextTimeoutEnabled=false (the
	// default) that strips the deadline to context.Background(), and with
	// ReadTimeout disabled a server that never replies would then block this
	// worker forever, wedging Close in stopCSCMissCoalescer's wg.Wait.
	if werr := cn.WithWriter(ctx, c.opt.WriteTimeout, func(wr *proto.Writer) error {
		for _, req := range batch {
			if e := writeCmd(wr, req.cmd); e != nil {
				return e
			}
		}
		return nil
	}); werr != nil {
		return 0, werr
	}

	rerr := cn.WithReader(ctx, c.opt.ReadTimeout, func(rd *proto.Reader) error {
		for settled < len(batch) {
			// Invalidation pushes share this connection with replies; drain them
			// first so a push frame is not read as a value.
			if e := c.processPendingPushNotificationWithReader(ctx, cn, rd); e != nil {
				internal.Logger.Printf(ctx, "csc: miss-coalesce push drain: %v", e)
			}
			raw, e := rd.ReadRawReply()
			if e != nil {
				return e
			}
			mc.applyAndSettle(batch[settled], raw, connID, capturedGen)
			settled++
		}
		return nil
	})
	return settled, rerr
}

// applyAndSettle applies raw to the caller's command, publishes to the cache
// when cacheable (under the reading connection's tracking generation), and wakes
// the caller. Sends req.done exactly once.
//
// It first claims the cmd interlock: if the caller has abandoned the req (its
// context was cancelled, or Close raced its enqueue) it has its Cmder back and
// may be reading or reusing it, so the reply must NOT be written there. The
// fetch is still not wasted — the reply is classified from a throwaway parse and,
// when cacheable, published to the shared cache for the next reader; only the
// caller's cmd write is skipped.
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
	req.done <- applyErr
}

// batchBudget bounds one coalesced batch's write + reads (connection
// acquisition is bounded separately by acquireCtx, which honors PoolTimeout and
// is cancelled on stop). It honors the client's configured timeouts instead of
// a fixed cap: a client that deliberately sets ReadTimeout/WriteTimeout high
// for slow cacheable reads must not see only its coalesced misses cut off early
// (WithReader clamps the read to min(ctx deadline, ReadTimeout), so a shorter
// ctx would win). A 5s floor covers scheduling overhead when the configured
// timeouts are tiny or disabled.
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

// countBatch records batch-size accounting.
func (mc *cscMissCoalescer) countBatch(batch []*cscMissReq) {
	mc.batches.Add(1)
	mc.batched.Add(uint64(len(batch)))
	if n := uint64(len(batch)); n > mc.maxBatchSz.Load() {
		mc.maxBatchSz.Store(n)
	}
}

// settleErr cancels the reservation and fails one waiting caller.
func (mc *cscMissCoalescer) settleErr(req *cscMissReq, err error) {
	mc.c.csc.Cancel(req.cacheKey, req.token)
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

// CSCMissCoalesceStats reports coalescer activity.
//
// Experimental: this API may change in a minor release.
type CSCMissCoalesceStats struct {
	Mode         string // "workers" | "pinned" | "fullduplex"
	Batched      uint64 // misses served through a batch
	Batches      uint64 // batches flushed
	Failed       uint64 // misses settled as errors (connection failure)
	MaxBatchSize uint64
}

// CSCMissCoalesceStats returns the client's miss-coalescer counters.
func (c *Client) CSCMissCoalesceStats() CSCMissCoalesceStats {
	mc := c.baseClient.cscMissCoalescer.Load()
	if mc == nil {
		return CSCMissCoalesceStats{}
	}
	return CSCMissCoalesceStats{
		Mode:         mc.mode,
		Batched:      mc.batched.Load(),
		Batches:      mc.batches.Load(),
		Failed:       mc.failed.Load(),
		MaxBatchSize: mc.maxBatchSz.Load(),
	}
}
