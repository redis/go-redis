package redis

import (
	"context"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

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
// MaxRetries/backoff/LOADING handling. Prototype-only; behind GOREDIS_CSC_COALESCE_MISSES.

var cscCoalesceMisses = os.Getenv("GOREDIS_CSC_COALESCE_MISSES") != ""

const (
	cscMissBatchMax   = 128
	cscMissQueueDepth = 4096
	// cscMissWorkers batchers run concurrently, each holding one main-pool
	// (tracked) connection while it writes+reads its batch. Size it to the
	// caching client's pool (env override): more workers than pool connections
	// just makes workers block in the pool Get.
	cscMissWorkersDefault = 8
)

var cscMissWorkers = envIntDefault("GOREDIS_CSC_COALESCE_WORKERS", cscMissWorkersDefault)

// cscMissReq is one caller waiting for its missed key. done carries the fetch
// result back (the reply is already applied to cmd by the time done fires).
type cscMissReq struct {
	cmd      Cmder
	cacheKey string
	token    uint64
	done     chan error
}

type cscMissCoalescer struct {
	c    *baseClient
	ch   chan *cscMissReq
	stop chan struct{}
	wg   sync.WaitGroup

	batched    atomic.Uint64 // reqs that went through a batch
	batches    atomic.Uint64 // batches flushed
	failed     atomic.Uint64 // reqs settled as errors (conn failure)
	maxBatchSz atomic.Uint64
}

// startCSCMissCoalescer launches the batcher goroutines. No-op unless the env
// flag is set and CSC is active.
func (c *baseClient) startCSCMissCoalescer() {
	if !cscCoalesceMisses || c.cscMissCoalescer != nil {
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
	c.cscMissCoalescer = mc
	n := cscMissWorkers
	if n < 1 {
		n = 1
	}
	for i := 0; i < n; i++ {
		mc.wg.Add(1)
		go mc.worker()
	}
}

func (c *baseClient) stopCSCMissCoalescer() {
	mc := c.cscMissCoalescer
	if mc == nil {
		return
	}
	c.cscMissCoalescer = nil
	close(mc.stop)
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

// fetch hands a reserved miss to the coalescer and waits for the result. The
// caller has already Reserved (shouldFetch==true) and this owns the token now.
// Honors the caller's context: if it cancels while waiting, the batch still
// settles the token and populates the cache (the fetch is not wasted), the
// caller just returns early.
func (mc *cscMissCoalescer) fetch(ctx context.Context, cmd Cmder, cacheKey string, token uint64) error {
	req := &cscMissReq{cmd: cmd, cacheKey: cacheKey, token: token, done: make(chan error, 1)}
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
	case <-ctx.Done():
		// The batch will still settle req.done (buffered, cap 1) and the token;
		// we just stop waiting.
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

// flush pipelines a batch of missed commands onto one tracked main-pool connection,
// applies each reply to its caller's command, and publishes each to the cache.
// Every req.done is sent exactly once and every token is settled.
func (mc *cscMissCoalescer) flush(batch []*cscMissReq) {
	c := mc.c
	mc.batches.Add(1)
	mc.batched.Add(uint64(len(batch)))
	if n := uint64(len(batch)); n > mc.maxBatchSz.Load() {
		mc.maxBatchSz.Store(n)
	}

	settled := 0
	settleRemainingErr := func(err error) {
		for ; settled < len(batch); settled++ {
			c.csc.Cancel(batch[settled].cacheKey, batch[settled].token)
			batch[settled].done <- err
			mc.failed.Add(1)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Fetch on the MAIN pool. A published entry is only invalidatable if the
	// connection that read it is running CLIENT TRACKING, so this path must land
	// on a tracked connection. initConn issues CLIENT TRACKING ON for every
	// pooled connection while CSC is active (it is pool-agnostic), so the main
	// pool is tracked and correct. withConn is used rather than withPipelineConn
	// so the choice stays correct even if a later change makes the dedicated
	// pipeline pool deliberately untracked (pipelines never populate the cache):
	// publishing a cache entry on an untracked connection would serve stale until
	// TTL. No contention cost: with the coalescer in front, reader misses are
	// handed to it, so the caching client's main pool has no other user (hits
	// need no connection; writes and uncacheable reads go to the separate
	// autopipelined client).
	err := c.withConn(ctx, func(ctx context.Context, cn *pool.Conn) error {
		connID := cn.GetID()
		// Coverage generation captured BEFORE the reads, same discipline as
		// refreshInvalidatedBatch/fulfillCached: if this conn loses tracking
		// mid-batch every reply is rejected by fulfillCached rather than
		// published under lost coverage.
		capturedGen := c.cscConnInitGen(connID)

		if err := cn.WithWriter(c.context(ctx), c.opt.WriteTimeout, func(wr *proto.Writer) error {
			for _, req := range batch {
				if err := writeCmd(wr, req.cmd); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}

		return cn.WithReader(c.context(ctx), c.opt.ReadTimeout, func(rd *proto.Reader) error {
			for settled < len(batch) {
				req := batch[settled]
				// Invalidation pushes share this connection with replies; drain
				// them first so a push frame is not read as a value.
				if err := c.processPendingPushNotificationWithReader(ctx, cn, rd); err != nil {
					internal.Logger.Printf(ctx, "csc: miss-coalesce push drain: %v", err)
				}
				raw, err := rd.ReadRawReply()
				if err != nil {
					return err
				}
				// Apply the reply to the caller's command first (so its Get()
				// returns a value), then publish to the cache when cacheable.
				applyErr := applyCachedReply(req.cmd, raw)
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
				settled++
			}
			return nil
		})
	})
	if err != nil {
		// Acquire/write/read failure: settle every request not yet handled as an
		// error so no caller hangs and no token is left IN_PROGRESS.
		settleRemainingErr(err)
	}
}

// CSCMissCoalesceStats reports coalescer activity.
//
// Experimental: this API may change in a minor release.
type CSCMissCoalesceStats struct {
	Batched      uint64 // misses served through a batch
	Batches      uint64 // batches flushed
	Failed       uint64 // misses settled as errors (connection failure)
	MaxBatchSize uint64
}

// CSCMissCoalesceStats returns the client's miss-coalescer counters.
func (c *Client) CSCMissCoalesceStats() CSCMissCoalesceStats {
	mc := c.baseClient.cscMissCoalescer
	if mc == nil {
		return CSCMissCoalesceStats{}
	}
	return CSCMissCoalesceStats{
		Batched:      mc.batched.Load(),
		Batches:      mc.batches.Load(),
		Failed:       mc.failed.Load(),
		MaxBatchSize: mc.maxBatchSz.Load(),
	}
}
