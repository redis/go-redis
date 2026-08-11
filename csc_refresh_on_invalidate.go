package redis

import (
	"context"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

// Refresh-on-invalidate: re-fetch a key as soon as its invalidation arrives,
// instead of dropping it and waiting for a reader to discover the miss.
//
// Why this is the lever worth pulling. The connection requirement of a CSC client
// is roughly `miss_rate x throughput x RTT`, so the hit rate is the only quantity
// that moves throughput and connection count in the same direction. Every other
// item measured in this investigation trades one against the other. Under churn
// the misses are exactly the invalidated keys, and they are known the moment the
// push arrives — a whole round trip before any reader asks.
//
// # Why it re-inserts rather than refreshes in place
//
// The time-based sweeper (csc_revalidate.go) can use RefreshValue, which replaces
// the value of a LIVE entry. This path cannot: the entry must be DELETED the
// instant the invalidation lands, because leaving a known-stale value readable
// while a refresh is in flight is the exact bug this whole line of work exists to
// remove. So the entry is dropped first and re-inserted through the ordinary
// Reserve/Fulfil path.
//
// That has a useful consequence: Reserve single-flights. If a reader misses the
// key while a background refresh is already in flight, it waits on that
// reservation instead of issuing a second fetch — so the key is fetched once, by
// one connection, no matter how many parties want it.
//
// # On fetching from the "owner" connection
//
// A reasonable worry is that refreshing on an arbitrary connection would register
// the key on several connections at once, so one write would produce several
// invalidations. It does not, because default (non-BCAST) tracking is ONE-SHOT:
// when the server sends `invalidate k` it removes k from its tracking table, so
// at the moment this path runs no connection is registered for k. The refresh
// creates exactly one registration, whichever connection performs it, and Reserve
// guarantees only one refresh runs.
//
// What connection choice does still affect is eviction blast radius: an entry is
// attributed to the connection that fetched it (see EvictByConn), so refreshing
// many entries onto one connection concentrates them there. Pinning the refresh to
// the original owner would fix that, but the pool has no "give me connection N"
// operation — withPipelineConn takes whichever is free. Noted as a follow-up; it
// is a robustness question, not a correctness or invalidation-count one.

// cscNoRefreshOnInvalidate force-disables the feature so a benchmark can A/B it
// against the same binary.
var cscNoRefreshOnInvalidate = os.Getenv("GOREDIS_CSC_NO_REFRESH_ON_INVAL") != ""

const (
	// cscRefreshQueueDepth bounds the pending-refresh backlog. Bounded, and it
	// DROPS rather than blocks: the producer is the invalidation drainer, and
	// stalling that would delay the invalidations themselves — trading a hit-rate
	// optimization for a correctness-critical path. A drop just means the key is
	// refetched by whichever reader wants it next, which is today's behaviour.
	cscRefreshQueueDepth = 4096

	// cscRefreshBatchMax caps how many keys ride one round trip.
	cscRefreshBatchMax = 128

	// cscRefreshRecencyTick is how often the "recently read" horizon advances.
	cscRefreshRecencyTick = 200 * time.Millisecond

	cscRefreshWarnEvery = 30 * time.Second
)

// cscRefreshWindow is the coalescing window. Invalidations are COLLECTED for this
// long before the batch is refetched, instead of firing a round trip as each
// trickle of pushes arrives. The window starts when the first key is collected
// and does NOT slide on later arrivals — under continuous churn a sliding reset
// would never fire and the design would degenerate to the size cap alone. The
// window is flushed early by demand (a reader touching a collected key) or by the
// size cap; whichever comes first. Longer window = fewer, larger round trips (the
// point: it lets refresh work at a small pipeline pool) at the cost of staleness
// bounded by the window for keys nobody reads.
var cscRefreshWindow = envDurationMs("GOREDIS_CSC_REFRESH_WINDOW_MS", 500*time.Millisecond)

// cscRefreshWindowMaxKeys flushes the window early once this many distinct keys
// have collected, so a burst cannot outrun the window ahead of the queue's own
// bounded backlog. A multiple of the per-round-trip cap.
const cscRefreshWindowMaxKeys = 4 * cscRefreshBatchMax

// cscDemandRefresh gates the demand trigger. On (default): a read for a key still
// sitting in the window flushes the whole window immediately, so an actively-read
// batch refreshes in ~one RTT instead of waiting out the window, while an idle
// batch waits the full window (and nobody is reading it, so the wait is free).
// Off: window + size cap only — kept as an A/B knob to price the trigger.
var cscDemandRefresh = os.Getenv("GOREDIS_CSC_REFRESH_DEMAND") != "0"

// cscRefreshCooldown optionally suppresses a refresh for an entry published within
// this window. DEFAULT 0, meaning off — measurement said to leave it off.
//
// The motivation was real. The same key read on several connections is registered
// by the server once per connection, so ONE write produces one invalidation push
// per registration, and that happens routinely: when a batched read finds another
// fetch already in flight it declines the reservation and goes to the wire anyway
// rather than stall the batch (see cscTryServePipelined), registering the key a
// second time. Measured: 34-38% of incoming invalidation pushes match no entry at
// all, which is the signature of exactly this.
//
// But a time window is the wrong instrument, for two reasons.
//
// First, Reserve already dedups the part that costs anything. A duplicate
// invalidation that lands while the refresh is still in flight finds the key
// reserved, is declined, and is dropped for one map operation. Only a duplicate
// arriving AFTER the refresh completed can start a second fetch — which needs the
// refresh to be faster than the gap between duplicates, i.e. loopback, not WAN.
//
// Second, the window cannot tell a duplicate of the write it already handled from a
// genuinely NEW write arriving a few milliseconds later, so it suppresses both. The
// A/B showed the cost of that: at 20k invalidations/s, enabling it cut refresh work
// 36% (289,822 -> 184,780) but LOST throughput (1,347,964 -> 1,326,234) and hit rate
// (93.6% -> 93.0%). Doing the extra work was cheaper than skipping some of the
// useful part.
//
// Left as a tunable (GOREDIS_CSC_REFRESH_COOLDOWN_MS) for a deployment that would
// rather cap refresh traffic than maximise the hit rate.
var cscRefreshCooldown = envDurationMs("GOREDIS_CSC_REFRESH_COOLDOWN_MS", 0)

// envDurationMs reads a millisecond count from the environment, so the cooldown
// can be swept (including to 0, which disables dedup) without a rebuild.
func envDurationMs(name string, def time.Duration) time.Duration {
	v := os.Getenv(name)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return def
	}
	return time.Duration(n) * time.Millisecond
}

// cscRefreshTarget is one entry to re-fetch: the cache key doubles as the wire
// form of the command that produced it, and redisKeys are already namespaced, so
// both can be handed straight back to Reserve.
type cscRefreshTarget struct {
	cacheKey  string
	redisKeys []string
	token     uint64
}

// cscRefreshQueue carries invalidated-but-hot keys from the drainer to the
// refresher.
type cscRefreshQueue struct {
	ch chan cscRefreshTarget

	// pendingSet mirrors the cache keys currently sitting in the refresher's
	// collection window, so the read path (processCached, miss branch) can test
	// membership without a lock and signal demand. Written and cleared only by
	// the refresher goroutine; read concurrently by every reader — hence sync.Map.
	pendingSet sync.Map
	// demandCh carries a single "flush now" nudge from a reader that touched a
	// pending key. Buffered depth 1 and sent non-blocking: one pending nudge is
	// all the refresher needs, and the window timer is the backstop if it is
	// dropped.
	demandCh chan struct{}

	// sinceToken is the "recently read" horizon: only entries whose last read is
	// newer than this are worth refetching. Without it the refresher would chase
	// cold keys, and because a refresh RE-REGISTERS the key with the server, that
	// would both pin the server's tracking table and manufacture the next
	// invalidation — a feedback loop that generates its own work. Same guard the
	// time-based sweeper uses for the same reason.
	sinceToken atomic.Int64

	enqueued      atomic.Uint64
	dropped       atomic.Uint64
	refreshed     atomic.Uint64
	demandFlushes atomic.Uint64
}

// signalDemand nudges the refresher to flush its collection window now, but only
// if cacheKey is actually sitting in that window — i.e. this read is a miss for a
// key whose invalidation we already collected. Called on the read miss path, so
// it must stay cheap: one sync.Map load and, on the rare hit, one non-blocking
// send. The read is already paying a server round trip for its own key; the
// nudge just warms the rest of the co-invalidated batch a window earlier.
func (q *cscRefreshQueue) signalDemand(cacheKey string) {
	if q == nil || !cscDemandRefresh {
		return
	}
	if _, ok := q.pendingSet.Load(cacheKey); !ok {
		return
	}
	select {
	case q.demandCh <- struct{}{}:
	default:
	}
}

// offer enqueues without blocking, counting what it had to drop.
func (q *cscRefreshQueue) offer(t cscRefreshTarget) {
	select {
	case q.ch <- t:
		q.enqueued.Add(1)
	default:
		q.dropped.Add(1)
	}
}

// CSCRefreshStats reports refresh-on-invalidate activity: keys queued, keys
// dropped because the backlog was full, and values actually republished.
//
// Experimental: this API may change in a minor release.
type CSCRefreshStats struct {
	Enqueued  uint64
	Dropped   uint64
	Refreshed uint64

	// DemandFlushes counts collection windows flushed early because a reader
	// missed a key still sitting in the window (vs flushed by the window timer or
	// the size cap). High relative to total flushes means the demand trigger is
	// doing its job — actively-read batches refresh in ~one RTT instead of
	// waiting out the window.
	DemandFlushes uint64

	// Invalidations counts keys named in incoming pushes; InvalidationsNoop counts
	// those that matched no entry, which is the direct measure of DUPLICATE
	// invalidations.
	Invalidations     uint64
	InvalidationsNoop uint64
}

// CSCRefreshStats returns the client's refresh-on-invalidate counters.
//
// Experimental: this API may change in a minor release.
func (c *Client) CSCRefreshStats() CSCRefreshStats {
	q := c.baseClient.cscRefreshQueue
	if q == nil {
		return CSCRefreshStats{}
	}
	st := CSCRefreshStats{
		Enqueued:      q.enqueued.Load(),
		Dropped:       q.dropped.Load(),
		Refreshed:     q.refreshed.Load(),
		DemandFlushes: q.demandFlushes.Load(),
	}
	if lc, ok := c.baseClient.csc.(*LocalCache); ok {
		st.Invalidations, st.InvalidationsNoop = lc.InvalidationStats()
	}
	return st
}

// cscRefreshOnInvalidateEnabled reports whether this client should re-fetch
// invalidated keys in the background.
func (c *baseClient) cscRefreshOnInvalidateEnabled() bool {
	if cscNoRefreshOnInvalidate || c.csc == nil || c.opt == nil {
		return false
	}
	if !c.opt.ClientSideCacheRefreshOnInvalidate {
		return false
	}
	// The collect step needs the concrete cache: a Cache implementation that only
	// satisfies the interface cannot report which entries it removed.
	_, ok := c.csc.(*LocalCache)
	return ok
}

// startCSCRefresher launches the refresher goroutine and hands the drainer the
// queue to feed.
func (c *baseClient) startCSCRefresher() {
	if !c.cscRefreshOnInvalidateEnabled() || c.cscRefreshQueue != nil {
		return
	}
	lc := c.csc.(*LocalCache)
	q := &cscRefreshQueue{
		ch:       make(chan cscRefreshTarget, cscRefreshQueueDepth),
		demandCh: make(chan struct{}, 1),
	}
	q.sinceToken.Store(lc.LRUClock())
	c.cscRefreshQueue = q

	h := &cscRevalidateHandle{stop: make(chan struct{}), done: make(chan struct{})}
	c.cscRefreshHandle = h

	// The invalidate handler is what sees the pushes, so it owns the producer end.
	if ih := lookupInvalidateHandler(c.opt.PushNotificationProcessor); ih != nil {
		ih.setRefreshQueue(q)
	}

	go c.runCSCRefresher(h, lc, q)
}

func (c *baseClient) runCSCRefresher(h *cscRevalidateHandle, lc *LocalCache, q *cscRefreshQueue) {
	defer close(h.done)

	recency := time.NewTicker(cscRefreshRecencyTick)
	defer recency.Stop()

	// The collection window. It starts (timer armed) when the first key of a
	// batch is collected and is NOT re-armed on later arrivals — a sliding reset
	// would never fire under continuous churn, leaving only the size cap. Flushed
	// by the timer, by demand, or by the size cap; whichever fires first.
	window := time.NewTimer(cscRefreshWindow)
	defer window.Stop()
	if !window.Stop() {
		<-window.C
	}
	windowArmed := false

	// pending is keyed by cache key so a key invalidated twice within one window
	// is refetched once. Owned solely by this goroutine.
	pending := make(map[string]cscRefreshTarget, cscRefreshWindowMaxKeys)
	var lastWarn time.Time

	collect := func(t cscRefreshTarget) {
		if _, dup := pending[t.cacheKey]; dup {
			return
		}
		pending[t.cacheKey] = t
		q.pendingSet.Store(t.cacheKey, struct{}{})
		if !windowArmed {
			windowArmed = true
			window.Reset(cscRefreshWindow)
		}
	}

	flush := func(demand bool) {
		if windowArmed {
			windowArmed = false
			if !window.Stop() {
				// Drain a possibly-already-fired timer so the next Reset is clean.
				select {
				case <-window.C:
				default:
				}
			}
		}
		if len(pending) == 0 {
			return
		}
		if demand {
			q.demandFlushes.Add(1)
		}
		// Snapshot to a slice and clear the collection state before any network
		// I/O, so keys invalidated during the refetch start a fresh window rather
		// than being lost or double-counted.
		targets := make([]cscRefreshTarget, 0, len(pending))
		for k, t := range pending {
			targets = append(targets, t)
			q.pendingSet.Delete(k)
			delete(pending, k)
		}
		// A window can hold more than one round trip's worth; chunk it. Keys that
		// self-healed during the window (a reader missed and repopulated them) are
		// now Valid, so Reserve inside refreshInvalidatedBatch declines them for
		// free — no MGET slot is spent rewriting fresh data.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		for start := 0; start < len(targets); start += cscRefreshBatchMax {
			end := start + cscRefreshBatchMax
			if end > len(targets) {
				end = len(targets)
			}
			n, err := c.refreshInvalidatedBatch(ctx, targets[start:end])
			q.refreshed.Add(uint64(n))
			if err != nil && time.Since(lastWarn) > cscRefreshWarnEvery {
				lastWarn = time.Now()
				internal.Logger.Printf(context.Background(),
					"csc: refresh-on-invalidate batch failed: %v", err)
			}
		}
		cancel()
	}

	for {
		select {
		case <-h.stop:
			flush(false)
			return

		case <-recency.C:
			// Advance the horizon: entries not read since the previous tick stop
			// being worth refetching.
			q.sinceToken.Store(lc.LRUClock())

		case t := <-q.ch:
			collect(t)
			// Opportunistically take whatever else is already waiting so one wake
			// drains the backlog into the window.
		drain:
			for len(pending) < cscRefreshWindowMaxKeys {
				select {
				case more := <-q.ch:
					collect(more)
				default:
					break drain
				}
			}
			if len(pending) >= cscRefreshWindowMaxKeys {
				flush(false)
			}

		case <-q.demandCh:
			// A reader missed a key still in the window: the batch is being used,
			// so refetch it now instead of waiting out the window.
			flush(true)

		case <-window.C:
			flush(false)
		}
	}
}

// stopCSCRefresher joins the refresher goroutine.
func (c *baseClient) stopCSCRefresher() {
	h := c.cscRefreshHandle
	if h == nil {
		return
	}
	c.cscRefreshHandle = nil
	if ih := lookupInvalidateHandler(c.opt.PushNotificationProcessor); ih != nil {
		ih.setRefreshQueue(nil)
	}
	close(h.stop)
	<-h.done
}

// refreshInvalidatedBatch re-fetches one chunk of invalidated keys in a single
// round trip and publishes each reply as a fresh entry. Returns how many entries
// it published.
//
// Every reservation this takes is either published or released before returning.
// An abandoned reservation is worse than no refresh at all: LocalCache.Get WAITS
// on an in-progress entry, so one orphan blocks every reader of that key until
// StaleTimeout. (Measured elsewhere in this work: that mistake cost 30x and 8500x
// throughput in the intra-batch rewrites.)
func (c *baseClient) refreshInvalidatedBatch(ctx context.Context, targets []cscRefreshTarget) (int, error) {
	prefix := c.cscKeyPrefix
	if prefix == "" {
		return 0, nil
	}
	if a := c.cscActive; a != nil && !a.Load() {
		return 0, nil
	}

	// Reserve before touching the network, so only keys this batch owns are sent.
	// A declined reservation means a reader is already fetching it — leave it to
	// them rather than duplicating the work.
	kept := make([]cscRefreshTarget, 0, len(targets))
	for _, t := range targets {
		token, shouldFetch := c.csc.Reserve(t.cacheKey, t.redisKeys)
		if !shouldFetch {
			continue
		}
		t.token = token
		kept = append(kept, t)
	}
	if len(kept) == 0 {
		return 0, nil
	}
	// Any reservation still holding a token when this returns was never settled;
	// release it rather than leave a placeholder readers would block on.
	defer func() {
		for i := range kept {
			if kept[i].token != 0 {
				c.csc.Cancel(kept[i].cacheKey, kept[i].token)
			}
		}
	}()

	published := 0
	err := c.withPipelineConn(ctx, func(ctx context.Context, cn *pool.Conn) error {
		connID := cn.GetID()
		// Coverage generation captured BEFORE the reads: if this conn loses
		// tracking mid-batch, every reply is discarded rather than published, the
		// same discipline fulfillCached and revalidateBatch use.
		capturedGen := c.cscConnInitGen(connID)

		if err := cn.WithWriter(c.context(ctx), c.opt.WriteTimeout, func(wr *proto.Writer) error {
			for i := range kept {
				// The cache key is the namespaced RESP encoding of the command that
				// produced the entry; strip the namespace and it is already wire form.
				if _, err := wr.Write([]byte(kept[i].cacheKey[len(prefix):])); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			return err
		}

		return cn.WithReader(c.context(ctx), c.opt.ReadTimeout, func(rd *proto.Reader) error {
			for i := range kept {
				// Invalidation pushes share this connection with replies, so drain
				// them first or a push frame would be read as a value and cached.
				if err := c.processPendingPushNotificationWithReader(ctx, cn, rd); err != nil {
					internal.Logger.Printf(ctx, "csc: refresh push drain: %v", err)
				}
				raw, err := rd.ReadRawReply()
				if err != nil {
					// The reply stream is now out of step with kept; abort so the
					// connection is retired rather than reused mid-batch. The deferred
					// release cancels every reservation still outstanding.
					return err
				}
				if len(raw) == 0 || raw[0] == proto.RespError || raw[0] == proto.RespBlobError {
					// Not cacheable (WRONGTYPE after a type change, NOPERM, ...). A nil
					// reply is NOT an error: a negative lookup is cacheable, and a key
					// that has since been deleted should cache as missing.
					c.csc.Cancel(kept[i].cacheKey, kept[i].token)
					kept[i].token = 0
					continue
				}
				fc := &cscFetchCapture{
					raw:     raw,
					connID:  connID,
					initGen: capturedGen,
					key:     kept[i].cacheKey,
					token:   kept[i].token,
				}
				if c.fulfillCached(kept[i].cacheKey, kept[i].token, fc) {
					published++
				}
				// fulfillCached cancels on its own failure paths, so the token is
				// settled either way.
				kept[i].token = 0
			}
			return nil
		})
	})
	return published, err
}
