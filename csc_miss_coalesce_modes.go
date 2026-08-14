package redis

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

// Alternate miss-coalescer engines (PROTOTYPE, benchmark comparison).
//
//   - pinnedWorker: one held tracked connection, serial half-duplex batches.
//     Removes pool Get/Put churn and connection-count contention, but each batch
//     costs a full RTT before the next starts.
//   - fullDuplexLoop: one held tracked connection with concurrent writer + reader
//     goroutines. Commands stream out while replies stream back, so many commands
//     are in flight on a single socket (the rueidis pipelining model). Hides RTT
//     with one connection instead of N.
//
// Both hold a connection outside the normal pool Get/Put cycle and redial on
// error. Correctness note: a held connection spans reconnects, so on ANY
// connection error every in-flight request is failed (token cancelled, caller
// woken) rather than matched to a reply across tracking generations, and a
// published reply is gated on the reading connection's id+generation still
// matching the one captured when the command was written.

const cscModeBackoff = 5 * time.Millisecond

func opCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

// acquireCtx bounds a session/batch connection acquisition. Unlike opCtx's
// fixed 5s it honors a configured PoolTimeout above that (the normal command
// path and the workers engine via batchBudget both wait the full pool budget
// under temporary saturation), and it is cancelled when the coalescer stops so
// Close does not stall behind a Get blocked on a saturated pool.
func (mc *cscMissCoalescer) acquireCtx() (context.Context, context.CancelFunc) {
	d := 5 * time.Second
	if pt := mc.c.opt.PoolTimeout; pt > d {
		d = pt
	}
	ctx, cancel := context.WithTimeout(context.Background(), d)
	done := make(chan struct{})
	go func() {
		select {
		case <-mc.stop:
			cancel()
		case <-done:
		}
	}()
	return ctx, func() { close(done); cancel() }
}

// pinnedWorker holds one tracked main-pool connection and flushes batches on it
// one at a time (half-duplex). The connection is reused across batches and only
// re-acquired after a connection error.
func (mc *cscMissCoalescer) pinnedWorker() {
	defer mc.wg.Done()
	c := mc.c

	var cn *pool.Conn
	release := func(err error) {
		if cn == nil {
			return
		}
		ctx, cancel := opCtx()
		c.releaseConn(ctx, cn, err)
		cancel()
		cn = nil
	}
	defer release(nil)

	batch := make([]*cscMissReq, 0, cscMissBatchMax)
	for {
		select {
		case <-mc.stop:
			return
		case req := <-mc.ch:
			batch = mc.grabInto(batch[:0], req)
			mc.countBatch(batch)

			if cn == nil {
				ctx, cancel := mc.acquireCtx()
				got, err := c.getConn(ctx)
				cancel()
				if err != nil {
					mc.settleAllErr(batch, 0, err)
					select {
					case <-mc.stop:
						return
					case <-time.After(cscModeBackoff):
					}
					continue
				}
				cn = got
			}

			// batchBudget, not opCtx: the flush includes the reply reads, which must
			// honor a deliberately long configured ReadTimeout (opCtx's fixed 5s
			// would clamp them; see batchBudget).
			ctx, cancel := context.WithTimeout(context.Background(), mc.batchBudget())
			settled, ferr := mc.flushBatch(ctx, cn, batch)
			cancel()
			if ferr != nil {
				release(ferr) // retire the bad connection; next batch re-acquires
				mc.settleAllErr(batch, settled, ferr)
			}
		}
	}
}

// ---- full-duplex engine (feature-complete) ----

// cscFullDuplexConnsDefault is how many independent full-duplex sessions (each
// its own held connection) the fullduplex engine runs. 1 keeps the footprint
// minimal (optimal at high concurrency). More would shard the miss +
// invalidation-drain load across connections, cutting the low-concurrency p99
// tail. Order-free: coalesced misses are independent per-key fetches.
const cscFullDuplexConnsDefault = 1

// cscFullDuplexIdleProbe is how often the reader drains server-initiated pushes
// on the held connection while no reply is pending. A var (not const) so a test
// can disable the idle drain (set it huge) as a negative control.
var cscFullDuplexIdleProbe = 5 * time.Millisecond

// cscFullDuplexSessionIdle is how long a session's writer waits for the next
// miss before ending the session and returning its held connection to the pool.
// Without it a session sits on a pool connection for up to fullDuplexRecycleAge
// (30s) with no miss in flight, and at a small pool (PoolSize:1) a non-cacheable
// command (PING/SET/uncached read) blocks behind it. The next miss re-acquires —
// runFullDuplexSession already waits for work BEFORE taking a connection. Under
// steady miss load the timer is reset per batch and never fires. A var so tests
// can tighten or disable it.
var cscFullDuplexSessionIdle = time.Second

// fullDuplexRecycleAge bounds how long a single session holds one connection,
// so the held connection is returned to the pool periodically (a Put runs the
// OnPut pool hooks: metrics, health, and any queued maintenance handoff). Honors
// a shorter ConnMaxLifetime.
func (c *baseClient) fullDuplexRecycleAge() time.Duration {
	age := 30 * time.Second
	if c.opt.ConnMaxLifetime > 0 && c.opt.ConnMaxLifetime < age {
		age = c.opt.ConnMaxLifetime
	}
	return age
}

// fullDuplexLoop runs full-duplex sessions back to back: each session holds one
// tracked connection until it errors, is recycled (handoff/close/age), or stop
// is requested. Exits on stop.
func (mc *cscMissCoalescer) fullDuplexLoop() {
	defer mc.wg.Done()
	for {
		select {
		case <-mc.stop:
			return
		default:
		}
		stopped, errored := mc.runFullDuplexSession()
		if stopped {
			return
		}
		// Back off only after an ERROR end, so a persistent dial failure does not
		// hot-spin; a clean idle/recycle end continues immediately — the next
		// session blocks on the miss queue anyway, and sleeping here would add a
		// flat cscModeBackoff of latency to a miss already waiting in mc.ch.
		if !errored {
			continue
		}
		select {
		case <-mc.stop:
			return
		case <-time.After(cscModeBackoff):
		}
	}
}

// runFullDuplexSession acquires one tracked connection and pipelines misses on
// it with concurrent writer and reader goroutines. It ends when:
//   - a socket read/write errors (fail path: fail in-flight, Close + Remove);
//   - the connection is marked for handoff/close, the session ages out, or stop
//     is requested (graceful path: drain in-flight, Put so OnPut runs pool hooks
//     and any queued maintenance handoff).
//
// Returns true iff stop was requested (the loop should exit).
func (mc *cscMissCoalescer) runFullDuplexSession() (stopped, backoff bool) {
	c := mc.c

	// Do not hold a pool connection while idle: wait for the first miss BEFORE
	// acquiring. An eagerly-held session connection would, at a small pool
	// (PoolSize:1), starve non-cacheable commands (PING/SET/uncached reads) until
	// PoolTimeout while the session sat waiting for work. The pulled miss is
	// written first by the writer below.
	var first *cscMissReq
	select {
	case <-mc.stop:
		return true, false
	case first = <-mc.ch:
	}

	getCtx, getCancel := mc.acquireCtx()
	cn, err := c.getConn(getCtx)
	getCancel()
	if err != nil {
		// No connection: fail the first miss plus everything queued so a caller on
		// a deadline-less context is not blocked forever and no reservation leaks
		// IN_PROGRESS (mirrors pinnedWorker's getConn-failure handling). The caller
		// backs off and retries; requests arriving during backoff are failed on the
		// next attempt's drain.
		mc.settleErr(first, err)
		mc.drainQueueErr(err)
		return false, true // error end: the loop backs off before re-acquiring
	}
	// CSC serving may have been disabled after the miss was queued (e.g. a HELLO 3
	// downgrade or CLIENT TRACKING rejected during initConn). Holding the conn to
	// wait for more misses would be pointless — none get routed here once serving
	// is off — and could tie up a small pool. Release it cleanly and fail pending.
	if a := c.cscActive; a != nil && !a.Load() {
		relCtx, relCancel := opCtx()
		c.releaseConn(relCtx, cn, nil)
		relCancel()
		// CSC is off now, but the commands are fine — settle with the retry-uncached
		// sentinel so processCached re-runs each on the normal path instead of
		// surfacing a spurious pool.ErrClosed for a valid cacheable read.
		mc.settleErr(first, errCSCRetryUncached)
		mc.drainQueueErr(errCSCRetryUncached)
		return false, false // clean end: no new misses route here while serving is off
	}
	connID := cn.GetID()
	gen := c.cscConnInitGen(connID)

	inflight := make(chan *cscMissReq, cscFullDuplexDepth)
	sctx, scancel := context.WithCancel(context.Background())
	defer scancel()

	var sessErr atomic.Value // error, set only on I/O failure
	var failOnce sync.Once
	fail := func(e error) {
		failOnce.Do(func() {
			if e == nil {
				e = pool.ErrClosed
			}
			sessErr.Store(e)
			scancel()
		})
	}
	errored := func() bool { _, ok := sessErr.Load().(error); return ok }
	reasonErr := func() error {
		if e, ok := sessErr.Load().(error); ok {
			return e
		}
		return pool.ErrClosed
	}

	recycle := make(chan struct{})
	var recycleOnce sync.Once
	doRecycle := func() { recycleOnce.Do(func() { close(recycle) }) }
	var stopFlag atomic.Bool

	// handoffWanted reports whether the connection should be returned to the pool
	// so the OnPut hooks (including a queued maintenance handoff) can run.
	handoffWanted := func() bool {
		return !cn.IsUsable() || cn.ShouldHandoff() || cn.CloseOnPutReason() != ""
	}

	var swg sync.WaitGroup
	swg.Add(2)

	// Writer: pull a request (plus whatever else is queued), write the batch,
	// then enqueue each written request to the reader in wire order. The writer
	// is the SOLE closer of inflight and closes it as its last act on every exit
	// path, which is how the reader learns the session drained.
	go func() {
		defer swg.Done()
		defer close(inflight)
		idleT := time.NewTimer(cscFullDuplexSessionIdle)
		defer idleT.Stop()
		buf := make([]*cscMissReq, 0, cscMissBatchMax)
		pending := first // the miss that woke the session; write it before blocking
		for {
			var req *cscMissReq
			if pending != nil {
				req, pending = pending, nil
			} else {
				if !idleT.Stop() {
					select {
					case <-idleT.C:
					default:
					}
				}
				idleT.Reset(cscFullDuplexSessionIdle)
				select {
				case <-recycle:
					return
				case <-mc.stop:
					stopFlag.Store(true)
					doRecycle()
					return
				case <-sctx.Done():
					return
				case <-idleT.C:
					// No miss for the grace period: end the session cleanly so the
					// held connection goes back to the pool instead of blocking
					// non-cacheable traffic at a small pool until the recycle age.
					// The reader drains anything still in flight (close(inflight)
					// is the graceful no-more signal); the next miss starts a fresh
					// session, which acquires only when work is in hand.
					return
				case req = <-mc.ch:
				}
			}
			buf = mc.grabInto(buf[:0], req)
			mc.countBatch(buf)

			werr := cn.WithWriter(c.context(sctx), c.opt.WriteTimeout, func(wr *proto.Writer) error {
				for _, r := range buf {
					if e := writeCmd(wr, r.cmd); e != nil {
						return e
					}
				}
				return nil
			})
			if werr != nil {
				fail(werr)
				mc.settleAllErr(buf, 0, werr)
				return
			}
			for i, r := range buf {
				select {
				case inflight <- r:
				case <-sctx.Done():
					mc.settleAllErr(buf, i, reasonErr())
					return
				}
			}
		}
	}()

	// Reader: settle replies in the order the writer enqueued them (== wire
	// order), and drain server-initiated pushes (invalidations, maintenance)
	// even while idle. Publishes only while the connection's id+generation still
	// match the session's.
	go func() {
		defer swg.Done()
		ticker := time.NewTicker(cscFullDuplexIdleProbe)
		defer ticker.Stop()

		readOne := func(req *cscMissReq) bool { // false => fatal, reader must exit
			rerr := cn.WithReader(c.context(sctx), c.opt.ReadTimeout, func(rd *proto.Reader) error {
				if e := c.processPendingPushNotificationWithReader(sctx, cn, rd); e != nil {
					internal.Logger.Printf(sctx, "csc: miss-coalesce push drain: %v", e)
				}
				raw, e := rd.ReadRawReply()
				if e != nil {
					return e
				}
				// Deliver the reply the caller is waiting for and let applyAndSettle
				// gate only the CACHE PUBLISH on the connection's id/generation
				// (fulfillCached checks the captured gen) — matching the workers and
				// pinned engines. Previously FD failed the caller with ErrClosed on a
				// mid-flight id/gen change even though the reply was read fine, losing
				// a good result (#3965).
				mc.applyAndSettle(req, raw, connID, gen)
				return nil
			})
			if rerr != nil {
				fail(rerr)
				mc.settleErr(req, rerr)
				return false
			}
			return true
		}

		// handoff/close is a rare, non-latency-critical event, so it is checked off
		// the per-reply hot path: on every idle tick, and once every readsPerHandoffCheck
		// replies (~a few ms of traffic at any throughput).
		const readsPerHandoffCheck = 128
		reads := 0
		for {
			select {
			case req, ok := <-inflight:
				if !ok {
					return // writer closed: session drained clean
				}
				if !readOne(req) {
					return
				}
				reads++
				if reads%readsPerHandoffCheck == 0 && handoffWanted() {
					doRecycle()
				}
			case <-ticker.C:
				// No reply was ready this tick: drain any server-initiated pushes
				// (invalidations, MOVING, ...) unconditionally — the held connection
				// is out of the pool, so the background drainer never visits it. Safe
				// even if a reply is on the wire: the push processor peeks the reply
				// type and consumes only push frames, leaving the reply for the next
				// inflight read.
				if e := c.peekAndProcessPushNotifications(sctx, cn); e != nil {
					fail(e)
					return
				}
				if handoffWanted() {
					doRecycle()
				}
			case <-sctx.Done():
				return
			}
		}
	}()

	// Supervisor: request a graceful recycle on stop or when the session ages
	// out. A handoff/close request comes from the reader via doRecycle().
	recycleTimer := time.NewTimer(c.fullDuplexRecycleAge())
	defer recycleTimer.Stop()
	superDone := make(chan struct{})
	go func() {
		select {
		case <-mc.stop:
			stopFlag.Store(true)
			doRecycle()
		case <-recycleTimer.C:
			doRecycle()
		case <-sctx.Done(): // I/O error: unblock a reader/writer parked on the socket
			_ = cn.Close()
		case <-superDone:
		}
	}()

	swg.Wait()
	close(superDone)

	// Teardown. Error path: the socket was (or is being) closed; Remove it.
	// Graceful path: the connection is clean (every written reply was read), so
	// Put it — OnPut runs the pool hooks and any queued maintenance handoff, and
	// the handoff/reinit path evicts this connection's now-uncovered CSC entries.
	relCtx, relCancel := opCtx()
	if errored() {
		_ = cn.Close()
		c.releaseConn(relCtx, cn, reasonErr())
	} else {
		c.releaseConn(relCtx, cn, nil)
	}
	relCancel()

	// Fail anything the writer enqueued that the reader never consumed (only
	// possible on the error path; the graceful path drains inflight fully). The
	// writer always closes inflight on exit, so a receive here can report the
	// channel closed (ok=false) — do not settle a nil request.
	for {
		select {
		case r, ok := <-inflight:
			if !ok {
				return stopFlag.Load(), errored()
			}
			mc.settleErr(r, reasonErr())
		default:
			return stopFlag.Load(), errored()
		}
	}
}
