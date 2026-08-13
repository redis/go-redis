package redis

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

// Ordered full-duplex dispatch for the async ordered AutoPipeline face.
//
// The half-duplex path executes one batch per round trip (MaxConcurrentBatches
// is 1 in ordered mode), so a slow link caps throughput at batch/RTT and late
// arrivals wait a full RTT behind the in-flight batch. Full-duplex holds ONE
// pipeline-pool connection and runs a writer + reader goroutine on it: the
// writer streams command groups back-to-back without waiting for reads, the
// reader drains replies in FIFO order and completes each command as its reply
// lands. One connection, ~1 RTT latency, pipe-saturated throughput.
//
// Ordering contract: each goroutine's commands execute in the order it
// submitted them; no ordering is promised between goroutines. A buffered Go
// channel is an MPSC queue (a single goroutine's sequential submits are received
// in order) and one connection is FIFO on the wire, so the in-flight deque's
// position is the reply-matching key.
//
// Retries: on a connection failure the unacked in-flight tail (in order) is
// re-issued on a fresh connection ahead of any newly queued work, respecting
// shouldRetry/MaxRetries/backoff AND the per-command NoRetry flag — a NoRetry
// command anywhere in the tail fails the whole tail instead of replaying, exactly
// as the half-duplex pipeline guards its retry with cmdsContainNoRetry. After
// exhaustion those commands are failed and the engine keeps serving new work on a
// fresh connection. This is the same at-least-once contract as a normal Pipeline:
// a command whose write reached the server but whose reply was lost may re-execute
// (relevant for non-idempotent writes) — the ambiguous set is only the unacked tail.
//
// Gated by AutoPipelineOptions.FullDuplex, honored only on the async ordered
// single-shard face on a standalone *Client with a pipeline pool. Window, idle
// return and max-hold are tuned via the FullDuplex* options (see their GoDoc).
// Cluster and window auto-tune are follow-ups (see AP_ORDERED_FULLDUPLEX_DESIGN.md).
// RESP3 push frames are demuxed inline.

var (
	errFDReaderGone   = errors.New("redis: autopipeline full-duplex reader exited")
	errFDConnUnusable = errors.New("redis: autopipeline full-duplex connection unusable")
)

// Full-duplex tuning defaults, applied by newFDEngine when the corresponding
// AutoPipelineOptions field is zero. See the FullDuplexWindow / FullDuplexIdleTimeout
// / FullDuplexMaxHold GoDoc for the rationale. The window must exceed the
// bandwidth-delay product (RTT × target rate) or it throttles throughput; the
// deque only holds ACTUAL in-flight (self-limited by throughput), so a generous
// default costs no memory until a stalled peer makes in-flight actually grow.
const (
	fdDefaultWindow  = 65536
	fdDefaultIdle    = time.Second
	fdDefaultMaxHold = 5 * time.Second
)

// fdResult is why a full-duplex session ended.
type fdResult int

const (
	fdGraceful fdResult = iota // AutoPipeliner Close: engine exits
	fdConnErr                  // connection failure: unacked tail returned for replay
	fdIdle                     // idle: conn returned cleanly; re-lease on next command
	fdRecycle                  // max-hold: conn returned cleanly; re-lease immediately (work pending)
)

// fdReq pairs a command with the per-command apBatch whose done channel is
// closed once that command's reply has landed (or it is finally failed).
//
// hookDone is non-nil only when the client has process hooks (OTel/custom): the
// command then has a host goroutine (see fdEngine.hostHook) running the hook
// chain, and finalizing the command closes hookDone (waking the host) instead of
// the batch — the host closes the batch after the hook returns, so the hook
// brackets the command and can rewrite its result before the waiter wakes. When
// hookDone is nil (the common, hook-free fast path) finalizing closes the batch
// directly.
type fdReq struct {
	cmd      Cmder
	batch    *apBatch
	hookDone chan struct{}
}

// complete finalizes a command whose result is already set on it: it wakes the
// caller directly, or (when hooks are present) hands off to the command's host
// goroutine, which runs the hook chain and then wakes the caller.
func (r fdReq) complete() {
	if r.hookDone != nil {
		close(r.hookDone)
		return
	}
	r.batch.close()
}

// fdInflight is an ordered FIFO of written-but-unacknowledged commands. The
// writer appends to the back; the reader reads the front's reply then pops it.
// On a connection failure the remaining entries (front→back) are exactly the
// unacked tail, in order, ready to replay. Two close modes:
//   - graceful: no more pushes, but the reader keeps reading the remaining
//     replies and exits once drained (clean Close).
//   - recover: hard stop; the reader abandons the remaining, which are returned
//     to the retry loop for replay.
type fdInflight struct {
	mu         sync.Mutex
	cond       *sync.Cond
	q          []fdReq
	noMorePush bool          // graceful: drain remaining then reader exits
	hardClosed bool          // recover: reader stops immediately, remaining replayed
	room       chan struct{} // cap-1 signal: the reader popped, so there is room
	peak       int           // high-water mark of len(q); observability for the backpressure test
}

func newFDInflight() *fdInflight {
	f := &fdInflight{room: make(chan struct{}, 1)}
	f.cond = sync.NewCond(&f.mu)
	return f
}

func (f *fdInflight) len() int {
	f.mu.Lock()
	n := len(f.q)
	f.mu.Unlock()
	return n
}

// pushBatch appends a whole write batch under one lock (fewer lock ops than
// per-command push — matters at loopback op rates).
func (f *fdInflight) pushBatch(reqs []fdReq) {
	f.mu.Lock()
	f.q = append(f.q, reqs...)
	if len(f.q) > f.peak {
		f.peak = len(f.q)
	}
	f.cond.Signal()
	f.mu.Unlock()
}

// peakLen returns the high-water mark of in-flight entries seen so far (test
// observability for the backpressure bound).
//
//nolint:unused // used by the full-duplex backpressure tests; lint runs with tests:false.
func (f *fdInflight) peakLen() int {
	f.mu.Lock()
	n := f.peak
	f.mu.Unlock()
	return n
}

// fdReadBatch caps how many replies the reader snapshots per lock acquisition:
// enough to amortize the mutex over many reads, small enough that the reader
// advances (and signals writer room) frequently even with a deep in-flight.
const fdReadBatch = 256

// frontBatch blocks until entries are available (or the deque is closing) and
// returns a snapshot of the front (up to fdReadBatch). ok=false means the reader
// should exit. The writer only ever appends, so this prefix stays the front
// until the reader advance()s it.
func (f *fdInflight) frontBatch(buf []fdReq) ([]fdReq, bool) {
	f.mu.Lock()
	for len(f.q) == 0 && !f.noMorePush && !f.hardClosed {
		f.cond.Wait()
	}
	if f.hardClosed || len(f.q) == 0 {
		f.mu.Unlock()
		return buf[:0], false
	}
	n := len(f.q)
	if n > fdReadBatch {
		n = fdReadBatch
	}
	buf = append(buf[:0], f.q[:n]...)
	f.mu.Unlock()
	return buf, true
}

// advance removes the front n entries the reader has completed and signals the
// writer that in-flight has room.
func (f *fdInflight) advance(n int) {
	if n <= 0 {
		return
	}
	f.mu.Lock()
	if n > len(f.q) {
		n = len(f.q)
	}
	f.q = f.q[n:]
	f.mu.Unlock()
	select {
	case f.room <- struct{}{}:
	default:
	}
}

func (f *fdInflight) empty() bool {
	f.mu.Lock()
	n := len(f.q)
	f.mu.Unlock()
	return n == 0
}

func (f *fdInflight) closeGraceful() {
	f.mu.Lock()
	f.noMorePush = true
	f.cond.Broadcast()
	f.mu.Unlock()
}

// hardClose signals the reader to stop immediately (used on a connection error).
// It deliberately does NOT take the queue: the caller must wait for the reader
// to exit (<-readerDone) and THEN call takeRemaining, so the reader and the
// recovery path never touch the queue concurrently. That ordering is what keeps
// each entry owned by exactly one of {the reader completed it, recovery
// replays/fails it}. A concurrent grab (the previous closeRecover) could scoop
// an entry the reader had just completed but not yet advanced, handing an
// already-executed command to the retry loop — double-executing it and, on the
// hooked path, double-closing its hookDone channel (a panic).
func (f *fdInflight) hardClose() {
	f.mu.Lock()
	f.hardClosed = true
	f.cond.Broadcast()
	f.mu.Unlock()
}

// takeRemaining returns the entries the reader left unacknowledged, in order,
// and clears the queue. Call ONLY after the reader has exited (<-readerDone):
// the reader advance()s every command it completes, so what remains is exactly
// the unacked tail, and with the reader gone there is no concurrent access.
func (f *fdInflight) takeRemaining() []fdReq {
	f.mu.Lock()
	rem := f.q
	f.q = nil
	f.mu.Unlock()
	return rem
}

type fdEngine struct {
	ap       *AutoPipeliner
	client   *Client
	pool     pool.Pooler
	ch       chan fdReq // MPSC ordered queue: many submitters -> the writer
	maxBatch int
	window   int           // max in-flight (written, unacked) before the writer waits
	idle     time.Duration // return the conn after this idle gap (0 = never)
	maxHold  time.Duration // force a clean return at least this often (0 = never)

	recycles    atomic.Int64               // clean returns (idle + max-hold); observability/tests
	curInflight atomic.Pointer[fdInflight] // current session's in-flight deque; test observability

	submitMu sync.RWMutex // guards closed; RLock across the submit send, WLock to close the gate
	closed   bool         // set once run() is tearing down; submit then rejects new work
}

func newFDEngine(ap *AutoPipeliner, client *Client) *fdEngine {
	mb := ap.config.MaxBatchSize
	if mb <= 0 {
		mb = 200
	}
	// Resolve tuning ONCE here: a zero field means "use the default". In
	// particular window must never be 0 — the writer's backpressure gate is
	// `for inflight.len() >= window`, so window==0 (0 >= 0) would block the
	// writer on the very first submit. Validate rejects negatives.
	w := ap.config.FullDuplexWindow
	if w <= 0 {
		w = fdDefaultWindow
	}
	idle := ap.config.FullDuplexIdleTimeout
	if idle <= 0 {
		idle = fdDefaultIdle
	}
	maxHold := ap.config.FullDuplexMaxHold
	if maxHold <= 0 {
		maxHold = fdDefaultMaxHold
	}
	return &fdEngine{
		ap:       ap,
		client:   client,
		pool:     client.getPipelinePool(),
		ch:       make(chan fdReq, w),
		maxBatch: mb,
		window:   w,
		idle:     idle,
		maxHold:  maxHold,
	}
}

// submit enqueues a command onto the ordered stream and returns its batch.
// Blocks when the queue is full (backpressure) or bails if the engine is
// closing. The caller (AutoPipeliner.submit) stamps setReady on the async face.
//
// When the client has process hooks (OTel/custom), a per-command host goroutine
// runs the hook chain for this command (see hostHook); ctx is the command's
// context, used so the hook's span parents correctly. The hook-free path skips
// the goroutine and channel entirely.
func (fd *fdEngine) submit(ctx context.Context, cmd Cmder) *apBatch {
	if fd.ap.isClosed() {
		cmd.SetErr(ErrClosed)
		return completedBatch
	}
	b := newAPBatch()
	var hookDone chan struct{}
	if fd.ap.pipeliner.hookCount() > 0 {
		hookDone = make(chan struct{})
		go fd.hostHook(ctx, cmd, b, hookDone)
	}
	req := fdReq{cmd: cmd, batch: b, hookDone: hookDone}

	// Send under RLock and re-check closed so a send can never win the race with
	// run()'s shutdown drain: drainQueue takes the WLock, sets closed, then
	// drains fd.ch. The RWMutex serialises those two, so once the final drain has
	// run no new req can land in fd.ch — otherwise a req enqueued after the drain
	// would never be completed and its caller would hang forever. A send that is
	// blocked on a full channel here is released by the ctx.Done() branch (Close
	// cancels ap.ctx), so holding the RLock cannot wedge the WLock.
	fd.submitMu.RLock()
	if fd.closed {
		fd.submitMu.RUnlock()
		cmd.SetErr(ErrClosed)
		req.complete()
		return b
	}
	select {
	case fd.ch <- req:
		fd.submitMu.RUnlock()
		return b
	case <-ctx.Done():
		// Caller's context expired/cancelled while backpressured (window/channel
		// full): honor it instead of blocking until room or Close (#3964).
		fd.submitMu.RUnlock()
		cmd.SetErr(ctx.Err())
		req.complete()
		return b
	case <-fd.ap.ctx.Done():
		fd.submitMu.RUnlock()
		cmd.SetErr(ErrClosed)
		req.complete()
		return b
	}
}

// hostHook runs the user process-hook chain for one full-duplex command on its
// own goroutine. The chain starts here (≈ submit time), its next() blocks until
// the reader — or a failure/close path — signals hookDone, and it ends after: so
// an observing hook (redisotel spans/metrics, custom ProcessHooks) records a span
// covering the command's real write→reply latency, and a hook that rewrites the
// result is honored before the waiter wakes. FD reports each command individually
// (withProcessHook), not as a pipeline batch. Only started when hookCount()>0.
func (fd *fdEngine) hostHook(ctx context.Context, cmd Cmder, b *apBatch, hookDone chan struct{}) {
	// A user ProcessHook runs on this goroutine; an unrecovered panic here would
	// crash the process (and leave the caller blocked on b). Recover, fail the
	// command, and close the batch so the waiter always wakes — mirroring the
	// dispatch path's recoverDispatchPanic.
	defer func() {
		if r := recover(); r != nil {
			if cmd.rawErr() == nil {
				cmd.SetErr(fmt.Errorf("redis: autopipeline: panic in full-duplex process hook: %v", r))
			}
			internal.Logger.Printf(ctx, "autopipeline: recovered full-duplex hook panic: %v\n%s", r, debug.Stack())
			b.close()
		}
	}()
	nextCalled := false
	err := fd.ap.pipeliner.withProcessHook(ctx, cmd, func(context.Context, Cmder) error {
		nextCalled = true
		<-hookDone          // reply landed (or the command was failed)
		return cmd.rawErr() // direct read: cmd.Err() would await batch.done, which
		//                     this goroutine itself closes below → self-deadlock.
	})
	// A hook that SHORT-CIRCUITS (returns without calling next) never received
	// hookDone — but the command is already on the wire and the reader will still
	// write its result into cmd. Wait for that here before releasing the caller so
	// the reader's writes to cmd happen-before the caller's reads (no data race).
	// The command still executed; the hook's returned error is honored regardless
	// (documented on AutoPipelineOptions.FullDuplex).
	if !nextCalled {
		<-hookDone
	}
	cmd.SetErr(err) // honor a hook that rewrote / short-circuited the result
	b.close()       // now wake the waiter
}

// run owns the engine for the AutoPipeliner's lifetime: acquire a pipeline-pool
// connection, run one full-duplex attempt on it, and on connection failure
// replay the unacked tail on a fresh connection (bounded by MaxRetries/backoff)
// while continuing to serve the queue. Exits only on graceful Close.
func (fd *fdEngine) run() {
	defer fd.ap.wg.Done()
	bg := context.Background()
	var carry []fdReq // unacked tail to re-issue at the start of the next attempt
	attempts := 0
	for {
		if fd.ap.ctx.Err() != nil {
			fd.failReqs(carry, ErrClosed)
			fd.drainQueue(ErrClosed)
			return
		}
		unacked, result, aerr := fd.attempt(bg, carry)
		switch result {
		case fdGraceful:
			return // Close: attempt drained written work; queue failed there.
		case fdIdle:
			// Conn returned cleanly to the pool (its per-conn hooks can run).
			// Block for the next command before re-leasing, so an idle engine
			// does not churn Get/Put.
			select {
			case r := <-fd.ch:
				carry, attempts = []fdReq{r}, 0
			case <-fd.ap.ctx.Done():
				fd.drainQueue(ErrClosed)
				return
			}
		case fdRecycle:
			// Max-hold reached; conn returned cleanly. Work is pending — re-lease
			// immediately.
			fd.recycles.Add(1)
			carry, attempts = nil, 0
		default: // fdConnErr — a real connection error occurred
			// retryTimeout=true: a read/write timeout is a retryable connection
			// failure here (re-issue the unacked tail on a fresh conn), matching
			// the cluster pipeline retry paths — otherwise a single WAN timeout
			// fails the whole tail. The NoRetry guard below still protects
			// non-idempotent writes, same as cmdsContainNoRetry.
			if len(unacked) > 0 && shouldRetry(aerr, true) && !fdReqsNoRetry(unacked) &&
				attempts < fd.client.opt.MaxRetries {
				attempts++
				fd.sleepBackoff(attempts)
				carry = unacked
				continue
			}
			// Not retrying the tail (none, non-retryable, or exhausted): fail it,
			// then ALWAYS back off before re-leasing so a dead server cannot spin
			// this loop. Keep the engine alive to serve new work when it recovers.
			fd.failReqs(unacked, aerr)
			carry = nil
			attempts++
			fd.sleepBackoff(attempts)
			if attempts >= fd.client.opt.MaxRetries {
				attempts = 0 // reset so backoff restarts small once we're serving again
			}
		}
	}
}

// attempt acquires a connection, runs one full-duplex session (re-issuing carry
// first), and releases the connection. Returns the unacked tail + error on
// connection failure, or graceful=true on Close.
func (fd *fdEngine) attempt(bg context.Context, carry []fdReq) (unacked []fdReq, result fdResult, aerr error) {
	// Acquire under ap.ctx (not bg): if Close cancels ap.ctx while this Get is
	// blocked on a saturated pool, it returns at once instead of waiting out
	// PoolTimeout, so shutdown is not delayed (#3964). Everything after — init and
	// the session's I/O — stays on bg so already-accepted commands still complete
	// during Close (Close waits for run() via ap.wg).
	cn, err := fd.pool.Get(fd.ap.ctx)
	if err != nil {
		return carry, fdConnErr, err
	}
	if !cn.IsInited() {
		if e := fd.client.initConn(bg, cn); e != nil {
			fd.pool.Remove(bg, cn, e)
			return carry, fdConnErr, e
		}
		if !cn.TryAcquire() {
			fd.pool.Remove(bg, cn, errFDConnUnusable)
			return carry, fdConnErr, errFDConnUnusable
		}
	}

	unacked, result, aerr = fd.session(bg, cn, carry)

	// ANY connection-error end (result==fdConnErr) leaves the conn desynced — an
	// unread reply tail, a partial write, or a reader protocol error — so it MUST
	// be removed; Put()ing it would poison the pool for the next caller. Keying on
	// result (not isBadConn) is deliberate: errors like errFDReaderGone or a plain
	// write timeout are not classified bad-conn, yet the conn is still unusable.
	//
	// Clean ends (graceful / idle / recycle) leave the conn at a RESP boundary.
	// Return it via the shared releaseConnToPool rather than a raw Put so pending
	// push notifications (MOVING/failover) are drained BEFORE the conn goes back
	// to the pool — otherwise a buffered push would sit until the next user, delaying
	// handoff (#3964) — and the conn is Removed instead if the drain shows it is
	// desynced. (Same path the main pool uses, so the two cannot drift.)
	if result == fdConnErr {
		fd.pool.Remove(bg, cn, aerr)
	} else {
		fd.client.releaseConnToPool(bg, fd.pool, cn, nil)
	}
	return unacked, result, aerr
}

// session runs the writer (this goroutine) + reader (spawned) on one connection
// until Close (graceful) or a connection error (returns the unacked tail).
func (fd *fdEngine) session(bg context.Context, cn *pool.Conn, carry []fdReq) (unacked []fdReq, result fdResult, aerr error) {
	inflight := newFDInflight()
	fd.curInflight.Store(inflight) // test observability (peak in-flight)
	readerDone := make(chan struct{})

	readTimeout := fd.client.opt.ReadTimeout
	if readTimeout == 0 {
		// Unset: keep a bound so the reader cannot block forever on a silent conn.
		readTimeout = 30 * time.Second
	}
	// readTimeout < 0 (ReadTimeout explicitly disabled) flows through unchanged:
	// WithReader/deadline() treats <= 0 as "no deadline", honoring the disabled
	// timeout for slow scripts / module commands. A genuinely stuck read is still
	// unblocked by the conn Close on the error path above (see the fdConnErr case).
	var errOnce sync.Once
	var sharedErr error
	failOnce := func(e error) { errOnce.Do(func() { sharedErr = e }) }

	// Reader: read replies in FIFO order, completing each command as its reply
	// lands. Works a bounded front-snapshot per lock (amortizes the mutex over
	// many reads), then advances. On a connection/protocol error it stops and
	// leaves the unread tail in the deque (it becomes the unacked recovery set).
	go func() {
		defer close(readerDone)
		var buf []fdReq
		for {
			var ok bool
			buf, ok = inflight.frontBatch(buf)
			if !ok {
				return
			}
			done := 0
			var rerr error
			// Read each reply as it lands (one WithReader per reply). Reading the
			// whole snapshot inside a single WithReader was measurably slower on
			// loopback: it blocks on commands the writer has pushed but not yet
			// flushed, collapsing writer/reader overlap.
			for i := range buf {
				req := buf[i]
				e := cn.WithReader(bg, readTimeout, func(rd *proto.Reader) error {
					// Drain RESP3 push frames buffered ahead of this reply so a
					// push is never misread as the command's reply (FIFO misalign).
					// A push-drain error is logged and NOT propagated (matching the
					// workers path in flushBatch): a custom push processor returning
					// an error must not fail this unrelated in-flight command or kill
					// the connection. A genuine transport error re-surfaces in the
					// readReply below and is handled as the connection error it is.
					if perr := fd.client.processPendingPushNotificationWithReader(bg, cn, rd); perr != nil {
						internal.Logger.Printf(bg, "autopipeline: full-duplex push drain: %v", perr)
					}
					return req.cmd.readReply(rd)
				})
				if e != nil && !isRedisError(e) {
					rerr = e // connection/protocol error: stop; unread tail stays
					break
				}
				req.cmd.SetErr(e) // nil or a per-command Redis error (a valid reply)
				req.complete()    // wake the caller, or hand off to the hook host
				done++
			}
			inflight.advance(done)
			if rerr != nil {
				failOnce(rerr)
				return
			}
		}
	}()

	// Idle / max-hold timers arm the clean-return paths. A disabled timer uses a
	// nil channel (never selected).
	var idleC, maxC <-chan time.Time
	var idleT, maxT *time.Timer
	if fd.idle > 0 {
		idleT = time.NewTimer(fd.idle)
		idleC = idleT.C
		defer idleT.Stop()
	}
	if fd.maxHold > 0 {
		maxT = time.NewTimer(fd.maxHold)
		maxC = maxT.C
		defer maxT.Stop()
	}
	resetIdle := func() {
		if idleT == nil {
			return
		}
		if !idleT.Stop() {
			select {
			case <-idleT.C:
			default:
			}
		}
		idleT.Reset(fd.idle)
	}

	result = fdConnErr // default until a break sets otherwise

	// Writer: re-issue the recovered tail first, then serve the queue.
	writeErr := fd.writeBatch(bg, cn, inflight, carry)
	if writeErr == nil {
		scratch := make([]fdReq, 0, fd.maxBatch)
		byteLimit := int64(fd.ap.config.MaxBatchBytes) // 0 = disabled
	serve:
		for {
			// Backpressure: bound the in-flight (written-but-unacked) deque. Wait
			// for the reader to drain below the window BEFORE taking new work, so
			// a slow/stalled peer cannot grow in-flight without bound. Done here
			// (not mid-batch) so no drained work is ever held during the wait.
			for inflight.len() >= fd.window {
				select {
				case <-inflight.room:
				case <-readerDone:
					break serve // reader hit a connection error
				case <-fd.ap.ctx.Done():
					result = fdGraceful
					break serve
				case <-maxC:
					result = fdRecycle
					break serve
				}
			}
			select {
			case req := <-fd.ch:
				batch := append(scratch[:0], req)
				batchBytes := cmdApproxBytes(req.cmd)
			drain:
				for len(batch) < fd.maxBatch {
					// Soft MaxBatchBytes cap (like the half-duplex path): stop
					// accumulating once the payload reaches the limit, so one flush
					// cannot buffer an unbounded write. The first command is always
					// included, so a lone oversized command still goes.
					if byteLimit > 0 && batchBytes >= byteLimit {
						break drain
					}
					select {
					case r := <-fd.ch:
						batch = append(batch, r)
						batchBytes += cmdApproxBytes(r.cmd)
					default:
						break drain
					}
				}
				if e := fd.writeBatch(bg, cn, inflight, batch); e != nil {
					writeErr = e
					break serve
				}
				resetIdle()
			case <-readerDone:
				break serve // reader hit a connection error (result stays fdConnErr)
			case <-fd.ap.ctx.Done():
				result = fdGraceful
				break serve
			case <-idleC:
				// Only return the conn when genuinely idle: nothing queued AND the
				// reader has drained the in-flight. Otherwise it is a spurious
				// fire mid-stream (e.g. the writer was busy in a large flush) —
				// re-arm and keep the hot session.
				if inflight.empty() && len(fd.ch) == 0 {
					result = fdIdle
					break serve
				}
				resetIdle()
			case <-maxC:
				result = fdRecycle
				break serve
			}
		}
	}
	if writeErr != nil {
		failOnce(writeErr)
		result = fdConnErr
	}

	switch result {
	case fdGraceful:
		// Clean Close: stop pushing, let the reader drain in-flight replies to a
		// RESP boundary, and fail only what never made it onto the wire.
		inflight.closeGraceful()
		fd.drainQueue(ErrClosed)
		<-readerDone
		if sharedErr != nil {
			// The reader hit a connection error during the final drain: the
			// in-flight tail it never reached would otherwise leave callers hung,
			// and the conn is desynced. Fail the stranded tail and report the
			// error so attempt() removes the conn. run() exits on its next loop
			// (ctx is already done), so this does not retry.
			fd.failReqs(inflight.takeRemaining(), sharedErr)
			return nil, fdConnErr, sharedErr
		}
		return nil, fdGraceful, nil
	case fdIdle, fdRecycle:
		// Clean return: no more pushes, reader drains remaining replies, then the
		// conn is at a RESP boundary and safe to Put back to the pool.
		inflight.closeGraceful()
		<-readerDone
		if sharedErr != nil {
			// Reader failed while draining for the clean return: recover the
			// unacked tail for replay and report the error so the conn is removed
			// instead of Put back poisoned. (Fixes the readerDone/clean-result
			// race where a protocol error would otherwise Put a bad conn.)
			return inflight.takeRemaining(), fdConnErr, sharedErr
		}
		return nil, result, nil
	default: // fdConnErr
		// Stop the reader, wait for it to exit, THEN take the unacked tail — so
		// the reader (which advances every command it completes) and this
		// recovery never touch the deque at once. The take moved after
		// <-readerDone closes the double-ownership race.
		//
		// Close the connection before waiting: on a WRITE error the reader is
		// typically blocked in WithReader awaiting a reply that will never arrive,
		// and hardClose only wakes a reader parked in frontBatch — not one parked
		// in a socket read. Closing makes that read return at once, so recovery
		// does not stall up to the full read deadline (cursor #3964). attempt()
		// removes this conn right after, so the close is safe and idempotent.
		inflight.hardClose()
		_ = cn.Close()
		<-readerDone
		unacked = inflight.takeRemaining()
		if sharedErr == nil {
			sharedErr = errFDReaderGone
		}
		return unacked, fdConnErr, sharedErr
	}
}

// writeBatch pushes each req onto the in-flight FIFO (so it is tracked as
// unacked even if the flush then fails) and writes the whole batch in one
// buffered flush. A write error leaves the reqs in the deque for recovery.
func (fd *fdEngine) writeBatch(bg context.Context, cn *pool.Conn, inflight *fdInflight, reqs []fdReq) error {
	if len(reqs) == 0 {
		return nil
	}
	inflight.pushBatch(reqs)
	return cn.WithWriter(bg, fd.client.opt.WriteTimeout, func(wr *proto.Writer) error {
		for i := range reqs {
			if e := writeCmd(wr, reqs[i].cmd); e != nil {
				return e
			}
		}
		return nil
	})
}

// fdReqsNoRetry reports whether any command in the tail forbids retry (NoRetry,
// e.g. a RawWriteTo command). The whole tail is then failed rather than replayed
// on a fresh connection — matching the half-duplex pipeline, which guards its
// retry with cmdsContainNoRetry (redis.go generalProcessPipeline).
func fdReqsNoRetry(reqs []fdReq) bool {
	for i := range reqs {
		if reqs[i].cmd.NoRetry() {
			return true
		}
	}
	return false
}

// failReqs completes a set of commands with err (used on retry exhaustion / Close).
func (fd *fdEngine) failReqs(reqs []fdReq, err error) {
	for i := range reqs {
		// rawErr(), not Err(): this runs on the engine goroutine, and Err()
		// awaits batch.done — the very channel complete() closes just below — so
		// awaiting here would self-deadlock (the same trap hostHook documents).
		if reqs[i].cmd.rawErr() == nil {
			reqs[i].cmd.SetErr(err)
		}
		reqs[i].complete()
	}
}

// drainQueue fails everything currently queued (unwritten) with err, without
// blocking. Used on Close for commands that never reached the wire.
//
// It first closes the submit gate: it takes the WLock (blocking until in-flight
// submit sends finish — each either landed in fd.ch, drained below, or took its
// ctx.Done() branch), sets closed, then drains. After this no submit can enqueue
// new work, so nothing is left un-completed behind the drain.
//
// INVARIANT: every drainQueue call is a terminal shutdown drain — run() exits
// right after, and every caller is past a ctx-cancel check. Never call it on a
// non-close path: a submit blocked on a full channel is unwedged only by its
// ctx.Done() branch, so without a cancelled ctx the WLock here would deadlock
// against the RLock held across that send.
func (fd *fdEngine) drainQueue(err error) {
	fd.submitMu.Lock()
	fd.closed = true
	fd.submitMu.Unlock()
	for {
		select {
		case r := <-fd.ch:
			r.cmd.SetErr(err)
			r.complete()
		default:
			return
		}
	}
}

// sleepBackoff waits the retry backoff, interruptible by Close.
func (fd *fdEngine) sleepBackoff(attempt int) {
	d := internal.RetryBackoff(attempt, fd.client.opt.MinRetryBackoff, fd.client.opt.MaxRetryBackoff)
	if d <= 0 {
		return
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
	case <-fd.ap.ctx.Done():
	}
}
