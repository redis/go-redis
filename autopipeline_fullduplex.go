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
	"github.com/redis/go-redis/v9/internal/otel"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

// Ordered full-duplex dispatch for the ordered AutoPipeline faces (async and
// blocking).
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
// Gated by AutoPipelineOptions.FullDuplex, honored on the ordered single-shard
// faces (async AND blocking) of a standalone *Client with a pipeline pool.
// Window, idle return and max-hold are tuned via the FullDuplex* options (see
// their GoDoc).
// Cluster and window auto-tune are follow-ups (see AP_ORDERED_FULLDUPLEX_DESIGN.md).
// RESP3 push frames are demuxed inline.

var errFDReaderGone = errors.New("redis: autopipeline full-duplex reader exited")

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
	fdDenied                   // acquisition denied (Limiter.Allow reject): fail carry + backlog, engine stays alive
	fdLeaseErr                 // could not lease/init a conn for a new session: retry, then fail carry + backlog after MaxRetries
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
	// ctx is the caller's submit context, kept so the per-command OTel metric can
	// be recorded against it (span/baggage correlation), mirroring process().
	ctx context.Context
	// writtenAt is stamped when the command is flushed to the wire; the reader
	// uses write→reply as the command's operation duration for the OTel metric.
	writtenAt time.Time
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
	// Zero the consumed prefix before reslicing: the reslice keeps the backing
	// array (curInflight holds the deque while the engine idles), so without this
	// a drained burst retains up to a window's worth of completed fdReq values —
	// command args, caller contexts, batches — until the next session overwrites
	// them.
	for i := 0; i < n; i++ {
		f.q[i] = fdReq{}
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

	retryWg  sync.WaitGroup // tracks off-pipe retries diverted to the normal client path; run() waits it so Close does too
	retrySem chan struct{}  // caps concurrent off-pipe retries at the window (see retryOnNormalConn)
	hostWg   sync.WaitGroup // tracks per-command hook-host goroutines (see hostHook); run() waits it so Close does not return while a post-next ProcessHook is still running
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
		retrySem: make(chan struct{}, w),
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
	}
	req := fdReq{cmd: cmd, batch: b, hookDone: hookDone, ctx: ctx}

	// Send under RLock and re-check closed so a send can never win the race with
	// run()'s shutdown drain: takeQueue takes the WLock, sets closed, then
	// drains fd.ch. The RWMutex serialises those two, so once the final drain has
	// run no new req can land in fd.ch — otherwise a req enqueued after the drain
	// would never be completed and its caller would hang forever. A send that is
	// blocked on a full channel here is released by the ctx.Done() branch (Close
	// cancels ap.ctx), so holding the RLock cannot wedge the WLock.
	fd.submitMu.RLock()
	if fd.closed {
		fd.submitMu.RUnlock()
		cmd.SetErr(ErrClosed)
		// Submit-time rejection: return the shared completedBatch sentinel (no host
		// was started) so processAsync surfaces the error from raw Process(ctx,cmd),
		// matching every other submit-time-rejection path.
		return completedBatch
	}
	select {
	case fd.ch <- req:
		// Accepted onto the stream. Start the hook host ONLY now — a submission that
		// is never admitted (the cancel paths below) must not spawn/leak a host
		// goroutine under backpressure. Tracked in hostWg (Add under the gate, so it
		// is ordered before the shutdown drain's WLock and run()'s hostWg.Wait never races an
		// Add on a zero counter) so Close waits for post-next hook work.
		if hookDone != nil {
			fd.hostWg.Add(1)
			go fd.hostHook(ctx, cmd, b, hookDone)
		}
		fd.submitMu.RUnlock()
		return b
	case <-ctx.Done():
		// Caller's context expired/cancelled while backpressured (window/channel
		// full): honor it instead of blocking until room or Close (#3964). Not
		// admitted and no host started, so this is a submit-time failure — return the
		// completedBatch sentinel so raw Process(ctx,cmd) reports the ctx error (the
		// typed async face reads the same error off the command).
		fd.submitMu.RUnlock()
		cmd.SetErr(ctx.Err())
		return completedBatch
	case <-fd.ap.ctx.Done():
		fd.submitMu.RUnlock()
		cmd.SetErr(ErrClosed)
		return completedBatch
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
	// Declared first so it runs last: Close waits hostWg (via run()), and the host
	// is done only after the recover defer below has also run.
	defer fd.hostWg.Done()
	// Mark this goroutine as the batch's executor so a user hook that reads its own
	// command's result after next() (cmd.Err()/cmd.String(), a documented pattern)
	// gets the not-yet/just-executed view instead of blocking on batch.done — which
	// only THIS goroutine closes, below. Without it such a hook self-deadlocks: the
	// reply closes hookDone (waking next), but the batch stays open until the hook
	// returns. Mirrors runOutsidePipeline's async dispatch guard.
	if fd.ap.armSelfDeadlockGuard() {
		b.dispGid.Store(curGoroutineID())
	}
	// A user ProcessHook runs on this goroutine; an unrecovered panic here would
	// crash the process (and leave the caller blocked on b). Recover, fail the
	// command, and close the batch so the waiter always wakes — mirroring the
	// dispatch path's recoverDispatchPanic.
	nextCalled := false
	defer func() {
		if r := recover(); r != nil {
			// The command was already streamed, so the reader still owns cmd and
			// will write its reply into it. If the panic happened BEFORE next()
			// (nextCalled false), next() never waited hookDone — wait for it here so
			// the reader's writes to cmd happen-before the caller's reads (no data
			// race), and so a non-idempotent command is not left half-settled. When
			// the panic came after next(), next() already waited hookDone.
			if !nextCalled {
				<-hookDone
			}
			if cmd.rawErr() == nil {
				cmd.SetErr(fmt.Errorf("redis: autopipeline: panic in full-duplex process hook: %v", r))
			}
			internal.Logger.Printf(ctx, "autopipeline: recovered full-duplex hook panic: %v\n%s", r, debug.Stack())
			b.close()
		}
	}()
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

// retryOnNormalConn re-runs a full-duplex command that came back with a retryable
// Redis error (LOADING/READONLY/CLUSTERDOWN/…) or a redirect (MOVED/ASK) on the
// client's NORMAL path (pipeliner.process): the cluster client routes redirects
// to the proper node, and processCommand's retry loop handles LOADING/backoff up
// to MaxRetries — neither of which the fixed single-conn FD socket can do. It
// runs on its own goroutine so it does not stall the FD reader, tracked by
// retryWg so Close waits for it, and settles the FD request with the outcome.
//
// process() is the raw single-command exec (no hook chain); when the client has
// process hooks the FD hostHook still brackets this command and reports its
// result via req.complete(). Background ctx: the command was already accepted, so
// it completes even under a concurrent Close (which waits via run()'s retryWg).
func (fd *fdEngine) retryOnNormalConn(req fdReq) {
	// Bound concurrent off-pipe retries to the FD window. Under a sustained
	// retryable stream (LOADING/READONLY on every reply) each reply would
	// otherwise spawn an independent retry goroutine while the reader keeps
	// advancing the window and admitting more writes — unbounded goroutines all
	// parked in backoff/pool acquisition. The acquire blocks the READER, which
	// stops advancing the deque, which fills the window and blocks the writer and
	// then submitters: end-to-end backpressure. No cycle: retries drain on the
	// main pool, independent of the reader that is waiting here.
	fd.retrySem <- struct{}{}
	fd.retryWg.Add(1)
	go func() {
		defer func() {
			<-fd.retrySem
			fd.retryWg.Done()
		}()
		// process runs user code (hooks, arg encoders) and can panic; without
		// recovery the batch never completes and the caller (and a hooked
		// command's host, parked on hookDone) waits forever — the writer, reader
		// and hostHook all recover, so this path must too.
		defer func() {
			if r := recover(); r != nil {
				req.cmd.SetErr(fmt.Errorf("redis: autopipeline: panic in full-duplex off-pipe retry: %v", r))
				internal.Logger.Printf(context.Background(),
					"autopipeline: recovered full-duplex retry panic: %v\n%s", r, debug.Stack())
				req.complete()
			}
		}()
		err := fd.ap.pipeliner.process(context.Background(), req.cmd)
		req.cmd.SetErr(err)
		req.complete()
	}()
}

// run owns the engine for the AutoPipeliner's lifetime: acquire a pipeline-pool
// connection, run one full-duplex attempt on it, and on connection failure
// replay the unacked tail on a fresh connection (bounded by MaxRetries/backoff)
// while continuing to serve the queue. Exits only on graceful Close.
func (fd *fdEngine) run() {
	defer fd.ap.wg.Done()
	// Runs before wg.Done (LIFO), so Close — which waits ap.wg — also waits for
	// any off-pipe retries still running on the normal client path.
	defer fd.retryWg.Wait()
	// Same discipline for per-command hook hosts: a ProcessHook doing work after
	// next() returns runs on a host goroutine that closes the command's batch, so
	// Close must not return while one is still running. Every hostWg.Add is gated
	// behind submitMu+closed (see submit) and every run() return is preceded by
	// the shutdown drain (which sets closed), so this Wait never races a live Add.
	defer fd.hostWg.Wait()
	bg := context.Background()
	var carry []fdReq // unacked tail to re-issue at the start of the next attempt
	// Two SEPARATE retry budgets, and both count only CONSECUTIVE failures of
	// their own kind. A shared counter would let transient lease failures eat the
	// reconnect budget: after MaxRetries lease errors, the first genuine
	// mid-session drop would fail the whole unacked tail with zero replay
	// attempts. leaseAttempts resets whenever a session actually ran (a lease
	// succeeded); retryAttempts resets on a clean session end (idle/recycle).
	leaseAttempts := 0 // consecutive fdLeaseErr/fdDenied acquisition failures
	retryAttempts := 0 // consecutive fdConnErr tail-replay failures
	for {
		if fd.ap.ctx.Err() != nil {
			fd.shutdownFlush(bg, carry)
			return
		}
		// Never lease a connection (or hit the Limiter / dial) without work in
		// hand: block for the first command whenever the carry is empty. This
		// covers the initial entry (an unused FD autopipeliner stays idle instead
		// of dialing in the background), the fdIdle return, AND the fail-fast
		// exits below (fdDenied / exhausted fdLeaseErr / failed fdConnErr tail),
		// which would otherwise loop straight back into attempt against an empty
		// queue — hammering the Limiter or dialing a down server forever (#3964).
		// Non-blocking when work is already queued, so fdRecycle with pending
		// work still re-leases immediately; an empty recycle parks here, which is
		// strictly better than holding a conn nothing uses.
		if len(carry) == 0 {
			select {
			case r := <-fd.ch:
				carry = []fdReq{r}
			case <-fd.ap.ctx.Done():
				fd.shutdownFlush(bg, nil)
				return
			}
		}
		unacked, result, aerr := fd.attempt(bg, carry)
		switch result {
		case fdGraceful:
			return // Close: attempt drained written work; queue failed there.
		case fdIdle:
			// Conn returned cleanly to the pool (its per-conn hooks can run).
			// The loop-top wait blocks for the next command before re-leasing, so
			// an idle engine does not churn Get/Put.
			carry, leaseAttempts, retryAttempts = nil, 0, 0
		case fdRecycle:
			// Max-hold reached; conn returned cleanly. Work is pending — re-lease
			// immediately.
			fd.recycles.Add(1)
			carry, leaseAttempts, retryAttempts = nil, 0, 0
		case fdLeaseErr:
			// Could not lease/init a connection for a new session (server down, pool
			// saturated). Retry the lease for a transient outage; once retries are
			// exhausted, fail-fast the carry tail AND the fd.ch backlog with the error
			// (matching half-duplex, which completes accepted batches with the conn
			// error after retries) rather than leaving accepted commands buffered
			// indefinitely. The engine stays alive and serves again once the
			// server/pool recovers. carry here is already NoRetry-safe (the mid-session
			// fdConnErr split produced it, or it is nil), and it was never written on a
			// new conn, so replaying it wholesale is fine. A mid-session drop is
			// fdConnErr (buffered work survives the reconnect); a persistent outage
			// converges to this on the next attempt.
			// Close racing the lease surfaces here as a lease "failure" (the
			// acquisition ctx is cancelled): the accepted work is fine — flush it
			// via the normal pipeline path, exactly like the loop-top shutdown
			// check would have, instead of failing it with a canceled error.
			if fd.ap.ctx.Err() != nil {
				fd.shutdownFlush(bg, carry)
				return
			}
			if shouldRetry(aerr, true) && leaseAttempts < fd.client.opt.MaxRetries {
				leaseAttempts++
				fd.sleepBackoff(leaseAttempts)
				continue // carry unchanged; re-lease
			}
			fd.failReqs(carry, aerr)
			fd.failQueue(aerr)
			carry = nil
			leaseAttempts++
			fd.sleepBackoff(leaseAttempts)
			if leaseAttempts >= fd.client.opt.MaxRetries {
				leaseAttempts = 0
			}
		case fdDenied:
			// Same Close-race guard as fdLeaseErr: on shutdown, flush instead of
			// failing accepted work with a denial that only exists because Close
			// interrupted the acquisition.
			if fd.ap.ctx.Err() != nil {
				fd.shutdownFlush(bg, carry)
				return
			}
			// The Limiter denied session acquisition. Fail-fast every accepted
			// command with the limiter error (matching the plain-client getConn
			// path) rather than leaving them buffered until the breaker closes:
			// the carry tail plus the whole fd.ch backlog. The engine stays alive
			// (no closed flag) and backs off, so it serves again once the limiter
			// admits — commands submitted after this drain are failed on the next
			// denied attempt or served once it recovers.
			fd.failReqs(carry, aerr)
			fd.failQueue(aerr)
			carry = nil
			leaseAttempts++
			fd.sleepBackoff(leaseAttempts)
			if leaseAttempts >= fd.client.opt.MaxRetries {
				leaseAttempts = 0
			}
		default: // fdConnErr — a real connection error occurred
			// A session ran, so the lease succeeded: acquisition failures are no
			// longer consecutive.
			leaseAttempts = 0
			// retryTimeout=true: a read/write timeout is a retryable connection
			// failure here (re-issue the unacked tail on a fresh conn), matching
			// the cluster pipeline retry paths — otherwise a single WAN timeout
			// fails the whole tail. The NoRetry guard below still protects
			// non-idempotent writes, same as cmdsContainNoRetry.
			if len(unacked) > 0 && shouldRetry(aerr, true) &&
				retryAttempts < fd.client.opt.MaxRetries {
				// Split at the first NoRetry command: retry the retryable PREFIX and
				// fail it plus everything ordered after (a NoRetry command must never
				// be re-sent). When the very first unacked command is NoRetry (n==0)
				// there is nothing retryable ahead of it, so fall through to fail the
				// whole tail. Mirrors the half-duplex split of retry-policy runs.
				if n := fdFirstNoRetry(unacked); n > 0 {
					if n < len(unacked) {
						fd.failReqs(unacked[n:], aerr)
					}
					retryAttempts++
					fd.sleepBackoff(retryAttempts)
					carry = unacked[:n]
					continue
				}
			}
			// Not retrying the tail (none, non-retryable, or exhausted): fail it,
			// then ALWAYS back off before re-leasing so a dead server cannot spin
			// this loop. Keep the engine alive to serve new work when it recovers.
			fd.failReqs(unacked, aerr)
			carry = nil
			retryAttempts++
			fd.sleepBackoff(retryAttempts)
			if retryAttempts >= fd.client.opt.MaxRetries {
				retryAttempts = 0 // reset so backoff restarts small once we're serving again
			}
		}
	}
}

// attempt acquires a connection, runs one full-duplex session (re-issuing carry
// first), and releases the connection. Returns the unacked tail + error on
// connection failure, or graceful=true on Close.
func (fd *fdEngine) attempt(bg context.Context, carry []fdReq) (unacked []fdReq, result fdResult, aerr error) {
	// Per-session Limiter (opt.Limiter): the Limiter gates connection acquisition,
	// and FD acquires ONE conn per session, so Allow() once here — if it rejects,
	// do not acquire and do not report (mirrors getConn). Otherwise ReportResult
	// exactly once at release, BEFORE the conn becomes available again (the
	// report-before-release ordering, so a breaker sees a failure before admitting
	// the next session). Both the Limiter report and the conn release therefore
	// live in ONE deferred func, in that order.
	limited := fd.client.opt.Limiter != nil
	if limited {
		if err := fd.client.opt.Limiter.Allow(); err != nil {
			// Acquisition denied (e.g. an open circuit breaker). Report nothing (no
			// conn was acquired, mirroring getConn) and signal fdDenied so run()
			// fail-fasts carry AND the fd.ch backlog with this error instead of
			// leaving accepted async commands buffered until the breaker closes.
			return carry, fdDenied, err
		}
	}

	var cn *pool.Conn
	defer func() {
		if limited {
			fd.client.opt.Limiter.ReportResult(aerr)
		}
		if cn == nil {
			return // nothing acquired, or already Removed inline below
		}
		// ANY connection-error end (result==fdConnErr) leaves the conn desynced —
		// an unread reply tail, a partial write, or a reader protocol error — so it
		// MUST be removed; Put()ing it would poison the pool. Keying on result (not
		// isBadConn) is deliberate: errFDReaderGone or a plain write timeout are not
		// classified bad-conn, yet the conn is still unusable. Clean ends go through
		// releaseConnToPool (drains pending pushes before Put; removes if desynced).
		if result == fdConnErr {
			fd.pool.Remove(bg, cn, aerr)
		} else {
			fd.client.releaseConnToPool(bg, fd.pool, cn, nil)
		}
	}()

	// Acquire under ap.ctx (not bg): if Close cancels ap.ctx while this Get is
	// blocked on a saturated pool, it returns at once instead of waiting out
	// PoolTimeout, so shutdown is not delayed (#3964). Everything after — init and
	// the session's I/O — stays on bg so already-accepted commands still complete
	// during Close (Close waits for run() via ap.wg).
	cn, aerr = fd.pool.Get(fd.ap.ctx)
	if aerr != nil {
		cn = nil
		return carry, fdLeaseErr, aerr
	}
	// Init + acquire through the shared helper (initPooledConn) rather than
	// hand-inlining initConn/TryAcquire — the main and pipeline paths drifted three
	// times when this was mirrored by hand. It records the create-time metric,
	// unwraps the init error, and Removes the conn on any failure (so the defer,
	// which sees cn=nil, does not double-release). It does NOT Put/return the conn,
	// which suits the held-conn full-duplex model.
	if e := fd.client.initPooledConn(bg, fd.pool, cn); e != nil {
		cn = nil // initPooledConn already Removed it
		return carry, fdLeaseErr, e
	}

	unacked, result, aerr = fd.session(bg, cn, carry)
	return unacked, result, aerr
}

// session runs the writer (this goroutine) + reader (spawned) on one connection
// until Close (graceful) or a connection error (returns the unacked tail).
func (fd *fdEngine) session(bg context.Context, cn *pool.Conn, carry []fdReq) (unacked []fdReq, result fdResult, aerr error) {
	inflight := newFDInflight()
	fd.curInflight.Store(inflight) // test observability (peak in-flight)
	readerDone := make(chan struct{})

	// Honor opt.ReadTimeout as-is for each per-reply read. After Options.init a
	// user's disabled timeout (ReadTimeout:-1) is 0, and WithReader/deadline() treat
	// <= 0 as "no deadline" — so a disabled timeout stays disabled instead of being
	// clamped to a fixed 30s (options.go maps -1 -> 0; a default client keeps its
	// positive 5s, which bounds each per-reply read). A genuinely stuck read is
	// still unblocked by the conn Close on the error path above (fdConnErr case).
	readTimeout := fd.client.opt.ReadTimeout
	var errOnce sync.Once
	var sharedErr error
	failOnce := func(e error) { errOnce.Do(func() { sharedErr = e }) }

	// Reader: read replies in FIFO order, completing each command as its reply
	// lands. Works a bounded front-snapshot per lock (amortizes the mutex over
	// many reads), then advances. On a connection/protocol error it stops and
	// leaves the unread tail in the deque (it becomes the unacked recovery set).
	go func() {
		defer close(readerDone)
		// done counts commands completed in the CURRENT frontBatch snapshot that
		// have not yet been advanced out of the in-flight deque; it is 0 outside the
		// inner read loop (reset at the top of each iteration, advanced at the end).
		done := 0
		// A reply decoder can panic (e.g. a RawWriteToCmd whose user io.Writer
		// panics while readReply streams the raw reply). Without recovery that would
		// crash the process and leave the outstanding tail incomplete. Recover and
		// mark the session failed (failOnce) so run() takes the connection-error
		// path: the reader exits, the unacked tail is recovered (retried per policy
		// or failed) and the conn is removed — mirroring recoverDispatchPanic on the
		// half-duplex async dispatchers. advance(done) FIRST so the commands already
		// completed in the panicking snapshot leave the deque: otherwise recovery
		// re-owns and re-completes them, overwriting good results and double-closing
		// hookDone (a second panic) when process hooks are installed.
		defer func() {
			if r := recover(); r != nil {
				inflight.advance(done)
				failOnce(fmt.Errorf("redis: autopipeline: panic in full-duplex reader: %v", r))
				internal.Logger.Printf(bg, "autopipeline: recovered full-duplex reader panic: %v\n%s", r, debug.Stack())
			}
		}()
		var buf []fdReq
		for {
			done = 0
			var ok bool
			buf, ok = inflight.frontBatch(buf)
			if !ok {
				return
			}
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
				// A retryable Redis error (LOADING/READONLY/CLUSTERDOWN/TRYAGAIN/…)
				// or a redirect (MOVED/ASK) is NOT the caller's final answer: the FD
				// conn is a single fixed socket/node, so re-run the command on the
				// client's NORMAL path, which routes redirects to the proper node
				// (cluster) and applies the standard retry/backoff (LOADING). Done off
				// the reader goroutine so it does not stall other in-flight replies,
				// and counted in `done` so the reader advances past it now. Per-caller
				// ordering is NOT promised across this divert — same exception as the
				// blocking-command divert. NoRetry commands keep their error.
				if e != nil && !req.cmd.NoRetry() {
					moved, ask, _ := isMovedError(e)
					// MOVED/ASK are redirects, not retries — always follow them (the
					// fixed FD socket cannot). A retryable error (LOADING/READONLY/...)
					// diverts only when retries are enabled: with MaxRetries < 0
					// (normalized to 0) the divert's process() would still run its
					// attempt zero, re-sending a command whose retry the caller
					// explicitly disabled — surface the Redis error instead.
					if moved || ask || (shouldRetry(e, false) && fd.client.opt.MaxRetries > 0) {
						fd.retryOnNormalConn(req)
						done++
						continue
					}
				}
				req.cmd.SetErr(e) // nil or a non-retryable Redis error (WRONGTYPE, …)
				// Record the per-command OTel duration metric (write→reply),
				// mirroring baseClient.process — the FD reader bypasses process, so
				// without this FD commands emit no native metric. Only for
				// inline-completed commands; a command diverted to the retry path
				// above runs through process, which emits its own.
				if cb := otel.GetOperationDurationCallback(); cb != nil {
					octx := req.ctx
					if octx == nil {
						octx = bg
					}
					cb(octx, time.Since(req.writtenAt), req.cmd, 1, e, cn, fd.client.opt.DB)
				}
				// Error-metric parity with the normal command path: an
				// inline-completed non-retryable Redis error (WRONGTYPE, NOPERM, …)
				// must reach the native error callback too — process() emits it via
				// classifyCommandError, and the FD reader bypasses process.
				if e != nil {
					if errorCallback := pool.GetMetricErrorCallback(); errorCallback != nil {
						errorType, statusCode, isInternal := classifyCommandError(e)
						errorCallback(bg, errorType, cn, statusCode, isInternal, 0)
					}
				}
				req.complete() // wake the caller, or hand off to the hook host
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

	// Writer: re-issue the recovered tail first, then serve the queue. The tail is
	// re-issued in the SAME MaxBatchSize/MaxBatchBytes-capped chunks as freshly
	// drained work — a recovered window can hold up to fd.window commands, so a
	// single flush would otherwise ignore MaxBatchBytes and hit the same
	// write-timeout/burst on the new connection.
	writeErr := fd.writeCarryChunked(bg, cn, inflight, carry, readerDone)
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
			// Priority check: Go's select picks randomly among ready cases, so with
			// work queued AND the reader gone (reply-decode panic, protocol error)
			// the main select below could take fd.ch and write a batch to a
			// connection the engine already knows has no reader — needlessly
			// enlarging the ambiguous at-least-once set. Check readerDone first.
			select {
			case <-readerDone:
				break serve // result stays fdConnErr; unacked tail is recovered
			default:
			}
			select {
			case req := <-fd.ch:
				batch := append(scratch[:0], req)
				batchBytes := cmdApproxBytes(req.cmd)
				// Cap this batch by the REMAINING window room, not just MaxBatchSize:
				// the backpressure gate above only ensures in-flight < window before
				// draining, so with FullDuplexWindow smaller than MaxBatchSize a
				// single drain could otherwise blow through the window (e.g.
				// window=1, batch=200 → 200 in flight). The first command always
				// goes (room is at least 1 after the gate).
				limit := fd.maxBatch
				if room := fd.window - inflight.len(); room < limit {
					limit = room
				}
			drain:
				for len(batch) < limit {
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
				// Max-hold reached. If the pipe is drained (nothing in-flight, nothing
				// queued), return fdIdle so run() blocks for the next command instead
				// of immediately re-leasing — otherwise a quiet engine configured with
				// FullDuplexMaxHold < FullDuplexIdleTimeout would Get/Put-churn (and
				// re-charge the Limiter/session hooks) every max-hold interval. With
				// work pending, recycle to keep serving.
				if inflight.empty() && len(fd.ch) == 0 {
					result = fdIdle
				} else {
					result = fdRecycle
				}
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
		// Clean Close: flush the accepted-but-unwritten fd.ch backlog on this
		// connection first (so Close honors "accepted ⇒ completes", matching the
		// half-duplex Close flush contract) rather than failing it ErrClosed, then
		// let the reader drain every in-flight reply to a RESP boundary. A write
		// error during the flush surfaces via the reader/sharedErr path below.
		if e := fd.flushBacklogForClose(bg, cn, inflight, readerDone); e != nil {
			// The backlog write failed partway: some flushed commands have no reply
			// coming, and writeCarryChunked pushed the unwritten suffix into inflight.
			// closeGraceful would leave the reader waiting on replies that never
			// arrive (hanging until ReadTimeout, or forever if it is disabled), so
			// take the connection-error path instead: close the conn to wake the
			// reader, then fail the whole unacked tail (the accepted suffix included).
			failOnce(e)
			inflight.hardClose()
			_ = cn.Close()
			<-readerDone
			fd.failReqs(inflight.takeRemaining(), e)
			return nil, fdConnErr, e
		}
		inflight.closeGraceful()
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
func (fd *fdEngine) writeBatch(bg context.Context, cn *pool.Conn, inflight *fdInflight, reqs []fdReq) (err error) {
	if len(reqs) == 0 {
		return nil
	}
	// A command encoder can panic (e.g. a user encoding.BinaryMarshaler whose
	// MarshalBinary panics while writeCmd serializes the args). This runs on the
	// FD writer goroutine (or run() during a carry replay / Close flush), where an
	// unrecovered panic would crash the process — the half-duplex dispatchers are
	// protected by recoverDispatchPanic. Convert it to a connection error: the
	// batch was pushed to the deque and the write may be partial, so the conn is
	// desynced and every caller settles through the normal conn-error recovery.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("redis: autopipeline: panic encoding full-duplex batch: %v", r)
			internal.Logger.Printf(bg, "autopipeline: recovered full-duplex write panic: %v\n%s", r, debug.Stack())
		}
	}()
	// Stamp the wire-write time so the reader can record write→reply as the
	// command's OTel operation duration. Done before pushBatch so the copies the
	// reader reads from the deque carry it.
	now := time.Now()
	for i := range reqs {
		reqs[i].writtenAt = now
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

// fdBatchEnd returns the exclusive end index of the next write chunk starting at
// `start`, applying the same caps as the drain loop: at most maxBatch commands,
// and (when byteLimit > 0) stop once the accumulated approximate payload reaches
// the limit — but always include the first command, so a lone oversized command
// still goes. Pure; the boundary logic is unit-tested.
func fdBatchEnd(reqs []fdReq, start, maxBatch int, byteLimit int64) int {
	end := start + 1
	bytes := cmdApproxBytes(reqs[start].cmd)
	for end < len(reqs) && end-start < maxBatch {
		if byteLimit > 0 && bytes >= byteLimit {
			break
		}
		bytes += cmdApproxBytes(reqs[end].cmd)
		end++
	}
	return end
}

// writeCarryChunked re-issues a recovered tail on a fresh connection in the same
// capped chunks as freshly drained work (see fdBatchEnd), so a large recovered
// window is not flushed in one oversized write. Returns the first write error.
func (fd *fdEngine) writeCarryChunked(bg context.Context, cn *pool.Conn, inflight *fdInflight, carry []fdReq, readerDone <-chan struct{}) error {
	byteLimit := int64(fd.ap.config.MaxBatchBytes) // 0 = disabled
	for i := 0; i < len(carry); {
		// Between chunks, stop if the reader is gone (decode panic, protocol
		// error mid-replay): writing further chunks to a reader-less connection
		// only enlarges the ambiguous at-least-once set — same priority rule as
		// the serve loop. Push the un-written remainder so takeRemaining recovers
		// the whole accepted set.
		if readerDone != nil {
			select {
			case <-readerDone:
				inflight.pushBatch(carry[i:])
				return errFDReaderGone
			default:
			}
		}
		end := fdBatchEnd(carry, i, fd.maxBatch, byteLimit)
		if e := fd.writeBatch(bg, cn, inflight, carry[i:end]); e != nil {
			// writeBatch pushed carry[i:end] into inflight before the failed write,
			// but the suffix carry[end:] was never pushed. Push it too so the whole
			// accepted set is recoverable: on the fdConnErr path takeRemaining replays
			// it, and on the Close path the caller fails it. Without this the suffix is
			// in neither fd.ch nor inflight and its callers hang. The suffix is
			// unwritten (writtenAt is zero), but it is only ever settled via failReqs
			// or replayed — never completed inline by the reader — so the reader's
			// write→reply OTel metric is not computed on it. This pairs with the Close
			// path closing the conn on a flush error (so the reader is woken rather
			// than waiting on replies that will never come).
			if end < len(carry) {
				inflight.pushBatch(carry[end:])
			}
			return e
		}
		i = end
	}
	return nil
}

// fdFirstNoRetry returns the index of the first NoRetry command in reqs, or
// len(reqs) when there is none. The unacked tail is retried up to this index and
// failed from it on, so retryable commands before a NoRetry still get their
// network retries while the NoRetry command (and anything ordered after it) is
// never re-sent — mirroring the half-duplex dispatcher's split of contiguous
// retry-policy runs.
func fdFirstNoRetry(reqs []fdReq) int {
	for i := range reqs {
		if reqs[i].cmd.NoRetry() {
			return i
		}
	}
	return len(reqs)
}

// failReqs completes a set of commands with err (used on retry exhaustion / Close).
func (fd *fdEngine) failReqs(reqs []fdReq, err error) {
	// Error-metric parity with the normal command path: commands terminated here
	// (lease failure, retry exhaustion, a NoRetry tail, Close) never reach the
	// reader's inline completion, so emit the native error callback per command —
	// process() does the same via classifyCommandError. One classification for
	// the whole set: every req fails with the same err. No duration metric: these
	// commands have no meaningful write→reply span (many were never written).
	errorCallback := pool.GetMetricErrorCallback()
	var errorType, statusCode string
	var isInternal bool
	if errorCallback != nil && len(reqs) > 0 {
		errorType, statusCode, isInternal = classifyCommandError(err)
	}
	for i := range reqs {
		// rawErr(), not Err(): this runs on the engine goroutine, and Err()
		// awaits batch.done — the very channel complete() closes just below — so
		// awaiting here would self-deadlock (the same trap hostHook documents).
		if reqs[i].cmd.rawErr() == nil {
			reqs[i].cmd.SetErr(err)
		}
		if errorCallback != nil {
			octx := reqs[i].ctx
			if octx == nil {
				octx = context.Background()
			}
			errorCallback(octx, errorType, nil, statusCode, isInternal, 0)
		}
		reqs[i].complete()
	}
}

// takeQueue closes the submit gate and returns everything buffered in fd.ch.
// It takes the WLock (blocking until in-flight submit sends finish — each
// either landed in fd.ch, drained below, or took its ctx.Done() branch), sets
// closed, then drains: after this no submit can enqueue new work, so nothing is
// left un-completed behind the drain.
//
// INVARIANT: every takeQueue call is a terminal shutdown drain — run() exits
// right after, and every caller is past a ctx-cancel check. Never call it on a
// non-close path: a submit blocked on a full channel is unwedged only by its
// ctx.Done() branch, so without a cancelled ctx the WLock here would deadlock
// against the RLock held across that send.
func (fd *fdEngine) takeQueue() []fdReq {
	fd.submitMu.Lock()
	fd.closed = true
	fd.submitMu.Unlock()
	var reqs []fdReq
	for {
		select {
		case r := <-fd.ch:
			reqs = append(reqs, r)
		default:
			return reqs
		}
	}
}

// shutdownFlush is the between-sessions Close flush: accepted commands sitting
// in carry (an unacked tail from a failed session, never re-leased) and fd.ch
// (accepted while no session held a connection) are dispatched as ONE pipeline
// batch on the client's normal pipeline path, then completed — honoring the
// "accepted ⇒ completes" Close contract exactly like the in-session
// flushBacklogForClose, instead of failing them ErrClosed just because Close
// won the race between sessions (#3964). Uses a background ctx (ap.ctx is
// already cancelled); processPipeline bounds it with the client's own
// timeouts/retries, and on failure the error lands on every command via
// setCmdsErr — callers always settle, never hang.
func (fd *fdEngine) shutdownFlush(bg context.Context, carry []fdReq) {
	backlog := append(carry, fd.takeQueue()...)
	if len(backlog) == 0 {
		return
	}
	// Same MaxBatchSize/MaxBatchBytes chunking as normal FD writes and the
	// in-session Close flush: the backlog can hold the carry plus the whole
	// channel (up to window commands), and one unchunked pipeline would ignore
	// MaxBatchBytes and hit the same oversized-write burst the caps exist to
	// prevent.
	byteLimit := int64(fd.ap.config.MaxBatchBytes) // 0 = disabled
	for i := 0; i < len(backlog); {
		end := fdBatchEnd(backlog, i, fd.maxBatch, byteLimit)
		cmds := make([]Cmder, end-i)
		for j := i; j < end; j++ {
			cmds[j-i] = backlog[j].cmd
		}
		_ = fd.client.processPipeline(bg, cmds) // per-command results/errors are set inside
		for j := i; j < end; j++ {
			backlog[j].complete()
		}
		i = end
	}
}

// failQueue fails every command currently buffered in fd.ch with err WITHOUT
// closing the engine, with the same native error-metric emission as failReqs —
// on fdLeaseErr/fdDenied the carry goes through failReqs and this drains the
// accepted backlog, and both halves must be visible to extra/redisotel-native.
// (Unlike takeQueue, which is the shutdown drain and sets
// closed). Used on an acquisition denial (Limiter reject): the accepted backlog
// is fail-fasted with the limiter error, but the engine stays alive to serve new
// work once the limiter admits again. A command submitted concurrently after this
// returns is failed on the next denied attempt or served once the limiter
// recovers. Channel receive is safe against a concurrent submit send, so no lock
// is taken here.
func (fd *fdEngine) failQueue(err error) {
	errorCallback := pool.GetMetricErrorCallback()
	var errorType, statusCode string
	var isInternal bool
	classified := false
	for {
		select {
		case r := <-fd.ch:
			r.cmd.SetErr(err)
			if errorCallback != nil {
				if !classified {
					errorType, statusCode, isInternal = classifyCommandError(err)
					classified = true
				}
				octx := r.ctx
				if octx == nil {
					octx = context.Background()
				}
				errorCallback(octx, errorType, nil, statusCode, isInternal, 0)
			}
			r.complete()
		default:
			return
		}
	}
}

// flushBacklogForClose is the graceful-Close flush. It stops new submits (sets
// closed) and re-issues every command still buffered in fd.ch on the current
// connection, in the same MaxBatchSize/MaxBatchBytes chunks as normal writes, so
// ACCEPTED commands complete instead of failing ErrClosed — matching half-duplex
// Close's "flush accepted work" contract. The caller then closeGraceful()s the
// deque so the reader drains these replies before exiting. Returns the first
// write error (the caller degrades to the connection-error path on failure).
func (fd *fdEngine) flushBacklogForClose(bg context.Context, cn *pool.Conn, inflight *fdInflight, readerDone <-chan struct{}) error {
	fd.submitMu.Lock()
	fd.closed = true
	fd.submitMu.Unlock()
	var backlog []fdReq
	for {
		select {
		case r := <-fd.ch:
			backlog = append(backlog, r)
		default:
			return fd.writeCarryChunked(bg, cn, inflight, backlog, readerDone)
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
