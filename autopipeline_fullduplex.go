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
// Half-duplex runs one batch per round trip, so a slow link caps throughput at
// batch/RTT and late arrivals wait a full RTT behind the in-flight batch.
// Full-duplex holds ONE pipeline-pool connection with a writer+reader goroutine
// pair: the writer streams command groups back-to-back without waiting for
// reads, the reader drains replies in FIFO order and completes each command as
// its reply lands — ~1 RTT latency, pipe-saturated throughput.
//
// Ordering contract: each goroutine's commands execute in the order it submitted
// them; nothing is promised between goroutines. The submit channel is MPSC (one
// goroutine's sequential submits arrive in order) and one connection is FIFO on
// the wire, so in-flight deque position is the reply-matching key.
//
// Retries: on a connection failure the unacked tail (in order) is re-issued on a
// fresh connection ahead of newly queued work, respecting
// shouldRetry/MaxRetries/backoff AND the per-command NoRetry flag — a NoRetry
// command in the tail fails the tail instead of replaying it (half-duplex does
// the same via cmdsContainNoRetry). After exhaustion those commands are failed
// and the engine keeps serving on a fresh connection. Same at-least-once contract
// as a normal Pipeline: a command whose write landed but whose reply was lost may
// re-execute (matters for non-idempotent writes); the ambiguous set is only the
// unacked tail.
//
// Gated by AutoPipelineOptions.FullDuplex, honored on the ordered single-shard
// faces of a standalone *Client with a pipeline pool and tuned by the
// FullDuplex* options (see their GoDoc). RESP3 push frames are demuxed inline;
// cluster support and window auto-tune are follow-ups (see
// AP_ORDERED_FULLDUPLEX_DESIGN.md).

var errFDReaderGone = errors.New("redis: autopipeline full-duplex reader exited")

// errFDPanicRecovered marks a session failure caused by a recovered panic
// (reply decode, batch encode). Wrapped with %w so the retry decision can
// recognize it: the connection is desynced exactly like a transport error, and
// the unacked tail — mostly commands the panic never touched — must be REPLAYED
// on a fresh connection, not failed (shouldRetry alone would reject these plain
// error values and permanently fail innocent in-flight commands).
var errFDPanicRecovered = errors.New("redis: autopipeline: full-duplex panic recovered")

// errFDConnMoving signals that carry replay stopped early because the held
// connection was marked for handoff (MOVING/FAILING_OVER) while a recovered carry
// was still being written. The connection is still alive, so writeCarryChunked
// returns the UNWRITTEN suffix out-of-band (NOT pushed into the in-flight deque);
// the session then drains the already-written prefix to completion (those callers
// get real replies — never re-executed) and Puts the connection back through a
// clean fdRecycle, so the maintnotifications OnPut hook performs the seamless
// handoff (queueHandoff + MarkQueuedForHandoff clears ShouldHandoff, so the conn is
// re-usable and the worker moves it to the new endpoint). ONLY the never-sent
// suffix is replayed on the next lease. Contrast errFDReaderGone / write errors,
// where the connection is dead: there the suffix IS pushed into the deque and the
// whole unacked tail is recovered and replayed, because a clean drain is impossible.
var errFDConnMoving = errors.New("redis: autopipeline: full-duplex connection moving")

// Full-duplex tuning defaults, applied by newFDEngine when the corresponding
// AutoPipelineOptions field is zero (rationale in the FullDuplex* GoDoc). The
// window must exceed the bandwidth-delay product (RTT × target rate) or it
// throttles throughput; the deque holds only ACTUAL in-flight, so a generous
// default costs no memory until a stalled peer makes in-flight grow.
const (
	fdDefaultWindow  = 65536
	fdDefaultIdle    = time.Second
	fdDefaultMaxHold = 5 * time.Second
)

// fdCloseFlushWait bounds how long the graceful-Close backlog flush waits for
// the reader to drain below the window before giving up the window bound (see
// writeCarryChunked). Long enough that a live reader always drains within it,
// short enough that a stuck reader (quiet peer, ReadTimeout disabled) does not
// stall Close.
const fdCloseFlushWait = time.Second

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
// hookDone is non-nil only when the client has process hooks: the command then
// has a host goroutine (hostHook) running the hook chain, and finalizing closes
// hookDone instead of the batch — the host closes the batch after the hook
// returns, so the hook brackets the command and can rewrite its result before the
// waiter wakes. Nil (the hook-free fast path) finalizes the batch directly.
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
	advanced   int           // total entries the reader completed this session (progress signal)
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
	// array (curInflight holds the deque while the engine idles), so otherwise a
	// drained burst retains a window's worth of completed fdReq values — command
	// args, caller contexts, batches — until the next session overwrites them.
	for i := 0; i < n; i++ {
		f.q[i] = fdReq{}
	}
	f.q = f.q[n:]
	f.advanced += n
	f.mu.Unlock()
	select {
	case f.room <- struct{}{}:
	default:
	}
}

// advancedTotal reports how many commands the reader completed this session —
// the progress signal that resets the reconnect retry budget (a session that
// completed work makes the next connection drop a NEW failure, not a
// consecutive one).
func (f *fdInflight) advancedTotal() int {
	f.mu.Lock()
	n := f.advanced
	f.mu.Unlock()
	return n
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
// It deliberately does NOT take the queue: the caller must wait for the reader to
// exit (<-readerDone) and THEN call takeRemaining, so every entry stays owned by
// exactly one of {the reader completed it, recovery replays/fails it}. A
// concurrent grab could scoop an entry the reader had completed but not yet
// advanced, handing an already-executed command to the retry loop — a double
// execution and, on the hooked path, a double close of hookDone (panic).
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
	curConn     atomic.Pointer[pool.Conn]  // current session's held conn; test observability (handoff)

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
	// The submit queue does not need window-sized storage: backpressure is
	// enforced by the in-flight deque (which grows only with ACTUAL in-flight),
	// while a buffered channel allocates its full capacity up front — at the
	// default window that is several MiB per engine before any command is
	// submitted. Cap the queue; total outstanding stays bounded by cap+window
	// and submit simply blocks a little earlier under a burst.
	chCap := w
	if chCap > 4096 {
		chCap = 4096
	}
	return &fdEngine{
		ap:       ap,
		client:   client,
		pool:     client.getPipelinePool(),
		ch:       make(chan fdReq, chCap),
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
// With process hooks installed a per-command host goroutine runs the hook chain
// (see hostHook) and ctx parents its span; the hook-free path skips that
// goroutine and channel entirely.
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
	// run()'s shutdown drain (takeQueue: WLock, set closed, drain fd.ch). Once the
	// final drain has run no new req can land in fd.ch, where it would never be
	// completed and would hang its caller forever. A send blocked on a full channel
	// is released by the ctx.Done() branch below, so holding the RLock cannot wedge
	// the WLock.
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
		// Accepted. Start the hook host ONLY now: a submission that is never admitted
		// (the cancel paths below) must not leak a host goroutine. The Add happens under
		// the gate, so it is ordered before the shutdown drain's WLock and run()'s
		// hostWg.Wait never races an Add on a zero counter.
		if hookDone != nil {
			fd.hostWg.Add(1)
			go fd.hostHook(ctx, cmd, b, hookDone)
		}
		fd.submitMu.RUnlock()
		return b
	case <-ctx.Done():
		// Caller's ctx expired while backpressured (window/channel full): honor it
		// instead of blocking until room or Close (#3964). Not admitted and no host
		// started, so this is a submit-time failure — return the completedBatch sentinel
		// so raw Process(ctx,cmd) reports the ctx error.
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
// own goroutine, started only when hookCount()>0. The chain starts at ≈ submit
// time and its next() blocks until the reader (or a failure/close path) signals
// hookDone, so an observing hook spans the command's real write→reply latency and
// a hook that rewrites the result is honored before the waiter wakes. Each
// command is reported individually (withProcessHook), not as a pipeline batch.
func (fd *fdEngine) hostHook(ctx context.Context, cmd Cmder, b *apBatch, hookDone chan struct{}) {
	// Declared first so it runs last: Close waits hostWg (via run()), and the host
	// is done only after the recover defer below has also run.
	defer fd.hostWg.Done()
	// Mark this goroutine as the batch's executor so a hook that reads its own
	// command's result after next() (cmd.Err(), a documented pattern) sees the
	// just-executed view instead of blocking on batch.done — which only THIS
	// goroutine closes, below, so without the mark such a hook self-deadlocks.
	// Mirrors runOutsidePipeline's async dispatch guard.
	if fd.ap.armSelfDeadlockGuard() {
		b.dispGid.Store(curGoroutineID())
	}
	// A user ProcessHook runs on this goroutine; an unrecovered panic here would
	// crash the process (and leave the caller blocked on b). Recover, fail the
	// command, and close the batch so the waiter always wakes — mirroring the
	// dispatch path's recoverDispatchPanic.
	// awaited tracks whether hookDone has already been received, so no path awaits
	// it twice. hookDone is closed (not sent) by complete(), so a second receive is
	// harmless in practice, but tracking it keeps the recover path unambiguously
	// free of a redundant await regardless of where a panic lands.
	awaited := false
	defer func() {
		if r := recover(); r != nil {
			// The command was already streamed, so the reader still owns cmd and will
			// write its reply into it. If hookDone was not yet awaited (a panic before
			// next(), or before the short-circuit await below), await it here so the
			// reader's writes happen-before the caller's reads.
			if !awaited {
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
		<-hookDone // reply landed (or the command was failed)
		awaited = true
		return cmd.rawErr() // direct read: cmd.Err() would await batch.done, which
		//                     this goroutine itself closes below → self-deadlock.
	})
	// A hook that SHORT-CIRCUITS (returns without calling next) never received
	// hookDone, but the command is already on the wire and the reader will still
	// write into cmd: await that before releasing the caller. The command still
	// executed; the hook's error is honored anyway (see the FullDuplex GoDoc).
	if !awaited {
		<-hookDone
		awaited = true
	}
	cmd.SetErr(err) // honor a hook that rewrote / short-circuited the result
	b.close()       // now wake the waiter
}

// retryOnNormalConn re-runs a full-duplex command that came back with a retryable
// Redis error (LOADING/READONLY/…) or a redirect (MOVED/ASK) on the client's
// NORMAL path: that path routes redirects to the proper node and applies the
// standard retry/backoff, neither of which the fixed single-conn FD socket can
// do. It runs on its own goroutine so it does not stall the FD reader, is tracked
// by retryWg so Close waits for it, and settles the FD request with the outcome.
// process() is the raw exec (no hook chain) — with hooks installed the FD
// hostHook still brackets the command and reports via req.complete(). Background
// ctx: the command was already accepted, so it completes even under a Close.
// retryStartAttempt returns the normal-path retry loop's starting attempt for an
// FD command diverted after a MOVING/ASK redirect (0 — the command did not execute
// on the FD socket, so it gets the full MaxRetries+1 budget) or a retryable reply
// such as LOADING/READONLY/TRYAGAIN (1 — the initial attempt was already spent on
// the FD socket, so counting it keeps the total at MaxRetries+1, not +2).
func retryStartAttempt(moved, ask bool) int {
	if moved || ask {
		return 0
	}
	return 1
}

func (fd *fdEngine) retryOnNormalConn(req fdReq, startAttempt int) {
	// Bound concurrent off-pipe retries to the FD window: a sustained retryable
	// stream would otherwise spawn one goroutine per reply, all parked in
	// backoff/pool acquisition. Blocking here blocks the READER, which stops
	// advancing the deque, which fills the window and blocks the writer and then
	// submitters — end-to-end backpressure. No cycle: retries drain on the main
	// pool, independent of the reader waiting here.
	// Interruptible acquire: the reader must not park here past Close. A wait is
	// otherwise BOUNDED — every slot holder is a retry running through process(),
	// whose own timeouts/backoff guarantee it releases. On Close, though, the
	// command was already ACCEPTED and came back with a retryable/redirect reply,
	// so failing it ErrClosed would break Close's accepted-work drain contract (and
	// every subsequent retryable reply in the tail would hit the same path). So on
	// ap.ctx cancellation, run the retry WITHOUT the sem bound — there is no
	// sustained new load during teardown — still tracked by retryWg so Close waits
	// for it to settle with its real outcome.
	acquired := false
	select {
	case fd.retrySem <- struct{}{}:
		acquired = true
	case <-fd.ap.ctx.Done():
	}
	fd.retryWg.Add(1)
	go func() {
		defer func() {
			if acquired {
				<-fd.retrySem
			}
			fd.retryWg.Done()
		}()
		// process runs user code (hooks, arg encoders) and can panic; without
		// recovery the batch never completes and the caller (and a hooked command's
		// host, parked on hookDone) waits forever.
		defer func() {
			if r := recover(); r != nil {
				req.cmd.SetErr(fmt.Errorf("redis: autopipeline: panic in full-duplex off-pipe retry: %v", r))
				internal.Logger.Printf(context.Background(),
					"autopipeline: recovered full-duplex retry panic: %v\n%s", r, debug.Stack())
				req.complete()
			}
		}()
		// Retry on the caller's context with cancellation removed (like the FD
		// lease init): a CredentialsProviderContext derives credentials from
		// context values, so context.Background() here would reject the retry or
		// authenticate it as the wrong identity even though the FD session
		// initialized correctly. WithoutCancel keeps the values but drops the
		// caller's deadline/cancel, so the accepted command still completes its
		// retry under a Close.
		rctx := context.Background()
		if req.ctx != nil {
			rctx = context.WithoutCancel(req.ctx)
		}
		// processStartingAt, not pipeliner.process: fd.client IS the pipeliner
		// (fdClient is the *Client behind it), so this is the same raw exec, but it
		// starts the retry loop at startAttempt — 1 for a retryable reply that already
		// spent an attempt on the FD socket, 0 for a redirect that did not execute.
		err := fd.client.processStartingAt(rctx, req.cmd, startAttempt)
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
	// Same for per-command hook hosts: a ProcessHook doing work after next()
	// closes the command's batch on its host goroutine, so Close must not return
	// while one runs. Every hostWg.Add is gated behind submitMu+closed, and every
	// run() return follows the shutdown drain, so this never races a live Add.
	defer fd.hostWg.Wait()
	bg := context.Background()
	var carry []fdReq // unacked tail to re-issue at the start of the next attempt
	// Two SEPARATE budgets, each counting only CONSECUTIVE failures of its own
	// kind: a shared counter would let transient lease failures eat the reconnect
	// budget, so the first genuine mid-session drop would fail the whole unacked
	// tail with zero replay attempts. leaseAttempts resets whenever a session
	// actually ran; retryAttempts resets on a clean session end (idle/recycle).
	leaseAttempts := 0 // consecutive fdLeaseErr/fdDenied acquisition failures
	retryAttempts := 0 // consecutive fdConnErr tail-replay failures
	for {
		if fd.ap.ctx.Err() != nil {
			fd.shutdownFlush(bg, carry)
			return
		}
		// Never lease a connection (or hit the Limiter / dial) without work in hand:
		// block for the first command whenever the carry is empty. That covers the
		// initial entry, the fdIdle return, and the fail-fast exits below (fdDenied /
		// exhausted fdLeaseErr / failed fdConnErr tail), which would otherwise loop
		// straight back into attempt against an empty queue — hammering the Limiter or
		// dialing a down server forever. Work already queued makes this non-blocking,
		// so fdRecycle re-leases immediately; an empty recycle parks here.
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
			// Conn returned cleanly to the pool (its per-conn hooks can run); the
			// loop-top wait keeps an idle engine from churning Get/Put.
			carry, leaseAttempts, retryAttempts = nil, 0, 0
		case fdRecycle:
			// Conn returned cleanly (max-hold, or a mid-carry handoff clean recycle).
			// unacked carries the never-sent suffix from a handoff recycle (nil for a
			// plain max-hold recycle); replay it on the next lease — it was never
			// written, so no command is re-executed. A handoff-marked conn was handed
			// off by the OnPut hook when attempt() Put it.
			fd.recycles.Add(1)
			carry, leaseAttempts, retryAttempts = unacked, 0, 0
		case fdLeaseErr:
			// Could not lease/init a connection for a new session (server down, pool
			// saturated). Retry for a transient outage; once retries are exhausted,
			// fail-fast the carry tail AND the fd.ch backlog rather than leaving accepted
			// commands buffered indefinitely, and stay alive to serve again once the
			// server/pool recovers. Replaying the carry wholesale is safe: it is already
			// NoRetry-split (or nil) and was never written on a new conn.
			// Close racing the lease surfaces here as a lease failure (the acquisition ctx
			// is cancelled), so flush the accepted work through the normal pipeline path
			// instead of failing it with a canceled error.
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
			// Same Close-race guard as fdLeaseErr: on shutdown, flush instead of failing
			// accepted work with a denial that only exists because Close interrupted the
			// acquisition.
			if fd.ap.ctx.Err() != nil {
				fd.shutdownFlush(bg, carry)
				return
			}
			// The Limiter denied acquisition: fail-fast the carry tail AND the whole fd.ch
			// backlog with the limiter error (as the plain-client getConn path does)
			// instead of leaving them buffered until the breaker closes. The engine stays
			// alive and backs off, so it serves again once the limiter admits.
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
			// A session that COMPLETED work (advanced the deque — including a
			// successful carry replay) makes this drop a new failure, not a
			// consecutive one: reset the reconnect budget so long-lived sessions
			// under continuous traffic do not inherit stale failure counts.
			if fi := fd.curInflight.Load(); fi != nil && fi.advancedTotal() > 0 {
				retryAttempts = 0
			}
			// retryTimeout=true: a read/write timeout is a retryable connection
			// failure here (re-issue the unacked tail on a fresh conn), matching
			// the cluster pipeline retry paths — otherwise a single WAN timeout
			// fails the whole tail. The engine's internal failure markers
			// (recovered panics, reader-gone) desync the conn exactly like a
			// transport error and the tail is mostly commands they never touched,
			// so they are replayable too — shouldRetry alone would reject them and
			// permanently fail innocent in-flight commands. The NoRetry guard
			// below still protects non-idempotent writes.
			replayable := shouldRetry(aerr, true) ||
				errors.Is(aerr, errFDReaderGone) || errors.Is(aerr, errFDPanicRecovered)
			if len(unacked) > 0 && replayable &&
				retryAttempts < fd.client.opt.MaxRetries {
				// Split at the first NoRetry command: replay the retryable PREFIX and fail
				// that command plus everything ordered after it (a NoRetry command must
				// never be re-sent). With NoRetry first (n==0) nothing is retryable ahead
				// of it, so fall through and fail the whole tail.
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
	// Per-session Limiter: FD acquires ONE conn per session, so Allow() once here —
	// if it rejects, do not acquire and do not report (mirrors getConn). Otherwise
	// ReportResult exactly once at release and BEFORE the conn becomes available
	// again, so a breaker sees the failure before admitting the next session; that
	// is why the report and the release share ONE deferred func, in that order.
	limited := fd.client.opt.Limiter != nil
	if limited {
		if err := fd.client.opt.Limiter.Allow(); err != nil {
			// Denied (e.g. an open circuit breaker): report nothing (no conn was
			// acquired) and signal fdDenied so run() fail-fasts the carry and the fd.ch
			// backlog instead of leaving them buffered until the breaker closes.
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
		// classified bad-conn, yet the conn is still unusable. Clean ends (including a
		// handoff recycle) go through releaseConnToPool: it drains pending pushes and
		// Puts, so the OnPut hook can perform the maintenance handoff on a marked conn.
		if result == fdConnErr {
			fd.pool.Remove(bg, cn, aerr)
		} else {
			fd.client.releaseConnToPool(bg, fd.pool, cn, nil)
		}
	}()

	// Acquire under ap.ctx (not bg): if Close cancels ap.ctx while this Get is
	// blocked on a saturated pool it returns at once instead of waiting out
	// PoolTimeout, so shutdown is not delayed. Everything after — init and the
	// session I/O — stays on bg so already-accepted commands still complete during
	// Close (Close waits for run() via ap.wg).
	cn, aerr = fd.pool.Get(fd.ap.ctx)
	if aerr != nil {
		cn = nil
		return carry, fdLeaseErr, aerr
	}
	// Init + acquire through initPooledConn rather than hand-inlining
	// initConn/TryAcquire (the main and pipeline paths drift when mirrored by hand).
	// It records the create-time metric, unwraps the init error, and Removes the
	// conn on any failure (so the defer, seeing cn=nil, does not double-release); it
	// does NOT Put the conn, which suits the held-conn model.
	//
	// Initialize with the SESSION-INITIATING caller's context (values only, via
	// WithoutCancel), not context.Background(): a CredentialsProviderContext
	// derives credentials from context values, and Background made those invisible
	// so such providers rejected FD sessions or authed with fallback identity.
	// Full-duplex holds ONE connection for MANY callers, so credentials are
	// necessarily SESSION-scoped (the first caller's), not per-call — the same
	// limitation every shared/pooled connection has; documented on FullDuplex.
	// WithoutCancel keeps the values but drops the caller's deadline/cancel, so one
	// caller's ctx expiry cannot abort an init the whole session depends on.
	initCtx := bg
	if len(carry) > 0 && carry[0].ctx != nil {
		initCtx = context.WithoutCancel(carry[0].ctx)
	}
	if e := fd.client.initPooledConn(initCtx, fd.pool, cn); e != nil {
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
	fd.curConn.Store(cn)           // test observability (handoff)
	defer fd.curConn.Store(nil)
	readerDone := make(chan struct{})

	// Honor opt.ReadTimeout as-is for each per-reply read: options.go maps a
	// disabled timeout (-1) to 0 and WithReader treats <= 0 as "no deadline", so
	// disabled stays disabled instead of being clamped to some fixed value (a
	// default client keeps its 5s, which bounds each read). A genuinely stuck read
	// is still unblocked by the conn Close on the fdConnErr path.
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
		// A reply decoder can panic (e.g. a RawWriteToCmd whose user io.Writer panics
		// while readReply streams the raw reply). Recover and mark the session failed
		// (failOnce) so run() takes the connection-error path: the reader exits, the
		// unacked tail is recovered and the conn is removed. advance(done) FIRST so
		// commands already completed in the panicking snapshot leave the deque —
		// otherwise recovery re-owns and re-completes them, overwriting good results
		// and double-closing hookDone (a second panic) when hooks are installed.
		defer func() {
			if r := recover(); r != nil {
				inflight.advance(done)
				failOnce(fmt.Errorf("%w: reader: %v", errFDPanicRecovered, r))
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
					// Drain RESP3 push frames buffered ahead of this reply so a push is
					// never misread as the command's reply (FIFO misalign). A drain error
					// is logged, NOT propagated (as in flushBatch): a custom push
					// processor's error must not fail this unrelated in-flight command or
					// kill the connection. A real transport error resurfaces in readReply.
					if perr := fd.client.processPendingPushNotificationWithReader(bg, cn, rd); perr != nil {
						internal.Logger.Printf(bg, "autopipeline: full-duplex push drain: %v", perr)
					}
					return req.cmd.readReply(rd)
				})
				if e != nil && !isRedisError(e) {
					rerr = e // connection/protocol error: stop; unread tail stays
					break
				}
				// A retryable Redis error or a redirect (MOVED/ASK) is NOT the caller's
				// final answer: the FD conn is one fixed socket/node, so re-run the
				// command on the client's NORMAL path, which routes redirects and applies
				// the standard retry/backoff. Done off the reader goroutine so it does not
				// stall other in-flight replies, and counted in `done` so the reader
				// advances past it now. Per-caller ordering is NOT promised across this
				// divert (same exception as the blocking-command divert); NoRetry commands
				// keep their error.
				if e != nil && !req.cmd.NoRetry() {
					moved, ask, _ := isMovedError(e)
					// MOVED/ASK are redirects, not retries — always follow them (the fixed
					// FD socket cannot). A retryable error diverts only when retries are
					// enabled: with MaxRetries normalized to 0 the divert's process() would
					// still run attempt zero, re-sending a command whose retry the caller
					// explicitly disabled — surface the Redis error instead.
					if moved || ask || (shouldRetry(e, false) && fd.client.opt.MaxRetries > 0) {
						fd.retryOnNormalConn(req, retryStartAttempt(moved, ask))
						done++
						continue
					}
				}
				req.cmd.SetErr(e) // nil or a non-retryable Redis error (WRONGTYPE, …)
				// Per-command OTel duration (write→reply): the FD reader bypasses
				// process, which is what normally emits it. Inline-completed commands
				// only — a diverted command emits its own through process.
				// req.ctx carries the caller's span for telemetry correlation
				// (exemplars, context-scoped attrs); fall back to bg only when nil.
				// Shared by the duration and error callbacks so both attribute to the
				// request context, matching process().
				octx := req.ctx
				if octx == nil {
					octx = bg
				}
				if cb := otel.GetOperationDurationCallback(); cb != nil {
					cb(octx, time.Since(req.writtenAt), req.cmd, 1, e, cn, fd.client.opt.DB)
				}
				// Same parity for errors: an inline-completed non-retryable Redis error
				// (WRONGTYPE, NOPERM, …) must reach the native error callback, which
				// process() would otherwise emit via classifyCommandError.
				if e != nil {
					if errorCallback := pool.GetMetricErrorCallback(); errorCallback != nil {
						errorType, statusCode, isInternal := classifyCommandError(e)
						errorCallback(octx, errorType, cn, statusCode, isInternal, 0)
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

	// Writer: re-issue the recovered tail first, then serve the queue. The tail goes
	// in the SAME MaxBatchSize/MaxBatchBytes-capped chunks as freshly drained work —
	// it can hold up to fd.window commands, so one flush would ignore MaxBatchBytes
	// and hit a write-timeout/burst on the new connection.
	carrySuffix, writeErr := fd.writeCarryChunked(bg, cn, inflight, carry, readerDone)
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
				// Poll handoff here too, not just after the gate: under sustained
				// backpressure the writer can sit in this loop, and room fires on
				// every reader advance, so a MOVING mark is observed within one
				// drained reply instead of waiting for max-hold.
				if cn.ShouldHandoff() {
					result = fdRecycle
					break serve
				}
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
			// Go's select picks randomly among ready cases, so with work queued AND
			// the reader gone (decode panic, protocol error) the main select below
			// could write a batch to a connection known to have no reader — needlessly
			// enlarging the ambiguous at-least-once set. Check readerDone first.
			select {
			case <-readerDone:
				break serve // result stays fdConnErr; unacked tail is recovered
			default:
			}
			// A maintenance MOVING/FAILING_OVER push (drained by the reader) marks
			// the held connection for handoff. The pool queues the handoff only when
			// the conn is Put back, so end this session promptly with a CLEAN recycle
			// (drain in-flight to a RESP boundary, then Put) instead of continuing to
			// write to a node known to be moving until idle/max-hold — otherwise the
			// handoff can miss its deadline. ShouldHandoff() is an atomic load, so it
			// is safe to poll here while the reader sets it; room fires on every
			// reader advance, so a writer parked on the window gate above re-checks
			// this within one drained reply.
			if cn.ShouldHandoff() {
				result = fdRecycle
				break serve
			}
			select {
			case req := <-fd.ch:
				batch := append(scratch[:0], req)
				batchBytes := cmdApproxBytes(req.cmd)
				// Cap this batch by the REMAINING window room, not just MaxBatchSize:
				// the gate above only ensures in-flight < window before draining, so a
				// window smaller than MaxBatchSize would let one drain blow through it
				// (window=1, batch=200 → 200 in flight). The first command always goes
				// (room is >= 1 after the gate).
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
				// in-flight drained. Otherwise the timer fired mid-stream (e.g. a long
				// flush) — re-arm and keep the hot session.
				if inflight.empty() && len(fd.ch) == 0 {
					result = fdIdle
					break serve
				}
				resetIdle()
			case <-maxC:
				// Max-hold reached. With the pipe drained (nothing in-flight, nothing
				// queued) return fdIdle so run() blocks for the next command: otherwise a
				// quiet engine with FullDuplexMaxHold < FullDuplexIdleTimeout would
				// Get/Put-churn (and re-charge the Limiter/session hooks) every interval.
				// With work pending, recycle to keep serving.
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
		if errors.Is(writeErr, errFDConnMoving) {
			// Handoff mid-carry-replay on a LIVE conn. Route to the clean fdRecycle arm
			// below: the reader drains the already-written prefix so those callers
			// complete normally (not re-executed), attempt then PUTS the conn so the
			// maintnotifications OnPut hook performs the seamless handoff, and run()
			// replays only the never-sent carrySuffix. Do NOT failOnce — nothing failed.
			result = fdRecycle
		} else {
			failOnce(writeErr)
			result = fdConnErr
		}
	}

	switch result {
	case fdGraceful:
		// Clean Close: flush the accepted-but-unwritten fd.ch backlog on this
		// connection first, so Close honors "accepted ⇒ completes" instead of failing
		// it ErrClosed, then let the reader drain every in-flight reply to a RESP
		// boundary. A flush write error surfaces via the sharedErr path below.
		if e := fd.flushBacklogForClose(bg, cn, inflight, readerDone); e != nil {
			// The backlog write failed partway: some flushed commands have no reply
			// coming and the unwritten suffix is in inflight (flushBacklogForClose pushes
			// it back on any early stop), so closeGraceful would park the reader on
			// replies that never arrive. Take the connection-error path: close the conn to
			// wake the reader, then fail the whole unacked tail (accepted suffix included).
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
			// Reader failed during the final drain: fail the stranded tail (its callers
			// would hang) and report the error so attempt() removes the desynced conn.
			// run() exits on its next loop, so this does not retry.
			fd.failReqs(inflight.takeRemaining(), sharedErr)
			return nil, fdConnErr, sharedErr
		}
		return nil, fdGraceful, nil
	case fdIdle, fdRecycle:
		// Clean return: no more pushes, the reader drains the remaining replies (the
		// already-written carry PREFIX included, so those callers complete normally and
		// are NOT replayed), then the conn is at a RESP boundary and safe to Put back —
		// a handoff-marked conn is handed off seamlessly by the OnPut hook, and run()
		// replays only the never-sent suffix (carrySuffix).
		inflight.closeGraceful()
		<-readerDone
		if sharedErr != nil {
			// Reader failed while draining for the clean return: recover the unacked tail
			// for replay and report the error so the conn is removed instead of reused
			// poisoned. Append carrySuffix (never-sent) after the drained tail, in order.
			// Fresh slice — takeRemaining returns a deque-owned backing array, so
			// appending onto it could corrupt the deque.
			rem := inflight.takeRemaining()
			out := make([]fdReq, 0, len(rem)+len(carrySuffix))
			out = append(out, rem...)
			out = append(out, carrySuffix...)
			return out, fdConnErr, sharedErr
		}
		// carrySuffix is the never-sent suffix to replay on the next lease (nil for a
		// plain idle/recycle; the unwritten tail on a handoff recycle).
		return carrySuffix, result, nil
	default: // fdConnErr
		// Stop the reader, wait for it to exit, THEN take the unacked tail: the reader
		// advances every command it completes, so taking only after <-readerDone is
		// what keeps an entry from being owned by both sides.
		//
		// Close the connection before waiting: on a WRITE error the reader is
		// typically parked in WithReader awaiting a reply that will never arrive, and
		// hardClose only wakes a reader parked in frontBatch. Closing makes that read
		// return at once so recovery does not stall for the read deadline; attempt()
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
	// A command encoder can panic (e.g. a user BinaryMarshaler failing while
	// writeCmd serializes the args) on the writer goroutine, where it would crash
	// the process. Convert it to a connection error: the batch is already in the
	// deque and the write may be partial, so the conn is desynced and every caller
	// settles through the normal conn-error recovery.
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: encoding batch: %v", errFDPanicRecovered, r)
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
// window is not flushed in one oversized write.
//
// Returns (unwritten, err):
//   - (nil, nil): the whole carry was written.
//   - (suffix, errFDConnMoving): the connection was marked for handoff mid-replay
//     on a still-alive connection. The unwritten suffix is returned OUT-OF-BAND
//     (not pushed into inflight) so the caller can drain the already-written prefix
//     to completion, REMOVE the moving connection, and replay ONLY the never-sent
//     suffix on a fresh connection — the written prefix is not re-executed.
//   - (nil, errFDReaderGone | write error): the connection is dead; the unwritten
//     suffix is pushed into inflight so the whole unacked tail is recovered and
//     replayed at-least-once (a clean drain is impossible).
func (fd *fdEngine) writeCarryChunked(bg context.Context, cn *pool.Conn, inflight *fdInflight, carry []fdReq, readerDone <-chan struct{}) (unwritten []fdReq, err error) {
	byteLimit := int64(fd.ap.config.MaxBatchBytes) // 0 = disabled
	// stuck becomes true if the reader is observed not draining under a full
	// window; from then on we stop window-gating and just write, so a graceful
	// Close cannot hang here waiting on a reader that never advances (a quiet peer
	// with ReadTimeout disabled). A truly stuck reader is caught downstream by the
	// graceful-drain <-readerDone / Close backstop.
	stuck := false
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
				return nil, errFDReaderGone
			default:
			}
		}
		// Between chunks, stop if the connection was marked for handoff, exactly as
		// the serve loop polls ShouldHandoff: a MOVING/FAILING_OVER push drained by
		// the reader marks cn, and carry replay runs BEFORE the serve loop, so a
		// large replay must not keep streaming to a moving node. The connection is
		// still ALIVE, so return the unwritten suffix OUT-OF-BAND (do NOT push it into
		// inflight): the caller drains the already-written prefix to completion (no
		// re-execution), REMOVES the moving connection, and replays only the suffix on
		// a fresh one. ShouldHandoff() is an atomic load, safe to poll here.
		if cn.ShouldHandoff() {
			return carry[i:], errFDConnMoving
		}
		// Bound in-flight to the window between chunks, like the serve loop — which
		// uses a PREDICATE LOOP, not a one-shot wait: inflight.room is a cap-1
		// channel that can hold a STALE signal (the reader popped, the writer
		// observed the room and refilled it without consuming the signal), so after
		// each wake recheck inflight.len() >= fd.window before writing. A one-shot if
		// could consume that stale signal with the deque still full, compute zero
		// remaining room, leave lim at MaxBatchSize, and write past FullDuplexWindow.
		// On graceful Close this writes the backlog on top of replies still in
		// flight, so without the gate a small FullDuplexWindow is exceeded by up to
		// ~2x (window + buffered backlog). The serve loop keys its wait on ap.ctx,
		// but here ap.ctx is already cancelled (that is why we are flushing), so the
		// wait is BOUNDED instead: if the reader does not drain within the bound,
		// stop gating (set stuck) so Close cannot block forever — correctness of a
		// terminating Close outranks a transient teardown overshoot.
		for !stuck && readerDone != nil && inflight.len() >= fd.window {
			// Poll handoff on every wake too, like the serve loop's window gate, so a
			// MOVING mark under backpressure recycles within one drained reply. Suffix
			// out-of-band (no push), same as the between-chunk check above.
			if cn.ShouldHandoff() {
				return carry[i:], errFDConnMoving
			}
			timer := time.NewTimer(fdCloseFlushWait)
			select {
			case <-inflight.room:
			case <-readerDone:
				timer.Stop()
				inflight.pushBatch(carry[i:])
				return nil, errFDReaderGone
			case <-timer.C:
				stuck = true
			}
			timer.Stop()
		}
		// Cap this chunk to the remaining window room (not just MaxBatchSize): right
		// after the wait releases, a full MaxBatchSize chunk on top of window-1
		// in-flight would still nearly double the bound. Once stuck, write full
		// chunks to finish the flush promptly.
		lim := fd.maxBatch
		if !stuck {
			if room := fd.window - inflight.len(); room > 0 && room < lim {
				lim = room
			}
		}
		end := fdBatchEnd(carry, i, lim, byteLimit)
		if e := fd.writeBatch(bg, cn, inflight, carry[i:end]); e != nil {
			// writeBatch pushed carry[i:end] into inflight before the failed write, but
			// the suffix carry[end:] was never pushed. Push it too, or it sits in neither
			// fd.ch nor inflight and its callers hang: on fdConnErr takeRemaining replays
			// it, on Close the caller fails it. It is only ever settled via failReqs or
			// replayed — never completed inline by the reader — so its zero writtenAt
			// never reaches the write→reply metric.
			if end < len(carry) {
				inflight.pushBatch(carry[end:])
			}
			return nil, e
		}
		i = end
	}
	return nil, nil
}

// fdFirstNoRetry returns the index of the first NoRetry command in reqs, or
// len(reqs) when there is none. The unacked tail is retried up to this index and
// failed from it on: retryable commands ahead of a NoRetry still get their
// network retries, while the NoRetry command and anything ordered after it is
// never re-sent.
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
	// Error-metric parity: commands terminated here (lease failure, retry
	// exhaustion, a NoRetry tail, Close) never reach the reader's inline
	// completion, so emit the native error callback per command. One classification
	// for the whole set (every req fails with the same err), and no duration metric
	// — many of these were never written.
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

// takeQueue closes the submit gate and returns everything buffered in fd.ch. The
// WLock blocks until in-flight submit sends finish (each either landed in fd.ch,
// is drained below, or took its ctx.Done() branch), so after the drain no submit
// can enqueue work that would be left un-completed.
//
// INVARIANT: every takeQueue call is a terminal shutdown drain — run() exits
// right after, past a ctx-cancel check. Never call it on a non-close path: a
// submit blocked on a full channel is unwedged only by its ctx.Done() branch, so
// without a cancelled ctx the WLock deadlocks against the RLock held across that
// send.
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

// shutdownFlush is the between-sessions Close flush: accepted commands in carry
// (an unacked tail from a failed session, never re-leased) and in fd.ch
// (accepted while no session held a connection) are executed on the client's
// normal pipeline path, honoring the "accepted ⇒ completes" Close contract
// instead of failing them ErrClosed just because Close won the race between
// sessions (#3964). Uses a background ctx (ap.ctx is already cancelled);
// processPipeline bounds it with the client's own timeouts/retries and setCmdsErr
// puts any failure on every command, so callers always settle.
func (fd *fdEngine) shutdownFlush(bg context.Context, carry []fdReq) {
	backlog := append(carry, fd.takeQueue()...)
	if len(backlog) == 0 {
		return
	}
	// A panic here (user arg encoder inside processPipeline) runs on the engine
	// goroutine with no other recovery; fail and complete the remainder so no
	// caller hangs.
	i := 0
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("redis: autopipeline: panic in shutdown flush: %v", r)
			internal.Logger.Printf(bg, "autopipeline: recovered shutdown-flush panic: %v\n%s", r, debug.Stack())
			fd.failReqs(backlog[i:], err)
		}
	}()
	// Same MaxBatchSize/MaxBatchBytes chunking as normal FD writes: the backlog can
	// hold the carry plus the whole channel, and one unchunked pipeline would
	// ignore MaxBatchBytes and burst the connection.
	byteLimit := int64(fd.ap.config.MaxBatchBytes) // 0 = disabled
	for i < len(backlog) {
		end := fdBatchEnd(backlog, i, fd.maxBatch, byteLimit)
		cmds := make([]Cmder, end-i)
		for j := i; j < end; j++ {
			cmds[j-i] = backlog[j].cmd
		}
		// Initialize the flush with a backlog request's own context (cancellation
		// removed), not the engine's background context: if this flush has to
		// initialize a fresh pooled connection — e.g. the preceding lease failed —
		// a CredentialsProviderContext resolves credentials from the request's
		// context values, so the flush authenticates as the right tenant instead of
		// a fallback. WithoutCancel because these requests' contexts may already be
		// cancelled (that is often what triggered Close), yet the accepted-⇒-completes
		// contract still requires the write to go through. A chunk can mix callers;
		// the first request in the chunk is the representative — a documented
		// approximation, matching how the diverted-retry and session-init paths pick
		// one context for a batch.
		fctx := bg
		if c := backlog[i].ctx; c != nil {
			fctx = context.WithoutCancel(c)
		}
		err := fd.client.processPipeline(fctx, cmds) // per-command results/errors are set inside
		for j := i; j < end; j++ {
			backlog[j].complete()
		}
		i = end
		// A transport failure that survived processPipeline's own retries means
		// the server is unreachable: stop, and fail the remaining chunks with the
		// same error instead of re-running the full retry cycle per chunk against
		// a dead endpoint (Close would otherwise stall chunks × retries × backoff).
		// Per-command Redis errors are normal results and do not stop the flush.
		if err != nil && !isRedisError(err) {
			fd.failReqs(backlog[i:], err)
			return
		}
	}
}

// failQueue fails every command currently buffered in fd.ch with err WITHOUT
// closing the engine (unlike takeQueue, the shutdown drain, which sets closed).
// Used on fdLeaseErr/fdDenied, where the carry goes through failReqs and this
// drains the accepted backlog — both halves emit the native error metric. The
// engine stays alive, so a command submitted after this returns is failed on the
// next denied attempt or served once the limiter admits. The channel receive is
// safe against a concurrent submit send, so no lock is taken here.
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

// flushBacklogForClose is the graceful-Close flush: it stops new submits (sets
// closed) and writes every command still buffered in fd.ch on the current
// connection, in the same MaxBatchSize/MaxBatchBytes chunks as normal writes, so
// ACCEPTED commands complete instead of failing ErrClosed. The caller then
// closeGraceful()s the deque so the reader drains these replies before exiting.
// Returns the first write error (the caller then degrades to the conn-error path).
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
			// Close flush: if a handoff mark fires mid-flush, writeCarryChunked returns
			// the unwritten suffix out-of-band; push it into inflight so the caller's
			// conn-error path (takeRemaining) recovers/fails it instead of hanging its
			// callers. The dead-conn paths already pushed their suffix and return an
			// empty one, so this never double-pushes.
			suffix, e := fd.writeCarryChunked(bg, cn, inflight, backlog, readerDone)
			if len(suffix) > 0 {
				inflight.pushBatch(suffix)
			}
			return e
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
