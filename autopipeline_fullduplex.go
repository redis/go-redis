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

// Ordered full-duplex dispatch for the async and blocking AutoPipeline faces.
//
// Half-duplex sends one batch per round trip. A slow link caps throughput at
// batch/RTT, and a late command waits one RTT behind the in-flight batch.
// Full-duplex holds one pipeline-pool connection with a writer goroutine and a
// reader goroutine. The writer streams command groups without waiting for
// replies. The reader drains replies in FIFO order and completes each command
// when its reply lands. Latency is about 1 RTT; throughput saturates the pipe.
//
// Ordering: each goroutine's commands run in submit order. Order between
// goroutines is not defined. The submit channel is MPSC and the connection is
// FIFO on the wire, so a command's position in the in-flight deque matches its
// reply.
//
// Retries: on a connection failure the engine re-issues the unacked tail, in
// order, on a fresh connection ahead of new work. It honors shouldRetry,
// MaxRetries, backoff, and the per-command NoRetry flag. A NoRetry command in the
// tail fails the tail instead of replaying it (half-duplex uses cmdsContainNoRetry
// for the same result). After the budget is spent it fails those commands and
// keeps serving on a fresh connection. The at-least-once contract matches a normal
// Pipeline: a command whose write landed but whose reply was lost can re-execute.
// Only the unacked tail is ambiguous.
//
// Enable it with AutoPipelineOptions.FullDuplex. It runs on the ordered
// single-shard faces of a standalone *Client with a pipeline pool. Tune it with
// the FullDuplex* options (see their GoDoc). RESP3 push frames are demuxed inline.
// Cluster support and window auto-tune are follow-ups (see
// AP_ORDERED_FULLDUPLEX_DESIGN.md).

var errFDReaderGone = errors.New("redis: autopipeline full-duplex reader exited")

// errFDPanicRecovered marks a session failure from a recovered panic (reply
// decode or batch encode). It wraps with %w so the retry decision recognizes it:
// the connection is desynced like a transport error, so the engine replays the
// unacked tail on a fresh connection instead of failing it. Most of that tail is
// commands the panic never touched. shouldRetry alone would reject these plain
// error values and fail innocent in-flight commands.
var errFDPanicRecovered = errors.New("redis: autopipeline: full-duplex panic recovered")

// errFDPushDrainFailed marks a session failure from a push-notification drain
// error on the reply path. A custom PushNotificationProcessor can consume part of
// a frame and desync the reader. It wraps with %w like errFDPanicRecovered so the
// retry decision recognizes it: the connection is desynced, so the next read would
// misalign the FIFO. The engine fails the session and replays the unacked tail on
// a fresh connection instead of reading shifted bytes.
var errFDPushDrainFailed = errors.New("redis: autopipeline: full-duplex push drain failed")

// fdReplyIsFatal reports whether a per-reply read error must ABORT the FD session
// (stop the reader and leave the unread tail for replay) instead of being treated
// as this command's reply. A push-drain desync is always fatal, even when it wraps
// a Redis-typed cause. errFDPushDrainFailed carries the processor error via %w for
// errors.Is/As, but that cause must not let isRedisError reclassify the desync as a
// normal reply: settling or diverting it would leave the unread frame in the stream
// and shift every later reply. Any other error is fatal only when it is not a Redis
// error (a real transport or protocol failure). A plain Redis error is a real reply
// and is handled inline.
func fdReplyIsFatal(e error) bool {
	return errors.Is(e, errFDPushDrainFailed) || !isRedisError(e)
}

// errFDRetryBudgetExhausted fails a carried command that has already spent its full
// retry budget (attempts > MaxRetries) when Close routes the unacked tail to
// shutdownFlush. The shutdown pipeline must not grant another MaxRetries+1
// executions and push a mutating command past its budget.
var errFDRetryBudgetExhausted = errors.New("redis: autopipeline: retry budget exhausted before shutdown flush")

// fdPartitionByBudget splits a carried tail into commands that still have retry
// budget (kept) and commands that have spent it (exhausted, attempts > maxRetries —
// one FD attempt plus maxRetries replays). It always allocates a fresh kept slice.
// carry is caller-owned (the unacked tail, or a handoff suffix that may alias the
// in-flight ring), and shutdownFlush appends the queue to kept, so a write into
// carry's backing array would corrupt the caller's slice.
func fdPartitionByBudget(carry []fdReq, maxRetries int) (kept, exhausted []fdReq) {
	kept = make([]fdReq, 0, len(carry))
	for _, r := range carry {
		if r.attempts > maxRetries {
			exhausted = append(exhausted, r)
			continue
		}
		kept = append(kept, r)
	}
	return kept, exhausted
}

// errFDConnMoving signals that carry replay stopped early because the held
// connection was marked for handoff (MOVING/FAILING_OVER) while a recovered carry
// was still being written. The connection is still alive, so writeCarryChunked
// returns the UNWRITTEN suffix out-of-band, not pushed into the in-flight deque.
// The session drains the already-written prefix to completion (those callers get
// real replies and are never re-executed) and Puts the connection back through a
// clean fdRecycle. The maintnotifications OnPut hook then performs the seamless
// handoff: queueHandoff plus MarkQueuedForHandoff clears ShouldHandoff, so the conn
// is reusable and the worker moves it to the new endpoint. Only the never-sent
// suffix replays on the next lease. Contrast errFDReaderGone and write errors,
// where the connection is dead: there the suffix IS pushed into the deque and the
// whole unacked tail replays, because a clean drain is impossible.
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
	// writtenAt is stamped at the command's FIRST flush to the wire and kept across
	// replays; the reader uses write→reply as the command's operation duration for
	// the OTel metric. Anchoring on the first write (not the last replay) makes the
	// duration span the whole retry sequence, matching the normal command path,
	// instead of timing only the final attempt.
	writtenAt time.Time
	// attempts counts how many times this command has been issued: 1 at submit,
	// incremented on each connection-error replay of the carried tail. Fed to the
	// OTel duration/error callbacks so a command that succeeded on a replacement
	// connection reports its real attempt count (retry_attempts), matching the
	// normal command path instead of always reporting a single attempt.
	attempts int
	// sent is set once the command MAY have reached the wire. writeBatch stamps it
	// before the flush, so a partial write or an encoder panic still counts. It is
	// sticky across replays. The NoRetry gate keys on it: a never-sent NoRetry command
	// is replayable, because issuing it is its FIRST send; a sent NoRetry command must
	// be failed rather than risk a second execution. Without the marker the gate failed
	// never-sent NoRetry commands recovered from a dead connection's backlog, returning
	// an error for a command the server never saw.
	sent bool
	// limReport is the Limiter obligation for the WRITTEN chunk this req closes:
	// non-nil ONLY on the LAST req of a chunk that Allow() admitted and writeBatch
	// then wrote cleanly. It rides the in-flight deque so the reply-side outcome
	// settles the chunk's single ReportResult — nil once every reply of the chunk
	// has been read (reader), or the transport error when the chunk's unread
	// replies are abandoned (settleTail). A write-failed chunk reports at write
	// time and carries no obligation here. See fdLimiterReport.
	limReport *fdLimiterReport
}

// fdLimiterReport is one chunk's outstanding Limiter obligation: the pending
// ReportResult that must fire exactly once for the Allow() that admitted the chunk
// in writeBatch. Reporting the WRITE outcome was wrong for a circuit breaker
// (finding ed53z): a peer that accepts the write but closes before replying, with
// replay writes also succeeding, would only ever feed the breaker success and never
// open it. So the obligation records the REPLY-side outcome. The reader settles
// ReportResult(nil) once every reply of the chunk has landed (a server that answers
// is healthy, reply-level errors included). A transport failure that abandons the
// chunk's unread replies settles the error (settleTail). The CAS makes it
// exactly-once, since the reader and a failure path can both reach the same
// obligation.
type fdLimiterReport struct {
	lim  Limiter
	done atomic.Bool
}

// settle fires ReportResult(err) at most once for this obligation. A nil
// receiver (a chunk with no Limiter) and every call after the first are no-ops.
//
// ReportResult is user code, run on FD background goroutines (the reader's success
// path, plus the write-failure and settleTail paths). A panic must not reach the
// reader's session-failure recovery, which would replay an already-consumed reply
// and execute the command twice, and must not escape fd.run. Recover and swallow it:
// the outcome is already decided, reporting is fire-and-forget, and the CAS already
// fired, so the strict Allow/ReportResult pairing still holds.
func (o *fdLimiterReport) settle(err error) {
	if o == nil {
		return
	}
	if o.done.CompareAndSwap(false, true) {
		defer func() {
			if r := recover(); r != nil {
				internal.Logger.Printf(context.Background(),
					"autopipeline: recovered full-duplex limiter ReportResult panic: %v\n%s", r, debug.Stack())
			}
		}()
		o.lim.ReportResult(err)
	}
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
	mu   sync.Mutex
	cond *sync.Cond
	// buf is a ring buffer of in-flight entries: the writer appends at the back
	// (head+count), the reader pops from the front (head). A grow-only slice
	// (append + reslice-off-front) reallocated its backing array on every window
	// churn — the single largest allocation source under load — because the
	// popped-off prefix was never reused. The ring reuses the whole array for the
	// life of the session; it only grows when live count would exceed capacity.
	buf        []fdReq
	head       int           // index of the front (oldest) live entry
	count      int           // number of live entries
	noMorePush bool          // graceful: drain remaining then reader exits
	hardClosed bool          // recover: reader stops immediately, remaining replayed
	room       chan struct{} // cap-1 signal: the reader popped, so there is room
	peak       int           // high-water mark of live count; observability for the backpressure test
	advanced   int           // total entries the reader completed this session (progress signal)
}

//nolint:unused // used by the full-duplex tests; lint runs with tests:false.
func newFDInflight() *fdInflight { return newFDInflightCap(0) }

// newFDInflightCap presizes the ring so a busy session avoids repeated early
// reallocations, capping the initial size at min(maxBatch, window). The window is
// the hard ceiling on live count: the writer blocks once in-flight reaches it and
// caps each drain by the remaining room. So a MaxBatchSize larger than the window
// must not presize beyond it (MaxBatchSize is an uncapped soft per-flush threshold):
// a huge MaxBatchSize with a small window would allocate that many entries up front
// (tens of MB, or an OOM) and again after every idle or recycle. Capping at maxBatch
// keeps the common small-batch default unchanged. grow() is the backstop up to the
// window as load ramps. Tests use the zero-cap no-arg form and let it grow.
func newFDInflightCap(initialCap int) *fdInflight {
	f := &fdInflight{room: make(chan struct{}, 1)}
	if initialCap > 0 {
		f.buf = make([]fdReq, initialCap)
	}
	f.cond = sync.NewCond(&f.mu)
	return f
}

func (f *fdInflight) len() int {
	f.mu.Lock()
	n := f.count
	f.mu.Unlock()
	return n
}

// grow ensures the ring can hold at least need entries, preserving FIFO order
// and normalizing the front to index 0. Caller holds f.mu.
func (f *fdInflight) grow(need int) {
	if need <= len(f.buf) {
		return
	}
	nc := len(f.buf) * 2
	if nc < need {
		nc = need
	}
	if nc < 8 {
		nc = 8
	}
	nb := make([]fdReq, nc)
	// Unwrap the live entries into the new buffer starting at 0.
	for i := 0; i < f.count; i++ {
		nb[i] = f.buf[(f.head+i)%len(f.buf)]
	}
	f.buf = nb
	f.head = 0
}

// pushBatch appends a whole write batch under one lock (fewer lock ops than
// per-command push — matters at loopback op rates).
func (f *fdInflight) pushBatch(reqs []fdReq) {
	f.mu.Lock()
	f.grow(f.count + len(reqs))
	for _, r := range reqs {
		f.buf[(f.head+f.count)%len(f.buf)] = r
		f.count++
	}
	if f.count > f.peak {
		f.peak = f.count
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
// should exit. The writer only ever appends at the back, so this prefix stays the
// front until the reader advance()s it. The snapshot is copied into the caller's
// buf (may span the ring's wrap seam as two segments), so it never aliases the
// backing array.
func (f *fdInflight) frontBatch(buf []fdReq) ([]fdReq, bool) {
	f.mu.Lock()
	for f.count == 0 && !f.noMorePush && !f.hardClosed {
		f.cond.Wait()
	}
	if f.hardClosed || f.count == 0 {
		f.mu.Unlock()
		return buf[:0], false
	}
	n := f.count
	if n > fdReadBatch {
		n = fdReadBatch
	}
	buf = buf[:0]
	// First segment: head .. min(end-of-array, head+n).
	seg := len(f.buf) - f.head
	if seg > n {
		seg = n
	}
	buf = append(buf, f.buf[f.head:f.head+seg]...)
	if seg < n {
		// Wrapped: remainder from the start of the array.
		buf = append(buf, f.buf[:n-seg]...)
	}
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
	if n > f.count {
		n = f.count
	}
	if n == 0 {
		// Nothing to drop (empty ring, or clamped away). Return before the modulo
		// below, which divides by len(f.buf) and would panic on a never-grown ring
		// (nil buf). The slice implementation tolerated advance on an empty queue;
		// preserve that.
		f.mu.Unlock()
		return
	}
	// Zero each consumed entry before dropping it: the ring keeps its backing
	// array for the whole session (curInflight holds the deque while the engine
	// idles), so otherwise a drained burst retains a window's worth of completed
	// fdReq values — command args, caller contexts, batches — until the slot is
	// overwritten by a later push.
	for i := 0; i < n; i++ {
		f.buf[(f.head+i)%len(f.buf)] = fdReq{}
	}
	f.head = (f.head + n) % len(f.buf)
	f.count -= n
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
	n := f.count
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
// the unacked tail, and with the reader gone there is no concurrent access. The
// live entries are unwrapped into a fresh contiguous slice (they may span the
// ring's wrap seam); the ring is terminal for the session, so the backing array
// is dropped.
func (f *fdInflight) takeRemaining() []fdReq {
	f.mu.Lock()
	if f.count == 0 {
		f.buf = nil
		f.head = 0
		f.mu.Unlock()
		return nil
	}
	rem := make([]fdReq, f.count)
	for i := 0; i < f.count; i++ {
		rem[i] = f.buf[(f.head+i)%len(f.buf)]
	}
	f.buf = nil
	f.head = 0
	f.count = 0
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

	recycles       atomic.Int64               // clean returns (idle + max-hold); observability/tests
	curInflight    atomic.Pointer[fdInflight] // current session's in-flight deque; test observability
	curConn        atomic.Pointer[pool.Conn]  // current session's held conn; test observability (handoff)
	fastSubmitTake atomic.Int64               // fast-path submits taken; test observability
	// curConnSpilled is true while the current session holds a MAIN-pool connection —
	// a spilled lease, or the no-dedicated-pool case where fd.pool IS the main pool.
	// retryOnNormalConn must not block the reader on retrySem then: the session pins a
	// main-pool conn until the reader drains, so off-pipe retries waiting on that same
	// pool would deadlock (see retryOnNormalConn). Set in attempt before the reader is
	// spawned; biased true when a lease is undecided so an unknown state never blocks.
	curConnSpilled atomic.Bool

	submitMu sync.RWMutex // guards closed; RLock across the submit send, WLock to close the gate
	closed   bool         // set once run() is tearing down; submit then rejects new work

	retryWg  sync.WaitGroup // tracks off-pipe retries diverted to the normal client path; run() waits it so Close does too
	retrySem chan struct{}  // caps concurrent off-pipe retries at the window (see retryOnNormalConn)
	hostWg   sync.WaitGroup // tracks per-command hook-host goroutines (see hostHook); run() waits it so Close does not return while a post-next ProcessHook is still running

	// fastSubmit tries a non-blocking channel send before the blocking three-arm
	// select in submit (from AutoPipelineOptions.FullDuplexFastSubmit). Gated on
	// submit-queue depth (fdFastSubmitGatePct) so it only runs while the queue is
	// shallow; a deep/bursting queue falls to the fair blocking select.
	fastSubmit bool

	// runPipeline runs a shutdown-flush chunk through the client's pipeline retry
	// loop. Test seam: nil in production (flushReqs falls back to
	// client.processPipelineRetries), so tests can drive flushReqs's chunk loop
	// without a live server.
	runPipeline func(ctx context.Context, cmds []Cmder, maxRetries int) error
}

// fdFastSubmitGatePct bounds fastSubmit to when the submit channel is below this
// percent full. The non-blocking send cuts the submit-path selectgo cost, but
// past saturation it would let producers that find room jump ahead of producers
// blocked on a full channel, starving them and inflating p99. Gating on len(ch)
// closes the fast path exactly as that backup starts, so contended traffic uses
// the fair blocking select and the tail is preserved. 10% is the measured
// tail-safe point (looser gates leave the tail elevated; see FD perf notes).
const fdFastSubmitGatePct = 10

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
	// Publish every resolved value so Config() reports what the engine actually
	// enforces (a zero field -> its default), not the raw 0 the user passed. Runs
	// once at construction before ap escapes newAutoPipeliner, so no Config() reader
	// races these writes. Validate rejects negatives. (MaxBatchSize is already
	// defaulted upstream in newAutoPipeliner, so it needs no write-back here.)
	ap.config.FullDuplexWindow = w
	idle := ap.config.FullDuplexIdleTimeout
	if idle <= 0 {
		idle = fdDefaultIdle
	}
	ap.config.FullDuplexIdleTimeout = idle
	maxHold := ap.config.FullDuplexMaxHold
	if maxHold <= 0 {
		maxHold = fdDefaultMaxHold
	}
	ap.config.FullDuplexMaxHold = maxHold
	// The submit queue does not need window-sized storage. Backpressure comes from
	// the in-flight deque, which grows only with ACTUAL in-flight, while a buffered
	// channel allocates its full capacity up front: several MiB per engine at the
	// default window, before any command is submitted. Cap the queue. Total
	// outstanding stays bounded by cap+window, and submit just blocks a little earlier
	// under a burst.
	chCap := w
	if chCap > 4096 {
		chCap = 4096
	}
	// The off-pipe retry bound is a GOROUTINE budget, not a memory window. Diverted
	// retries serialize on the main pool's PoolSize connections, so slots beyond about
	// 2x the pool only hold 8 KiB stacks and pool-wait turns. Sizing it to w (default
	// 65536) let a retryable-reply storm — e.g. LOADING during a server restart, which
	// diverts EVERY reply — park tens of thousands of goroutines. 2x the pool keeps
	// backoff sleepers overlapping with pool waiters. The reader blocking on a full sem
	// is the designed end-to-end backpressure (see retryOnNormalConn); it now just
	// engages earlier.
	retryCap := 2 * client.opt.PoolSize
	if retryCap > w {
		retryCap = w
	}
	if retryCap < 1 {
		retryCap = 1
	}
	return &fdEngine{
		ap:         ap,
		client:     client,
		pool:       client.getPipelinePool(),
		ch:         make(chan fdReq, chCap),
		maxBatch:   mb,
		window:     w,
		idle:       idle,
		maxHold:    maxHold,
		retrySem:   make(chan struct{}, retryCap),
		fastSubmit: ap.config.FullDuplexFastSubmit,
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
	var hookDone chan struct{}
	// With process hooks installed, run the chain on a per-command host goroutine
	// (see hostHook). Hooks are added before the client serves traffic, so the host
	// loads live hook state like the synchronous path. hookCount is one atomic load.
	if fd.ap.pipeliner.hookCount() > 0 {
		hookDone = make(chan struct{})
	}
	// Hook-free blocking face: the batch is a single-waiter completion signal
	// discarded after Wait, so draw it from the pool (buffered done, recycled in
	// processBlocking). Every other shape — the async face (batch installed on
	// the command, read repeatedly) and the hooked path (host goroutine owns
	// completion) — needs the close()-once channel.
	var b *apBatch
	if hookDone == nil && fd.ap.blocking {
		b = getFDBlockingBatch()
	} else {
		b = newAPBatch()
	}
	req := fdReq{cmd: cmd, batch: b, hookDone: hookDone, ctx: ctx, attempts: 1}

	// Send under RLock and re-check closed so a send can never win the race with
	// run()'s shutdown drain (takeQueue: WLock, set closed, drain fd.ch). Once the
	// final drain has run no new req can land in fd.ch, where it would never be
	// completed and would hang its caller forever. A send blocked on a full channel
	// is released by the ctx.Done() branch below, so holding the RLock cannot wedge
	// the WLock.
	fd.submitMu.RLock()
	if fd.closed {
		fd.submitMu.RUnlock()
		// Recycle the pooled completion batch drawn above but never admitted, so a
		// rejection does not discard one pooled batch + channel per command (a no-op
		// for the newAPBatch shape, which is not pooled).
		putFDBlockingBatch(b)
		cmd.SetErr(ErrClosed)
		// Submit-time rejection: return the shared completedBatch sentinel (no host
		// was started) so processAsync surfaces the error from raw Process(ctx,cmd),
		// matching every other submit-time-rejection path.
		return completedBatch
	}
	// Fast path (opt-in via FullDuplexFastSubmit): while the submit queue is
	// shallow, a non-blocking send skips the blocking three-arm selectgo that
	// dominates submit CPU at high producer counts. Admission is IDENTICAL to the
	// blocking case below (same gate held, same host start, same return); only the
	// wait is skipped. The queue-depth gate keeps this off once the channel bursts
	// deep, so contended traffic takes the fair blocking select and the p99 tail is
	// preserved. On a miss (or a full channel) it falls through to that select.
	if fd.fastSubmit && len(fd.ch)*100 < cap(fd.ch)*fdFastSubmitGatePct {
		select {
		case fd.ch <- req:
			fd.fastSubmitTake.Add(1) // test observability; single atomic on the (already RLock'd) fast path
			if hookDone != nil {
				fd.hostWg.Add(1)
				go fd.hostHook(ctx, cmd, b, hookDone)
			}
			fd.submitMu.RUnlock()
			return b
		default:
		}
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
		putFDBlockingBatch(b) // recycle the unadmitted pooled batch (no-op if not pooled)
		cmd.SetErr(ctx.Err())
		return completedBatch
	case <-fd.ap.ctx.Done():
		fd.submitMu.RUnlock()
		putFDBlockingBatch(b) // recycle the unadmitted pooled batch (no-op if not pooled)
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

// retryStartAttempt returns the normal-path retry loop's starting attempt for an FD
// command diverted to it. A MOVING/ASK redirect returns 0: the command did not
// execute on the FD socket, so it gets the full MaxRetries+1 budget. A retryable
// reply such as LOADING/READONLY/TRYAGAIN returns 1: the initial attempt was already
// spent on the FD socket, so counting it keeps the total at MaxRetries+1, not +2.
func retryStartAttempt(moved, ask bool) int {
	if moved || ask {
		return 0
	}
	return 1
}

// emitMetricsGuarded runs a fire-and-forget user metric callback under panic
// recovery. Every FD metric emit funnels through here: reportReplyMetrics on the
// reader's inline completion, and the failure paths failReqs and failQueue. So a
// panicking user callback is logged and swallowed, not propagated. An escaped panic
// would leave accepted commands unsettled (callers wedged forever) and crash fd.run;
// with process hooks it would deadlock Close's hostWg.Wait while callers block on
// hookDone. On the reader path it would also reach the session-failure recovery
// BEFORE the reply is advanced out of the in-flight deque, which treats an unadvanced
// req as an unacked tail and replays it — an already-consumed mutating command run
// twice. The reply outcome is already decided; reporting is advisory.
func (fd *fdEngine) emitMetricsGuarded(octx context.Context, emit func()) {
	defer func() {
		if r := recover(); r != nil {
			internal.Logger.Printf(octx,
				"autopipeline: recovered full-duplex metric-callback panic: %v\n%s", r, debug.Stack())
		}
	}()
	emit()
}

// reportReplyMetrics runs the inline-completed command's user-settable metric
// callbacks (OTel operation-duration; native error callback for a non-retryable
// Redis error, reporting attempts-1 retries like processWithRetry) for parity
// with the process() path the FD reader bypasses. Guarded by emitMetricsGuarded
// (panic containment; see there).
func (fd *fdEngine) reportReplyMetrics(octx context.Context, req fdReq, e error, cn *pool.Conn) {
	fd.emitMetricsGuarded(octx, func() {
		if cb := otel.GetOperationDurationCallback(); cb != nil {
			// The reader has set req.cmd's final result but has NOT completed the
			// batch yet — req.complete() runs only after this returns. A custom
			// duration callback that reads its own command (cmd.Err()/cmd.String())
			// would await batch.done and wedge the reader, since the batch completes
			// only after this returns and the reader is not otherwise the batch's
			// executor. Register the reader as the batch's executor for the call so
			// the accessor guard hands back the just-set view without blocking — the
			// same escape a dispatch hook reading its own command uses. hostHook gets
			// this SAME batch from submit, so one registration covers the hook and
			// hook-free async faces alike. NOT moved after complete(): complete()
			// hands off to the hook host, which may rewrite/free the command, racing
			// the callback's read. The curGoroutineID() cost is paid only when a
			// duration callback is registered.
			if req.batch != nil {
				unregister := req.batch.enterNodeDispatch()
				defer unregister()
			}
			cb(octx, time.Since(req.writtenAt), req.cmd, req.attempts, e, cn, fd.client.opt.DB)
		}
		if e != nil {
			if errorCallback := pool.GetMetricErrorCallback(); errorCallback != nil {
				errorType, statusCode, isInternal := classifyCommandError(e)
				errorCallback(octx, errorType, cn, statusCode, isInternal, req.attempts-1)
			}
		}
	})
}

// retryOnNormalConn re-runs a full-duplex command that came back with a retryable
// Redis error (LOADING/READONLY/…) or a redirect (MOVED/ASK) on the client's NORMAL
// path. That path routes redirects to the proper node and applies the standard
// retry/backoff, neither of which the fixed single-conn FD socket can do. It runs on
// its own goroutine so it does not stall the FD reader, is tracked by retryWg so
// Close waits for it, and settles the FD request with the outcome. process() is the
// raw exec (no hook chain); with hooks installed the FD hostHook still brackets the
// command and reports via req.complete(). Background ctx: the command was already
// accepted, so it completes even under a Close.
func (fd *fdEngine) retryOnNormalConn(req fdReq, startAttempt int) {
	// Bound concurrent off-pipe retries to about 2x the main pool (see newFDEngine's
	// retryCap): a sustained retryable stream would otherwise spawn one goroutine per
	// reply, all parked in backoff/pool acquisition. Blocking here blocks the READER,
	// which stops advancing the deque, which fills the window and blocks the writer and
	// then submitters — end-to-end backpressure. No cycle: retries drain on the main
	// pool, independent of the reader waiting here.
	//
	// Take a free slot if one is available. Otherwise, by state:
	//   - Close (ap.ctx done): FAIL ErrClosed. Never block (a spilled session pins a
	//     main-pool conn until the reader drains, so parking the reader deadlocks the
	//     retries that need that pool) and never run slot-less (a Close-time storm would
	//     spawn a goroutine per in-flight reply, up to FullDuplexWindow, and OOM). The
	//     engine is closing; retryWg still covers granted slots.
	//   - spilled session (curConnSpilled): run SLOT-LESS rather than block, for the
	//     same deadlock reason — the reader must keep draining so the session releases
	//     its pinned main-pool conn (fCCni). Residual: up to a window of transient retry
	//     goroutines while spilled, reachable only when the pipeline pool is saturated
	//     at a small PoolSize; documented, and the lesser evil versus a hang.
	//   - otherwise: BLOCK for a slot — end-to-end backpressure, safe because a
	//     pipeline-pool session does not compete with the main-pool retries.
	slot := false
	select {
	case fd.retrySem <- struct{}{}:
		slot = true
	case <-fd.ap.ctx.Done():
		select {
		case fd.retrySem <- struct{}{}:
			slot = true
		default:
			req.cmd.SetErr(ErrClosed)
			req.complete()
			return
		}
	default:
		if fd.curConnSpilled.Load() {
			// slot stays false: run slot-less, do NOT block the reader.
		} else {
			// Block for a slot, but stay cancellable. A Close that arrives while
			// the reader is parked here must not deadlock: cancelAndDrain waits on
			// the reader to advance the deque, and the reader is the goroutine
			// blocked on this send. On ctx cancel, FAIL ErrClosed (same as the
			// ctx.Done arm above) rather than park forever.
			select {
			case fd.retrySem <- struct{}{}:
				slot = true
			case <-fd.ap.ctx.Done():
				req.cmd.SetErr(ErrClosed)
				req.complete()
				return
			}
		}
	}
	fd.retryWg.Add(1)
	go func() {
		defer func() {
			if slot {
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
		// Pass req.writtenAt as the operation start so the duration metric spans the
		// initial FD write, not just this diverted attempt (the attempt count already
		// includes the FD attempt).
		// Register this retry goroutine as the batch's executor for the call, mirroring
		// reportReplyMetrics: a custom RecordOperationDuration callback inside
		// processStartingAt that reads its own command (cmd.Err()/cmd.String()) awaits
		// batch.done, which req.complete() closes only AFTER this returns — so without the
		// guard the callback wedges this goroutine and Close waits for the backstop. On the
		// executor goroutine the accessor hands back the just-set view instead. The reader's
		// reply-side guard does not cover this off-pipe retry path.
		err := func() error {
			if req.batch != nil {
				defer req.batch.enterNodeDispatch()()
			}
			return fd.client.processStartingAt(rctx, req.cmd, startAttempt, req.writtenAt)
		}()
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
	//
	// Reentrancy caveat: this wait CANNOT exclude its own caller, so a ProcessHook
	// that synchronously calls Close (Client.Close or AutoPipeliner.Close) from its
	// host goroutine deadlocks here until the close backstop (autoPipelineCloseBackstop):
	// Close waits ap.wg -> run() -> this hostWg.Wait, which waits for the very host
	// blocked inside Close. A reentrancy fix was rejected as unsafe: cancelAndDrain is
	// NOT once-only (the shared-pool close hook leaves ap.closed false and can run
	// again), and an early hostWg.Done races submit's hostWg.Add. The contract is
	// documented on the FullDuplex GoDoc instead: such a hook must call Close from a
	// separate goroutine.
	defer fd.hostWg.Wait()
	// Release the last session's in-flight ring when the engine exits (Close /
	// ctx-cancel): curInflight is a grow-only deque that can hold up to fd.window
	// commands (~MiB at the default), and it would otherwise stay resident for as
	// long as the caller holds the Client. Safe against the progress read below —
	// this defer only fires once run() has returned, after that read is done.
	defer fd.curInflight.Store(nil)
	bg := context.Background()
	var carry []fdReq // unacked tail to re-issue at the start of the next attempt
	// Two SEPARATE budgets, each counting only CONSECUTIVE failures of its own
	// kind: a shared counter would let transient lease failures eat the reconnect
	// budget, so the first genuine mid-session drop would fail the whole unacked
	// tail with zero replay attempts. leaseAttempts resets whenever a session
	// actually ran; retryAttempts resets on a clean session end (idle/recycle).
	leaseAttempts := 0 // consecutive fdLeaseErr acquisition failures
	retryAttempts := 0 // consecutive fdConnErr tail-replay failures
	for {
		if fd.ap.ctx.Err() != nil {
			fd.shutdownFlush(bg, carry)
			return
		}
		// Never lease a connection (or dial) without work in hand: block for the
		// first command whenever the carry is empty. That covers the initial entry,
		// the fdIdle return, and the fail-fast exits below (exhausted fdLeaseErr /
		// failed fdConnErr tail), which would otherwise loop straight back into
		// attempt against an empty queue — dialing a down server forever. Work
		// already queued makes this non-blocking, so fdRecycle re-leases
		// immediately; an empty recycle parks here.
		if len(carry) == 0 {
			// About to park with no session running: release the previous session's
			// in-flight ring so a drained deque does not stay resident through the idle
			// gap until the next session stores a fresh one. The fdConnErr progress read
			// (fd.curInflight.Load, below) already ran for the prior iteration, and the
			// next session re-stores before that read runs again, so this never races it.
			fd.curInflight.Store(nil)
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
			// Close: attempt() drained the written work and released the session conn
			// via its defer. The defer Puts the conn, so the OnPut hook can hand a
			// marked conn off, or Removes it if the release drain failed. Here unacked
			// is the never-WRITTEN handoff suffix (nil on a plain Close), NOT written
			// commands to re-execute; complete it on a fresh connection now that
			// attempt() freed this one. Doing it here rather than inside session avoids
			// failing the suffix against a saturated pool while the session conn is
			// still held.
			if len(unacked) > 0 {
				fd.shutdownFlush(bg, unacked)
			}
			return
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
			// server/pool recovers. Replaying the carry wholesale is safe: any SENT
			// NoRetry was already failed by the split (a never-sent NoRetry in the carry
			// gets its first send), and nothing here was written on a new conn.
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
			// transport error. The tail is mostly commands they never touched, so
			// they are replayable too. shouldRetry alone would reject them and
			// permanently fail innocent in-flight commands. The NoRetry guard
			// below still protects non-idempotent writes.
			replayable := shouldRetry(aerr, true) ||
				errors.Is(aerr, errFDReaderGone) || errors.Is(aerr, errFDPanicRecovered) ||
				errors.Is(aerr, errFDPushDrainFailed)
			// Bound each command by its OWN attempt count, not the session-level
			// retryAttempts (which resets on any session progress — see advancedTotal
			// above — so a flaky peer that acks some replies then drops could hand the
			// tail a fresh budget on every partial success). Partition the tail: a
			// command that has spent its budget (attempts > MaxRetries) is failed; the
			// rest stay eligible to replay. Gating the whole tail on the OLDEST command's
			// count instead would deny a newer command — written behind an exhausted one,
			// so carrying fewer attempts — the retries it is still owed. Carried commands
			// are the oldest (written first, attempts bumped together), so the exhausted
			// set is the leading run and the eligible suffix keeps FIFO order.
			// retryAttempts still drives the backoff escalation only.
			eligible := unacked
			if replayable && len(unacked) > 0 {
				// unacked is PRE-BUMP here (a command sent A times carries attempts==A),
				// so attempts > MaxRetries is exactly the spent-budget set. The Close-path
				// flush (flushCarryBudgeted) instead sees POST-BUMP carry, where the same
				// command carries attempts==A+1 — do not unify the two thresholds.
				var exhausted []fdReq
				eligible, exhausted = fdPartitionByBudget(unacked, fd.client.opt.MaxRetries)
				if len(exhausted) > 0 {
					fd.failReqs(exhausted, aerr) // spent MaxRetries+1 attempts; fail with the real cause
				}
				// Split the eligible suffix at the first SENT NoRetry command: replay the
				// prefix and fail that command plus everything ordered after it (a NoRetry
				// command whose bytes may have reached the wire must never be re-sent). A
				// never-sent NoRetry stays in the replay prefix — issuing it is its first
				// send (see fdReq.sent). With a sent NoRetry at the eligible head (n==0)
				// nothing ahead of it is retryable, so fall through and fail whatever
				// eligible remains (exhausted was already failed above). If the scan
				// itself panicked (a custom Cmder's NoRetry() is user code on this
				// recover-less serve loop), we cannot classify the tail: skip the replay
				// and fall through to fail it — a command that may be a sent NoRetry must
				// never be re-sent when in doubt.
				if n, scanPanic := fdFirstNoRetrySafe(eligible); !scanPanic && n > 0 {
					if n < len(eligible) {
						fd.failReqs(eligible[n:], aerr)
					}
					retryAttempts++
					fd.sleepBackoff(retryAttempts)
					carry = eligible[:n]
					// Already issued on the failed connection and about to be re-issued;
					// bump attempts so a later success/failure reports the real
					// retry_attempts (not always 1). The clean-recycle suffix path
					// (fdRecycle) leaves attempts at 1 — that tail was never sent, so its
					// replay is a first attempt.
					for i := range carry {
						carry[i].attempts++
					}
					continue
				}
			}
			// Not retrying (not replayable, none eligible, or NoRetry-headed): fail the
			// remaining unfailed commands, then ALWAYS back off before re-leasing so a
			// dead server cannot spin this loop. Keep the engine alive to serve new work
			// when it recovers.
			fd.failReqs(eligible, aerr)
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
	// Options.Limiter is deliberately NOT consulted here: the lease is not the
	// Limiter's unit. Admission is per written chunk, in writeBatch — see the
	// comment there for the rationale.
	var cn *pool.Conn
	// connPool records which pool cn was leased from — the pipeline pool normally, or
	// the main pool on a spill (see the acquire below) — so the deferred remove/release
	// returns it to the pool that owns it.
	connPool := fd.pool
	// Bias to spilled==true until the lease is decided below: an unknown state must
	// never let retryOnNormalConn block the reader (a leftover true only makes off-pipe
	// retries slot-less, which is safe; a stale false is the deadlock this guards).
	fd.curConnSpilled.Store(true)
	defer func() {
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
			connPool.Remove(bg, cn, aerr)
		} else {
			fd.client.releaseConnToPool(bg, connPool, cn, nil)
		}
	}()

	// Panic boundary for the ACQUISITION/INITIALIZATION phase. initPooledConn runs
	// user-controlled init (Options.OnConnect, credentials providers); a panic there
	// would otherwise escape the sole fd.run goroutine and crash the process, and the
	// release defer above — seeing the zero-value result (fdGraceful) — would Put the
	// half-initialized conn back into the pool. Registered AFTER that defer so it runs
	// FIRST (LIFO): retire the leased conn (Remove), set cn=nil so the release defer is a
	// no-op, and return the carry as fdLeaseErr — the SAME disposition an initPooledConn
	// error gets below, so run() applies the lease-retry budget and fails accepted work
	// fast on a deterministic panic instead of poisoning the pool. A session-body panic
	// is contained by session()'s own recover, which returns fdConnErr normally, so this
	// boundary fires only for the lease/init phase (and as a last-resort backstop).
	defer func() {
		if r := recover(); r != nil {
			aerr = fmt.Errorf("%w: full-duplex attempt: %v", errFDPanicRecovered, r)
			internal.Logger.Printf(bg, "autopipeline: recovered full-duplex attempt panic: %v\n%s", r, debug.Stack())
			if cn != nil {
				connPool.Remove(bg, cn, aerr)
				cn = nil
			}
			unacked = carry
			result = fdLeaseErr
		}
	}()

	// initCtx: initialize with the SESSION-INITIATING caller's context (values only,
	// via WithoutCancel), not context.Background(). A CredentialsProviderContext
	// derives credentials from context values, and Background made those invisible so
	// such providers rejected FD sessions or authed with fallback identity. Full-duplex
	// holds ONE connection for MANY callers, so credentials are session-scoped (the
	// first caller's), like any shared/pooled connection (documented on FullDuplex).
	// WithoutCancel keeps the values but drops the caller's deadline/cancel, so one
	// caller's ctx expiry cannot abort an init the whole session depends on. init goes
	// through initPooledConn (shared with the main/pipeline paths): it records the
	// create-time metric and Removes the conn on any failure, so the defer, seeing
	// cn=nil, does not double-release.
	initCtx := bg
	if len(carry) > 0 && carry[0].ctx != nil {
		initCtx = context.WithoutCancel(carry[0].ctx)
	}

	// Acquire+init from the pipeline pool; SPILL to the main pool when the pipeline
	// pool cannot serve the lease, mirroring withPipelineConn — an FD lease must not
	// fail already-accepted commands while the main pool has idle capacity. Unlike a
	// per-round-trip pipeline borrow, a spilled FD session holds the main-pool conn for
	// its whole lifetime (until idle/maxHold); that is the accepted cost of not
	// stranding the backlog. TryGet (non-blocking) so a saturated pipeline pool spills
	// at once instead of stalling up to PoolTimeout. Spill on saturation
	// (ErrPoolTryFull / ErrPoolExhausted) or a pipeline-conn init failure (the main
	// pool may have an idle conn); NOT on a non-saturation error (ctx cancelled, pool
	// closed) — the main pool would fail the same way. Acquire under ap.ctx (not bg) so
	// a Close cancelling ap.ctx returns at once instead of waiting out PoolTimeout;
	// init and session I/O stay on bg so accepted commands still complete during Close.
	if ref := fd.client.loadPipelinePool(); ref != nil {
		spill := false
		cn, aerr = ref.pool.TryGet(fd.ap.ctx)
		if aerr != nil {
			cn = nil
			if !errors.Is(aerr, pool.ErrPoolTryFull) && !errors.Is(aerr, pool.ErrPoolExhausted) {
				return carry, fdLeaseErr, aerr
			}
			spill = true
		} else if e := fd.client.initPooledConn(initCtx, ref.pool, cn); e != nil {
			cn = nil // initPooledConn already Removed it from the pipeline pool
			spill = true
		}
		if spill {
			cn, aerr = fd.client.connPool.Get(fd.ap.ctx)
			if aerr != nil {
				cn = nil
				return carry, fdLeaseErr, aerr
			}
			connPool = fd.client.connPool
			if e := fd.client.initPooledConn(initCtx, fd.client.connPool, cn); e != nil {
				cn = nil // initPooledConn already Removed it from the main pool
				return carry, fdLeaseErr, e
			}
		}
	} else {
		// No dedicated pipeline pool (PipelinePoolSize < 0, or an internal wrapper
		// client): fd.pool IS the main pool, so acquire directly with no spill.
		cn, aerr = fd.pool.Get(fd.ap.ctx)
		if aerr != nil {
			cn = nil
			return carry, fdLeaseErr, aerr
		}
		if e := fd.client.initPooledConn(initCtx, fd.pool, cn); e != nil {
			cn = nil // initPooledConn already Removed it
			return carry, fdLeaseErr, e
		}
	}

	// The lease is decided: spilled iff the conn came from the main pool (a spill, or
	// no dedicated pipeline pool). Stored before session() spawns the reader, so the
	// reader's retryOnNormalConn sees the right value (happens-before).
	fd.curConnSpilled.Store(connPool == fd.client.connPool)

	unacked, result, aerr = fd.session(bg, cn, carry)
	return unacked, result, aerr
}

// session runs the writer (this goroutine) + reader (spawned) on one connection
// until Close (graceful) or a connection error (returns the unacked tail).
func (fd *fdEngine) session(bg context.Context, cn *pool.Conn, carry []fdReq) (unacked []fdReq, result fdResult, aerr error) {
	inflight := newFDInflightCap(min(fd.maxBatch, fd.window)) // capped by the window (peak) and the batch; grow() backstops
	fd.curInflight.Store(inflight)                            // test observability (peak in-flight)
	fd.curConn.Store(cn)                                      // test observability (handoff)
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
		// commands already completed in the panicking snapshot leave the deque.
		// Otherwise recovery re-owns and re-completes them, overwriting good results
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
					// never misread as the command's reply (FIFO misalign). PROPAGATE a
					// drain error, do NOT log-and-continue: a custom PushNotificationProcessor
					// can return after consuming only part of a frame, leaving the reader
					// desynced, so reading this reply would shift every later reply. Fail the
					// session instead (the shared pre-command drainer treats a custom-processor
					// error as connection-fatal for the same reason); the unacked tail is
					// replayable (errFDPushDrainFailed is in the replay predicate) and re-runs
					// on a fresh connection rather than reading shifted bytes.
					if perr := fd.client.processPendingPushNotificationWithReader(bg, cn, rd); perr != nil {
						internal.Logger.Printf(bg, "autopipeline: full-duplex push drain: %v", perr)
						return fmt.Errorf("%w: %w", errFDPushDrainFailed, perr)
					}
					return req.cmd.readReply(rd)
				})
				if e != nil && fdReplyIsFatal(e) {
					// Connection/protocol error, OR a push-drain desync (fatal even when it
					// wraps a Redis-typed cause — see fdReplyIsFatal): stop; the unread tail
					// stays in the deque and becomes the unacked recovery set for replay.
					rerr = e
					break
				}
				// The reply landed (nil, or a reply-LEVEL Redis error / redirect — a
				// server that answers is healthy, NOT a transport failure). If this req
				// closes an admitted chunk, settle its Limiter obligation with success:
				// exactly one ReportResult(nil) per Allow, on the reply side. Fires for
				// both the inline completion below and the retryable-divert branch (the
				// reply WAS read; the divert re-runs the command elsewhere under its own
				// getConn Allow/Report pairing).
				if req.limReport != nil {
					req.limReport.settle(nil)
				}
				// A retryable Redis error or a redirect (MOVED/ASK) is NOT the caller's
				// final answer: the FD conn is one fixed socket/node, so re-run the
				// command on the client's NORMAL path, which routes redirects and applies
				// the standard retry/backoff. Done off the reader goroutine so it does not
				// stall other in-flight replies, and counted in `done` so the reader
				// advances past it now. Per-caller ordering is NOT promised across this
				// divert (same exception as the blocking-command divert); NoRetry commands
				// keep their error.
				if e != nil && !fdNoRetrySafe(req.cmd) {
					moved, ask, _ := isMovedError(e)
					// Do NOT divert a redirect. FD is enabled only for a standalone
					// *Client, whose normal path cannot follow a MOVED/ASK — it neither
					// re-routes to the target node nor sends ASKING, it just re-hits the
					// same endpoint. Diverting would waste a round trip and return the
					// redirect anyway, so surface it inline (below), exactly as a plain
					// standalone command does. (A cluster-capable FD/CSC would route it
					// here instead — follow-up.)
					//
					// Divert a RETRYABLE reply only while retries are enabled AND the
					// budget is not already spent. req.attempts counts FD attempts spent
					// (1 at submit, +1 on each fdConnErr carry replay). Once it reaches
					// MaxRetries+1 another execution would exceed the budget, so fall
					// through to the inline settle, which surfaces the reply as the final
					// error and reports the true attempt count. Without this guard the
					// startAttempt clamp in processWithRetry would turn an exhausted budget
					// into one more send.
					if !moved && !ask &&
						shouldRetry(e, false) &&
						fd.client.opt.MaxRetries > 0 &&
						req.attempts <= fd.client.opt.MaxRetries {
						// The retryable reply executed on the FD socket, so the divert starts
						// one attempt in; add req.attempts-1 for FD attempts already spent on
						// carry replays so a carried-then-diverted command does not run the
						// full loop from the base. The guard above keeps this within
						// MaxRetries+1.
						fd.retryOnNormalConn(req, retryStartAttempt(false, false)+req.attempts-1)
						done++
						continue
					}
				}
				req.cmd.SetErr(e) // nil, a redirect (MOVED/ASK), or a non-retryable Redis error
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
				// Emit the per-command metric callbacks under a recover boundary (see
				// reportReplyMetrics): they are user-settable, and an unrecovered panic
				// here would reach the reader's session-failure recovery BEFORE this req
				// is advanced, so recovery would re-own the already-consumed reply and
				// replay it — a mutating command twice.
				fd.reportReplyMetrics(octx, req, e, cn)
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

	// Panic boundary for the WRITER path. The reader goroutine above has its own recover;
	// the writer (this goroutine) runs writeCarryChunked, the serve loop and the
	// Close-backlog flush with no top-level recover, so an unguarded user-code panic (a
	// Cmder Args()/encoder, a Limiter, a metrics callback) would kill the sole fd.run
	// goroutine and leave attempt()'s defer to Put a live conn. Registered AFTER the
	// reader is spawned, so readerDone is guaranteed to close. On a panic, run the
	// fdConnErr teardown (stop the reader, wait it out, recover the unacked tail) and
	// return fdConnErr NORMALLY, so attempt()'s release defer Removes the desynced conn
	// and run() replays the eligible tail (errFDPanicRecovered is replayable, bounded by
	// each command's own attempt budget). The known sizing/limiter/metrics panics are
	// already contained at their sites (cmdApproxBytesSafe, fdBatchEndSafe, fdAllow,
	// reportReplyMetrics); this backstop guarantees the goroutine survives any other.
	defer func() {
		if r := recover(); r != nil {
			e := fmt.Errorf("%w: full-duplex session: %v", errFDPanicRecovered, r)
			internal.Logger.Printf(bg, "autopipeline: recovered full-duplex session writer panic: %v\n%s", r, debug.Stack())
			failOnce(e)
			inflight.hardClose()
			_ = cn.Close()
			<-readerDone
			unacked = fd.settleTail(inflight.takeRemaining(), e)
			result = fdConnErr
			aerr = e
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
		// Cap the drain scratch at the window, not MaxBatchSize: the writer can never
		// have more than fd.window commands in flight, so a large MaxBatchSize with a
		// small window would over-allocate (up to OOM) for no gain. Matches the
		// in-flight ring's min(maxBatch, window) cap.
		scratch := make([]fdReq, 0, min(fd.maxBatch, fd.window))
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
			// write to a node known to be moving until idle/max-hold. Otherwise the
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
				batchBytes, sizeErr := cmdApproxBytesSafe(req.cmd)
				if sizeErr != nil {
					// req.cmd.Args() panicked (custom Cmder) while sizing the batch, on
					// this recover-less serve loop. Fail just that command and take the
					// next: nothing was written and nothing is in flight, so dropping it
					// here avoids letting it reach writeBatch, whose write-time recover
					// would tear the whole session down and replay its batch-mates
					// at-least-once. It is the only command in the batch, so skip the flush.
					// Do not resetIdle: a dropped command is not session activity, and the
					// idle timer firing normally is harmless.
					fd.failReqs(batch, sizeErr)
					continue
				}
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
						rb, sizeErr := cmdApproxBytesSafe(r.cmd)
						if sizeErr != nil {
							// r.cmd.Args() panicked while sizing. Fail just r, DROP it from
							// the batch, and flush the good prefix accumulated so far. Failing
							// it while leaving it in batch would double-complete it: writeBatch
							// would push it into inflight and the reader would settle it again.
							fd.failReqs(batch[len(batch)-1:], sizeErr)
							batch = batch[:len(batch)-1]
							break drain
						}
						batchBytes += rb
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
				// Get/Put-churn (and re-run the session hooks) every interval.
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
		// boundary.
		unwritten, e := fd.flushBacklogForClose(bg, cn, inflight, readerDone)
		if e != nil && !errors.Is(e, errFDConnMoving) {
			// A real write error (dead conn) failed the backlog partway: some flushed
			// commands have no reply coming and the unwritten suffix is already in
			// inflight (writeCarryChunked pushed it), so closeGraceful would park the
			// reader on replies that never arrive. Close the conn to wake the reader,
			// then RETURN the unacked tail as the recovery set instead of failing it
			// here. As fdConnErr it flows through run()'s standard tail recovery — the
			// per-command budget partition and the NoRetry split. Because ap.ctx
			// is cancelled (this is Close), the eligible prefix is then executed by
			// shutdownFlush on a fresh connection. That honors "accepted ⇒ completes"
			// for the never-sent fd.ch backlog (attempts==1, never touched the dead
			// socket) instead of failing it errFDReaderGone, while the NoRetry split
			// still keeps a written-but-unacked NoRetry command from a second execution
			// (it is failed by the split, never reaching shutdownFlush). takeRemaining
			// holds only UNACKED commands — the reader advanced completed ones out — so
			// nothing already settled is re-run.
			failOnce(e)
			inflight.hardClose()
			_ = cn.Close()
			<-readerDone
			return fd.settleTail(inflight.takeRemaining(), e), fdConnErr, e
		}
		// e == nil (fully flushed) OR errFDConnMoving (handoff mid-flush on a LIVE
		// conn). Either way the connection is healthy: drain the already-written prefix
		// to a boundary so those callers complete with real replies (never failed with a
		// synthetic moving error), then let attempt() Put the conn — a handoff-marked
		// conn is handed off seamlessly by OnPut, exactly like the serve-loop and
		// carry-replay recycles.
		inflight.closeGraceful()
		<-readerDone
		if sharedErr != nil {
			// Reader failed during the final drain: RECOVER the stranded tail (plus the
			// never-sent handoff suffix, in order) instead of failing it wholesale, and
			// report the error so attempt() removes the desynced conn. As fdConnErr the
			// set flows through run()'s standard tail recovery — per-command budget
			// partition and the sent-NoRetry split. Because ap.ctx is cancelled
			// (this is Close), the eligible prefix is then executed by shutdownFlush on a
			// fresh connection, honoring "accepted ⇒ completes" exactly like the
			// backlog-flush failure branch above. Failing here handed every acked-write
			// (replayable reads included) the read error just because Close raced a slow
			// reply.
			return fd.settleTail(fdRecoverTail(inflight.takeRemaining(), unwritten), sharedErr), fdConnErr, sharedErr
		}
		// Handoff mid-flush: the prefix drained cleanly and the conn will be Put
		// (OnPut handoff). RETURN the never-sent suffix so run() completes it on
		// ANOTHER connection AFTER attempt() has Put this conn — accepted ⇒
		// completes. Flushing it HERE would run while attempt() still holds this
		// conn, so with both pools saturated the suffix would deterministically
		// fail. Empty on a plain Close with no handoff. Mirrors the recycle
		// path, which likewise returns its suffix for run() to replay.
		return unwritten, fdGraceful, nil
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
			// poisoned. carrySuffix (never-sent, e.g. the unwritten tail of a handoff
			// recycle) rides behind the drained tail, in order, and is REFUNDED by
			// fdRecoverTail: writeCarryChunked deliberately did not refund it (the clean
			// fdRecycle return below does not re-bump), but this path re-enters run()'s
			// fdConnErr recovery, which does — without the refund the suffix would be
			// charged for a send that never happened and could be declared
			// budget-exhausted one replay early (e.g. MaxRetries=1: one real send on a
			// dropped session, then MOVING mid-replay plus a reader failure here).
			return fd.settleTail(fdRecoverTail(inflight.takeRemaining(), carrySuffix), sharedErr), fdConnErr, sharedErr
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
		return fd.settleTail(unacked, sharedErr), fdConnErr, sharedErr
	}
}

// fdAllow calls the user Limiter's Allow under a recovery boundary. Allow is user
// code that runs on the engine's background writer goroutine, BEFORE writeBatch
// arms its serialize-panic defer, so a panicking Allow would otherwise escape
// fd.run and crash the process with the whole accepted chunk unsettled (half-duplex
// wraps user code via recoverDispatchPanic). A recovered panic is converted to a
// wrapped error and handled EXACTLY like a deny: no permit was granted, so the
// caller reports nothing (strict Allow/ReportResult pairing) and fails only this
// chunk, leaving the connection healthy and untouched.
func fdAllow(ctx context.Context, lim Limiter) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: limiter Allow: %v", errFDPanicRecovered, r)
			internal.Logger.Printf(ctx, "autopipeline: recovered full-duplex limiter Allow panic: %v\n%s", r, debug.Stack())
		}
	}()
	return lim.Allow()
}

// writeBatch pushes each req onto the in-flight FIFO (so it is tracked as
// unacked even if the flush then fails) and writes the whole batch in one
// buffered flush. A write error leaves the reqs in the deque for recovery.
func (fd *fdEngine) writeBatch(bg context.Context, cn *pool.Conn, inflight *fdInflight, reqs []fdReq) (err error) {
	if len(reqs) == 0 {
		return nil
	}
	// Per-chunk Limiter admission. The Limiter's unit everywhere else in the
	// client is one connection-acquiring wire operation — a single-command
	// attempt, a pipeline exec, a half-duplex autopipeline flush — and the FD
	// equivalent of that unit is one written chunk, so Allow/ReportResult pair
	// here, per chunk. The session LEASE deliberately does not pay: a lease-long
	// permit pinned a breaker's half-open probe budget for the whole session
	// lifetime (probe starvation) and gave near-zero failure-signal density (one
	// report per session, however long it ran). The obligation is settled on the
	// REPLY side (see fdLimiterReport): reply-LEVEL errors report success (nil) — a
	// server that answers is healthy — while a transport failure that abandons the
	// chunk's unread replies reports that error (settleTail); further failures also
	// surface through the next chunk's write attempt on the replacement conn and
	// through diverted retries, which keep their own Allow/Report pairing via
	// getConn (parity with single-command retries paying per attempt).
	//
	// A deny fails ONLY this chunk, with the Limiter's error verbatim (failReqs
	// sets it on each command, emits the error metric, and completes the
	// callers): the connection is healthy and untouched — nothing stamped sent,
	// nothing pushed in-flight — so the session continues and the NEXT chunk
	// pays Allow again (fast-fail while a breaker is open, automatic resume when
	// it closes). This early return sits BEFORE the recovery defer below is
	// armed, so a denied chunk can never be pushed into the in-flight deque. A
	// panicking Allow (user code on the writer goroutine) is caught by fdAllow and
	// folded into this same deny path — no permit, so no ReportResult.
	var report *fdLimiterReport
	if lim := fd.client.opt.Limiter; lim != nil {
		if aerr := fdAllow(bg, lim); aerr != nil {
			fd.failReqs(reqs, aerr)
			return nil
		}
		// Admitted: one obligation for this chunk's Allow, settled exactly once on
		// the REPLY side, not at write time (finding ed53z: a peer that accepts the
		// write then drops before replying must be a FAILURE the breaker sees). A
		// clean write hands the obligation to the reader via the chunk's last req
		// (settle nil once every reply lands); a transport failure that abandons the
		// unread replies settles the error (settleTail). A WRITE failure/panic HERE
		// means the replies will never come, so report the write error now — this
		// defer is registered BEFORE the recovery defer below so it runs AFTER it
		// (LIFO) and sees the final err (an encoder panic converted to
		// errFDPanicRecovered is a failed write, not a skipped report). On a clean
		// write it is a no-op; the obligation rides the deque. A denied Allow reports
		// nothing, per the Limiter contract.
		report = &fdLimiterReport{lim: lim}
		defer func() {
			if err != nil {
				report.settle(err)
			}
		}()
	}
	// Publish the batch to the in-flight deque only AFTER a clean serialize+flush
	// (see below). Then the reader arms its per-reply ReadTimeout once the write has
	// reached the socket, not while a slow user encoder (a BinaryMarshaler) is still
	// running. A slow encoder could time out a healthy connection and trigger a
	// spurious replay that duplicates a mutating command. On a partial write or an
	// encoder panic the conn is desynced, so the batch must still land in the deque
	// for the normal conn-error recovery to settle every caller; this defer does that
	// if the happy-path push below did not run. A command encoder panic (writeCmd on a
	// bad BinaryMarshaler) runs on the writer goroutine, where it would otherwise
	// crash the process — convert it to a connection error.
	pushed := false
	// written is the count of commands the serialize loop REACHED this call (index+1
	// of the last one it touched): the attempt-local twin of the lifetime `sent`
	// stamp, reset every call. The recovery defer refunds by it, not by `sent`.
	written := 0
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("%w: encoding batch: %v", errFDPanicRecovered, r)
			internal.Logger.Printf(bg, "autopipeline: recovered full-duplex write panic: %v\n%s", r, debug.Stack())
		}
		if !pushed {
			// Recovery push (partial write or encoder panic): refund the optimistic
			// submit/replay attempt for every command the serializer never REACHED
			// THIS call (index >= written). run()'s fdConnErr recovery partitions the
			// tail by attempt count (fdPartitionByBudget) BEFORE it consults the
			// NoRetry/sent gate, and that partition keys on attempts, not sent. So a
			// command left at its pre-charged attempt count is declared budget-exhausted
			// and FAILED without ever executing (acute at MaxRetries<=1).
			//
			// Refund by `written`, NOT by the lifetime `sent` flag: `sent` is sticky
			// across replays, so on a SECOND-session replay whose earlier command's
			// encoder panics the later commands still carry sent==true from the first
			// session. A `!sent` refund would skip that suffix, leave it over-charged,
			// and lose a retry it never got (MaxRetries==1: exhausted after only its
			// original send). `written` is attempt-local, so it refunds exactly the
			// suffix this call did not reach, regardless of a prior session's send. The
			// prefix reached this call (< written, including a command whose own writeCmd
			// panicked) keeps its charge. It was attempted at-least-once. Mirrors the
			// never-written-suffix refunds in writeCarryChunked / fdRecoverTail.
			fdRefundUnsentAttempt(reqs[written:])
			inflight.pushBatch(reqs)
		}
	}()
	// Stamp the wire-write time and mark each command SENT per-command, immediately
	// before its writeCmd runs, NOT in a bulk loop before the flush. Only commands
	// the serializer actually REACHES are marked sent: if an earlier command's encoder
	// panics (a bad BinaryMarshaler) or a partial write aborts the loop, the
	// never-serialized suffix keeps sent=false, so the NoRetry gate replays it
	// (issuing it is its FIRST send) instead of failing a command the server never
	// saw. A command whose own writeCmd fails/panics stays sent=true: its bytes may
	// have reached the buffer/wire, so the conservative choice avoids re-sending a
	// NoRetry twice; a WithWriter flush error after N commands serialized leaves those
	// N sent. Stamped before pushBatch so the deque copies the reader reads carry both.
	now := time.Now()
	err = cn.WithWriter(bg, fd.client.opt.WriteTimeout, func(wr *proto.Writer) error {
		for i := range reqs {
			if reqs[i].writtenAt.IsZero() {
				reqs[i].writtenAt = now // first write anchors the duration; replays keep it
			}
			reqs[i].sent = true
			written = i + 1 // reached this command this call (attempt-local; see refund defer)
			if e := writeCmd(wr, reqs[i].cmd); e != nil {
				return e
			}
		}
		return nil
	})
	if err == nil {
		// Clean flush: attach this chunk's Limiter obligation to its LAST req so the
		// reader settles ReportResult(nil) once every reply lands, then publish — the
		// reader only starts its reply deadline once the bytes are on the wire. On
		// error/panic the report defer above fires the write error and the recovery
		// defer pushes the reqs WITHOUT an obligation, so nothing double-reports.
		if report != nil {
			reqs[len(reqs)-1].limReport = report
		}
		inflight.pushBatch(reqs)
		pushed = true
	}
	return err
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

// fdBatchEndSafe is fdBatchEnd with a per-command recover. cmd.Args() (used by
// cmdApproxBytes) is user code and may panic. A carried chunk can include commands
// that never passed the serve loop's cmdApproxBytesSafe admission — the session-start
// command taken straight from fd.ch and the Close backlog drained by takeQueue — so a
// deterministic panicking Args() can reach here; it must be contained WITHOUT tearing
// down a healthy connection (nothing in the chunk is written yet, so the conn is not
// desynced). On a panic it returns the clean prefix end (start..end, end>=start) and
// bad = the index of the offending command, plus the wrapped error; the caller writes
// [start:end), fails+drops carry[bad], and resumes at bad+1. bad == -1 means the whole
// chunk sized cleanly.
func fdBatchEndSafe(reqs []fdReq, start, maxBatch int, byteLimit int64) (end, bad int, err error) {
	end, bad = start, -1
	idx := start // the command currently being sized; the recover reports it as bad
	defer func() {
		if r := recover(); r != nil {
			end, bad = idx, idx // clean prefix is [start:idx); idx is what panicked
			err = fmt.Errorf("%w: Args: %v", errFDPanicRecovered, r)
			internal.Logger.Printf(context.Background(),
				"autopipeline: recovered full-duplex carry Args() panic: %v\n%s", r, debug.Stack())
		}
	}()
	bytes := cmdApproxBytes(reqs[start].cmd)
	end = start + 1
	for end < len(reqs) && end-start < maxBatch {
		if byteLimit > 0 && bytes >= byteLimit {
			break
		}
		idx = end
		bytes += cmdApproxBytes(reqs[end].cmd)
		end++
	}
	return end, -1, nil
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
				fdRefundUnsentAttempt(carry[i:]) // never written: do not charge this send
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
		// observed the room and refilled it without consuming the signal). So after
		// each wake recheck inflight.len() >= fd.window before writing. A one-shot if
		// could consume that stale signal with the deque still full, compute zero
		// remaining room, leave lim at MaxBatchSize, and write past FullDuplexWindow.
		// On graceful Close this writes the backlog on top of replies still in
		// flight, so without the gate a small FullDuplexWindow is exceeded by up to
		// ~2x (window + buffered backlog). Two wait modes, split on ap.ctx:
		//   - LIVE engine (ordinary carry replay at session start): honor the window
		//     like the serve loop — wait for room with no bail-out. The wait is
		//     bounded by the reader itself (a dead peer trips its ReadTimeout, the
		//     reader exits, readerDone fires), and ap.ctx.Done switches a concurrent
		//     Close to the bounded mode without waiting on a drain.
		//   - CLOSE-time flush (ap.ctx cancelled): the wait is BOUNDED instead — if
		//     the reader does not drain within fdCloseFlushWait, stop gating (set
		//     stuck) so Close cannot block forever; correctness of a terminating
		//     Close outranks a transient teardown overshoot.
		for !stuck && readerDone != nil && inflight.len() >= fd.window {
			// Poll handoff on every wake too, like the serve loop's window gate, so a
			// MOVING mark under backpressure recycles within one drained reply. Suffix
			// out-of-band (no push), same as the between-chunk check above.
			if cn.ShouldHandoff() {
				return carry[i:], errFDConnMoving
			}
			if fd.ap.ctx.Err() == nil {
				select {
				case <-inflight.room:
				case <-readerDone:
					fdRefundUnsentAttempt(carry[i:]) // never written: do not charge this send
					inflight.pushBatch(carry[i:])
					return nil, errFDReaderGone
				case <-fd.ap.ctx.Done():
					// Close raced in: re-enter the loop in bounded mode.
				}
				continue
			}
			timer := time.NewTimer(fdCloseFlushWait)
			select {
			case <-inflight.room:
			case <-readerDone:
				timer.Stop()
				fdRefundUnsentAttempt(carry[i:]) // never written: do not charge this send
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
		// Carry re-sizing uses cmd.Args() (user code), guarded by fdBatchEndSafe. A
		// carried chunk CAN include commands that never passed the serve loop's
		// cmdApproxBytesSafe admission — the session-start command taken straight from
		// fd.ch (run() blocks on the first command and hands it in as carry) and the
		// Close backlog drained by takeQueue — so a deterministic panicking Args() can
		// reach here. Contain it WITHOUT tearing the session down: nothing in this chunk
		// is written yet, so the conn is healthy. Fail+drop just the offending command
		// (like the serve loop's sizing guard) and resume with the rest, instead of
		// killing the engine goroutine and letting attempt()'s defer Put a live conn.
		// The contract (see AddHook / a Cmder's Args) still requires deterministic,
		// panic-free Args(); this only stops one bad command from stranding a whole
		// accepted backlog.
		end, bad, sizeErr := fdBatchEndSafe(carry, i, lim, byteLimit)
		if end > i {
			if e := fd.writeBatch(bg, cn, inflight, carry[i:end]); e != nil {
				// writeBatch pushed carry[i:end] into inflight before the failed write (it
				// was attempted — at-least-once — so the serialized prefix keeps its bumped count), but
				// the suffix carry[end:] was never pushed. Push it too, or it sits in neither
				// fd.ch nor inflight and its callers hang: on fdConnErr takeRemaining replays
				// it, on Close the caller fails it. It is only ever settled via failReqs or
				// replayed — never completed inline by the reader — so its zero writtenAt
				// never reaches the write→reply metric. carry[end:] was NEVER written, so
				// refund its optimistic attempt bump here; the never-serialized tail of
				// carry[i:end] (behind an encoder panic) is refunded by writeBatch itself, so
				// only its serialized prefix keeps the charge.
				if end < len(carry) {
					fdRefundUnsentAttempt(carry[end:])
					inflight.pushBatch(carry[end:])
				}
				return nil, e
			}
		}
		if bad >= 0 {
			// carry[bad] (== carry[end]) panicked while sizing. It was never written and
			// is not in inflight, so failing it here cannot double-complete it. Refund the
			// optimistic attempt bump (run() charged the whole carry a send before
			// re-issuing it), fail just this command, and resume past it — the healthy
			// conn keeps serving the rest of the carry.
			fdRefundUnsentAttempt(carry[bad : bad+1])
			fd.failReqs(carry[bad:bad+1], sizeErr)
			i = bad + 1
			continue
		}
		i = end
	}
	return nil, nil
}

// fdRefundUnsentAttempt undoes the optimistic attempt bump for a carried suffix
// that was NEVER written this session (the reader died or the connection broke
// before this chunk was sent). run() bumps the whole carry's attempts before
// re-issuing it, charging each command for a send; a command that was not actually
// sent must not keep that charge, or with a tight MaxRetries it can be declared
// budget-exhausted one replay early. The next replay re-bumps it when it is really
// sent. Floored at 0. Callers apply this to the never-sent suffix BEFORE pushing it
// into the in-flight deque, so the recovered copies carry the corrected count.
func fdRefundUnsentAttempt(reqs []fdReq) {
	for i := range reqs {
		if reqs[i].attempts > 0 {
			reqs[i].attempts--
		}
	}
}

// fdFirstNoRetry returns the index of the first command that must not be
// (re-)issued: a NoRetry command that was already SENT (its bytes may have
// reached the wire — see fdReq.sent), or len(reqs) when there is none. The
// unacked tail is retried up to this index and failed from it on: retryable
// commands ahead of it still get their network retries, while the sent NoRetry
// command and anything ordered after it is never re-sent. A NEVER-SENT NoRetry
// command does not split the tail — replaying it is its first send, so failing
// it would error a command the server never saw.
func fdFirstNoRetry(reqs []fdReq) int {
	for i := range reqs {
		if reqs[i].cmd.NoRetry() && reqs[i].sent {
			return i
		}
	}
	return len(reqs)
}

// fdFirstNoRetrySafe wraps fdFirstNoRetry with a recover. cmd.NoRetry() may be a
// custom Cmder's user code, and the retry-classification scan runs on run()'s
// serve loop, which has no top-level recover — a panic there would kill the
// engine and strand every in-flight and future command. On panic it returns
// panicked=true; the caller then declines to replay and fails the tail (the
// conservative choice: a command that cannot be classified as retryable, and may
// be a sent NoRetry, must never be re-sent).
func fdFirstNoRetrySafe(reqs []fdReq) (n int, panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			internal.Logger.Printf(context.Background(),
				"autopipeline: recovered full-duplex NoRetry() scan panic: %v\n%s", r, debug.Stack())
			n, panicked = 0, true
		}
	}()
	return fdFirstNoRetry(reqs), false
}

// fdNoRetrySafe wraps a single cmd.NoRetry() call with a recover. On the reader's
// reply path NoRetry() is consulted AFTER the reply has already been consumed but
// BEFORE the request is counted complete; a custom Cmder whose NoRetry() panics there
// would otherwise reach the reader's session-failure recover, which treats the request
// as an unacked tail and REPLAYS it — running an already-answered mutating command
// twice. Recover locally and report the command as non-retryable (true) so the caller
// surfaces the already-landed reply inline and never diverts or replays it.
func fdNoRetrySafe(cmd Cmder) (noRetry bool) {
	defer func() {
		if r := recover(); r != nil {
			internal.Logger.Printf(context.Background(),
				"autopipeline: recovered full-duplex NoRetry() panic: %v\n%s", r, debug.Stack())
			noRetry = true
		}
	}()
	return cmd.NoRetry()
}

// fdRecoverTail builds an fdConnErr recovery set from a drained unacked tail and
// a never-sent suffix, in order. Fresh slice: rem is takeRemaining's deque-owned
// backing array, so appending onto it could corrupt the deque. The suffix is
// refunded (fdRefundUnsentAttempt): it was never written this session, and run()'s
// fdConnErr recovery bumps the whole replay set for the NEXT issue — without the
// refund a never-sent command would be charged for a send that never happened and
// could be declared budget-exhausted one replay early.
func fdRecoverTail(rem, suffix []fdReq) []fdReq {
	out := make([]fdReq, 0, len(rem)+len(suffix))
	out = append(out, rem...)
	out = append(out, suffix...)
	fdRefundUnsentAttempt(out[len(rem):])
	return out
}

// settleTail settles every Limiter obligation carried by an fdConnErr recovery
// set with err, exactly once, and returns the SAME slice so it wraps a recovery
// expression inline. Called at each session() point that hands a
// written-but-unacked tail back for replay/failure: the reader never completed
// these chunks, so their reply-side outcome is this transport error. Settling
// here — inside session(), inseparable from producing the recovery set — keeps
// "no obligation outlives its session" readable in one function and pairs every
// Allow whose replies never arrived. The field is cleared so the obligation
// never travels into the replay (the rewrite's writeBatch mints a fresh Allow +
// obligation).
func (fd *fdEngine) settleTail(reqs []fdReq, err error) []fdReq {
	for i := range reqs {
		if reqs[i].limReport != nil {
			reqs[i].limReport.settle(err)
			reqs[i].limReport = nil
		}
	}
	return reqs
}

// failReqs completes a set of commands with err (used on retry exhaustion / Close).
// classifyCommandErrorGuarded wraps classifyCommandError with a recover: it calls
// err.Error(), which is user-reachable (e.g. an error returned by a custom Limiter) and
// can panic. The failing paths classify BEFORE completing their requests, and the chunk
// is not in the in-flight queue, so an escaping panic here would leave every caller
// blocked forever (session recovery cannot reclaim it). On panic, fall back to empty
// classification so the requests still settle.
func classifyCommandErrorGuarded(err error) (errorType, statusCode string, isInternal bool) {
	defer func() {
		if r := recover(); r != nil {
			internal.Logger.Printf(context.Background(),
				"autopipeline: recovered full-duplex error classification panic: %v", r)
			errorType, statusCode, isInternal = "", "", false
		}
	}()
	return classifyCommandError(err)
}

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
		errorType, statusCode, isInternal = classifyCommandErrorGuarded(err)
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
			// Report attempts-1 retries (like processWithRetry), so a carried tail
			// failed after replays is not undercounted as zero. max() guards a req that
			// somehow carries attempts==0. Guarded per-req (not around the loop) so a
			// panicking callback still lets THIS req and every later one settle below.
			retries := max(0, reqs[i].attempts-1)
			fd.emitMetricsGuarded(octx, func() {
				errorCallback(octx, errorType, nil, statusCode, isInternal, retries)
			})
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
	// Drain and close the queue now (before flushing carry) so no new submit lands
	// mid-flush. fresh commands (attempts == 1) have not run yet.
	fresh := fd.takeQueue()
	// Flush the carried tail honoring EACH command's remaining retry budget across
	// the Close boundary (flushCarryBudgeted), then the fresh queue at the full
	// budget. Flushing carry first keeps FIFO order across the two sets.
	if err := fd.flushCarryBudgeted(bg, carry); err != nil {
		// Carry hit an unreachable endpoint; do not run the fresh queue through a full
		// retry cycle against the same dead endpoint (Close would otherwise stall for
		// chunks × retries × backoff) — fail it with the same transport error.
		fd.failReqs(fresh, err)
		return
	}
	// Last flush: a transport failure is already handled inside flushReqs (it fails
	// the remainder), and there is nothing after it, so the returned error is moot.
	_ = fd.flushReqs(bg, fresh, fd.client.opt.MaxRetries)
}

// fdCarryRemainingRetries returns the retry bound for a carried command flushed on
// Close. carry commands are POST-BUMP: run() bumps attempts before re-issuing, so a
// command carried at attempts=A has completed A-1 executions (contrast the pre-bump
// unacked tail in run(), where A executions are done). Of its MaxRetries+1 total
// budget it may still run MaxRetries+2-A times, i.e. a retry bound of
// MaxRetries+1-A. A negative result means the budget is spent (drop the command).
// attempts is clamped to >=1: attempts==0 is only reachable from test-constructed
// fdReq literals, and the clamp keeps such a command at full budget rather than
// granting MaxRetries+2.
func fdCarryRemainingRetries(attempts, maxRetries int) int {
	if attempts < 1 {
		attempts = 1
	}
	return maxRetries + 1 - attempts
}

// flushCarryBudgeted flushes the carried tail on Close so no command exceeds — or
// falls short of — its configured MaxRetries+1 total executions. Commands are
// flushed in contiguous groups of equal attempt count, each with its own remaining
// budget (fdCarryRemainingRetries); a group whose budget is spent is failed, not
// re-run. Grouping equal-attempt RUNS is correct regardless of ordering (carry is
// normally attempts-descending, so groups are few, but a non-sorted slice just
// yields more groups). Returns a transport error that aborted the remainder.
func (fd *fdEngine) flushCarryBudgeted(bg context.Context, carry []fdReq) error {
	mr := fd.client.opt.MaxRetries
	for i := 0; i < len(carry); {
		a := carry[i].attempts
		j := i + 1
		for j < len(carry) && carry[j].attempts == a {
			j++
		}
		group := carry[i:j]
		i = j
		rem := fdCarryRemainingRetries(a, mr)
		if rem < 0 {
			fd.failReqs(group, errFDRetryBudgetExhausted) // budget spent; do not re-run
			continue
		}
		if err := fd.flushReqs(bg, group, rem); err != nil {
			if i < len(carry) {
				fd.failReqs(carry[i:], err) // transport failure: fail the rest of the carry too
			}
			return err
		}
	}
	return nil
}

// flushReqs runs reqs through the client pipeline in the same
// MaxBatchSize/MaxBatchBytes chunks as normal FD writes and completes each
// request. maxRetries bounds each chunk's retry loop (0 = a single execution,
// used for already-attempted carried commands so their per-command budget is not
// exceeded). Returns a non-nil error when the remainder was aborted and failed
// here: a transport failure, a desynchronized reply stream (errConnUnusable — e.g.
// a custom push processor errored during the drain, so the chunk was never sent),
// or a recovered serialize panic. A plain per-command Redis error is a normal
// result and does not abort.
func (fd *fdEngine) flushReqs(bg context.Context, reqs []fdReq, maxRetries int) (retErr error) {
	if len(reqs) == 0 {
		return nil
	}
	// A panic here (user arg encoder inside the pipeline) runs on the engine
	// goroutine with no other recovery; fail and complete the remainder so no caller
	// hangs. Set the NAMED return so an ordered shutdown flush aborts like a
	// transport failure: an unnamed result would zero to nil after recovery, and
	// flushCarryBudgeted/shutdownFlush would then treat the failed group as success
	// and run later groups + the fresh queue even though an earlier command never
	// completed.
	i := 0
	defer func() {
		if r := recover(); r != nil {
			retErr = fmt.Errorf("%w: shutdown flush: %v", errFDPanicRecovered, r)
			internal.Logger.Printf(bg, "autopipeline: recovered shutdown-flush panic: %v\n%s", r, debug.Stack())
			fd.failReqs(reqs[i:], retErr)
		}
	}()
	// Test seam: nil in production, so this is exactly client.processPipelineRetries.
	run := fd.runPipeline
	if run == nil {
		run = fd.client.processPipelineRetries
	}
	// Same MaxBatchSize/MaxBatchBytes chunking as normal FD writes: reqs can hold a
	// large window, and one unchunked pipeline would ignore MaxBatchBytes and burst
	// the connection.
	byteLimit := int64(fd.ap.config.MaxBatchBytes) // 0 = disabled
	for i < len(reqs) {
		end := fdBatchEnd(reqs, i, fd.maxBatch, byteLimit)
		// Do not mix retry policies in one chunk: generalProcessPipeline disables
		// retries for the WHOLE chunk if any command is NoRetry (cmdsContainNoRetry).
		// That would strip retryable commands in the same accepted backlog of their
		// budget. Break the chunk at the first NoRetry-policy change so a NoRetry
		// command (e.g. RawWriteToCmd) is isolated from its retryable neighbors, like
		// the half-duplex dispatcher's contiguous retry-policy runs. The clamp starts
		// at i+1, so end stays > i and the chunk is never empty (no infinite loop).
		policy := reqs[i].cmd.NoRetry()
		for k := i + 1; k < end; k++ {
			if reqs[k].cmd.NoRetry() != policy {
				end = k
				break
			}
		}
		cmds := make([]Cmder, end-i)
		for j := i; j < end; j++ {
			cmds[j-i] = reqs[j].cmd
		}
		// Initialize the flush with a request's own context (cancellation removed), not
		// the engine's background context: if this flush initializes a fresh pooled
		// connection, a CredentialsProviderContext resolves credentials from the
		// request's context values, so it authenticates as the right tenant.
		// WithoutCancel because these contexts may already be cancelled (often what
		// triggered Close), yet accepted-⇒-completes still requires the write. A chunk
		// can mix callers; the first request is the representative — a documented
		// approximation, matching the diverted-retry and session-init paths.
		fctx := bg
		if c := reqs[i].ctx; c != nil {
			fctx = context.WithoutCancel(c)
		}
		err := run(fctx, cmds, maxRetries) // per-command results/errors set inside
		for j := i; j < end; j++ {
			reqs[j].complete()
		}
		i = end
		// Stop and fail the remaining chunks when the error means the earlier chunk
		// may never have been written correctly: a transport failure that survived
		// the retry loop (dead endpoint), OR a desynchronized reply stream marked
		// errConnUnusable (e.g. a custom push processor errored during the close-time
		// drain, so the chunk was never sent). Continuing would run an ordered
		// shutdown flush out of order. pipelineErrShouldStamp is the same
		// errConnUnusable precedence used in generalProcessPipeline; a plain
		// per-command Redis error is a normal result and does not abort.
		if err != nil && pipelineErrShouldStamp(err) {
			fd.failReqs(reqs[i:], err)
			return err
		}
	}
	return nil
}

// failQueue fails every command currently buffered in fd.ch with err WITHOUT
// closing the engine (unlike takeQueue, the shutdown drain, which sets closed).
// Used on fdLeaseErr, where the carry goes through failReqs and this drains the
// accepted backlog — both halves emit the native error metric. The engine stays
// alive, so a command submitted after this returns is served once the
// server/pool recovers, or failed when the next lease exhausts its retries. The
// channel receive is safe against a concurrent submit send, so no lock is taken
// here.
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
					errorType, statusCode, isInternal = classifyCommandErrorGuarded(err)
					classified = true
				}
				octx := r.ctx
				if octx == nil {
					octx = context.Background()
				}
				// Guarded per-req so a panicking callback still lets r.complete() run.
				fd.emitMetricsGuarded(octx, func() {
					errorCallback(octx, errorType, nil, statusCode, isInternal, 0)
				})
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
// Returns (unwritten, err) from writeCarryChunked: on errFDConnMoving (handoff
// mid-flush, live conn) unwritten is the never-sent suffix the caller flushes
// elsewhere; on a real write error unwritten is nil (that suffix is already in
// inflight) and the caller degrades to the conn-error path.
func (fd *fdEngine) flushBacklogForClose(bg context.Context, cn *pool.Conn, inflight *fdInflight, readerDone <-chan struct{}) ([]fdReq, error) {
	fd.submitMu.Lock()
	fd.closed = true
	fd.submitMu.Unlock()
	var backlog []fdReq
	for {
		select {
		case r := <-fd.ch:
			backlog = append(backlog, r)
		default:
			// Return the unwritten suffix OUT-OF-BAND (do not push it into inflight) so
			// the caller can tell a live handoff (errFDConnMoving) from a dead-conn write
			// error: on handoff it clean-recycles — drains the written prefix, Puts the
			// conn for the OnPut maintenance handoff, and completes the never-sent suffix
			// on another connection — instead of failing accepted work. The dead-conn
			// paths inside writeCarryChunked already pushed their suffix into inflight and
			// return an empty one here.
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
