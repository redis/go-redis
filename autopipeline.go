package redis

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9/internal"
)

// AutoPipelineConfig configures the autopipelining behavior.
type AutoPipelineConfig struct {
	// MaxBatchSize is the maximum number of commands to batch before flushing.
	// Default: 100
	MaxBatchSize int

	// MaxConcurrentBatches is the maximum number of pipeline batches that may
	// execute concurrently.
	//
	// Default: 1, which gives a single ordered command stream — batches execute
	// serially in submit order, so even a windowed caller (submit many, read
	// later) sees strict ordering, while still reaching high throughput via deep
	// pipelines (~3M ops/sec locally).
	//
	// Setting this above 1 runs batches in parallel for maximum throughput, but
	// commands then have NO guaranteed execution order. Because that trades away
	// ordering, it is only allowed together with Unordered: true — otherwise the
	// configuration is rejected (see Validate). This makes the trade-off
	// explicit: you cannot accidentally lose ordering by raising concurrency.
	MaxConcurrentBatches int

	// Unordered must be set to true to allow MaxConcurrentBatches > 1. It is the
	// caller's explicit acknowledgement that parallel batch execution gives up
	// command ordering in exchange for throughput. With the default (false),
	// MaxConcurrentBatches is forced to 1 (an ordered stream) and any value > 1
	// is a configuration error.
	Unordered bool

	// MaxFlushDelay is the maximum delay after flushing before checking for more commands.
	// A small delay (e.g., 100μs) can significantly reduce CPU usage by allowing
	// more commands to batch together, at the cost of slightly higher latency.
	//
	// Trade-off:
	// - 0 (default): Lowest latency, higher CPU usage
	// - 100μs: Balanced (recommended for most workloads)
	// - 500μs: Lower CPU usage, higher latency
	//
	// Based on benchmarks, 100μs can reduce CPU usage by 50%
	// while adding only ~100μs average latency per command.
	// Default: 0 (no delay)
	MaxFlushDelay time.Duration

	// AdaptiveDelay enables smart delay calculation based on queue fill level.
	// When enabled, the delay is automatically adjusted:
	// - Queue ≥75% full: No delay (flush immediately to prevent overflow)
	// - Queue ≥50% full: 25% of MaxFlushDelay (queue filling up)
	// - Queue ≥25% full: 50% of MaxFlushDelay (moderate load)
	// - Queue <25% full: 100% of MaxFlushDelay (low load, maximize batching)
	//
	// This provides automatic adaptation to varying load patterns without
	// manual tuning. Uses integer-only arithmetic for optimal performance.
	// Default: false (use fixed MaxFlushDelay)
	AdaptiveDelay bool
}

// defaultAccumulateWindow is the batch-coalescing window used by the flusher
// when MaxFlushDelay is not set. It is small enough to add negligible latency
// but long enough to let concurrent callers fill a pipeline instead of each
// command being flushed on its own.
const defaultAccumulateWindow = 200 * time.Microsecond

// numAutoPipelineShards chooses how many independent queue+flusher shards to
// run. More shards reduce enqueue mutex contention and let several batches be
// assembled in parallel, but there is no point exceeding the concurrent-batch
// budget or the number of CPUs.
func numAutoPipelineShards(maxConcurrentBatches int) int {
	n := runtime.GOMAXPROCS(0)
	if n > maxConcurrentBatches {
		n = maxConcurrentBatches
	}
	if n < 1 {
		n = 1
	}
	const maxShards = 16
	if n > maxShards {
		n = maxShards
	}
	return n
}

// DefaultAutoPipelineConfig returns the default autopipelining configuration.
//
// The default is ordered: MaxConcurrentBatches is 1, so batches execute
// serially in submit order (a single ordered command stream) while still
// reaching high throughput via deep pipelines when callers submit in windows.
// To trade ordering for parallel-batch throughput, set MaxConcurrentBatches > 1
// together with Unordered: true.
func DefaultAutoPipelineConfig() *AutoPipelineConfig {
	return &AutoPipelineConfig{
		MaxBatchSize:         200,
		MaxConcurrentBatches: 1, // ordered by default
		MaxFlushDelay:        0, // No delay by default (lowest latency)
	}
}

// DefaultBlockingAutoPipelineConfig is the default for the blocking face
// (Client.AutoPipeline). It runs many batches in parallel (MaxConcurrentBatches:
// 50) for high throughput. Unordered is set because the engine requires it for
// MaxConcurrentBatches>1 — but a blocking caller still observes per-goroutine
// ordering, since it waits for each command's result before issuing the next.
// Global cross-goroutine order (which is not meaningful for independent callers)
// is what Unordered relaxes.
func DefaultBlockingAutoPipelineConfig() *AutoPipelineConfig {
	return &AutoPipelineConfig{
		MaxBatchSize:         300,
		MaxConcurrentBatches: 50,
		Unordered:            true,
	}
}

// Validate reports whether the configuration is self-consistent. It returns an
// error if MaxConcurrentBatches > 1 without Unordered: true — raising
// concurrency gives up command ordering, so the caller must opt in explicitly.
func (cfg *AutoPipelineConfig) Validate() error {
	if cfg.MaxConcurrentBatches > 1 && !cfg.Unordered {
		return fmt.Errorf("redis: AutoPipelineConfig.MaxConcurrentBatches=%d requires Unordered:true "+
			"(parallel batches do not preserve command ordering); set Unordered:true to allow it, "+
			"or keep MaxConcurrentBatches=1 for an ordered stream", cfg.MaxConcurrentBatches)
	}
	// Reject obviously-wrong negatives so a typo surfaces at construction rather
	// than being silently coerced to a default. Zero is allowed and means "use
	// the default" (MaxBatchSize) or "no delay" (MaxFlushDelay).
	if cfg.MaxBatchSize < 0 {
		return fmt.Errorf("redis: AutoPipelineConfig.MaxBatchSize=%d must be >= 0", cfg.MaxBatchSize)
	}
	if cfg.MaxConcurrentBatches < 0 {
		return fmt.Errorf("redis: AutoPipelineConfig.MaxConcurrentBatches=%d must be >= 0", cfg.MaxConcurrentBatches)
	}
	if cfg.MaxFlushDelay < 0 {
		return fmt.Errorf("redis: AutoPipelineConfig.MaxFlushDelay=%s must be >= 0", cfg.MaxFlushDelay)
	}
	return nil
}

// cmdableClient is an interface for clients that support pipelining.
// Both Client and ClusterClient implement this interface.
type cmdableClient interface {
	Cmdable
	Process(ctx context.Context, cmd Cmder) error
}

// apBatch is the completion signal shared by every command flushed together.
// Its done channel is closed exactly once, when the batch's pipeline has
// executed. Closing one channel wakes all waiters in a single operation,
// instead of doing one buffered-channel send per command — under high
// concurrency the per-command sends dominated CPU (channel-lock contention and
// one goroutine wake-up apiece).
type apBatch struct {
	done chan struct{}
}

func newAPBatch() *apBatch { return &apBatch{done: make(chan struct{})} }

// The shard queue stores bare Cmders. The batch a command waits on is the
// shard's curBatch at enqueue time — read once to wire the command's ready
// channel and never needed per-command afterward (the flusher closes the one
// shared batch). Storing []Cmder removes a per-command wrapper allocation.

var queueSlicePool = sync.Pool{
	New: func() interface{} { s := make([]Cmder, 0, 100); return &s },
}

func getQueueSlice(capacity int) []Cmder {
	slice := (*queueSlicePool.Get().(*[]Cmder))[:0]
	if cap(slice) < capacity {
		queueSlicePool.Put(&slice)
		return make([]Cmder, 0, capacity)
	}
	return slice
}

func putQueueSlice(slice []Cmder) {
	if cap(slice) <= 1000 {
		full := slice[:cap(slice)]
		for i := range full {
			full[i] = nil
		}
		queueSlicePool.Put(&slice)
	}
}

// AutoPipeliner automatically batches commands and executes them in pipelines.
// It's safe for concurrent use by multiple goroutines.
//
// AutoPipeliner works by:
// 1. Collecting commands from multiple goroutines into a shared queue
// 2. Automatically flushing the queue when:
//   - The batch size reaches MaxBatchSize
//
// 3. Executing batched commands using Redis pipelining
//
// This provides significant performance improvements for workloads with many
// concurrent small operations, as it reduces the number of network round-trips.
//
// AutoPipeliner implements the Cmdable interface, so you can use it like a regular client:
//
//	ap := client.AutoPipeline()
//	ap.Set(ctx, "key", "value", 0)
//	ap.Get(ctx, "key")
//	ap.Close()
//
// Per-command contexts: a command is batched and executed on the AutoPipeliner's
// own long-lived context, NOT the context passed to the command. A per-command
// deadline or cancellation is therefore not honored once the command is queued
// (this is deliberate — a per-batch timer per command would cost a goroutine
// each). Use a plain client for commands that need their own deadline.
//
// Lifetime: AutoPipeline() returns a single, client-owned instance shared by all
// callers. Close()ing it (or closing the client) stops the shared pipeliner for
// everyone; a later AutoPipeline() call builds a fresh one.
type AutoPipeliner struct {
	cmdable // Embed cmdable to get all Redis command methods

	pipeliner cmdableClient
	config    *AutoPipelineConfig
	// blocking selects how the typed command surface (Set, Get, ...) behaves:
	// when true the command call itself blocks until the command has executed
	// (drop-in, synchronous shape); when false the call returns immediately and
	// the result accessors (Val/Result/Err) block. See AutoPipeline (blocking)
	// vs AsyncAutoPipeline (deferred).
	blocking bool

	// Sharded command queues. Each shard has its own queue, mutex and flusher
	// goroutine, so enqueues from many goroutines spread across shards instead
	// of all contending on a single mutex and being drained by a single
	// flusher. Commands are assigned to shards round-robin; per-goroutine
	// ordering is still guaranteed because Do blocks for each command's result
	// before issuing the next one.
	shards []*apShard
	next   atomic.Uint32 // round-robin shard selector
	// shardFn, when set, picks a command's shard from its content (cluster mode
	// sets it to route by slot so all commands for one node land in the same
	// shard's batch — keeping per-node pipelines deep instead of splitting every
	// batch across nodes). When nil, commands are assigned round-robin.
	shardFn func(Cmder) int

	// Lifecycle
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup // Tracks flusher goroutines
	batchWg sync.WaitGroup // Tracks batch execution goroutines
	closed  atomic.Bool
}

// apShard is one queue + flusher. Its fields are touched only by enqueuing
// goroutines (under mu) and by its own single flusher goroutine.
type apShard struct {
	ap *AutoPipeliner

	mu       sync.Mutex
	queue    []Cmder
	queueLen atomic.Int32
	curBatch *apBatch                // completion signal for currently-queued cmds
	notify   chan struct{}           // buffered (cap 1) enqueue wake-up
	sem      *internal.FIFOSemaphore // per-shard concurrent-batch budget
}

// NewAutoPipeliner creates a deferred (async) autopipeliner for the given
// client (*Client or *ClusterClient): command methods return immediately and
// the result accessors block. For the blocking drop-in form, use
// newAutoPipeliner with blocking=true (exposed via Client.AutoPipeline).
//
// It returns an error if the config is invalid (e.g. MaxConcurrentBatches>1
// without Unordered, or a negative size); a nil config uses defaults.
func NewAutoPipeliner(pipeliner cmdableClient, config *AutoPipelineConfig) (*AutoPipeliner, error) {
	return newAutoPipeliner(pipeliner, config, false)
}

// newAutoPipeliner builds an autopipeliner in either blocking or deferred mode.
func newAutoPipeliner(pipeliner cmdableClient, config *AutoPipelineConfig, blocking bool) (*AutoPipeliner, error) {
	if config == nil {
		config = DefaultAutoPipelineConfig()
	} else {
		// Copy so default-filling below doesn't mutate the caller's struct — the
		// same *AutoPipelineConfig may be shared across clients (e.g. a reused
		// Options.AutoPipelineConfig), and callers may inspect it afterward.
		cfgCopy := *config
		config = &cfgCopy
	}

	// Apply defaults for zero values
	if config.MaxBatchSize <= 0 {
		config.MaxBatchSize = 200
	}

	if config.MaxConcurrentBatches <= 0 {
		// Default to an ordered single stream. Callers raise this (with
		// Unordered:true) to opt into parallel-batch throughput.
		config.MaxConcurrentBatches = 1
	}

	// Reject configurations that silently drop ordering. This is a deterministic
	// misconfiguration; surface it as an error so the post-init AutoPipeline /
	// AsyncAutoPipeline calls never panic on a bad config.
	if err := config.Validate(); err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	ap := &AutoPipeliner{
		pipeliner: pipeliner,
		config:    config,
		blocking:  blocking,
		ctx:       ctx,
		cancel:    cancel,
	}

	// Route the typed command surface. Blocking: the command call blocks until
	// executed (synchronous drop-in shape). Deferred: the call returns at once
	// and the result accessors block until the batch executes.
	if blocking {
		ap.cmdable = ap.processBlocking
	} else {
		ap.cmdable = ap.processAsync
	}

	// Pick a shard count: enough to spread mutex/flusher contention across
	// cores, but never more than the concurrent-batch budget (extra shards
	// could not all flush at once anyway) and capped so tiny configs stay
	// single-shard.
	nShards := numAutoPipelineShards(config.MaxConcurrentBatches)
	// Split the concurrent-batch budget across shards so each shard has its own
	// semaphore. A single shared semaphore became a contention point once the
	// per-shard queue mutexes were no longer the bottleneck.
	perShard := config.MaxConcurrentBatches / nShards
	if perShard < 1 {
		perShard = 1
	}
	ap.shards = make([]*apShard, nShards)
	for i := range ap.shards {
		s := &apShard{
			ap:       ap,
			notify:   make(chan struct{}, 1),
			queue:    getQueueSlice(config.MaxBatchSize),
			sem:      internal.NewFIFOSemaphore(int32(perShard)),
			curBatch: newAPBatch(),
		}
		ap.shards[i] = s
		ap.wg.Add(1)
		go s.flusher()
	}

	return ap, nil
}

// Do queues a command for autopipelined execution and returns immediately.
// The returned command will block when you access its result (Err(), Val(), Result(), etc.)
// until the command has been executed.
//
// This allows sequential usage without goroutines:
//
//	cmd1 := ap.Do(ctx, "GET", "key1")
//	cmd2 := ap.Do(ctx, "GET", "key2")
//	// Commands are queued, will be batched and flushed automatically
//	val1, err1 := cmd1.Result()  // Blocks until command executes
//	val2, err2 := cmd2.Result()  // Blocks until command executes
func (ap *AutoPipeliner) Do(ctx context.Context, args ...interface{}) Cmder {
	cmd := NewCmd(ctx, args...)
	if len(args) == 0 {
		cmd.SetErr(ErrClosed)
		return cmd
	}

	// Follow the autopipeliner's mode: blocking executes before returning,
	// deferred returns immediately (read the result to block).
	if ap.blocking {
		_ = ap.processBlocking(ctx, cmd)
	} else {
		_ = ap.processAsync(ctx, cmd)
	}
	return cmd
}

// Process queues a command for autopipelined execution and returns immediately.
// The command is executed asynchronously when its batch is flushed; reading the
// command's result (Val/Result/Err) blocks until then.
func (ap *AutoPipeliner) Process(ctx context.Context, cmd Cmder) error {
	return ap.processAsync(ctx, cmd)
}

// AutoFuture is the handle returned by Submit. Call Wait (or Result on the
// command after Wait) once the result is needed; it blocks only until the
// command's batch has executed.
type AutoFuture struct {
	cmd   Cmder
	batch *apBatch
}

// Wait blocks until the submitted command has executed, then returns its error.
func (f AutoFuture) Wait() error {
	<-f.batch.done
	return f.cmd.Err()
}

// Cmd returns the underlying command (call Wait first before reading results).
func (f AutoFuture) Cmd() Cmder { return f.cmd }

// submit queues a command without blocking and returns its completion future.
func (ap *AutoPipeliner) submit(ctx context.Context, cmd Cmder) AutoFuture {
	if cmd.readTimeout() != nil {
		// Blocking commands are executed directly, outside the pipeline.
		_ = ap.pipeliner.Process(ctx, cmd)
		return AutoFuture{cmd: cmd, batch: closedBatch}
	}
	return AutoFuture{cmd: cmd, batch: ap.enqueue(cmd)}
}

// Submit queues a command without blocking and returns an AutoFuture; Wait on
// it when the result is needed. This is the explicit form for working with raw
// Cmders; the typed methods (Set, Get, ...) provide the same deferred behaviour
// returning the usual *XxxCmd.
func (ap *AutoPipeliner) Submit(ctx context.Context, cmd Cmder) AutoFuture {
	return ap.submit(ctx, cmd)
}

// processAsync is the cmdable backing the typed command surface: it queues a
// command without blocking the caller and marks it ready so the command's
// result accessors (Val/Result/Err) block until the batch executes. This gives
// the autopipeliner the full typed surface (ap.Set, ap.Get, ...) with the exact
// same call shape as a normal client — only the wait is deferred to the point a
// result is read.
func (ap *AutoPipeliner) processAsync(ctx context.Context, cmd Cmder) error {
	f := ap.submit(ctx, cmd)
	cmd.setReady(f.batch.done)
	return nil
}

// processBlocking is the cmdable backing the blocking face: it queues the
// command and blocks until its batch has executed, so the command call has the
// same synchronous shape as a normal client (the returned *XxxCmd already holds
// its result). The flusher still batches this command with other concurrent
// callers' commands into a pipeline, so throughput is far above a plain client
// even though each caller waits. Per-goroutine ordering holds regardless of
// MaxConcurrentBatches: a caller cannot issue its next command until this one
// returns, so its commands execute in submit order.
func (ap *AutoPipeliner) processBlocking(ctx context.Context, cmd Cmder) error {
	return ap.submit(ctx, cmd).Wait()
}

// closedBatch is a reusable already-completed batch for error cases, so a
// caller that enqueues after Close returns immediately.
var closedBatch = func() *apBatch {
	b := newAPBatch()
	close(b.done)
	return b
}()

// enqueue queues a command and returns the batch whose done channel completes
// when it has executed. On a closed autopipeliner it errors the command and
// returns the already-closed batch.
func (ap *AutoPipeliner) enqueue(cmd Cmder) *apBatch {
	if ap.closed.Load() {
		cmd.SetErr(ErrClosed)
		return closedBatch
	}

	// Pick a shard. With shardFn (cluster mode) route by command content so all
	// commands for one node collect in the same shard's batch; otherwise spread
	// round-robin to keep each shard's mutex lightly contended.
	var s *apShard
	if ap.shardFn != nil {
		idx := ap.shardFn(cmd)
		if idx < 0 {
			idx = -idx
		}
		s = ap.shards[idx%len(ap.shards)]
	} else {
		s = ap.shards[int(ap.next.Add(1)-1)%len(ap.shards)]
	}

	s.mu.Lock()
	// Re-check closed under the shard lock (see Close): either we win the lock
	// first and the shutdown drain flushes us, or the drain ran first and we
	// reject here — so a late enqueue never hangs on an unclosed done.
	if ap.closed.Load() {
		s.mu.Unlock()
		cmd.SetErr(ErrClosed)
		return closedBatch
	}
	batch := s.curBatch
	s.queue = append(s.queue, cmd)
	s.queueLen.Store(int32(len(s.queue)))
	s.mu.Unlock()

	s.wake()
	return batch
}

// wake signals the shard's flusher that work is available without blocking.
func (s *apShard) wake() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// process is the internal method that queues a command and returns its done channel.
// func (ap *AutoPipeliner) process(ctx context.Context, cmd Cmder) <-chan struct{} {
//  	return ap.processWithQueuedCmd(ctx, cmd).done
// }

// IsClosed reports whether the AutoPipeliner has been closed, either by an
// explicit Close or by closing the owning client. A closed AutoPipeliner
// rejects new commands with ErrClosed.
func (ap *AutoPipeliner) IsClosed() bool {
	return ap.closed.Load()
}

// numShards reports how many shards this autopipeliner runs.
func (ap *AutoPipeliner) numShards() int { return len(ap.shards) }

// setShardFn installs a content-based shard selector (cluster mode routes by
// slot so each shard's batch stays on one node). Must be called before the
// autopipeliner is used. Not safe to change concurrently with enqueues.
func (ap *AutoPipeliner) setShardFn(fn func(Cmder) int) { ap.shardFn = fn }

// Close stops the autopipeliner and flushes any pending commands.
func (ap *AutoPipeliner) Close() error {
	if !ap.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}

	// Cancel context to stop flushers
	ap.cancel()

	// Wake every shard's flusher so each observes the cancelled context promptly.
	for _, s := range ap.shards {
		s.wake()
	}

	// Wait for flushers to finish
	ap.wg.Wait()

	// Wait for all batch execution goroutines to finish
	ap.batchWg.Wait()

	return nil
}

// flusher is the per-shard background goroutine that flushes batches.
func (s *apShard) flusher() {
	defer s.ap.wg.Done()
	ap := s.ap

	for {
		// Wait for a command to arrive (or shutdown). The notify channel is a
		// cheap buffered wake-up; no lock is taken on the hot enqueue path.
		if s.Len() == 0 {
			select {
			case <-s.notify:
			case <-ap.ctx.Done():
			}
		}

		// Check if context is cancelled
		if ap.ctx.Err() != nil {
			// Final flush before shutdown - use background context to avoid immediate cancellation
			s.flushBatchSliceShutdown()
			return
		}

		// Coalesce: after the first command wakes us, briefly wait for more
		// commands to accumulate so we flush a full batch instead of one
		// command at a time. This is what makes autopipelining batch under
		// concurrent load. We stop waiting as soon as the queue reaches
		// MaxBatchSize or MaxFlushDelay elapses (whichever comes first).
		s.accumulateBatch()

		// Flush all pending commands
		for s.Len() > 0 {
			select {
			case <-ap.ctx.Done():
				// Final flush before shutdown
				s.flushBatchSliceShutdown()
				return
			default:
			}

			s.flushBatchSlice()

			// Between batches, briefly coalesce again so the next pipeline is
			// also full rather than draining one straggler at a time.
			if s.Len() > 0 && s.Len() < ap.config.MaxBatchSize {
				s.accumulateBatch()
			}
		}
	}
}

// accumulateBatch waits briefly for commands to pile up before the flusher
// drains the queue, so pipelines carry many commands instead of one. It
// returns as soon as any of these holds:
//
//   - the queue reaches MaxBatchSize (batch is full);
//   - the queue stops growing between two polls (all current callers are
//     blocked on their results, so no more commands are coming right now);
//   - MaxFlushDelay (or defaultAccumulateWindow when unset) elapses.
//
// The "stops growing" check is what keeps latency low: with N concurrent
// blocking callers the queue fills to N within a couple of polls and flushes
// immediately, rather than always waiting the whole window.
func (s *apShard) accumulateBatch() {
	ap := s.ap
	batchSize := ap.config.MaxBatchSize
	if batchSize <= 0 {
		batchSize = 1
	}
	if s.Len() >= batchSize {
		return
	}

	// Pick the accumulation window. With AdaptiveDelay set, calculateDelay scales
	// it down as the queue fills (and returns 0 once it's ≥75% full, flushing
	// immediately); otherwise it's the fixed MaxFlushDelay. Fall back to the
	// default window only when no delay is configured at all.
	window := ap.calculateDelay()
	if window <= 0 {
		if ap.config.MaxFlushDelay > 0 || ap.config.AdaptiveDelay {
			// A delay/adaptive mode was configured and the current fill level
			// resolved to "flush now" — don't wait.
			return
		}
		window = defaultAccumulateWindow
	}

	// Wait for enqueue wake-ups (no polling, no sleep) until the batch is full
	// or the window elapses. Each enqueue sends on notify, so we re-check the
	// queue length on every wake-up and return as soon as a full batch is ready.
	deadline := time.NewTimer(window)
	defer deadline.Stop()
	for {
		select {
		case <-ap.ctx.Done():
			return
		case <-deadline.C:
			return
		case <-s.notify:
			if s.Len() >= batchSize {
				return
			}
		}
	}
}

// flushBatchSlice takes the shard's currently-queued commands as one batch,
// swaps in a fresh batch for subsequent enqueues, and dispatches the taken
// batch. Completion is signalled by closing the batch's done channel once
// (waking every waiter in a single operation) rather than one channel send
// per command.
func (s *apShard) flushBatchSlice() {
	ap := s.ap

	s.mu.Lock()
	if len(s.queue) == 0 {
		s.mu.Unlock()
		s.queueLen.Store(0)
		return
	}
	// Take the whole queue as this batch and roll a fresh batch for the
	// commands enqueued after this point. accumulateBatch already bounds the
	// queue to roughly MaxBatchSize before we get here, so taking it whole
	// keeps batches large without a split that would span two batch signals.
	queuedCmds := s.queue
	batch := s.curBatch
	s.queue = getQueueSlice(ap.config.MaxBatchSize)
	s.curBatch = newAPBatch()
	s.queueLen.Store(0)
	s.mu.Unlock()

	// Acquire semaphore (limit concurrent batches)
	if !s.sem.TryAcquire() {
		err := s.sem.Acquire(ap.ctx, 5*time.Second, context.DeadlineExceeded)
		if err != nil {
			// Context cancelled: error all commands and signal completion.
			for _, qc := range queuedCmds {
				qc.SetErr(ErrClosed)
			}
			close(batch.done)
			putQueueSlice(queuedCmds)
			return
		}
	}

	// Fast path for single command. Run inside a func so close(batch.done) and
	// the semaphore release are deferred: a panic in Process still signals
	// completion (waking await()) and frees the permit before it propagates.
	if len(queuedCmds) == 1 {
		func() {
			defer s.sem.Release()
			defer putQueueSlice(queuedCmds)
			defer close(batch.done)
			_ = ap.pipeliner.Process(ap.ctx, queuedCmds[0])
		}()
		return
	}

	// Track this goroutine in the batchWg so Close() waits for it.
	// IMPORTANT: Add to WaitGroup AFTER semaphore is acquired to avoid deadlock.
	ap.batchWg.Add(1)
	go func() {
		defer ap.batchWg.Done()
		defer s.sem.Release()
		defer putQueueSlice(queuedCmds)
		// Signal completion with a single close. Deferred so a panic in
		// Process/Exec (e.g. a malformed command or encoder panic) still wakes
		// every waiter in await() instead of hanging them forever; the close
		// runs after Exec on the happy path, so results are populated first.
		defer close(batch.done)

		// Use the autopipeliner's long-lived context (cancelled by Close)
		// rather than a per-batch context.WithTimeout, which allocated a timer
		// and spawned a runtime timer goroutine for every batch.
		ctx := ap.ctx

		pipe := ap.Pipeline()
		defer putPipeliner(pipe)

		for _, qc := range queuedCmds {
			_ = pipe.Process(ctx, qc)
		}
		_, _ = pipe.Exec(ctx)
	}()
}

// flushBatchSliceShutdown flushes commands during shutdown.
// Unlike flushBatchSlice, this doesn't use ap.ctx for semaphore acquisition
// because ap.ctx is already cancelled during shutdown.
// Executes synchronously to preserve command order.
func (s *apShard) flushBatchSliceShutdown() {
	ap := s.ap
	// Flush all remaining commands synchronously to preserve order.
	for s.Len() > 0 {
		s.mu.Lock()
		if len(s.queue) == 0 {
			s.mu.Unlock()
			s.queueLen.Store(0)
			return
		}
		// Take the whole queue as one batch and roll a fresh batch.
		queuedCmds := s.queue
		batch := s.curBatch
		s.queue = getQueueSlice(ap.config.MaxBatchSize)
		s.curBatch = newAPBatch()
		s.queueLen.Store(0)
		s.mu.Unlock()

		// Execute each batch in a func so close(batch.done) is deferred: a panic
		// in Process/Exec still signals completion (waking await()) before it
		// propagates, instead of leaving shutdown waiters hung.
		func() {
			defer putQueueSlice(queuedCmds)
			defer close(batch.done)

			// ap.ctx is already cancelled here (Close cancels it before draining),
			// so use a fresh background context with no artificial deadline. The
			// wire timeout is then governed by the connection's ReadTimeout /
			// WriteTimeout — exactly like the normal flush path and a plain client
			// Exec. Crucially this lets a relaxed timeout (set by maintnotifications
			// during a failover/migration) take effect; a hardcoded short deadline
			// here would cap that relaxed window and time out in-flight commands the
			// relaxation was meant to protect. (A user who wants shutdown bounded
			// sets ReadTimeout/WriteTimeout on the client, as for any command.)
			ctx := context.Background()
			pipe := ap.Pipeline()
			defer putPipeliner(pipe)
			for _, qc := range queuedCmds {
				_ = pipe.Process(ctx, qc)
			}
			_, _ = pipe.Exec(ctx)
		}()
	}
}

// Len returns the number of queued commands in this shard.
func (s *apShard) Len() int {
	return int(s.queueLen.Load())
}

// Len returns the current number of queued commands across all shards.
func (ap *AutoPipeliner) Len() int {
	total := 0
	for _, s := range ap.shards {
		total += s.Len()
	}
	return total
}

// calculateDelay calculates the delay based on current queue length.
// Uses integer-only arithmetic for optimal performance (no float operations).
// Returns 0 if MaxFlushDelay is 0.
func (ap *AutoPipeliner) calculateDelay() time.Duration {
	maxDelay := ap.config.MaxFlushDelay
	if maxDelay == 0 {
		return 0
	}

	// If adaptive delay is disabled, return fixed delay
	if !ap.config.AdaptiveDelay {
		return maxDelay
	}

	// Get current queue length
	queueLen := ap.Len()
	if queueLen == 0 {
		return 0
	}

	maxBatch := ap.config.MaxBatchSize

	// Use integer arithmetic to avoid float operations
	// Calculate thresholds: 75%, 50%, 25% of maxBatch
	// Multiply by 4 to avoid division: queueLen * 4 vs maxBatch * 3 (75%)
	//
	// Adaptive delay strategy:
	// - ≥75% full: No delay (flush immediately to prevent overflow)
	// - ≥50% full: 25% of max delay (queue filling up)
	// - ≥25% full: 50% of max delay (moderate load)
	// - <25% full: 100% of max delay (low load, maximize batching)
	switch {
	case queueLen*4 >= maxBatch*3: // queueLen >= 75% of maxBatch
		return 0 // Flush immediately
	case queueLen*2 >= maxBatch: // queueLen >= 50% of maxBatch
		return maxDelay >> 2 // Divide by 4 using bit shift (faster)
	case queueLen*4 >= maxBatch: // queueLen >= 25% of maxBatch
		return maxDelay >> 1 // Divide by 2 using bit shift (faster)
	default:
		return maxDelay
	}
}

// Pipeline returns a new pipeline that uses the underlying pipeliner.
// This allows you to create a traditional pipeline from an autopipeliner.
func (ap *AutoPipeliner) Pipeline() Pipeliner {
	return ap.pipeliner.Pipeline()
}

// AutoPipeline returns itself.
// This satisfies the Cmdable interface.
func (ap *AutoPipeliner) AutoPipeline() *AutoPipeliner {
	return ap
}

// Pipelined executes a function in a pipeline context.
// This is a convenience method that creates a pipeline, executes the function,
// and returns the results.
func (ap *AutoPipeliner) Pipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	return ap.pipeliner.Pipeline().Pipelined(ctx, fn)
}

// TxPipelined executes a function in a transaction pipeline context.
// This is a convenience method that creates a transaction pipeline, executes the function,
// and returns the results.
//
// Note: This uses the underlying client's TxPipeline if available (Client, Ring, ClusterClient).
// For other clients, this will panic.
func (ap *AutoPipeliner) TxPipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	return ap.pipeliner.TxPipeline().Pipelined(ctx, fn)
}

// TxPipeline returns a new transaction pipeline that uses the underlying pipeliner.
// This allows you to create a traditional transaction pipeline from an autopipeliner.
//
// Note: This uses the underlying client's TxPipeline if available (Client, Ring, ClusterClient).
// For other clients, this will panic.
func (ap *AutoPipeliner) TxPipeline() Pipeliner {
	return ap.pipeliner.TxPipeline()
}

// validate AutoPipeliner implements Cmdable
var _ Cmdable = (*AutoPipeliner)(nil)
