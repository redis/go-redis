package redis

import (
	"context"
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

	// MaxConcurrentBatches is the maximum number of concurrent pipeline executions.
	// This prevents overwhelming the server with too many concurrent pipelines.
	// Default: 10
	MaxConcurrentBatches int

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

// DefaultAutoPipelineConfig returns the default autopipelining configuration.
func DefaultAutoPipelineConfig() *AutoPipelineConfig {
	return &AutoPipelineConfig{
		MaxBatchSize:         50,
		MaxConcurrentBatches: 10,
		MaxFlushDelay:        0, // No delay by default (lowest latency)
	}
}

// pipelineExecutor is an interface for clients that can execute pipelines directly.
// This allows bypassing Pipeline object creation for better performance.
type pipelineExecutor interface {
	// execPipeline executes a batch of commands as a pipeline.
	// This is the internal pipeline execution function.
	execPipeline(ctx context.Context, cmds []Cmder) error
}

// cmdableClient is an interface for clients that support pipelining.
// Both Client and ClusterClient implement this interface.
type cmdableClient interface {
	Cmdable
	Process(ctx context.Context, cmd Cmder) error
}

// cmdBatch holds a batch of commands and their done channels in parallel slices.
// This avoids the need for a wrapper struct and eliminates conversion loops.
type cmdBatch struct {
	cmds  []Cmder
	dones []chan struct{}
}

// cmdBatchPool pools cmdBatch objects to reduce allocations
var cmdBatchPool = sync.Pool{
	New: func() interface{} {
		return &cmdBatch{
			cmds:  make([]Cmder, 0, 100),
			dones: make([]chan struct{}, 0, 100),
		}
	},
}

// getCmdBatch gets a cmdBatch from the pool
func getCmdBatch() *cmdBatch {
	batch := cmdBatchPool.Get().(*cmdBatch)
	batch.cmds = batch.cmds[:0]
	batch.dones = batch.dones[:0]
	return batch
}

// putCmdBatch returns a cmdBatch to the pool
func putCmdBatch(batch *cmdBatch) {
	// Only pool batches that aren't too large
	if cap(batch.cmds) <= 1000 {
		// Clear slices to avoid holding references
		clear(batch.cmds)
		clear(batch.dones)
		batch.cmds = batch.cmds[:0]
		batch.dones = batch.dones[:0]
		cmdBatchPool.Put(batch)
	}
}

// doneChanPool pools done channels to reduce allocations
var doneChanPool = sync.Pool{
	New: func() interface{} {
		return make(chan struct{}, 1)
	},
}

// getDoneChan gets a done channel from the pool
func getDoneChan() chan struct{} {
	ch := doneChanPool.Get().(chan struct{})
	// Drain any leftover signal
	select {
	case <-ch:
	default:
	}
	return ch
}

// putDoneChan returns a done channel to the pool
func putDoneChan(ch chan struct{}) {
	// Drain before returning
	select {
	case <-ch:
	default:
	}
	doneChanPool.Put(ch)
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
type AutoPipeliner struct {
	cmdable // Embed cmdable to get all Redis command methods

	pipeliner    cmdableClient
	pipelineExec pipelineExecer // Direct pipeline execution function (avoids Pipeline object creation)
	config       *AutoPipelineConfig
	maxBatchSize int // Cached for fast access

	// Command queue - parallel slices for commands and done channels
	// This avoids wrapper structs and eliminates conversion loops
	mu        sync.Mutex
	queueCmds []Cmder          // Commands queue
	queueDone []chan struct{}  // Done channels queue (parallel to queueCmds)
	queueLen  atomic.Int32     // Fast path check without lock

	// Flush control
	flushCond *sync.Cond // Condition variable to signal flusher

	// Concurrency control
	sem *internal.FIFOSemaphore // Semaphore for concurrent batch limit

	// Lifecycle
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup // Tracks flusher goroutine
	batchWg sync.WaitGroup // Tracks batch execution goroutines
	closed  atomic.Bool
}

// NewAutoPipeliner creates a new autopipeliner for the given client.
// The client can be either *Client or *ClusterClient.
func NewAutoPipeliner(pipeliner cmdableClient, config *AutoPipelineConfig) *AutoPipeliner {
	if config == nil {
		config = DefaultAutoPipelineConfig()
	}

	// Apply defaults for zero values
	if config.MaxBatchSize <= 0 {
		config.MaxBatchSize = 50
	}

	if config.MaxConcurrentBatches <= 0 {
		config.MaxConcurrentBatches = 10
	}

	ctx, cancel := context.WithCancel(context.Background())

	ap := &AutoPipeliner{
		pipeliner:    pipeliner,
		config:       config,
		maxBatchSize: config.MaxBatchSize, // Cache for fast access
		sem:          internal.NewFIFOSemaphore(int32(config.MaxConcurrentBatches)),
		ctx:          ctx,
		cancel:       cancel,
	}

	// Try to get direct pipeline execution function for better performance
	// This avoids creating Pipeline objects for each batch
	if pe, ok := pipeliner.(pipelineExecutor); ok {
		ap.pipelineExec = pe.execPipeline
	}

	// Initialize condition variable for flush signaling
	// Use a separate mutex for the condition variable to avoid contention with queue operations
	ap.flushCond = sync.NewCond(&sync.Mutex{})

	// Initialize cmdable to route all commands through processAndBlock
	ap.cmdable = ap.processAndBlock

	// Initialize queue slices with capacity for typical batch size
	ap.queueCmds = make([]Cmder, 0, config.MaxBatchSize)
	ap.queueDone = make([]chan struct{}, 0, config.MaxBatchSize)

	// Start background flusher
	ap.wg.Add(1)
	go ap.flusher()

	return ap
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

	_ = ap.processAndBlock(ctx, cmd)
	return cmd
}

// Process queues a command for autopipelined execution and returns immediately.
// The command will be executed asynchronously when the batch is flushed.
//
// Unlike Do(), this does NOT wrap the command, so accessing results will NOT block.
// Use this only when you're managing synchronization yourself (e.g., with goroutines).
//
// For sequential usage, use Do() instead.
func (ap *AutoPipeliner) Process(ctx context.Context, cmd Cmder) error {
	return ap.processAndBlock(ctx, cmd)
}

// processAndBlock is used by the cmdable interface.
// It queues the command and blocks until execution completes.
// This allows typed methods like Get(), Set(), etc. to work correctly with autopipelining.
func (ap *AutoPipeliner) processAndBlock(ctx context.Context, cmd Cmder) error {
	// Check if this is a blocking command (has read timeout set)
	// Blocking commands like BLPOP, BRPOP, BZMPOP should not be autopipelined
	if cmd.readTimeout() != nil {
		// Execute blocking commands directly without autopipelining
		return ap.pipeliner.Process(ctx, cmd)
	}

	done := ap.enqueue(cmd)

	// Block until the command is executed
	<-done

	// Return the done channel to the pool
	putDoneChan(done)

	return cmd.Err()
}

// closedChan is a reusable closed channel for error cases
var closedChan = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// enqueue adds a command to the queue and returns its done channel.
// The caller is responsible for returning the done channel to the pool after use.
func (ap *AutoPipeliner) enqueue(cmd Cmder) chan struct{} {
	if ap.closed.Load() {
		cmd.SetErr(ErrClosed)
		return closedChan
	}

	// Get done channel from pool
	done := getDoneChan()

	// Add to queue - try fast path first, then slow path
	if !ap.mu.TryLock() {
		ap.mu.Lock()
	}
	ap.queueCmds = append(ap.queueCmds, cmd)
	ap.queueDone = append(ap.queueDone, done)
	queueLen := len(ap.queueCmds)
	ap.queueLen.Store(int32(queueLen))
	ap.mu.Unlock()

	// Signal the flusher - only signal when queue was empty (first command)
	// or when batch size is reached. This reduces signal overhead.
	// Must hold the lock when signaling to avoid race condition where signal is lost
	// if flusher is between Unlock() and the next Wait().
	if queueLen == 1 || queueLen >= ap.maxBatchSize {
		ap.flushCond.L.Lock()
		ap.flushCond.Signal()
		ap.flushCond.L.Unlock()
	}

	return done
}

// process is the internal method that queues a command and returns its done channel.
func (ap *AutoPipeliner) process(ctx context.Context, cmd Cmder) <-chan struct{} {
	return ap.enqueue(cmd)
}

// Close stops the autopipeliner and flushes any pending commands.
func (ap *AutoPipeliner) Close() error {
	if !ap.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}

	// Cancel context to stop flusher
	ap.cancel()

	// Signal the flusher to wake up and check context
	// Use Broadcast instead of Signal to ensure wake-up even if multiple waiters
	ap.flushCond.L.Lock()
	ap.flushCond.Broadcast()
	ap.flushCond.L.Unlock()

	// Wait for flusher to finish
	ap.wg.Wait()

	// Wait for all batch execution goroutines to finish
	ap.batchWg.Wait()

	return nil
}

// flusher is the background goroutine that flushes batches.
func (ap *AutoPipeliner) flusher() {
	defer ap.wg.Done()

	for {
		// Wait for a command to arrive using condition variable
		ap.flushCond.L.Lock()
		for ap.Len() == 0 && ap.ctx.Err() == nil {
			ap.flushCond.Wait()
		}
		ap.flushCond.L.Unlock()

		// Check if context is cancelled
		if ap.ctx.Err() != nil {
			// Final flush before shutdown - use background context to avoid immediate cancellation
			ap.flushBatchSliceShutdown()
			return
		}

		// Flush all pending commands
		for ap.Len() > 0 {
			select {
			case <-ap.ctx.Done():
				// Final flush before shutdown
				ap.flushBatchSliceShutdown()
				return
			default:
			}

			ap.flushBatchSlice()

			// Apply delay if configured and queue still has items
			if ap.Len() > 0 {
				delay := ap.calculateDelay()
				if delay > 0 {
					time.Sleep(delay)
				}
			}
		}
	}
}

// flushBatchSlice flushes commands from the slice-based queue.
func (ap *AutoPipeliner) flushBatchSlice() {
	// Get commands from queue
	ap.mu.Lock()
	queueLen := len(ap.queueCmds)
	if queueLen == 0 {
		ap.mu.Unlock()
		ap.queueLen.Store(0)
		return
	}

	// Get a batch from pool to hold the commands
	batch := getCmdBatch()
	batchSize := ap.maxBatchSize

	if queueLen <= batchSize {
		// Take all commands - swap slices
		batch.cmds, ap.queueCmds = ap.queueCmds, batch.cmds
		batch.dones, ap.queueDone = ap.queueDone, batch.dones
		ap.queueLen.Store(0)
	} else {
		// Take only MaxBatchSize commands, leave the rest in the queue
		// Copy first batchSize elements to batch
		batch.cmds = append(batch.cmds, ap.queueCmds[:batchSize]...)
		batch.dones = append(batch.dones, ap.queueDone[:batchSize]...)

		// Shift remaining elements to front (avoid allocation)
		remaining := queueLen - batchSize
		copy(ap.queueCmds, ap.queueCmds[batchSize:])
		copy(ap.queueDone, ap.queueDone[batchSize:])
		ap.queueCmds = ap.queueCmds[:remaining]
		ap.queueDone = ap.queueDone[:remaining]
		ap.queueLen.Store(int32(remaining))
	}
	ap.mu.Unlock()

	// Acquire semaphore (limit concurrent batches)
	// Try fast path first
	if !ap.sem.TryAcquire() {
		// Fast path failed, need to wait
		err := ap.sem.Acquire(ap.ctx, 5*time.Second, context.DeadlineExceeded)
		if err != nil {
			// Context cancelled, set error on all commands and notify
			for i, cmd := range batch.cmds {
				cmd.SetErr(ErrClosed)
				batch.dones[i] <- struct{}{}
			}
			putCmdBatch(batch)
			return
		}
	}

	if len(batch.cmds) == 0 {
		ap.sem.Release()
		putCmdBatch(batch)
		return
	}

	// Fast path for single command - use Process directly (no pipeline overhead)
	if len(batch.cmds) == 1 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = ap.pipeliner.Process(ctx, batch.cmds[0])
		cancel()
		batch.dones[0] <- struct{}{}
		ap.sem.Release()
		putCmdBatch(batch)
		return
	}

	// Execute the batch - use goroutine to allow concurrent batch execution
	// Track this goroutine in the batchWg so Close() waits for it
	// IMPORTANT: Add to WaitGroup AFTER semaphore is acquired to avoid deadlock
	ap.batchWg.Add(1)
	go ap.executeBatch(batch)
}

// executeBatch executes a batch of commands.
// This is extracted to avoid closure allocation overhead.
func (ap *AutoPipeliner) executeBatch(batch *cmdBatch) {
	defer ap.batchWg.Done()
	defer ap.sem.Release()
	defer putCmdBatch(batch)

	// Use a timeout context to prevent hanging forever
	// This ensures Close() won't block indefinitely if Redis is unresponsive
	// 5 seconds should be enough for most Redis operations
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)

	// Use direct pipeline execution if available (avoids Pipeline object creation)
	// No conversion needed - batch.cmds is already []Cmder!
	if ap.pipelineExec != nil {
		_ = ap.pipelineExec(ctx, batch.cmds)
	} else {
		// Fallback: use Pipeline object
		pipe := ap.Pipeline()
		_ = pipe.BatchProcess(ctx, batch.cmds...)
		_, _ = pipe.Exec(ctx)
		putPipeliner(pipe)
	}

	cancel()

	// IMPORTANT: Only notify after pipeline execution is complete
	// This ensures command results are fully populated before waiters proceed
	for _, done := range batch.dones {
		done <- struct{}{}
	}
}

// flushBatchSliceShutdown flushes commands during shutdown.
// Unlike flushBatchSlice, this doesn't use ap.ctx for semaphore acquisition
// because ap.ctx is already cancelled during shutdown.
// Executes synchronously to preserve command order.
func (ap *AutoPipeliner) flushBatchSliceShutdown() {
	// Flush all remaining commands synchronously to preserve order
	for ap.Len() > 0 {
		// Get commands from queue
		ap.mu.Lock()
		queueLen := len(ap.queueCmds)
		if queueLen == 0 {
			ap.mu.Unlock()
			ap.queueLen.Store(0)
			return
		}

		// Get a batch from pool
		batch := getCmdBatch()
		batchSize := ap.maxBatchSize

		if queueLen <= batchSize {
			// Take all commands - swap slices
			batch.cmds, ap.queueCmds = ap.queueCmds, batch.cmds
			batch.dones, ap.queueDone = ap.queueDone, batch.dones
			ap.queueLen.Store(0)
		} else {
			// Take only MaxBatchSize commands
			batch.cmds = append(batch.cmds, ap.queueCmds[:batchSize]...)
			batch.dones = append(batch.dones, ap.queueDone[:batchSize]...)

			// Shift remaining elements to front
			remaining := queueLen - batchSize
			copy(ap.queueCmds, ap.queueCmds[batchSize:])
			copy(ap.queueDone, ap.queueDone[batchSize:])
			ap.queueCmds = ap.queueCmds[:remaining]
			ap.queueDone = ap.queueDone[:remaining]
			ap.queueLen.Store(int32(remaining))
		}
		ap.mu.Unlock()

		if len(batch.cmds) == 0 {
			putCmdBatch(batch)
			return
		}

		// Execute batch synchronously to preserve order
		// Use a reasonable timeout - if Redis is unresponsive, fail fast
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

		// Use direct pipeline execution if available (avoids Pipeline object creation)
		// No conversion needed - batch.cmds is already []Cmder!
		if ap.pipelineExec != nil {
			_ = ap.pipelineExec(ctx, batch.cmds)
		} else {
			// Fallback: use Pipeline object
			pipe := ap.Pipeline()
			_ = pipe.BatchProcess(ctx, batch.cmds...)
			_, _ = pipe.Exec(ctx)
			putPipeliner(pipe)
		}

		cancel()

		// Signal completion
		for _, done := range batch.dones {
			done <- struct{}{}
		}
		putCmdBatch(batch)
	}
}

// Len returns the current number of queued commands.
func (ap *AutoPipeliner) Len() int {
	return int(ap.queueLen.Load())
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
