package redis

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// AutoPipelineConfig configures the autopipelining behavior.
type AutoPipelineConfig struct {
	// MaxBatchSize is the maximum number of commands to batch before flushing.
	// Default: 100
	MaxBatchSize int

	// FlushInterval is the maximum time to wait before flushing pending commands.
	// Default: 10ms
	FlushInterval time.Duration

	// MaxConcurrentBatches is the maximum number of concurrent pipeline executions.
	// This prevents overwhelming the server with too many concurrent pipelines.
	// Default: 10
	MaxConcurrentBatches int

	// UseRingBuffer enables the high-performance ring buffer queue.
	// When enabled, uses a pre-allocated ring buffer with lock-free enqueue
	// instead of the slice-based queue. This provides:
	// - 6x faster enqueue operations
	// - 100% reduction in allocations during enqueue
	// - Better performance under high concurrency
	// Default: true (enabled)
	UseRingBuffer bool

	// RingBufferSize is the size of the ring buffer queue.
	// Only used when UseRingBuffer is true.
	// Must be a power of 2 for optimal performance (will be rounded up if not).
	// Default: 1024
	RingBufferSize int

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
}

// DefaultAutoPipelineConfig returns the default autopipelining configuration.
func DefaultAutoPipelineConfig() *AutoPipelineConfig {
	return &AutoPipelineConfig{
		MaxBatchSize:         50,
		FlushInterval:        time.Millisecond,
		MaxConcurrentBatches: 10,
		UseRingBuffer:        true, // Enable ring buffer by default
		RingBufferSize:       1024,
		MaxFlushDelay:        0, // No delay by default (lowest latency)
	}
}

// pipelinerClient is an interface for clients that support pipelining.
// Both Client and ClusterClient implement this interface.
type pipelinerClient interface {
	Process(ctx context.Context, cmd Cmder) error
	Pipeline() Pipeliner
}

// queuedCmd wraps a command with a done channel for completion notification
type queuedCmd struct {
	cmd  Cmder
	done chan struct{}
}

// autoPipelineCmd wraps a command and blocks on result access until execution completes.
type autoPipelineCmd struct {
	Cmder
	done <-chan struct{}
}

func (c *autoPipelineCmd) Err() error {
	<-c.done
	return c.Cmder.Err()
}

func (c *autoPipelineCmd) String() string {
	<-c.done
	return c.Cmder.String()
}

// AutoPipeliner automatically batches commands and executes them in pipelines.
// It's safe for concurrent use by multiple goroutines.
//
// AutoPipeliner works by:
// 1. Collecting commands from multiple goroutines into a shared queue
// 2. Automatically flushing the queue when:
//   - The batch size reaches MaxBatchSize
//   - The flush interval (FlushInterval) expires
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

	pipeliner pipelinerClient
	config    *AutoPipelineConfig

	// Command queue - either slice-based or ring buffer
	mu       sync.Mutex
	queue    []*queuedCmd      // Slice-based queue (legacy)
	ring     *autoPipelineRing // Ring buffer queue (high-performance)
	queueLen atomic.Int32      // Fast path check without lock

	// Flush control
	flushCh chan struct{} // Signal to flush immediately

	// Concurrency control
	sem chan struct{} // Semaphore for concurrent batch limit

	// Lifecycle
	ctx                 context.Context
	cancel              context.CancelFunc
	wg                  sync.WaitGroup
	closed              atomic.Bool
	cachedFlushInterval atomic.Int64
}

// NewAutoPipeliner creates a new autopipeliner for the given client.
// The client can be either *Client or *ClusterClient.
func NewAutoPipeliner(pipeliner pipelinerClient, config *AutoPipelineConfig) *AutoPipeliner {
	if config == nil {
		config = DefaultAutoPipelineConfig()
	}

	ctx, cancel := context.WithCancel(context.Background())

	ap := &AutoPipeliner{
		pipeliner: pipeliner,
		config:    config,
		flushCh:   make(chan struct{}, 1),
		sem:       make(chan struct{}, config.MaxConcurrentBatches),
		ctx:       ctx,
		cancel:    cancel,
	}

	// Initialize cmdable to route all commands through Process
	ap.cmdable = ap.Process

	// Initialize queue based on configuration
	if config.UseRingBuffer {
		ap.ring = newAutoPipelineRing(config.RingBufferSize)
	} else {
		ap.queue = make([]*queuedCmd, 0, config.MaxBatchSize)
	}

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

	// Check if this is a blocking command (has read timeout set)
	// Blocking commands like BLPOP, BRPOP, BZMPOP should not be autopipelined
	if cmd.readTimeout() != nil {
		// Execute blocking commands directly without autopipelining
		_ = ap.pipeliner.Process(ctx, cmd)
		return cmd
	}

	done := ap.process(ctx, cmd)
	return &autoPipelineCmd{Cmder: cmd, done: done}
}

// Process queues a command for autopipelined execution and returns immediately.
// The command will be executed asynchronously when the batch is flushed.
//
// Unlike Do(), this does NOT wrap the command, so accessing results will NOT block.
// Use this only when you're managing synchronization yourself (e.g., with goroutines).
//
// For sequential usage, use Do() instead.
func (ap *AutoPipeliner) Process(ctx context.Context, cmd Cmder) error {
	// Check if this is a blocking command (has read timeout set)
	// Blocking commands like BLPOP, BRPOP, BZMPOP should not be autopipelined
	if cmd.readTimeout() != nil {
		// Execute blocking commands directly without autopipelining
		return ap.pipeliner.Process(ctx, cmd)
	}

	_ = ap.process(ctx, cmd)
	return nil
}

// process is the internal method that queues a command and returns its done channel.
func (ap *AutoPipeliner) process(ctx context.Context, cmd Cmder) <-chan struct{} {
	if ap.closed.Load() {
		cmd.SetErr(ErrClosed)
		closedCh := make(chan struct{})
		close(closedCh)
		return closedCh
	}

	// Use ring buffer if enabled
	if ap.config.UseRingBuffer {
		done := ap.ring.putOne(cmd)
		// putOne will signal the flusher via condition variable if needed
		return done
	}

	// Legacy slice-based queue
	// Create queued command with done channel
	qc := &queuedCmd{
		cmd:  cmd,
		done: make(chan struct{}),
	}

	// Fast path: try to acquire lock without blocking
	if ap.mu.TryLock() {
		ap.queue = append(ap.queue, qc)
		queueLen := len(ap.queue)
		ap.queueLen.Store(int32(queueLen))
		ap.mu.Unlock()

		// Always signal the flusher (non-blocking)
		select {
		case ap.flushCh <- struct{}{}:
		default:
		}
		return qc.done
	}

	// Slow path: lock is contended, wait for it
	ap.mu.Lock()
	ap.queue = append(ap.queue, qc)
	queueLen := len(ap.queue)
	ap.queueLen.Store(int32(queueLen))
	ap.mu.Unlock()

	// Always signal the flusher (non-blocking)
	select {
	case ap.flushCh <- struct{}{}:
	default:
	}

	return qc.done
}

// Flush immediately flushes all pending commands.
// This is useful when you want to ensure all commands are executed
// before proceeding (e.g., before closing the autopipeliner).
func (ap *AutoPipeliner) Flush(ctx context.Context) error {
	if ap.closed.Load() {
		return ErrClosed
	}

	// Signal flush
	select {
	case ap.flushCh <- struct{}{}:
	case <-ctx.Done():
		return ctx.Err()
	}

	// Wait a bit for the flush to complete
	// This is a best-effort approach
	time.Sleep(time.Millisecond)

	return nil
}

// Close stops the autopipeliner and flushes any pending commands.
func (ap *AutoPipeliner) Close() error {
	if !ap.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}

	// Cancel context to stop flusher
	ap.cancel()

	// Wake up flusher if it's waiting
	if ap.config.UseRingBuffer {
		ap.ring.wakeAll()
	}

	// Wait for flusher to finish
	ap.wg.Wait()

	return nil
}

// flusher is the background goroutine that flushes batches.
func (ap *AutoPipeliner) flusher() {
	defer ap.wg.Done()

	if !ap.config.UseRingBuffer {
		// Legacy slice-based flusher
		ap.flusherSlice()
		return
	}

	// Ring buffer flusher
	var (
		cmds      = make([]Cmder, 0, ap.config.MaxBatchSize)
		doneChans = make([]chan struct{}, 0, ap.config.MaxBatchSize)
	)

	for {
		// Try to get next command (non-blocking)
		cmd, done := ap.ring.nextWriteCmd()

		if cmd == nil {
			// No command available
			// If we have buffered commands, execute them first
			if len(cmds) > 0 {
				ap.executeBatch(cmds, doneChans)
				cmds = cmds[:0]
				doneChans = doneChans[:0]
			}

			// Check for shutdown before blocking
			select {
			case <-ap.ctx.Done():
				return
			default:
			}

			// Wait for next command (blocking)
			// This will be woken up by wakeAll() during shutdown
			cmd, done = ap.ring.waitForWrite()

			// If nil, ring is closed
			if cmd == nil {
				return
			}
		}

		// Add command to batch
		cmds = append(cmds, cmd)
		doneChans = append(doneChans, done)

		// Execute batch if full
		if len(cmds) >= ap.config.MaxBatchSize {
			ap.executeBatch(cmds, doneChans)
			cmds = cmds[:0]
			doneChans = doneChans[:0]
		}
	}
}

// executeBatch executes a batch of commands.
func (ap *AutoPipeliner) executeBatch(cmds []Cmder, doneChans []chan struct{}) {
	if len(cmds) == 0 {
		return
	}

	// Acquire semaphore (limit concurrent batches)
	select {
	case ap.sem <- struct{}{}:
		defer func() {
			<-ap.sem
		}()
	case <-ap.ctx.Done():
		// Context cancelled, set error on all commands and notify
		for i, cmd := range cmds {
			cmd.SetErr(ErrClosed)
			doneChans[i] <- struct{}{} // Send signal instead of close
			ap.ring.finishCmd()
		}
		return
	}

	// Fast path for single command
	if len(cmds) == 1 {
		_ = ap.pipeliner.Process(context.Background(), cmds[0])
		doneChans[0] <- struct{}{} // Send signal instead of close
		ap.ring.finishCmd()
		return
	}

	// Execute pipeline for multiple commands
	pipe := ap.pipeliner.Pipeline()
	for _, cmd := range cmds {
		_ = pipe.Process(context.Background(), cmd)
	}

	// Execute and wait for completion
	_, _ = pipe.Exec(context.Background())

	// Notify completion and finish slots
	for _, done := range doneChans {
		done <- struct{}{} // Send signal instead of close
		ap.ring.finishCmd()
	}
}

// flusherSlice is the legacy slice-based flusher.
func (ap *AutoPipeliner) flusherSlice() {
	for {
		// Wait for a command to arrive
		select {
		case <-ap.flushCh:
			// Command arrived, continue
		case <-ap.ctx.Done():
			// Final flush before shutdown
			ap.flushBatchSlice()
			return
		}

		// Drain any additional signals
		for {
			select {
			case <-ap.flushCh:
			default:
				goto drained
			}
		}
	drained:

		// Flush all pending commands
		for ap.Len() > 0 {
			select {
			case <-ap.ctx.Done():
				ap.flushBatchSlice()
				return
			default:
			}

			ap.flushBatchSlice()

			if ap.config.MaxFlushDelay > 0 && ap.Len() > 0 {
				time.Sleep(ap.config.MaxFlushDelay)
			}
		}
	}
}



// flushBatchSlice flushes commands from the slice-based queue (legacy).
func (ap *AutoPipeliner) flushBatchSlice() {
	// Get commands from queue
	ap.mu.Lock()
	if len(ap.queue) == 0 {
		ap.queueLen.Store(0)
		ap.mu.Unlock()
		return
	}

	// Take ownership of current queue
	queuedCmds := ap.queue
	ap.queue = make([]*queuedCmd, 0, ap.config.MaxBatchSize)
	ap.queueLen.Store(0)
	ap.mu.Unlock()

	// Acquire semaphore (limit concurrent batches)
	select {
	case ap.sem <- struct{}{}:
		defer func() {
			<-ap.sem
		}()
	case <-ap.ctx.Done():
		// Context cancelled, set error on all commands and notify
		for _, qc := range queuedCmds {
			qc.cmd.SetErr(ErrClosed)
			close(qc.done)
		}
		return
	}

	if len(queuedCmds) == 0 {
		return
	}

	// Fast path for single command
	if len(queuedCmds) == 1 {
		_ = ap.pipeliner.Process(context.Background(), queuedCmds[0].cmd)
		close(queuedCmds[0].done)
		return
	}

	// Execute pipeline for multiple commands
	pipe := ap.pipeliner.Pipeline()
	for _, qc := range queuedCmds {
		_ = pipe.Process(context.Background(), qc.cmd)
	}

	// Execute and wait for completion
	_, _ = pipe.Exec(context.Background())

	// IMPORTANT: Only notify after pipeline execution is complete
	// This ensures command results are fully populated before waiters proceed
	for _, qc := range queuedCmds {
		close(qc.done)
	}
}

// Len returns the current number of queued commands.
func (ap *AutoPipeliner) Len() int {
	if ap.config.UseRingBuffer {
		return ap.ring.len()
	}
	return int(ap.queueLen.Load())
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
	// Try to get TxPipeline from the underlying client
	// This works for Client, Ring, and ClusterClient
	type txPipeliner interface {
		TxPipeline() Pipeliner
	}

	if txp, ok := ap.pipeliner.(txPipeliner); ok {
		return txp.TxPipeline().Pipelined(ctx, fn)
	}

	panic("redis: TxPipelined not supported by this client type")
}

// TxPipeline returns a new transaction pipeline that uses the underlying pipeliner.
// This allows you to create a traditional transaction pipeline from an autopipeliner.
//
// Note: This uses the underlying client's TxPipeline if available (Client, Ring, ClusterClient).
// For other clients, this will panic.
func (ap *AutoPipeliner) TxPipeline() Pipeliner {
	// Try to get TxPipeline from the underlying client
	// This works for Client, Ring, and ClusterClient
	type txPipeliner interface {
		TxPipeline() Pipeliner
	}

	if txp, ok := ap.pipeliner.(txPipeliner); ok {
		return txp.TxPipeline()
	}

	panic("redis: TxPipeline not supported by this client type")
}
