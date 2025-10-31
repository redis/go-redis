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
}

// DefaultAutoPipelineConfig returns the default autopipelining configuration.
func DefaultAutoPipelineConfig() *AutoPipelineConfig {
	return &AutoPipelineConfig{
		MaxBatchSize:         50,
		MaxConcurrentBatches: 10,
		MaxFlushDelay:        0, // No delay by default (lowest latency)
	}
}

// cmdableClient is an interface for clients that support pipelining.
// Both Client and ClusterClient implement this interface.
type cmdableClient interface {
	Cmdable
	Process(ctx context.Context, cmd Cmder) error
}

// queuedCmd wraps a command with a done channel for completion notification
type queuedCmd struct {
	cmd  Cmder
	done chan struct{}
}

// doneChanPool is a sync.Pool for done channels to reduce allocations
// We use buffered channels so we can signal completion without blocking
var doneChanPool = sync.Pool{
	New: func() interface{} {
		return make(chan struct{}, 1)
	},
}

// getDoneChan gets a done channel from the pool
func getDoneChan() chan struct{} {
	ch := doneChanPool.Get().(chan struct{})
	// Make sure the channel is empty
	select {
	case <-ch:
	default:
	}
	return ch
}

// putDoneChan returns a done channel to the pool after draining it
func putDoneChan(ch chan struct{}) {
	// Drain the channel completely
	for {
		select {
		case <-ch:
		default:
			doneChanPool.Put(ch)
			return
		}
	}
}

// queuedCmdPool is a sync.Pool for queuedCmd to reduce allocations
var queuedCmdPool = sync.Pool{
	New: func() interface{} {
		return &queuedCmd{}
	},
}

// getQueuedCmd gets a queuedCmd from the pool and initializes it
func getQueuedCmd(cmd Cmder) *queuedCmd {
	qc := queuedCmdPool.Get().(*queuedCmd)
	qc.cmd = cmd
	qc.done = getDoneChan()
	return qc
}

// putQueuedCmd returns a queuedCmd to the pool after clearing it
func putQueuedCmd(qc *queuedCmd) {
	qc.cmd = nil
	if qc.done != nil {
		putDoneChan(qc.done)
		qc.done = nil
	}
	queuedCmdPool.Put(qc)
}

// queueSlicePool is a sync.Pool for queue slices to reduce allocations
var queueSlicePool = sync.Pool{
	New: func() interface{} {
		// Create a slice with capacity for typical batch size
		return make([]*queuedCmd, 0, 100)
	},
}

// getQueueSlice gets a queue slice from the pool
func getQueueSlice(capacity int) []*queuedCmd {
	slice := queueSlicePool.Get().([]*queuedCmd)
	// Clear the slice but keep capacity
	slice = slice[:0]
	// If the capacity is too small, allocate a new one
	if cap(slice) < capacity {
		return make([]*queuedCmd, 0, capacity)
	}
	return slice
}

// putQueueSlice returns a queue slice to the pool
func putQueueSlice(slice []*queuedCmd) {
	// Only pool slices that aren't too large (avoid memory bloat)
	if cap(slice) <= 1000 {
		queueSlicePool.Put(slice)
	}
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

	pipeliner cmdableClient
	config    *AutoPipelineConfig

	// Command queue - either slice-based or ring buffer
	mu       sync.Mutex
	queue    []*queuedCmd // Slice-based queue (legacy)
	queueLen atomic.Int32 // Fast path check without lock

	// Flush control
	flushCh chan struct{} // Signal to flush immediately

	// Concurrency control
	sem *internal.FastSemaphore // Semaphore for concurrent batch limit

	// Lifecycle
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup
	closed atomic.Bool
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
		pipeliner: pipeliner,
		config:    config,
		flushCh:   make(chan struct{}, 1),
		sem:       internal.NewFastSemaphore(int32(config.MaxConcurrentBatches)),
		ctx:       ctx,
		cancel:    cancel,
	}

	// Initialize cmdable to route all commands through processAndBlock
	ap.cmdable = ap.processAndBlock

	// Initialize queue based on configuration
	ap.queue = getQueueSlice(config.MaxBatchSize)

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

	done := ap.process(ctx, cmd)

	// Block until the command is executed
	<-done

	return cmd.Err()
}

// closedChan is a reusable closed channel for error cases
var closedChan = func() chan struct{} {
	ch := make(chan struct{})
	close(ch)
	return ch
}()

// process is the internal method that queues a command and returns its done channel.
func (ap *AutoPipeliner) process(ctx context.Context, cmd Cmder) <-chan struct{} {
	if ap.closed.Load() {
		cmd.SetErr(ErrClosed)
		return closedChan
	}

	// Get queued command from pool
	qc := getQueuedCmd(cmd)

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

	// always signal the flusher (non-blocking)
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

	// Wait for flusher to finish
	ap.wg.Wait()

	return nil
}

// flusher is the background goroutine that flushes batches.
func (ap *AutoPipeliner) flusher() {
	defer ap.wg.Done()
	ap.flusherSlice()
	return
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
				if ap.Len() >= ap.config.MaxBatchSize {
					goto drained
				}
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
	ap.queue = getQueueSlice(ap.config.MaxBatchSize)
	ap.queueLen.Store(0)
	ap.mu.Unlock()

	// Acquire semaphore (limit concurrent batches)
	// Try fast path first
	if !ap.sem.TryAcquire() {
		// Fast path failed, need to wait
		// essentially, this is a blocking call
		err := ap.sem.Acquire(ap.ctx, 5*time.Second, context.DeadlineExceeded)
		if err != nil {
			// Context cancelled, set error on all commands and notify
			for _, qc := range queuedCmds {
				qc.cmd.SetErr(ErrClosed)
				// Signal completion by sending to buffered channel
				qc.done <- struct{}{}
				putQueuedCmd(qc)
			}
			putQueueSlice(queuedCmds)
			return
		}
	}

	if len(queuedCmds) == 0 {
		ap.sem.Release()
		return
	}

	// Fast path for single command
	if len(queuedCmds) == 1 {
		qc := queuedCmds[0]
		_ = ap.pipeliner.Process(context.Background(), qc.cmd)
		// Signal completion by sending to buffered channel
		qc.done <- struct{}{}
		ap.sem.Release()
		putQueuedCmd(qc)
		putQueueSlice(queuedCmds)
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
		// Signal completion by sending to buffered channel
		qc.done <- struct{}{}
		putQueuedCmd(qc)
	}
	ap.sem.Release()
	putQueueSlice(queuedCmds)
}

// Len returns the current number of queued commands.
func (ap *AutoPipeliner) Len() int {
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
