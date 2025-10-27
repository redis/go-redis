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
}

// DefaultAutoPipelineConfig returns the default autopipelining configuration.
func DefaultAutoPipelineConfig() *AutoPipelineConfig {
	return &AutoPipelineConfig{
		MaxBatchSize:         30,
		FlushInterval:        10 * time.Millisecond,
		MaxConcurrentBatches: 20,
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
type AutoPipeliner struct {
	pipeliner pipelinerClient
	config    *AutoPipelineConfig

	// Command queue
	mu       sync.Mutex
	queue    []*queuedCmd
	queueLen atomic.Int32 // Fast path check without lock

	// Flush control
	flushTimer *time.Timer
	flushCh    chan struct{} // Signal to flush immediately

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
		pipeliner:  pipeliner,
		config:     config,
		queue:      make([]*queuedCmd, 0, config.MaxBatchSize),
		flushTimer: time.NewTimer(config.FlushInterval),
		flushCh:    make(chan struct{}, 1),
		sem:        make(chan struct{}, config.MaxConcurrentBatches),
		ctx:        ctx,
		cancel:     cancel,
	}

	// Stop the timer initially
	if !ap.flushTimer.Stop() {
		<-ap.flushTimer.C
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

	// Create queued command with done channel
	qc := &queuedCmd{
		cmd:  cmd,
		done: make(chan struct{}),
	}

	ap.mu.Lock()
	ap.queue = append(ap.queue, qc)
	queueLen := len(ap.queue)
	ap.queueLen.Store(int32(queueLen))

	// Check if we should flush immediately
	shouldFlush := queueLen >= ap.config.MaxBatchSize

	// Start flush timer if this is the first command
	if queueLen == 1 {
		ap.flushTimer.Reset(ap.config.FlushInterval)
	}
	if queueLen > 1 {
		cachedFlushInterval := ap.cachedFlushInterval.Load()
		if cachedFlushInterval == 0 && ap.cachedFlushInterval.CompareAndSwap(cachedFlushInterval, 0) {
			ap.config.FlushInterval = time.Duration(cachedFlushInterval) * time.Nanosecond
		}
	}

	ap.mu.Unlock()

	if shouldFlush {
		// Signal immediate flush (non-blocking)
		select {
		case ap.flushCh <- struct{}{}:
		default:
		}
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

	for {
		select {
		case <-ap.ctx.Done():
			// Final flush before shutdown
			ap.flushBatch()
			return

		case <-ap.flushTimer.C:
			// Timer expired, flush if we have commands
			if ap.queueLen.Load() > 0 {
				ap.flushBatch()
			}
			// Reset timer for next interval if queue is not empty
			ap.mu.Lock()
			if len(ap.queue) > 0 {
				ap.flushTimer.Reset(ap.config.FlushInterval)
			}
			ap.mu.Unlock()

		case <-ap.flushCh:
			// Immediate flush requested
			ap.flushBatch()
		}
	}
}

// flushBatch flushes the current batch of commands.
func (ap *AutoPipeliner) flushBatch() {
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

	// Stop timer
	if !ap.flushTimer.Stop() {
		select {
		case <-ap.flushTimer.C:
		default:
		}
	}

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
	if len(queuedCmds) == 1 {
		if ap.cachedFlushInterval.CompareAndSwap(0, ap.config.FlushInterval.Nanoseconds()) {
			ap.config.FlushInterval = time.Nanosecond
		}
		_ = ap.pipeliner.Process(context.Background(), queuedCmds[0].cmd)
		close(queuedCmds[0].done)
		return
	}

	cachedFlushInterval := ap.cachedFlushInterval.Load()
	if cachedFlushInterval != 0 && ap.cachedFlushInterval.CompareAndSwap(cachedFlushInterval, 0) {
		ap.config.FlushInterval = time.Duration(ap.cachedFlushInterval.Load()) * time.Nanosecond
	}
	// Execute pipeline
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
	return int(ap.queueLen.Load())
}
