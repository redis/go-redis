package hitless

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/pool"
)

// HitlessManagerInterface defines the interface for completing handoff operations
type HitlessManagerInterface interface {
	TrackMovingOperationWithConnID(ctx context.Context, newEndpoint string, deadline time.Time, seqID int64, connID uint64) error
	UntrackOperationWithConnID(seqID int64, connID uint64)
}

// HandoffRequest represents a request to handoff a connection to a new endpoint
type HandoffRequest struct {
	Conn     *pool.Conn
	ConnID   uint64 // Unique connection identifier
	Endpoint string
	SeqID    int64
	Pool     pool.Pooler // Pool to remove connection from on failure
}

// PoolHook implements pool.PoolHook for Redis-specific connection handling
// with hitless upgrade support.
type PoolHook struct {
	// Base dialer for creating connections to new endpoints during handoffs
	// args are network and address
	baseDialer func(context.Context, string, string) (net.Conn, error)

	// Network type (e.g., "tcp", "unix")
	network string

	// Event-driven handoff support
	handoffQueue chan HandoffRequest // Queue for handoff requests
	shutdown     chan struct{}       // Shutdown signal
	shutdownOnce sync.Once           // Ensure clean shutdown
	workerWg     sync.WaitGroup      // Track worker goroutines

	// On-demand worker management
	maxWorkers     int
	activeWorkers  atomic.Int32
	workerTimeout  time.Duration // How long workers wait for work before exiting
	workersScaling atomic.Bool

	// Simple state tracking
	pending sync.Map // map[uint64]int64 (connID -> seqID)

	// Configuration for the hitless upgrade
	config *Config

	// Hitless manager for operation completion tracking
	hitlessManager HitlessManagerInterface

	// Pool interface for removing connections on handoff failure
	pool pool.Pooler
}

// NewPoolHook creates a new pool hook
func NewPoolHook(baseDialer func(context.Context, string, string) (net.Conn, error), network string, config *Config, hitlessManager HitlessManagerInterface) *PoolHook {
	return NewPoolHookWithPoolSize(baseDialer, network, config, hitlessManager, 0)
}

// NewPoolHookWithPoolSize creates a new pool hook with pool size for better worker defaults
func NewPoolHookWithPoolSize(baseDialer func(context.Context, string, string) (net.Conn, error), network string, config *Config, hitlessManager HitlessManagerInterface, poolSize int) *PoolHook {
	// Apply defaults if config is nil or has zero values
	if config == nil {
		config = config.ApplyDefaultsWithPoolSize(poolSize)
	}

	ph := &PoolHook{
		// baseDialer is used to create connections to new endpoints during handoffs
		baseDialer: baseDialer,
		network:    network,
		// handoffQueue is a buffered channel for queuing handoff requests
		handoffQueue: make(chan HandoffRequest, config.HandoffQueueSize),
		// shutdown is a channel for signaling shutdown
		shutdown:      make(chan struct{}),
		maxWorkers:    config.MaxWorkers,
		activeWorkers: atomic.Int32{}, // Start with no workers - create on demand
		// NOTE: maybe we would like to make this configurable?
		workerTimeout: 15 * time.Second, // Workers exit after 15s of inactivity
		config:        config,
		// Hitless manager for operation completion tracking
		hitlessManager: hitlessManager,
	}
	return ph
}

// SetPool sets the pool interface for removing connections on handoff failure
func (ph *PoolHook) SetPool(pooler pool.Pooler) {
	ph.pool = pooler
}

// GetCurrentWorkers returns the current number of active workers (for testing)
func (ph *PoolHook) GetCurrentWorkers() int {
	return int(ph.activeWorkers.Load())
}

// IsHandoffPending returns true if the given connection has a pending handoff
func (ph *PoolHook) IsHandoffPending(conn *pool.Conn) bool {
	_, pending := ph.pending.Load(conn.GetID())
	return pending
}

// OnGet is called when a connection is retrieved from the pool
func (ph *PoolHook) OnGet(ctx context.Context, conn *pool.Conn, _ bool) error {
	// NOTE: There are two conditions to make sure we don't return a connection that should be handed off or is
	// in a handoff state at the moment.

	// Check if connection is usable (not in a handoff state)
	// Should not happen since the pool will not return a connection that is not usable.
	if !conn.IsUsable() {
		return ErrConnectionMarkedForHandoff
	}

	// Check if connection is marked for handoff, which means it will be queued for handoff on put.
	if conn.ShouldHandoff() {
		return ErrConnectionMarkedForHandoff
	}

	return nil
}

// OnPut is called when a connection is returned to the pool
func (ph *PoolHook) OnPut(ctx context.Context, conn *pool.Conn) (shouldPool bool, shouldRemove bool, err error) {
	// first check if we should handoff for faster rejection
	if !conn.ShouldHandoff() {
		// Default behavior (no handoff): pool the connection
		return true, false, nil
	}

	// check pending handoff to not queue the same connection twice
	_, hasPendingHandoff := ph.pending.Load(conn.GetID())
	if hasPendingHandoff {
		// Default behavior (pending handoff): pool the connection
		return true, false, nil
	}

	if err := ph.queueHandoff(conn); err != nil {
		// Failed to queue handoff, remove the connection
		internal.Logger.Printf(ctx, "Failed to queue handoff: %v", err)
		// Don't pool, remove connection, no error to caller
		return false, true, nil
	}

	// Check if handoff was already processed by a worker before we can mark it as queued
	if !conn.ShouldHandoff() {
		// Handoff was already processed - this is normal and the connection should be pooled
		return true, false, nil
	}

	if err := conn.MarkQueuedForHandoff(); err != nil {
		// If marking fails, check if handoff was processed in the meantime
		if !conn.ShouldHandoff() {
			// Handoff was processed - this is normal, pool the connection
			return true, false, nil
		}
		// Other error - remove the connection
		return false, true, nil
	}
	return true, false, nil
}

// ensureWorkerAvailable ensures at least one worker is available to process requests
// Creates a new worker if needed and under the max limit
func (ph *PoolHook) ensureWorkerAvailable() {
	select {
	case <-ph.shutdown:
		return
	default:
		if ph.workersScaling.CompareAndSwap(false, true) {
			defer ph.workersScaling.Store(false)
			// Check if we need a new worker
			currentWorkers := ph.activeWorkers.Load()
			workersWas := currentWorkers
			for currentWorkers <= int32(ph.maxWorkers) {
				ph.workerWg.Add(1)
				go ph.onDemandWorker()
				currentWorkers++
			}
			// workersWas is always <= currentWorkers
			// currentWorkers will be maxWorkers, but if we have a worker that was closed
			// while we were creating new workers, just add the difference between
			// the currentWorkers and the number of workers we observed initially (i.e. the number of workers we created)
			ph.activeWorkers.Add(currentWorkers - workersWas)
		}
	}
}

// onDemandWorker processes handoff requests and exits when idle
func (ph *PoolHook) onDemandWorker() {
	defer func() {
		// Decrement active worker count when exiting
		ph.activeWorkers.Add(-1)
		ph.workerWg.Done()
	}()

	for {
		select {
		case <-ph.shutdown:
			return
		case <-time.After(ph.workerTimeout):
			// Worker has been idle for too long, exit to save resources
			if ph.config != nil && ph.config.LogLevel >= LogLevelDebug { // Debug level
				internal.Logger.Printf(context.Background(),
					"hitless: worker exiting due to inactivity timeout (%v)", ph.workerTimeout)
			}
			return
		case request := <-ph.handoffQueue:
			// Check for shutdown before processing
			select {
			case <-ph.shutdown:
				// Clean up the request before exiting
				ph.pending.Delete(request.ConnID)
				return
			default:
				// Process the request
				ph.processHandoffRequest(request)
			}
		}
	}
}

// processHandoffRequest processes a single handoff request
func (ph *PoolHook) processHandoffRequest(request HandoffRequest) {
	// Remove from pending map
	defer ph.pending.Delete(request.Conn.GetID())
	internal.Logger.Printf(context.Background(), "hitless: conn[%d] Processing handoff request start", request.Conn.GetID())

	// Create a context with handoff timeout from config
	handoffTimeout := 30 * time.Second // Default fallback
	if ph.config != nil && ph.config.HandoffTimeout > 0 {
		handoffTimeout = ph.config.HandoffTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), handoffTimeout)
	defer cancel()

	// Create a context that also respects the shutdown signal
	shutdownCtx, shutdownCancel := context.WithCancel(ctx)
	defer shutdownCancel()

	// Monitor shutdown signal in a separate goroutine
	go func() {
		select {
		case <-ph.shutdown:
			shutdownCancel()
		case <-shutdownCtx.Done():
		}
	}()

	// Perform the handoff with cancellable context
	shouldRetry, err := ph.performConnectionHandoff(shutdownCtx, request.Conn)
	minRetryBackoff := 500 * time.Millisecond
	if err != nil {
		if shouldRetry {
			now := time.Now()
			deadline, ok := shutdownCtx.Deadline()
			thirdOfTimeout := handoffTimeout / 3
			if !ok || deadline.Before(now) {
				// wait half the timeout before retrying if no deadline or deadline has passed
				deadline = now.Add(thirdOfTimeout)
			}

			afterTime := deadline.Sub(now)
			if afterTime > thirdOfTimeout {
				afterTime = thirdOfTimeout
			}
			if afterTime < minRetryBackoff {
				afterTime = minRetryBackoff
			}

			internal.Logger.Printf(context.Background(), "Handoff failed for conn[%d] WILL RETRY After %v: %v", request.ConnID, afterTime, err)
			time.AfterFunc(afterTime, func() {
				if err := ph.queueHandoff(request.Conn); err != nil {
					internal.Logger.Printf(context.Background(), "can't queue handoff for retry: %v", err)
					ph.closeConnFromRequest(context.Background(), request, err)
				}
			})
			return
		} else {
			go ph.closeConnFromRequest(ctx, request, err)
		}

		// Clear handoff state if not returned for retry
		seqID := request.Conn.GetMovingSeqID()
		connID := request.Conn.GetID()
		if ph.hitlessManager != nil {
			ph.hitlessManager.UntrackOperationWithConnID(seqID, connID)
		}
	}
}

// closeConn closes the connection and logs the reason
func (ph *PoolHook) closeConnFromRequest(ctx context.Context, request HandoffRequest, err error) {
	pooler := request.Pool
	conn := request.Conn
	if pooler != nil {
		pooler.CloseConn(conn)
		if ph.config != nil && ph.config.LogLevel >= LogLevelWarn { // Warning level
			internal.Logger.Printf(ctx,
				"hitless: removed conn[%d] from pool due to max handoff retries reached: %v",
				conn.GetID(), err)
		}
	} else {
		conn.Close()
		if ph.config != nil && ph.config.LogLevel >= LogLevelWarn { // Warning level
			internal.Logger.Printf(ctx,
				"hitless: no pool provided for conn[%d], cannot remove due to handoff initialization failure: %v",
				conn.GetID(), err)
		}
	}
}

// queueHandoff queues a handoff request for processing
// if err is returned, connection will be removed from pool
func (ph *PoolHook) queueHandoff(conn *pool.Conn) error {
	// Create handoff request
	request := HandoffRequest{
		Conn:     conn,
		ConnID:   conn.GetID(),
		Endpoint: conn.GetHandoffEndpoint(),
		SeqID:    conn.GetMovingSeqID(),
		Pool:     ph.pool, // Include pool for connection removal on failure
	}

	select {
	// priority to shutdown
	case <-ph.shutdown:
		return ErrShutdown
	default:
		select {
		case <-ph.shutdown:
			return ErrShutdown
		case ph.handoffQueue <- request:
			// Store in pending map
			ph.pending.Store(request.ConnID, request.SeqID)
			// Ensure we have a worker to process this request
			ph.ensureWorkerAvailable()
			return nil
		default:
			select {
			case <-ph.shutdown:
				return ErrShutdown
			case ph.handoffQueue <- request:
				// Store in pending map
				ph.pending.Store(request.ConnID, request.SeqID)
				// Ensure we have a worker to process this request
				ph.ensureWorkerAvailable()
				return nil
			case <-time.After(100 * time.Millisecond): // give workers a chance to process
				// Queue is full - log and attempt scaling
				queueLen := len(ph.handoffQueue)
				queueCap := cap(ph.handoffQueue)
				if ph.config != nil && ph.config.LogLevel >= LogLevelWarn { // Warning level
					internal.Logger.Printf(context.Background(),
						"hitless: handoff queue is full (%d/%d), cant queue handoff request for conn[%d] seqID[%d]",
						queueLen, queueCap, request.ConnID, request.SeqID)
					if ph.config.LogLevel >= LogLevelDebug { // Debug level
						ph.pending.Range(func(k, v interface{}) bool {
							internal.Logger.Printf(context.Background(), "hitless: pending handoff for conn[%d] seqID[%d]", k, v)
							return true
						})
					}
				}
			}
		}
	}

	// Ensure we have workers available to handle the load
	ph.ensureWorkerAvailable()
	return ErrHandoffQueueFull
}

// performConnectionHandoff performs the actual connection handoff
// When error is returned, the connection handoff should be retried if err is not ErrMaxHandoffRetriesReached
func (ph *PoolHook) performConnectionHandoff(ctx context.Context, conn *pool.Conn) (shouldRetry bool, err error) {
	// Clear handoff state after successful handoff
	connID := conn.GetID()

	newEndpoint := conn.GetHandoffEndpoint()
	if newEndpoint == "" {
		return false, ErrConnectionInvalidHandoffState
	}

	retries := conn.IncrementAndGetHandoffRetries(1)
	internal.Logger.Printf(ctx, "hitless: conn[%d] Retry %d: Performing handoff to %s(was %s)", conn.GetID(), retries, newEndpoint, conn.RemoteAddr().String())
	maxRetries := 3 // Default fallback
	if ph.config != nil {
		maxRetries = ph.config.MaxHandoffRetries
	}

	if retries > maxRetries {
		if ph.config != nil && ph.config.LogLevel >= LogLevelWarn { // Warning level
			internal.Logger.Printf(ctx,
				"hitless: reached max retries (%d) for handoff of conn[%d] to %s",
				maxRetries, conn.GetID(), conn.GetHandoffEndpoint())
		}
		// won't retry on ErrMaxHandoffRetriesReached
		return false, ErrMaxHandoffRetriesReached
	}

	// Create endpoint-specific dialer
	endpointDialer := ph.createEndpointDialer(newEndpoint)

	// Create new connection to the new endpoint
	newNetConn, err := endpointDialer(ctx)
	if err != nil {
		internal.Logger.Printf(ctx, "hitless: conn[%d] Failed to dial new endpoint %s: %v", conn.GetID(), newEndpoint, err)
		// hitless: will retry
		// Maybe a network error - retry after a delay
		return true, err
	}

	// Get the old connection
	oldConn := conn.GetNetConn()

	// Apply relaxed timeout to the new connection for the configured post-handoff duration
	// This gives the new connection more time to handle operations during cluster transition
	// Setting this here (before initing the connection) ensures that the connection is going
	// to use the relaxed timeout for the first operation (auth/ACL select)
	if ph.config != nil && ph.config.PostHandoffRelaxedDuration > 0 {
		relaxedTimeout := ph.config.RelaxedTimeout
		// Set relaxed timeout with deadline - no background goroutine needed
		deadline := time.Now().Add(ph.config.PostHandoffRelaxedDuration)
		conn.SetRelaxedTimeoutWithDeadline(relaxedTimeout, relaxedTimeout, deadline)

		if ph.config.LogLevel >= 2 { // Info level
			internal.Logger.Printf(context.Background(),
				"hitless: conn[%d] applied post-handoff relaxed timeout (%v) until %v",
				connID, relaxedTimeout, deadline.Format("15:04:05.000"))
		}
	}

	// Replace the connection and execute initialization
	err = conn.SetNetConnAndInitConn(ctx, newNetConn)
	if err != nil {
		// hitless: won't retry
		// Initialization failed - remove the connection
		return false, err
	}
	defer func() {
		if oldConn != nil {
			oldConn.Close()
		}
	}()

	conn.ClearHandoffState()
	internal.Logger.Printf(ctx, "hitless: conn[%d] Handoff to %s successful", conn.GetID(), newEndpoint)

	return false, nil
}

// createEndpointDialer creates a dialer function that connects to a specific endpoint
func (ph *PoolHook) createEndpointDialer(endpoint string) func(context.Context) (net.Conn, error) {
	return func(ctx context.Context) (net.Conn, error) {
		// Parse endpoint to extract host and port
		host, port, err := net.SplitHostPort(endpoint)
		if err != nil {
			// If no port specified, assume default Redis port
			host = endpoint
			if port == "" {
				port = "6379"
			}
		}

		// Use the base dialer to connect to the new endpoint
		return ph.baseDialer(ctx, ph.network, net.JoinHostPort(host, port))
	}
}

// Shutdown gracefully shuts down the processor, waiting for workers to complete
func (ph *PoolHook) Shutdown(ctx context.Context) error {
	ph.shutdownOnce.Do(func() {
		close(ph.shutdown)
		// workers will exit when they finish their current request
	})

	// Wait for workers to complete
	done := make(chan struct{})
	go func() {
		ph.workerWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
