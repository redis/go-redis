package hitless

import (
	"context"
	"errors"
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
	maxWorkers    int
	activeWorkers int32         // Atomic counter for active workers
	workerTimeout time.Duration // How long workers wait for work before exiting

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
	// Apply defaults to any missing configuration fields, using pool size for worker calculations
	config = config.ApplyDefaultsWithPoolSize(poolSize)

	ph := &PoolHook{
		// baseDialer is used to create connections to new endpoints during handoffs
		baseDialer: baseDialer,
		network:    network,
		// handoffQueue is a buffered channel for queuing handoff requests
		handoffQueue: make(chan HandoffRequest, config.HandoffQueueSize),
		// shutdown is a channel for signaling shutdown
		shutdown:      make(chan struct{}),
		maxWorkers:    config.MaxWorkers,
		activeWorkers: 0,                // Start with no workers - create on demand
		workerTimeout: 30 * time.Second, // Workers exit after 30s of inactivity
		config:        config,
		// Hitless manager for operation completion tracking
		hitlessManager: hitlessManager,
	}

	// No upfront worker creation - workers are created on demand

	return ph
}

// SetPool sets the pool interface for removing connections on handoff failure
func (ph *PoolHook) SetPool(pooler pool.Pooler) {
	ph.pool = pooler
}

// GetCurrentWorkers returns the current number of active workers (for testing)
func (ph *PoolHook) GetCurrentWorkers() int {
	return int(atomic.LoadInt32(&ph.activeWorkers))
}

// GetScaleLevel returns 1 if workers are active, 0 if none (for testing compatibility)
func (ph *PoolHook) GetScaleLevel() int {
	if atomic.LoadInt32(&ph.activeWorkers) > 0 {
		return 1
	}
	return 0
}

// IsHandoffPending returns true if the given connection has a pending handoff
func (ph *PoolHook) IsHandoffPending(conn *pool.Conn) bool {
	_, pending := ph.pending.Load(conn.GetID())
	return pending
}

// OnGet is called when a connection is retrieved from the pool
func (ph *PoolHook) OnGet(ctx context.Context, conn *pool.Conn, isNewConn bool) error {
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
	if conn.ShouldHandoff() {
		// check pending handoff to not queue the same connection twice
		_, hasPendingHandoff := ph.pending.Load(conn.GetID())
		if !hasPendingHandoff {
			// Check for empty endpoint first (synchronous check)
			if conn.GetHandoffEndpoint() == "" {
				conn.ClearHandoffState()
			} else {
				if err := ph.queueHandoff(conn); err != nil {
					// Failed to queue handoff, remove the connection
					internal.Logger.Printf(ctx, "Failed to queue handoff: %v", err)
					return false, true, nil // Don't pool, remove connection, no error to caller
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
		}
	}
	// Default: pool the connection
	return true, false, nil
}

// ensureWorkerAvailable ensures at least one worker is available to process requests
// Creates a new worker if needed and under the max limit
func (ph *PoolHook) ensureWorkerAvailable() {
	select {
	case <-ph.shutdown:
		return
	default:
		// Check if we need a new worker
		currentWorkers := atomic.LoadInt32(&ph.activeWorkers)
		if currentWorkers < int32(ph.maxWorkers) {
			// Try to create a new worker (atomic increment to prevent race)
			if atomic.CompareAndSwapInt32(&ph.activeWorkers, currentWorkers, currentWorkers+1) {
				ph.workerWg.Add(1)
				go ph.onDemandWorker()
			}
		}
	}
}

// onDemandWorker processes handoff requests and exits when idle
func (ph *PoolHook) onDemandWorker() {
	defer func() {
		// Decrement active worker count when exiting
		atomic.AddInt32(&ph.activeWorkers, -1)
		ph.workerWg.Done()
	}()

	for {
		select {
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

		case <-time.After(ph.workerTimeout):
			// Worker has been idle for too long, exit to save resources
			if ph.config != nil && ph.config.LogLevel >= 3 { // Debug level
				internal.Logger.Printf(context.Background(),
					"hitless: worker exiting due to inactivity timeout (%v)", ph.workerTimeout)
			}
			return

		case <-ph.shutdown:
			return
		}
	}
}

// processHandoffRequest processes a single handoff request
func (ph *PoolHook) processHandoffRequest(request HandoffRequest) {
	// Remove from pending map
	defer ph.pending.Delete(request.Conn.GetID())

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
	shouldRetry, err := ph.performConnectionHandoffWithPool(shutdownCtx, request.Conn, request.Pool)
	if err != nil {
		if shouldRetry {
			now := time.Now()
			deadline, ok := shutdownCtx.Deadline()
			if !ok || deadline.Before(now) {
				// wait half the timeout before retrying if no deadline or deadline has passed
				deadline = now.Add(handoffTimeout / 2)
			}

			afterTime := deadline.Sub(now)
			if afterTime < handoffTimeout/2 {
				afterTime = handoffTimeout / 2
			}

			internal.Logger.Printf(context.Background(), "Handoff failed for connection WILL RETRY After %v: %v", afterTime, err)
			time.AfterFunc(afterTime, func() {
				ph.queueHandoff(request.Conn)
			})
		} else {
			pooler := request.Pool
			conn := request.Conn
			if pooler != nil {
				go pooler.Remove(ctx, conn, err)
				if ph.config != nil && ph.config.LogLevel >= 1 { // Warning level
					internal.Logger.Printf(ctx,
						"hitless: removed connection %d from pool due to max handoff retries reached",
						conn.GetID())
				}
			} else {
				go conn.Close()
				if ph.config != nil && ph.config.LogLevel >= 1 { // Warning level
					internal.Logger.Printf(ctx,
						"hitless: no pool provided for connection %d, cannot remove due to handoff initialization failure: %v",
						conn.GetID(), err)
				}
			}
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
		return errors.New("shutdown")
	default:
		select {
		case <-ph.shutdown:
			return errors.New("shutdown")
		case ph.handoffQueue <- request:
			// Store in pending map
			ph.pending.Store(request.ConnID, request.SeqID)
			// Ensure we have a worker to process this request
			ph.ensureWorkerAvailable()
			return nil
		default:
			// Queue is full - log and attempt scaling
			queueLen := len(ph.handoffQueue)
			queueCap := cap(ph.handoffQueue)
			if ph.config != nil && ph.config.LogLevel >= 1 { // Warning level
				internal.Logger.Printf(context.Background(),
					"hitless: handoff queue is full (%d/%d), attempting timeout queuing and scaling workers",
					queueLen, queueCap)
			}
		}
	}

	// Ensure we have workers available to handle the load
	ph.ensureWorkerAvailable()
	return ErrHandoffQueueFull
}

// performConnectionHandoffWithPool performs the actual connection handoff with pool for connection removal on failure
// When error is returned, the connection handoff should be retried if err is not ErrMaxHandoffRetriesReached
func (ph *PoolHook) performConnectionHandoffWithPool(ctx context.Context, conn *pool.Conn, pooler pool.Pooler) (shouldRetry bool, err error) {
	// Clear handoff state after successful handoff
	seqID := conn.GetMovingSeqID()
	connID := conn.GetID()

	// Notify hitless manager of completion if available
	if ph.hitlessManager != nil {
		defer ph.hitlessManager.UntrackOperationWithConnID(seqID, connID)
	}

	newEndpoint := conn.GetHandoffEndpoint()
	if newEndpoint == "" {
		return false, ErrInvalidHandoffState
	}

	retries := conn.IncrementAndGetHandoffRetries(1)
	maxRetries := 3 // Default fallback
	if ph.config != nil {
		maxRetries = ph.config.MaxHandoffRetries
	}

	if retries > maxRetries {
		if ph.config != nil && ph.config.LogLevel >= 1 { // Warning level
			internal.Logger.Printf(ctx,
				"hitless: reached max retries (%d) for handoff of connection %d to %s",
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
		// hitless: will retry
		// Maybe a network error - retry after a delay
		return true, err
	}

	// Get the old connection
	oldConn := conn.GetNetConn()

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

	// Apply relaxed timeout to the new connection for the configured post-handoff duration
	// This gives the new connection more time to handle operations during cluster transition
	if ph.config != nil && ph.config.PostHandoffRelaxedDuration > 0 {
		relaxedTimeout := ph.config.RelaxedTimeout
		postHandoffDuration := ph.config.PostHandoffRelaxedDuration

		// Set relaxed timeout with deadline - no background goroutine needed
		deadline := time.Now().Add(postHandoffDuration)
		conn.SetRelaxedTimeoutWithDeadline(relaxedTimeout, relaxedTimeout, deadline)

		if ph.config.LogLevel >= 2 { // Info level
			internal.Logger.Printf(context.Background(),
				"hitless: applied post-handoff relaxed timeout (%v) until %v for connection %d",
				relaxedTimeout, deadline.Format("15:04:05.000"), connID)
		}
	}

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

		// No timers to clean up with on-demand workers
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

// ErrConnectionMarkedForHandoff is returned when a connection is marked for handoff
// and should not be used until the handoff is complete
var ErrConnectionMarkedForHandoff = errors.New("connection marked for handoff")
