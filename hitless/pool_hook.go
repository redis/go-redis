package hitless

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
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
	Conn              *pool.Conn
	ConnID            uint64 // Unique connection identifier
	Endpoint          string
	SeqID             int64
	StopWorkerRequest bool
	Pool              pool.Pooler // Pool to remove connection from on failure
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

	// Dynamic worker scaling
	minWorkers     int
	maxWorkers     int
	currentWorkers int
	scalingMu      sync.Mutex
	scaleLevel     int // 0=min, 1=max

	// Scale down optimization
	scaleDownTimer     *time.Timer
	scaleDownMu        sync.Mutex
	lastCompletionTime time.Time
	scaleDownDelay     time.Duration

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
		shutdown:   make(chan struct{}),
		minWorkers: config.MinWorkers,
		maxWorkers: config.MaxWorkers,
		// Start with minimum workers
		currentWorkers: config.MinWorkers,
		scaleLevel:     0, // Start at minimum
		config:         config,
		// Hitless manager for operation completion tracking
		hitlessManager: hitlessManager,
		scaleDownDelay: config.ScaleDownDelay,
	}

	// Start worker goroutines at minimum level
	ph.startWorkers(ph.minWorkers)

	return ph
}

// SetPool sets the pool interface for removing connections on handoff failure
func (ph *PoolHook) SetPool(pooler pool.Pooler) {
	ph.pool = pooler
}

// GetCurrentWorkers returns the current number of workers (for testing)
func (ph *PoolHook) GetCurrentWorkers() int {
	ph.scalingMu.Lock()
	defer ph.scalingMu.Unlock()
	return ph.currentWorkers
}

// GetScaleLevel returns the current scale level (for testing)
func (ph *PoolHook) GetScaleLevel() int {
	ph.scalingMu.Lock()
	defer ph.scalingMu.Unlock()
	return ph.scaleLevel
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
				if err := conn.MarkQueuedForHandoff(); err != nil {
					// If marking fails, remove the connection instead
					return false, true, nil
				}
				return true, false, nil
			}
		}
	}
	// Default: pool the connection
	return true, false, nil
}

// startWorkers starts the worker goroutines for processing handoff requests
func (ph *PoolHook) startWorkers(count int) {
	for i := 0; i < count; i++ {
		ph.workerWg.Add(1)
		go ph.handoffWorker()
	}
}

// scaleUpWorkers scales up workers when queue is full (single step: min → max)
func (ph *PoolHook) scaleUpWorkers() {
	ph.scalingMu.Lock()
	defer ph.scalingMu.Unlock()

	if ph.scaleLevel >= 1 {
		return // Already at maximum scale
	}

	previousWorkers := ph.currentWorkers
	targetWorkers := ph.maxWorkers

	// Ensure we don't go below current workers
	if targetWorkers <= ph.currentWorkers {
		return
	}

	additionalWorkers := targetWorkers - ph.currentWorkers
	ph.startWorkers(additionalWorkers)
	ph.currentWorkers = targetWorkers
	ph.scaleLevel = 1

	if ph.config != nil && ph.config.LogLevel >= 2 { // Info level
		internal.Logger.Printf(context.Background(),
			"hitless: scaled up workers from %d to %d (max level) due to queue pressure",
			previousWorkers, ph.currentWorkers)
	}
}

// scaleDownWorkers returns to minimum worker count when queue is empty
func (ph *PoolHook) scaleDownWorkers() {
	ph.scalingMu.Lock()
	defer ph.scalingMu.Unlock()

	if ph.scaleLevel == 0 {
		return // Already at minimum scale
	}

	// Send stop worker requests to excess workers
	excessWorkers := ph.currentWorkers - ph.minWorkers
	previousWorkers := ph.currentWorkers

	for i := 0; i < excessWorkers; i++ {
		stopRequest := HandoffRequest{
			StopWorkerRequest: true,
		}

		// Try to send stop request without blocking
		select {
		case ph.handoffQueue <- stopRequest:
			// Stop request sent successfully
		default:
			// Queue is full, worker will naturally exit when queue empties
			break
		}
	}

	ph.currentWorkers = ph.minWorkers
	ph.scaleLevel = 0

	if ph.config != nil && ph.config.LogLevel >= 2 { // Info level
		internal.Logger.Printf(context.Background(),
			"hitless: scaling down workers from %d to %d (sent %d stop requests)",
			previousWorkers, ph.minWorkers, excessWorkers)
	}
}

// queueHandoffWithTimeout attempts to queue a handoff request with timeout and scaling
func (ph *PoolHook) queueHandoffWithTimeout(request HandoffRequest, cn *pool.Conn) {
	// First attempt - try immediate queuing
	select {
	case ph.handoffQueue <- request:
		return
	case <-ph.shutdown:
		ph.pending.Delete(cn.GetID())
		return
	default:
		// Queue is full - log and attempt scaling
		if ph.config != nil && ph.config.LogLevel >= 1 { // Warning level
			internal.Logger.Printf(context.Background(),
				"hitless: handoff queue is full (%d/%d), attempting timeout queuing and scaling workers",
				len(ph.handoffQueue), cap(ph.handoffQueue))
		}

		// Scale up workers to handle the load
		ph.scaleUpWorkers()
	}

	queueTimeout := 5 * time.Second // Default fallback
	if ph.config != nil {
		queueTimeout = ph.config.HandoffQueueTimeout
	}

	timeout := time.NewTimer(queueTimeout)
	defer timeout.Stop()

	select {
	case ph.handoffQueue <- request:
		// Queued successfully after timeout
		if ph.config != nil && ph.config.LogLevel >= 2 { // Info level
			internal.Logger.Printf(context.Background(),
				"hitless: handoff queued successfully after scaling workers")
		}
		return
	case <-timeout.C:
		// Timeout expired - drop the connection
		err := fmt.Errorf("handoff queue timeout after %v", queueTimeout)
		ph.pending.Delete(cn.GetID())
		if ph.config != nil && ph.config.LogLevel >= 0 { // Error level
			internal.Logger.Printf(context.Background(), err.Error())
		}
		return
	case <-ph.shutdown:
		ph.pending.Delete(cn.GetID())
		return
	}
}

// scheduleScaleDownCheck schedules a scale down check after a delay
// This is called after completing a handoff request to avoid expensive immediate checks
func (ph *PoolHook) scheduleScaleDownCheck() {
	ph.scaleDownMu.Lock()
	defer ph.scaleDownMu.Unlock()

	// Update last completion time
	ph.lastCompletionTime = time.Now()

	// If timer already exists, reset it
	if ph.scaleDownTimer != nil {
		ph.scaleDownTimer.Reset(ph.scaleDownDelay)
		return
	}

	// Create new timer
	ph.scaleDownTimer = time.AfterFunc(ph.scaleDownDelay, func() {
		ph.performScaleDownCheck()
	})
}

// performScaleDownCheck performs the actual scale down check
// This runs in a background goroutine after the delay
func (ph *PoolHook) performScaleDownCheck() {
	ph.scaleDownMu.Lock()
	defer ph.scaleDownMu.Unlock()

	// Clear the timer since it has fired
	ph.scaleDownTimer = nil

	// Check if we should scale down
	if ph.shouldScaleDown() {
		ph.scaleDownWorkers()
	}
}

// shouldScaleDown checks if conditions are met for scaling down
// This is the expensive check that we want to minimize
func (ph *PoolHook) shouldScaleDown() bool {
	// Quick check: if we're already at minimum scale, no need to scale down
	if ph.scaleLevel == 0 {
		return false
	}

	// Quick check: if queue is not empty, don't scale down
	if len(ph.handoffQueue) > 0 {
		return false
	}

	// Expensive check: count pending handoffs
	pendingCount := 0
	ph.pending.Range(func(key, value interface{}) bool {
		pendingCount++
		return pendingCount < 5 // Early exit if we find several pending
	})

	// Only scale down if no pending handoffs
	return pendingCount == 0
}

// handoffWorker processes handoff requests from the queue
func (ph *PoolHook) handoffWorker() {
	defer ph.workerWg.Done()

	for {
		select {
		case request := <-ph.handoffQueue:
			// Check if this is a stop worker request
			if request.StopWorkerRequest {
				if ph.config != nil && ph.config.LogLevel >= 2 { // Info level
					internal.Logger.Printf(context.Background(),
						"hitless: worker received stop request, exiting")
				}
				return // Exit this worker
			}

			ph.processHandoffRequest(request)
		case <-ph.shutdown:
			return
		}
	}
}

// processHandoffRequest processes a single handoff request
func (ph *PoolHook) processHandoffRequest(request HandoffRequest) {
	// Safety check: ignore stop worker requests (should be handled in worker)
	if request.StopWorkerRequest {
		return
	}

	// Remove from pending map
	defer ph.pending.Delete(request.Conn.GetID())

	// Create a context that respects shutdown signal
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	err := ph.performConnectionHandoffWithPool(shutdownCtx, request.Conn, request.Pool)

	// If handoff failed, restore the handoff state for potential retry
	if err != nil {
		request.Conn.RestoreHandoffState()
		internal.Logger.Printf(context.Background(), "Handoff failed for connection WILL RETRY: %v", err)
	}

	// Schedule a scale down check after completing this handoff request
	// This avoids expensive immediate checks and prevents rapid scaling cycles
	ph.scheduleScaleDownCheck()
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

	// Store in pending map
	ph.pending.Store(request.ConnID, request.SeqID)

	go ph.queueHandoffWithTimeout(request, conn)
	return nil
}

// performConnectionHandoffWithPool performs the actual connection handoff with pool for connection removal on failure
// if err is returned, connection will be removed from pool
func (ph *PoolHook) performConnectionHandoffWithPool(ctx context.Context, conn *pool.Conn, pooler pool.Pooler) error {
	// Clear handoff state after successful handoff
	seqID := conn.GetMovingSeqID()
	connID := conn.GetID()

	// Notify hitless manager of completion if available
	if ph.hitlessManager != nil {
		defer ph.hitlessManager.UntrackOperationWithConnID(seqID, connID)
	}

	newEndpoint := conn.GetHandoffEndpoint()
	if newEndpoint == "" {
		// TODO(hitless): Handle by performing the handoff to the current endpoint in N seconds,
		// Where N is the time in the moving notification...
		// For now, clear the handoff state and return
		conn.ClearHandoffState()
		return nil
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
		err := ErrMaxHandoffRetriesReached
		if pooler != nil {
			pooler.Remove(ctx, conn, err)
		} else {
			conn.Close()
			internal.Logger.Printf(ctx,
				"hitless: no pool provided for connection %d, cannot remove due to handoff initialization failure: %v",
				conn.GetID(), err)
		}
		return err
	}

	// Create endpoint-specific dialer
	endpointDialer := ph.createEndpointDialer(newEndpoint)

	// Create new connection to the new endpoint
	newNetConn, err := endpointDialer(ctx)
	if err != nil {
		// TODO(hitless): retry
		// This is the only case where we should retry the handoff request
		// Should we do anything else other than return the error?
		return err
	}

	// Get the old connection
	oldConn := conn.GetNetConn()

	// Replace the connection and execute initialization
	err = conn.SetNetConnWithInitConn(ctx, newNetConn)
	if err != nil {
		// Remove the connection from the pool since it's in a bad state
		if pooler != nil {
			// Use pool.Pooler interface directly - no adapter needed
			pooler.Remove(ctx, conn, err)
			if ph.config != nil && ph.config.LogLevel >= 1 { // Warning level
				internal.Logger.Printf(ctx,
					"hitless: removed connection %d from pool due to handoff initialization failure: %v",
					conn.GetID(), err)
			}
		} else {
			conn.Close()
			internal.Logger.Printf(ctx,
				"hitless: no pool provided for connection %d, cannot remove due to handoff initialization failure: %v",
				conn.GetID(), err)
		}

		// Keep the handoff state for retry
		return err
	}
	// Note: CLIENT MAINT_NOTIFICATIONS is sent during client initialization, not per connection
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

	return nil
}

// createEndpointDialer creates a dialer function that connects to a specific endpoint
func (ph *PoolHook) createEndpointDialer(endpoint string) func(context.Context) (net.Conn, error) {
	return func(ctx context.Context) (net.Conn, error) {
		// Parse endpoint to extract host and port
		host, port, err := net.SplitHostPort(endpoint)
		if err != nil {
			// If no port specified, assume default Redis port
			host = endpoint
			port = "6379"
		}

		// Use the base dialer to connect to the new endpoint
		return ph.baseDialer(ctx, ph.network, net.JoinHostPort(host, port))
	}
}

// Shutdown gracefully shuts down the processor, waiting for workers to complete
func (ph *PoolHook) Shutdown(ctx context.Context) error {
	ph.shutdownOnce.Do(func() {
		close(ph.shutdown)

		// Clean up scale down timer
		ph.scaleDownMu.Lock()
		if ph.scaleDownTimer != nil {
			ph.scaleDownTimer.Stop()
			ph.scaleDownTimer = nil
		}
		ph.scaleDownMu.Unlock()
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
