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
	"github.com/redis/go-redis/v9/internal/proto"
)

// HitlessManagerInterface defines the interface for completing handoff operations
type HitlessManagerInterface interface {
	UntrackOperationWithConnID(seqID int64, connID uint64)
}

// HandoffRequest represents a request to handoff a connection to a new endpoint
type HandoffRequest struct {
	Conn              *pool.Conn
	ConnID            uint64 // Unique connection identifier
	Endpoint          string
	SeqID             int64
	Result            chan HandoffResult
	StopWorkerRequest bool
	Pool              pool.Pooler // Pool to remove connection from on failure
}

// HandoffResult represents the result of a handoff operation
type HandoffResult struct {
	Conn *pool.Conn
	Err  error
}

// RedisConnectionProcessor implements interfaces.ConnectionProcessor for Redis-specific connection handling
// with hitless upgrade support.
type RedisConnectionProcessor struct {
	// Protocol version (2 = RESP2, 3 = RESP3 with push notifications)
	protocol int

	// Base dialer for creating connections to new endpoints during handoffs
	baseDialer func(context.Context, string, string) (net.Conn, error)

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

	// Configuration for the processor
	config *Config

	// Hitless manager for operation completion tracking
	hitlessManager HitlessManagerInterface

	// Pool interface for removing connections on handoff failure
	pool pool.Pooler
}

// NewRedisConnectionProcessor creates a new Redis connection processor
func NewRedisConnectionProcessor(protocol int, baseDialer func(context.Context, string, string) (net.Conn, error), config *Config, hitlessManager HitlessManagerInterface) *RedisConnectionProcessor {
	return NewRedisConnectionProcessorWithPoolSize(protocol, baseDialer, config, hitlessManager, 0)
}

// NewRedisConnectionProcessorWithPoolSize creates a new Redis connection processor with pool size for better worker defaults
func NewRedisConnectionProcessorWithPoolSize(protocol int, baseDialer func(context.Context, string, string) (net.Conn, error), config *Config, hitlessManager HitlessManagerInterface, poolSize int) *RedisConnectionProcessor {
	// Apply defaults to any missing configuration fields, using pool size for worker calculations
	config = config.ApplyDefaultsWithPoolSize(poolSize)

	rcp := &RedisConnectionProcessor{
		// Protocol version (2 = RESP2, 3 = RESP3 with push notifications)
		protocol: protocol,
		// baseDialer is used to create connections to new endpoints during handoffs
		baseDialer: baseDialer,
		// Note: CLIENT MAINT_NOTIFICATIONS is handled during client initialization
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
	rcp.startWorkers(rcp.minWorkers)

	return rcp
}

// SetPool sets the pool interface for removing connections on handoff failure
func (rcp *RedisConnectionProcessor) SetPool(pooler pool.Pooler) {
	rcp.pool = pooler
}

// GetCurrentWorkers returns the current number of workers (for testing)
func (rcp *RedisConnectionProcessor) GetCurrentWorkers() int {
	rcp.scalingMu.Lock()
	defer rcp.scalingMu.Unlock()
	return rcp.currentWorkers
}

// GetScaleLevel returns the current scale level (for testing)
func (rcp *RedisConnectionProcessor) GetScaleLevel() int {
	rcp.scalingMu.Lock()
	defer rcp.scalingMu.Unlock()
	return rcp.scaleLevel
}

// log logs a message if the log level is appropriate
func (rcp *RedisConnectionProcessor) log(level int, message string) {
	if rcp.config.LogLevel >= level {
		internal.Logger.Printf(context.Background(), message)
	}
}

// IsHandoffPending returns true if the given connection has a pending handoff
func (rcp *RedisConnectionProcessor) IsHandoffPending(conn *pool.Conn) bool {
	_, pending := rcp.pending.Load(conn)
	return pending
}

// ProcessConnectionOnGet is called when a connection is retrieved from the pool
func (rcp *RedisConnectionProcessor) ProcessConnectionOnGet(ctx context.Context, conn interface{}) error {
	cn, ok := conn.(*pool.Conn)
	if !ok {
		return fmt.Errorf("hitless: expected *pool.Conn, got %T", conn)
	}

	// NOTE: There are two conditions to make sure we don't return a connection that should be handed off or is
	// in a handoff state at the moment.

	// Check if connection is usable (not in a handoff state)
	if !cn.IsUsable() {
		return ErrConnectionMarkedForHandoff
	}

	// Check if connection is marked for handoff, which means it will be queued for handoff on put.
	if cn.ShouldHandoff() {
		return ErrConnectionMarkedForHandoff
	}

	// Note: CLIENT MAINT_NOTIFICATIONS command is sent during client initialization
	// in redis.go, so no need to send it here per connection

	return nil
}

// ProcessConnectionOnPut is called when a connection is returned to the pool
func (rcp *RedisConnectionProcessor) ProcessConnectionOnPut(ctx context.Context, conn interface{}) (shouldPool bool, shouldRemove bool, err error) {
	cn, ok := conn.(*pool.Conn)
	if !ok {
		return false, true, fmt.Errorf("hitless: expected *pool.Conn, got %T", conn)
	}

	if cn.HasBufferedData() {
		// Check for buffered data that might be push notifications
		// Check if this might be push notification data
		if rcp.protocol == 3 {
			// For RESP3, peek at the reply type to check if it's a push notification
			if replyType, err := cn.PeekReplyTypeSafe(); err != nil || replyType != proto.RespPush {
				// Not a push notification or error peeking, remove connection
				internal.Logger.Printf(ctx, "Conn has unread data (not push notification), removing it")
				return false, true, nil
			}
			// It's a push notification, allow pooling (client will handle it)
		} else {
			// For RESP2, any buffered data is unexpected
			internal.Logger.Printf(ctx, "Conn has unread data, removing it")
			return false, true, nil
		}
	}

	// first check if we should handoff for faster rejection
	if cn.ShouldHandoff() {
		// check pending handoff to not queue the same connection twice
		_, hasPendingHandoff := rcp.pending.Load(cn)
		if !hasPendingHandoff {
			// Check for empty endpoint first (synchronous check)
			if cn.GetHandoffEndpoint() == "" {
				cn.ClearHandoffState()
			} else {
				if err := rcp.queueHandoff(cn); err != nil {
					// Failed to queue handoff, remove the connection
					internal.Logger.Printf(ctx, "Failed to queue handoff: %v", err)
					return false, true, nil // Don't pool, remove connection, no error to caller
				}
				cn.MarkQueuedForHandoff()
				return true, false, nil
			}
		}
	}
	// Default: pool the connection
	return true, false, nil
}

// startWorkers starts the worker goroutines for processing handoff requests
func (rcp *RedisConnectionProcessor) startWorkers(count int) {
	for i := 0; i < count; i++ {
		rcp.workerWg.Add(1)
		go rcp.handoffWorker()
	}
}

// scaleUpWorkers scales up workers when queue is full (single step: min → max)
func (rcp *RedisConnectionProcessor) scaleUpWorkers() {
	rcp.scalingMu.Lock()
	defer rcp.scalingMu.Unlock()

	if rcp.scaleLevel >= 1 {
		return // Already at maximum scale
	}

	previousWorkers := rcp.currentWorkers
	targetWorkers := rcp.maxWorkers

	// Ensure we don't go below current workers
	if targetWorkers <= rcp.currentWorkers {
		return
	}

	additionalWorkers := targetWorkers - rcp.currentWorkers
	rcp.startWorkers(additionalWorkers)
	rcp.currentWorkers = targetWorkers
	rcp.scaleLevel = 1

	if rcp.config != nil && rcp.config.LogLevel >= 2 { // Info level
		internal.Logger.Printf(context.Background(),
			"hitless: scaled up workers from %d to %d (max level) due to queue pressure",
			previousWorkers, rcp.currentWorkers)
	}
}

// scaleDownWorkers returns to minimum worker count when queue is empty
func (rcp *RedisConnectionProcessor) scaleDownWorkers() {
	rcp.scalingMu.Lock()
	defer rcp.scalingMu.Unlock()

	if rcp.scaleLevel == 0 {
		return // Already at minimum scale
	}

	// Send stop worker requests to excess workers
	excessWorkers := rcp.currentWorkers - rcp.minWorkers
	previousWorkers := rcp.currentWorkers

	for i := 0; i < excessWorkers; i++ {
		stopRequest := HandoffRequest{
			StopWorkerRequest: true,
		}

		// Try to send stop request without blocking
		select {
		case rcp.handoffQueue <- stopRequest:
			// Stop request sent successfully
		default:
			// Queue is full, worker will naturally exit when queue empties
			break
		}
	}

	rcp.currentWorkers = rcp.minWorkers
	rcp.scaleLevel = 0

	if rcp.config != nil && rcp.config.LogLevel >= 2 { // Info level
		internal.Logger.Printf(context.Background(),
			"hitless: scaling down workers from %d to %d (sent %d stop requests)",
			previousWorkers, rcp.minWorkers, excessWorkers)
	}
}

// queueHandoffWithTimeout attempts to queue a handoff request with timeout and scaling
func (rcp *RedisConnectionProcessor) queueHandoffWithTimeout(request HandoffRequest, cn *pool.Conn) {
	// First attempt - try immediate queuing
	select {
	case rcp.handoffQueue <- request:
		return
	case <-rcp.shutdown:
		rcp.pending.Delete(cn)
		return
	default:
		// Queue is full - log and attempt scaling
		if rcp.config != nil && rcp.config.LogLevel >= 1 { // Warning level
			internal.Logger.Printf(context.Background(),
				"hitless: handoff queue is full (%d/%d), attempting timeout queuing and scaling workers",
				len(rcp.handoffQueue), cap(rcp.handoffQueue))
		}

		// Scale up workers to handle the load
		rcp.scaleUpWorkers()
	}

	// TODO: reimplement? extract as config?
	// Second attempt - try queuing with timeout of 2 seconds
	queueTimeout := 5 * time.Second // Default fallback
	if rcp.config != nil {
		queueTimeout = rcp.config.HandoffQueueTimeout
	}

	timeout := time.NewTimer(queueTimeout)
	defer timeout.Stop()

	select {
	case rcp.handoffQueue <- request:
		// Queued successfully after timeout
		if rcp.config != nil && rcp.config.LogLevel >= 2 { // Info level
			internal.Logger.Printf(context.Background(),
				"hitless: handoff queued successfully after scaling workers")
		}
		return
	case <-timeout.C:
		// Timeout expired - drop the connection
		err := fmt.Errorf("handoff queue timeout after %v", queueTimeout)
		rcp.pending.Delete(cn)
		if rcp.config != nil && rcp.config.LogLevel >= 0 { // Error level
			internal.Logger.Printf(context.Background(), err.Error())
		}
		return
	case <-rcp.shutdown:
		rcp.pending.Delete(cn)
		return
	}
}

// scheduleScaleDownCheck schedules a scale down check after a delay
// This is called after completing a handoff request to avoid expensive immediate checks
func (rcp *RedisConnectionProcessor) scheduleScaleDownCheck() {
	rcp.scaleDownMu.Lock()
	defer rcp.scaleDownMu.Unlock()

	// Update last completion time
	rcp.lastCompletionTime = time.Now()

	// If timer already exists, reset it
	if rcp.scaleDownTimer != nil {
		rcp.scaleDownTimer.Reset(rcp.scaleDownDelay)
		return
	}

	// Create new timer
	rcp.scaleDownTimer = time.AfterFunc(rcp.scaleDownDelay, func() {
		rcp.performScaleDownCheck()
	})
}

// performScaleDownCheck performs the actual scale down check
// This runs in a background goroutine after the delay
func (rcp *RedisConnectionProcessor) performScaleDownCheck() {
	rcp.scaleDownMu.Lock()
	defer rcp.scaleDownMu.Unlock()

	// Clear the timer since it has fired
	rcp.scaleDownTimer = nil

	// Check if we should scale down
	if rcp.shouldScaleDown() {
		rcp.scaleDownWorkers()
	}
}

// shouldScaleDown checks if conditions are met for scaling down
// This is the expensive check that we want to minimize
func (rcp *RedisConnectionProcessor) shouldScaleDown() bool {
	// Quick check: if we're already at minimum scale, no need to scale down
	if rcp.scaleLevel == 0 {
		return false
	}

	// Quick check: if queue is not empty, don't scale down
	if len(rcp.handoffQueue) > 0 {
		return false
	}

	// Expensive check: count pending handoffs
	pendingCount := 0
	rcp.pending.Range(func(key, value interface{}) bool {
		pendingCount++
		return pendingCount < 5 // Early exit if we find several pending
	})

	// Only scale down if no pending handoffs
	return pendingCount == 0
}

// handoffWorker processes handoff requests from the queue
func (rcp *RedisConnectionProcessor) handoffWorker() {
	defer rcp.workerWg.Done()

	for {
		select {
		case request := <-rcp.handoffQueue:
			// Check if this is a stop worker request
			if request.StopWorkerRequest {
				if rcp.config != nil && rcp.config.LogLevel >= 2 { // Info level
					internal.Logger.Printf(context.Background(),
						"hitless: worker received stop request, exiting")
				}
				return // Exit this worker
			}

			rcp.processHandoffRequest(request)
		case <-rcp.shutdown:
			return
		}
	}
}

// processHandoffRequest processes a single handoff request
func (rcp *RedisConnectionProcessor) processHandoffRequest(request HandoffRequest) {
	// Safety check: ignore stop worker requests (should be handled in worker)
	if request.StopWorkerRequest {
		return
	}

	// Remove from pending map
	defer rcp.pending.Delete(request.Conn)

	// Perform the handoff
	err := rcp.performConnectionHandoffWithPool(context.Background(), request.Conn, request.Pool)

	// If handoff failed, restore the handoff state for potential retry
	if err != nil {
		request.Conn.RestoreHandoffState()
		internal.Logger.Printf(context.Background(), "Handoff failed for connection WILL RETRY: %v", err)
	}

	// Schedule a scale down check after completing this handoff request
	// This avoids expensive immediate checks and prevents rapid scaling cycles
	rcp.scheduleScaleDownCheck()
}

// queueHandoff queues a handoff request for processing
// if err is returned, connection will be removed from pool
func (rcp *RedisConnectionProcessor) queueHandoff(cn *pool.Conn) error {
	// Create handoff request
	request := HandoffRequest{
		Conn:     cn,
		ConnID:   cn.GetID(),
		Endpoint: cn.GetHandoffEndpoint(),
		SeqID:    cn.GetMovingSeqID(),
		Pool:     rcp.pool, // Include pool for connection removal on failure
	}

	// Store in pending map
	rcp.pending.Store(request.ConnID, request.SeqID)

	go rcp.queueHandoffWithTimeout(request, cn)
	return nil
}

// performConnectionHandoffWithPool performs the actual connection handoff with pool for connection removal on failure
// if err is returned, connection will be removed from pool
func (rcp *RedisConnectionProcessor) performConnectionHandoffWithPool(ctx context.Context, cn *pool.Conn, pooler pool.Pooler) error {
	// Clear handoff state after successful handoff
	seqID := cn.GetMovingSeqID()
	connID := cn.GetID()

	// Notify hitless manager of completion if available
	if rcp.hitlessManager != nil {
		defer rcp.hitlessManager.UntrackOperationWithConnID(seqID, connID)
	}

	newEndpoint := cn.GetHandoffEndpoint()
	if newEndpoint == "" {
		// TODO(hitless): maybe auto?
		// Handle by performing the handoff to the current endpoint in N seconds,
		// Where N is the time in the moving notification...
		// Won't work for now!
		cn.ClearHandoffState()
		return nil
	}

	retries := cn.IncrementAndGetHandoffRetries(1)
	maxRetries := 3 // Default fallback
	if rcp.config != nil {
		maxRetries = rcp.config.MaxHandoffRetries
	}

	if retries > maxRetries {
		if rcp.config != nil && rcp.config.LogLevel >= 1 { // Warning level
			internal.Logger.Printf(ctx,
				"hitless: reached max retries (%d) for handoff of connection %d to %s",
				maxRetries, cn.GetID(), cn.GetHandoffEndpoint())
		}
		err := ErrMaxHandoffRetriesReached
		if pooler != nil {
			pooler.Remove(ctx, cn, err)
		} else {
			cn.Close()
			internal.Logger.Printf(ctx,
				"hitless: no pool provided for connection %d, cannot remove due to handoff initialization failure: %v",
				cn.GetID(), err)
		}
		return err
	}

	// Create endpoint-specific dialer
	endpointDialer := rcp.createEndpointDialer(newEndpoint)

	// Create new connection to the new endpoint
	newNetConn, err := endpointDialer(ctx)
	if err != nil {
		// TODO(hitless): requeue the handoff request
		// This is the only case where we should retry the handoff request
		return err
	}

	// Get the old connection
	oldConn := cn.GetNetConn()

	// Replace the connection and execute initialization
	err = cn.SetNetConnWithInitConn(ctx, newNetConn)
	if err != nil {
		// Remove the connection from the pool since it's in a bad state
		if pooler != nil {
			// Use pool.Pooler interface directly - no adapter needed
			pooler.Remove(ctx, cn, err)
			if rcp.config != nil && rcp.config.LogLevel >= 1 { // Warning level
				internal.Logger.Printf(ctx,
					"hitless: removed connection %d from pool due to handoff initialization failure: %v",
					cn.GetID(), err)
			}
		} else {
			cn.Close()
			internal.Logger.Printf(ctx,
				"hitless: no pool provided for connection %d, cannot remove due to handoff initialization failure: %v",
				cn.GetID(), err)
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

	cn.ClearHandoffState()

	// Apply relaxed timeout to the new connection for the configured post-handoff duration
	// This gives the new connection more time to handle operations during cluster transition
	if rcp.config != nil && rcp.config.PostHandoffRelaxedDuration > 0 {
		relaxedTimeout := rcp.config.RelaxedTimeout
		postHandoffDuration := rcp.config.PostHandoffRelaxedDuration

		// Set relaxed timeout with deadline - no background goroutine needed
		deadline := time.Now().Add(postHandoffDuration)
		cn.SetRelaxedTimeoutWithDeadline(relaxedTimeout, relaxedTimeout, deadline)

		if rcp.config.LogLevel >= 2 { // Info level
			internal.Logger.Printf(context.Background(),
				"hitless: applied post-handoff relaxed timeout (%v) until %v for connection %d",
				relaxedTimeout, deadline.Format("15:04:05.000"), connID)
		}
	}

	return nil
}

// createEndpointDialer creates a dialer function that connects to a specific endpoint
func (rcp *RedisConnectionProcessor) createEndpointDialer(endpoint string) func(context.Context) (net.Conn, error) {
	return func(ctx context.Context) (net.Conn, error) {
		// Parse endpoint to extract host and port
		host, port, err := net.SplitHostPort(endpoint)
		if err != nil {
			// If no port specified, assume default Redis port
			host = endpoint
			port = "6379"
		}

		// Use the base dialer to connect to the new endpoint
		return rcp.baseDialer(ctx, "tcp", net.JoinHostPort(host, port))
	}
}

// Shutdown gracefully shuts down the processor, waiting for workers to complete
func (rcp *RedisConnectionProcessor) Shutdown(ctx context.Context) error {
	rcp.shutdownOnce.Do(func() {
		close(rcp.shutdown)

		// Clean up scale down timer
		rcp.scaleDownMu.Lock()
		if rcp.scaleDownTimer != nil {
			rcp.scaleDownTimer.Stop()
			rcp.scaleDownTimer = nil
		}
		rcp.scaleDownMu.Unlock()
	})

	// Wait for workers to complete
	done := make(chan struct{})
	go func() {
		rcp.workerWg.Wait()
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
