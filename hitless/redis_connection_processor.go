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
	CompleteOperationWithConnID(seqID int64, connID uint64)
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
	pending sync.Map // map[*pool.Conn]chan HandoffResult

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
		protocol:       protocol,
		baseDialer:     baseDialer,
		handoffQueue:   make(chan HandoffRequest, config.HandoffQueueSize),
		shutdown:       make(chan struct{}),
		minWorkers:     config.MinWorkers,
		maxWorkers:     config.MaxWorkers,
		currentWorkers: config.MinWorkers,
		scaleLevel:     0, // Start at minimum
		config:         config,
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

	// Check if connection is usable (not marked for handoff)
	if !cn.IsUsable() {
		return ErrConnectionMarkedForHandoff
	}

	// Check if connection has a pending handoff
	if _, pending := rcp.pending.Load(cn); pending {
		return ErrConnectionMarkedForHandoff
	}
	return nil
}

// ProcessConnectionOnPut is called when a connection is returned to the pool
func (rcp *RedisConnectionProcessor) ProcessConnectionOnPut(ctx context.Context, conn interface{}) (shouldPool bool, shouldRemove bool, err error) {
	cn, ok := conn.(*pool.Conn)
	if !ok {
		return false, true, fmt.Errorf("hitless: expected *pool.Conn, got %T", conn)
	}
	// Handle connection handoff if marked
	if cn.ShouldHandoff() {
		// Check for empty endpoint first (synchronous check)
		if cn.GetHandoffEndpoint() == "" {
			cn.ClearHandoffState()
		} else {
			if err := rcp.queueHandoff(cn); err != nil {
				// Failed to queue handoff, remove the connection
				internal.Logger.Printf(ctx, "Failed to queue handoff: %v", err)
				return false, true, nil // Don't pool, remove connection, no error to caller
			}
			// Handoff queued successfully, pool the connection (it will be skipped until handoff completes)
			cn.SetUsable(false) // Not usable until handoff completes
		}
	}

	// Check for buffered data that might be push notifications
	if cn.HasBufferedData() {
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
func (rcp *RedisConnectionProcessor) queueHandoffWithTimeout(request HandoffRequest, cn *pool.Conn) error {
	// First attempt - try immediate queuing
	select {
	case rcp.handoffQueue <- request:
		// Queued successfully
		return nil
	case <-rcp.shutdown:
		rcp.pending.Delete(cn)
		return errors.New("processor shutting down")
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

	// Second attempt - try with 5-second timeout
	timeout := time.NewTimer(5 * time.Second)
	defer timeout.Stop()

	select {
	case rcp.handoffQueue <- request:
		// Queued successfully after timeout
		if rcp.config != nil && rcp.config.LogLevel >= 2 { // Info level
			internal.Logger.Printf(context.Background(),
				"hitless: handoff queued successfully after scaling workers")
		}
		return nil
	case <-timeout.C:
		// Timeout expired - drop the connection
		err := errors.New("handoff queue timeout after 5 seconds")
		rcp.pending.Delete(cn)
		if rcp.config != nil && rcp.config.LogLevel >= 0 { // Error level
			internal.Logger.Printf(context.Background(), err.Error())
		}
		return err
	case <-rcp.shutdown:
		rcp.pending.Delete(cn)
		return errors.New("processor shutting down")
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

	// Send result back
	result := HandoffResult{
		Conn: request.Conn,
		Err:  err,
	}

	select {
	case request.Result <- result:
		// Result sent successfully
	default:
		// Result channel closed or full, log the outcome
		if err != nil {
			internal.Logger.Printf(context.Background(), "Handoff failed for connection: %v", err)
		}
	}

	// Schedule a scale down check after completing this handoff request
	// This avoids expensive immediate checks and prevents rapid scaling cycles
	rcp.scheduleScaleDownCheck()
}

// queueHandoff queues a handoff request for processing
// if err is returned, connection will be removed from pool
func (rcp *RedisConnectionProcessor) queueHandoff(cn *pool.Conn) error {
	// Create result channel
	resultChan := make(chan HandoffResult, 1)

	// Create handoff request
	request := HandoffRequest{
		Conn:     cn,
		ConnID:   cn.GetID(),
		Endpoint: cn.GetHandoffEndpoint(),
		SeqID:    cn.GetMovingSeqID(),
		Result:   resultChan,
		Pool:     rcp.pool, // Include pool for connection removal on failure
	}

	// Store in pending map
	rcp.pending.Store(cn, resultChan)

	// Try to queue the request with optimizations
	return rcp.queueHandoffWithTimeout(request, cn)
}

// performConnectionHandoffWithPool performs the actual connection handoff with pool for connection removal on failure
// if err is returned, connection will be removed from pool
func (rcp *RedisConnectionProcessor) performConnectionHandoffWithPool(ctx context.Context, cn *pool.Conn, pooler pool.Pooler) error {
	if !cn.ShouldHandoff() {
		return nil
	}

	newEndpoint := cn.GetHandoffEndpoint()
	if newEndpoint == "" {
		cn.ClearHandoffState()
		return nil
	}

	retries := cn.IncrementAndGetHandoffRetries(1)
	if retries > 3 {
		if rcp.config != nil && rcp.config.LogLevel >= 1 { // Warning level
			internal.Logger.Printf(ctx,
				"hitless: reached max retries (%d) for handoff of connection %d to %s",
				retries, cn.GetID(), cn.GetHandoffEndpoint())
		}
		err := errors.New("max handoff retries reached")
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
		// Keep the handoff state for retry
		return err
	}

	// Get the old connection
	oldConn := cn.GetNetConn()
	defer func() {
		if oldConn != nil {
			oldConn.Close()
		}
	}()

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

	// Clear handoff state after successful handoff
	seqID := cn.GetMovingSeqID()
	connID := cn.GetID()
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

	// Notify hitless manager of completion if available
	if rcp.hitlessManager != nil {
		rcp.hitlessManager.CompleteOperationWithConnID(seqID, connID)
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
