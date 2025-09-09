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

// handoffWorkerManager manages background workers and queue for connection handoffs
type handoffWorkerManager struct {
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

	// Pool hook reference for handoff processing
	poolHook *PoolHook

	// Circuit breaker manager for endpoint failure handling
	circuitBreakerManager *CircuitBreakerManager
}

// newHandoffWorkerManager creates a new handoff worker manager
func newHandoffWorkerManager(config *Config, poolHook *PoolHook) *handoffWorkerManager {
	return &handoffWorkerManager{
		handoffQueue:          make(chan HandoffRequest, config.HandoffQueueSize),
		shutdown:              make(chan struct{}),
		maxWorkers:            config.MaxWorkers,
		activeWorkers:         atomic.Int32{},   // Start with no workers - create on demand
		workerTimeout:         15 * time.Second, // Workers exit after 15s of inactivity
		config:                config,
		poolHook:              poolHook,
		circuitBreakerManager: newCircuitBreakerManager(config),
	}
}

// getCurrentWorkers returns the current number of active workers (for testing)
func (hwm *handoffWorkerManager) getCurrentWorkers() int {
	return int(hwm.activeWorkers.Load())
}

// getPendingMap returns the pending map for testing purposes
func (hwm *handoffWorkerManager) getPendingMap() *sync.Map {
	return &hwm.pending
}

// getMaxWorkers returns the max workers for testing purposes
func (hwm *handoffWorkerManager) getMaxWorkers() int {
	return hwm.maxWorkers
}

// getHandoffQueue returns the handoff queue for testing purposes
func (hwm *handoffWorkerManager) getHandoffQueue() chan HandoffRequest {
	return hwm.handoffQueue
}

// getCircuitBreakerStats returns circuit breaker statistics for monitoring
func (hwm *handoffWorkerManager) getCircuitBreakerStats() []CircuitBreakerStats {
	return hwm.circuitBreakerManager.GetAllStats()
}

// resetCircuitBreakers resets all circuit breakers (useful for testing)
func (hwm *handoffWorkerManager) resetCircuitBreakers() {
	hwm.circuitBreakerManager.Reset()
}

// isHandoffPending returns true if the given connection has a pending handoff
func (hwm *handoffWorkerManager) isHandoffPending(conn *pool.Conn) bool {
	_, pending := hwm.pending.Load(conn.GetID())
	return pending
}

// ensureWorkerAvailable ensures at least one worker is available to process requests
// Creates a new worker if needed and under the max limit
func (hwm *handoffWorkerManager) ensureWorkerAvailable() {
	select {
	case <-hwm.shutdown:
		return
	default:
		if hwm.workersScaling.CompareAndSwap(false, true) {
			defer hwm.workersScaling.Store(false)
			// Check if we need a new worker
			currentWorkers := hwm.activeWorkers.Load()
			workersWas := currentWorkers
			for currentWorkers < int32(hwm.maxWorkers) {
				hwm.workerWg.Add(1)
				go hwm.onDemandWorker()
				currentWorkers++
			}
			// workersWas is always <= currentWorkers
			// currentWorkers will be maxWorkers, but if we have a worker that was closed
			// while we were creating new workers, just add the difference between
			// the currentWorkers and the number of workers we observed initially (i.e. the number of workers we created)
			hwm.activeWorkers.Add(currentWorkers - workersWas)
		}
	}
}

// onDemandWorker processes handoff requests and exits when idle
func (hwm *handoffWorkerManager) onDemandWorker() {
	defer func() {
		// Handle panics to ensure proper cleanup
		if r := recover(); r != nil {
			internal.Logger.Printf(context.Background(),
				"hitless: worker panic recovered: %v", r)
		}

		// Decrement active worker count when exiting
		hwm.activeWorkers.Add(-1)
		hwm.workerWg.Done()
	}()

	// Create reusable timer to prevent timer leaks
	timer := time.NewTimer(hwm.workerTimeout)
	defer timer.Stop()

	for {
		// Reset timer for next iteration
		if !timer.Stop() {
			select {
			case <-timer.C:
			default:
			}
		}
		timer.Reset(hwm.workerTimeout)

		select {
		case <-hwm.shutdown:
			return
		case <-timer.C:
			// Worker has been idle for too long, exit to save resources
			if hwm.config != nil && hwm.config.LogLevel.InfoOrAbove() {
				internal.Logger.Printf(context.Background(),
					"hitless: worker exiting due to inactivity timeout (%v)", hwm.workerTimeout)
			}
			return
		case request := <-hwm.handoffQueue:
			// Check for shutdown before processing
			select {
			case <-hwm.shutdown:
				// Clean up the request before exiting
				hwm.pending.Delete(request.ConnID)
				return
			default:
				// Process the request
				hwm.processHandoffRequest(request)
			}
		}
	}
}

// processHandoffRequest processes a single handoff request
func (hwm *handoffWorkerManager) processHandoffRequest(request HandoffRequest) {
	// Remove from pending map
	defer hwm.pending.Delete(request.Conn.GetID())
	internal.Logger.Printf(context.Background(), "hitless: conn[%d] Processing handoff request start", request.Conn.GetID())

	// Create a context with handoff timeout from config
	handoffTimeout := 15 * time.Second // Default timeout
	if hwm.config != nil && hwm.config.HandoffTimeout > 0 {
		handoffTimeout = hwm.config.HandoffTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), handoffTimeout)
	defer cancel()

	// Create a context that also respects the shutdown signal
	shutdownCtx, shutdownCancel := context.WithCancel(ctx)
	defer shutdownCancel()

	// Monitor shutdown signal in a separate goroutine
	go func() {
		select {
		case <-hwm.shutdown:
			shutdownCancel()
		case <-shutdownCtx.Done():
		}
	}()

	// Perform the handoff with cancellable context
	shouldRetry, err := hwm.performConnectionHandoff(shutdownCtx, request.Conn)
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
			if afterTime < minRetryBackoff {
				afterTime = minRetryBackoff
			}

			internal.Logger.Printf(context.Background(), "Handoff failed for conn[%d] WILL RETRY After %v: %v", request.ConnID, afterTime, err)
			time.AfterFunc(afterTime, func() {
				if err := hwm.queueHandoff(request.Conn); err != nil {
					internal.Logger.Printf(context.Background(), "can't queue handoff for retry: %v", err)
					hwm.closeConnFromRequest(context.Background(), request, err)
				}
			})
			return
		} else {
			go hwm.closeConnFromRequest(ctx, request, err)
		}

		// Clear handoff state if not returned for retry
		seqID := request.Conn.GetMovingSeqID()
		connID := request.Conn.GetID()
		if hwm.poolHook.hitlessManager != nil {
			hwm.poolHook.hitlessManager.UntrackOperationWithConnID(seqID, connID)
		}
	}
}

// queueHandoff queues a handoff request for processing
// if err is returned, connection will be removed from pool
func (hwm *handoffWorkerManager) queueHandoff(conn *pool.Conn) error {
	// Get handoff info atomically to prevent race conditions
	shouldHandoff, endpoint, seqID := conn.GetHandoffInfo()
	if !shouldHandoff {
		return errors.New("connection is not marked for handoff")
	}

	// Create handoff request with atomically retrieved data
	request := HandoffRequest{
		Conn:     conn,
		ConnID:   conn.GetID(),
		Endpoint: endpoint,
		SeqID:    seqID,
		Pool:     hwm.poolHook.pool, // Include pool for connection removal on failure
	}

	select {
	// priority to shutdown
	case <-hwm.shutdown:
		return ErrShutdown
	default:
		select {
		case <-hwm.shutdown:
			return ErrShutdown
		case hwm.handoffQueue <- request:
			// Store in pending map
			hwm.pending.Store(request.ConnID, request.SeqID)
			// Ensure we have a worker to process this request
			hwm.ensureWorkerAvailable()
			return nil
		default:
			select {
			case <-hwm.shutdown:
				return ErrShutdown
			case hwm.handoffQueue <- request:
				// Store in pending map
				hwm.pending.Store(request.ConnID, request.SeqID)
				// Ensure we have a worker to process this request
				hwm.ensureWorkerAvailable()
				return nil
			case <-time.After(100 * time.Millisecond): // give workers a chance to process
				// Queue is full - log and attempt scaling
				queueLen := len(hwm.handoffQueue)
				queueCap := cap(hwm.handoffQueue)
				if hwm.config != nil && hwm.config.LogLevel.WarnOrAbove() { // Warning level
					internal.Logger.Printf(context.Background(),
						"hitless: handoff queue is full (%d/%d), cannot queue new handoff requests - consider increasing HandoffQueueSize or MaxWorkers in configuration",
						queueLen, queueCap)
				}
			}
		}
	}

	// Ensure we have workers available to handle the load
	hwm.ensureWorkerAvailable()
	return ErrHandoffQueueFull
}

// shutdownWorkers gracefully shuts down the worker manager, waiting for workers to complete
func (hwm *handoffWorkerManager) shutdownWorkers(ctx context.Context) error {
	hwm.shutdownOnce.Do(func() {
		close(hwm.shutdown)
		// workers will exit when they finish their current request

		// Shutdown circuit breaker manager cleanup goroutine
		if hwm.circuitBreakerManager != nil {
			hwm.circuitBreakerManager.Shutdown()
		}
	})

	// Wait for workers to complete
	done := make(chan struct{})
	go func() {
		hwm.workerWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// performConnectionHandoff performs the actual connection handoff
// When error is returned, the connection handoff should be retried if err is not ErrMaxHandoffRetriesReached
func (hwm *handoffWorkerManager) performConnectionHandoff(ctx context.Context, conn *pool.Conn) (shouldRetry bool, err error) {
	// Clear handoff state after successful handoff
	connID := conn.GetID()

	newEndpoint := conn.GetHandoffEndpoint()
	if newEndpoint == "" {
		return false, ErrConnectionInvalidHandoffState
	}

	// Use circuit breaker to protect against failing endpoints
	circuitBreaker := hwm.circuitBreakerManager.GetCircuitBreaker(newEndpoint)

	// Check if circuit breaker is open before attempting handoff
	if circuitBreaker.IsOpen() {
		internal.Logger.Printf(ctx, "hitless: conn[%d] handoff to %s failed fast due to circuit breaker", connID, newEndpoint)
		return false, ErrCircuitBreakerOpen // Don't retry when circuit breaker is open
	}

	// Perform the handoff
	shouldRetry, err = hwm.performHandoffInternal(ctx, conn, newEndpoint, connID)

	// Update circuit breaker based on result
	if err != nil {
		// Only track dial/network errors in circuit breaker, not initialization errors
		if shouldRetry {
			circuitBreaker.recordFailure()
		}
		return shouldRetry, err
	}

	// Success - record in circuit breaker
	circuitBreaker.recordSuccess()
	return false, nil
}

// performHandoffInternal performs the actual handoff logic (extracted for circuit breaker integration)
func (hwm *handoffWorkerManager) performHandoffInternal(ctx context.Context, conn *pool.Conn, newEndpoint string, connID uint64) (shouldRetry bool, err error) {

	retries := conn.IncrementAndGetHandoffRetries(1)
	internal.Logger.Printf(ctx, "hitless: conn[%d] Retry %d: Performing handoff to %s(was %s)", connID, retries, newEndpoint, conn.RemoteAddr().String())
	maxRetries := 3 // Default fallback
	if hwm.config != nil {
		maxRetries = hwm.config.MaxHandoffRetries
	}

	if retries > maxRetries {
		if hwm.config != nil && hwm.config.LogLevel.WarnOrAbove() { // Warning level
			internal.Logger.Printf(ctx,
				"hitless: reached max retries (%d) for handoff of conn[%d] to %s",
				maxRetries, connID, newEndpoint)
		}
		// won't retry on ErrMaxHandoffRetriesReached
		return false, ErrMaxHandoffRetriesReached
	}

	// Create endpoint-specific dialer
	endpointDialer := hwm.createEndpointDialer(newEndpoint)

	// Create new connection to the new endpoint
	newNetConn, err := endpointDialer(ctx)
	if err != nil {
		internal.Logger.Printf(ctx, "hitless: conn[%d] Failed to dial new endpoint %s: %v", connID, newEndpoint, err)
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
	if hwm.config != nil && hwm.config.PostHandoffRelaxedDuration > 0 {
		relaxedTimeout := hwm.config.RelaxedTimeout
		// Set relaxed timeout with deadline - no background goroutine needed
		deadline := time.Now().Add(hwm.config.PostHandoffRelaxedDuration)
		conn.SetRelaxedTimeoutWithDeadline(relaxedTimeout, relaxedTimeout, deadline)

		if hwm.config.LogLevel.InfoOrAbove() {
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
	internal.Logger.Printf(ctx, "hitless: conn[%d] Handoff to %s successful", connID, newEndpoint)

	return false, nil
}

// createEndpointDialer creates a dialer function that connects to a specific endpoint
func (hwm *handoffWorkerManager) createEndpointDialer(endpoint string) func(context.Context) (net.Conn, error) {
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
		return hwm.poolHook.baseDialer(ctx, hwm.poolHook.network, net.JoinHostPort(host, port))
	}
}

// closeConnFromRequest closes the connection and logs the reason
func (hwm *handoffWorkerManager) closeConnFromRequest(ctx context.Context, request HandoffRequest, err error) {
	pooler := request.Pool
	conn := request.Conn
	if pooler != nil {
		pooler.Remove(ctx, conn, err)
		if hwm.config != nil && hwm.config.LogLevel.WarnOrAbove() { // Warning level
			internal.Logger.Printf(ctx,
				"hitless: removed conn[%d] from pool due: %v",
				conn.GetID(), err)
		}
	} else {
		conn.Close()
		if hwm.config != nil && hwm.config.LogLevel.WarnOrAbove() { // Warning level
			internal.Logger.Printf(ctx,
				"hitless: no pool provided for conn[%d], cannot remove due to: %v",
				conn.GetID(), err)
		}
	}
}
