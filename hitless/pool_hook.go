package hitless

import (
	"context"
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

	// Worker manager for background handoff processing
	workerManager *handoffWorkerManager

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
		config:     config,
		// Hitless manager for operation completion tracking
		hitlessManager: hitlessManager,
	}

	// Create worker manager
	ph.workerManager = newHandoffWorkerManager(config, ph)

	return ph
}

// SetPool sets the pool interface for removing connections on handoff failure
func (ph *PoolHook) SetPool(pooler pool.Pooler) {
	ph.pool = pooler
}

// GetCurrentWorkers returns the current number of active workers (for testing)
func (ph *PoolHook) GetCurrentWorkers() int {
	return ph.workerManager.getCurrentWorkers()
}

// IsHandoffPending returns true if the given connection has a pending handoff
func (ph *PoolHook) IsHandoffPending(conn *pool.Conn) bool {
	return ph.workerManager.isHandoffPending(conn)
}

// GetPendingMap returns the pending map for testing purposes
func (ph *PoolHook) GetPendingMap() *sync.Map {
	return ph.workerManager.getPendingMap()
}

// GetMaxWorkers returns the max workers for testing purposes
func (ph *PoolHook) GetMaxWorkers() int {
	return ph.workerManager.getMaxWorkers()
}

// GetHandoffQueue returns the handoff queue for testing purposes
func (ph *PoolHook) GetHandoffQueue() chan HandoffRequest {
	return ph.workerManager.getHandoffQueue()
}

// GetCircuitBreakerStats returns circuit breaker statistics for monitoring
func (ph *PoolHook) GetCircuitBreakerStats() []CircuitBreakerStats {
	return ph.workerManager.getCircuitBreakerStats()
}

// ResetCircuitBreakers resets all circuit breakers (useful for testing)
func (ph *PoolHook) ResetCircuitBreakers() {
	ph.workerManager.resetCircuitBreakers()
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
	if ph.workerManager.isHandoffPending(conn) {
		// Default behavior (pending handoff): pool the connection
		return true, false, nil
	}

	if err := ph.workerManager.queueHandoff(conn); err != nil {
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

// Shutdown gracefully shuts down the processor, waiting for workers to complete
func (ph *PoolHook) Shutdown(ctx context.Context) error {
	return ph.workerManager.shutdownWorkers(ctx)
}
