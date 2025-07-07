package hitless

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/push"
)

// ClientIntegrator provides integration between hitless upgrade handlers and Redis clients
type ClientIntegrator struct {
	upgradeHandler *UpgradeHandler
	mu             sync.RWMutex

	// Simple atomic state for pool redirection
	isMoving    int32  // atomic: 0 = not moving, 1 = moving
	newEndpoint string // only written during MOVING, read-only after
}

// NewClientIntegrator creates a new client integrator with client timeout configuration
func NewClientIntegrator(defaultReadTimeout, defaultWriteTimeout time.Duration) *ClientIntegrator {
	return &ClientIntegrator{
		upgradeHandler: NewUpgradeHandler(defaultReadTimeout, defaultWriteTimeout),
	}
}

// GetUpgradeHandler returns the upgrade handler for direct access
func (ci *ClientIntegrator) GetUpgradeHandler() *UpgradeHandler {
	return ci.upgradeHandler
}

// HandlePushNotification is the main entry point for processing upgrade notifications
func (ci *ClientIntegrator) HandlePushNotification(ctx context.Context, handlerCtx push.NotificationHandlerContext, notification []interface{}) error {
	// Handle MOVING notifications for pool redirection
	if len(notification) > 0 {
		if notificationType, ok := notification[0].(string); ok && notificationType == "MOVING" {
			if len(notification) >= 3 {
				if newEndpoint, ok := notification[2].(string); ok {
					// Simple atomic state update - no locks needed
					ci.newEndpoint = newEndpoint
					atomic.StoreInt32(&ci.isMoving, 1)
				}
			}
		}
	}

	return ci.upgradeHandler.HandlePushNotification(ctx, handlerCtx, notification)
}

// Close shuts down the client integrator
func (ci *ClientIntegrator) Close() error {
	ci.mu.Lock()
	defer ci.mu.Unlock()

	// Reset atomic state
	atomic.StoreInt32(&ci.isMoving, 0)
	ci.newEndpoint = ""

	return nil
}

// IsMoving returns true if the pool is currently moving to a new endpoint
// Uses atomic read - no locks needed
func (ci *ClientIntegrator) IsMoving() bool {
	return atomic.LoadInt32(&ci.isMoving) == 1
}

// GetNewEndpoint returns the new endpoint if moving, empty string otherwise
// Safe to read without locks since it's only written during MOVING
func (ci *ClientIntegrator) GetNewEndpoint() string {
	if ci.IsMoving() {
		return ci.newEndpoint
	}
	return ""
}

// PushNotificationHandlerInterface defines the interface for push notification handlers
// This implements the interface expected by the push notification system
type PushNotificationHandlerInterface interface {
	HandlePushNotification(ctx context.Context, handlerCtx push.NotificationHandlerContext, notification []interface{}) error
}

// Ensure ClientIntegrator implements the interface
var _ PushNotificationHandlerInterface = (*ClientIntegrator)(nil)

// PoolRedirector provides pool redirection functionality for hitless upgrades
type PoolRedirector struct {
	poolManager *PoolEndpointManager
	mu          sync.RWMutex
}

// NewPoolRedirector creates a new pool redirector
func NewPoolRedirector() *PoolRedirector {
	return &PoolRedirector{
		poolManager: NewPoolEndpointManager(),
	}
}

// RedirectPool redirects a connection pool to a new endpoint
func (pr *PoolRedirector) RedirectPool(ctx context.Context, pooler pool.Pooler, newEndpoint string, timeout time.Duration) error {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	return pr.poolManager.RedirectPool(ctx, pooler, newEndpoint, timeout)
}

// IsPoolRedirected checks if a pool is currently redirected
func (pr *PoolRedirector) IsPoolRedirected(pooler pool.Pooler) bool {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	return pr.poolManager.IsPoolRedirected(pooler)
}

// GetRedirection returns redirection information for a pool
func (pr *PoolRedirector) GetRedirection(pooler pool.Pooler) (*EndpointRedirection, bool) {
	pr.mu.RLock()
	defer pr.mu.RUnlock()

	return pr.poolManager.GetRedirection(pooler)
}

// Close shuts down the pool redirector
func (pr *PoolRedirector) Close() error {
	pr.mu.Lock()
	defer pr.mu.Unlock()

	// Clean up all redirections
	ctx := context.Background()
	pr.poolManager.CleanupExpiredRedirections(ctx)

	return nil
}

// ConnectionStateTracker tracks connection states during upgrades
type ConnectionStateTracker struct {
	upgradeHandler *UpgradeHandler
	mu             sync.RWMutex
}

// NewConnectionStateTracker creates a new connection state tracker with timeout configuration
func NewConnectionStateTracker(defaultReadTimeout, defaultWriteTimeout time.Duration) *ConnectionStateTracker {
	return &ConnectionStateTracker{
		upgradeHandler: NewUpgradeHandler(defaultReadTimeout, defaultWriteTimeout),
	}
}

// IsConnectionTransitioning checks if a connection is currently transitioning
func (cst *ConnectionStateTracker) IsConnectionTransitioning(conn *pool.Conn) bool {
	cst.mu.RLock()
	defer cst.mu.RUnlock()

	return cst.upgradeHandler.IsConnectionTransitioning(conn)
}

// GetConnectionState returns the current state of a connection
func (cst *ConnectionStateTracker) GetConnectionState(conn *pool.Conn) (*ConnectionState, bool) {
	cst.mu.RLock()
	defer cst.mu.RUnlock()

	return cst.upgradeHandler.GetConnectionState(conn)
}

// CleanupConnection removes tracking for a connection
func (cst *ConnectionStateTracker) CleanupConnection(conn *pool.Conn) {
	cst.mu.Lock()
	defer cst.mu.Unlock()

	cst.upgradeHandler.CleanupConnection(conn)
}

// Close shuts down the connection state tracker
func (cst *ConnectionStateTracker) Close() error {
	cst.mu.Lock()
	defer cst.mu.Unlock()

	// Clean up all expired states
	cst.upgradeHandler.CleanupExpiredStates()

	return nil
}

// HitlessUpgradeConfig provides configuration for hitless upgrades
type HitlessUpgradeConfig struct {
	Enabled           bool
	TransitionTimeout time.Duration
	CleanupInterval   time.Duration
}

// DefaultHitlessUpgradeConfig returns default configuration for hitless upgrades
func DefaultHitlessUpgradeConfig() *HitlessUpgradeConfig {
	return &HitlessUpgradeConfig{
		Enabled:           true,
		TransitionTimeout: 60 * time.Second, // Longer timeout for transitioning connections
		CleanupInterval:   30 * time.Second, // How often to clean up expired states
	}
}
