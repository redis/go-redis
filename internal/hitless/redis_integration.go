package hitless

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/push"
)

// UpgradeStatus represents the current status of all upgrade operations
type UpgradeStatus struct {
	ConnectionStates map[*pool.Conn]*ConnectionState
	IsMoving         bool
	NewEndpoint      string
	Timestamp        time.Time
}

// UpgradeStatistics provides statistics about upgrade operations
type UpgradeStatistics struct {
	ActiveConnections      int
	IsMoving               bool
	MigratingConnections   int
	FailingOverConnections int
	Timestamp              time.Time
}

// RedisClientIntegration provides complete hitless upgrade integration for Redis clients
type RedisClientIntegration struct {
	clientIntegrator       *ClientIntegrator
	connectionStateTracker *ConnectionStateTracker
	config                 *HitlessUpgradeConfig

	mu      sync.RWMutex
	enabled bool
}

// NewRedisClientIntegration creates a new Redis client integration for hitless upgrades
// This is used internally by the main hitless.go package
func NewRedisClientIntegration(config *HitlessUpgradeConfig, defaultReadTimeout, defaultWriteTimeout time.Duration) *RedisClientIntegration {
	// Start with defaults
	defaults := DefaultHitlessUpgradeConfig()

	// If config is nil, use all defaults
	if config == nil {
		config = defaults
	} else {
		// Ensure all fields are set with defaults if they are zero values
		if config.TransitionTimeout == 0 {
			config = &HitlessUpgradeConfig{
				Enabled:           config.Enabled,
				TransitionTimeout: defaults.TransitionTimeout,
				CleanupInterval:   config.CleanupInterval,
			}
		}
		if config.CleanupInterval == 0 {
			config = &HitlessUpgradeConfig{
				Enabled:           config.Enabled,
				TransitionTimeout: config.TransitionTimeout,
				CleanupInterval:   defaults.CleanupInterval,
			}
		}
	}

	return &RedisClientIntegration{
		clientIntegrator:       NewClientIntegrator(defaultReadTimeout, defaultWriteTimeout),
		connectionStateTracker: NewConnectionStateTracker(defaultReadTimeout, defaultWriteTimeout),
		config:                 config,
		enabled:                config.Enabled,
	}
}

// EnableHitlessUpgrades enables hitless upgrade functionality
func (rci *RedisClientIntegration) EnableHitlessUpgrades() {
	rci.mu.Lock()
	defer rci.mu.Unlock()
	rci.enabled = true
}

// DisableHitlessUpgrades disables hitless upgrade functionality
func (rci *RedisClientIntegration) DisableHitlessUpgrades() {
	rci.mu.Lock()
	defer rci.mu.Unlock()
	rci.enabled = false
}

// IsEnabled returns whether hitless upgrades are enabled
func (rci *RedisClientIntegration) IsEnabled() bool {
	rci.mu.RLock()
	defer rci.mu.RUnlock()
	return rci.enabled
}

// No client registration needed - each client has its own hitless integration instance

// HandlePushNotification processes push notifications for hitless upgrades
func (rci *RedisClientIntegration) HandlePushNotification(ctx context.Context, handlerCtx push.NotificationHandlerContext, notification []interface{}) error {
	if !rci.IsEnabled() {
		// If disabled, just log and return without processing
		internal.Logger.Printf(ctx, "hitless: received notification but hitless upgrades are disabled")
		return nil
	}

	return rci.clientIntegrator.HandlePushNotification(ctx, handlerCtx, notification)
}

// IsConnectionTransitioning checks if a connection is currently transitioning
func (rci *RedisClientIntegration) IsConnectionTransitioning(conn *pool.Conn) bool {
	if !rci.IsEnabled() {
		return false
	}

	return rci.connectionStateTracker.IsConnectionTransitioning(conn)
}

// GetConnectionState returns the current state of a connection
func (rci *RedisClientIntegration) GetConnectionState(conn *pool.Conn) (*ConnectionState, bool) {
	if !rci.IsEnabled() {
		return nil, false
	}

	return rci.connectionStateTracker.GetConnectionState(conn)
}

// GetUpgradeStatus returns comprehensive status of all ongoing upgrades
func (rci *RedisClientIntegration) GetUpgradeStatus() *UpgradeStatus {
	connStates := rci.clientIntegrator.GetUpgradeHandler().GetActiveTransitions()

	return &UpgradeStatus{
		ConnectionStates: connStates,
		IsMoving:         rci.clientIntegrator.IsMoving(),
		NewEndpoint:      rci.clientIntegrator.GetNewEndpoint(),
		Timestamp:        time.Now(),
	}
}

// GetUpgradeStatistics returns statistics about upgrade operations
func (rci *RedisClientIntegration) GetUpgradeStatistics() *UpgradeStatistics {
	connStates := rci.clientIntegrator.GetUpgradeHandler().GetActiveTransitions()

	stats := &UpgradeStatistics{
		ActiveConnections: len(connStates),
		IsMoving:          rci.clientIntegrator.IsMoving(),
		Timestamp:         time.Now(),
	}

	// Count by type
	stats.MigratingConnections = 0
	stats.FailingOverConnections = 0
	for _, state := range connStates {
		switch state.TransitionType {
		case "MIGRATING":
			stats.MigratingConnections++
		case "FAILING_OVER":
			stats.FailingOverConnections++
		}
	}

	return stats
}

// GetConnectionTimeout returns the appropriate timeout for a connection
// If the connection is transitioning (MIGRATING/FAILING_OVER), returns the longer TransitionTimeout
// Otherwise returns the provided defaultTimeout
func (rci *RedisClientIntegration) GetConnectionTimeout(conn *pool.Conn, defaultTimeout time.Duration) time.Duration {
	if !rci.IsEnabled() {
		return defaultTimeout
	}

	// Check if connection is transitioning
	if rci.connectionStateTracker.IsConnectionTransitioning(conn) {
		// Use longer timeout for transitioning connections
		return rci.config.TransitionTimeout
	}

	return defaultTimeout
}

// GetConnectionTimeouts returns both read and write timeouts for a connection
func (rci *RedisClientIntegration) GetConnectionTimeouts(conn *pool.Conn, defaultReadTimeout, defaultWriteTimeout time.Duration) (time.Duration, time.Duration) {
	if !rci.IsEnabled() {
		return defaultReadTimeout, defaultWriteTimeout
	}

	// Use the upgrade handler to get appropriate timeouts
	upgradeHandler := rci.clientIntegrator.GetUpgradeHandler()
	return upgradeHandler.GetConnectionTimeouts(conn, defaultReadTimeout, defaultWriteTimeout, rci.config.TransitionTimeout)
}

// MarkConnectionAsBlocking marks a connection as having blocking commands
func (rci *RedisClientIntegration) MarkConnectionAsBlocking(conn *pool.Conn, isBlocking bool) {
	if !rci.IsEnabled() {
		return
	}

	// Use the upgrade handler to mark connection as blocking
	upgradeHandler := rci.clientIntegrator.GetUpgradeHandler()
	upgradeHandler.MarkConnectionAsBlocking(conn, isBlocking)
}

// IsConnectionMarkedForClosing checks if a connection should be closed
func (rci *RedisClientIntegration) IsConnectionMarkedForClosing(conn *pool.Conn) bool {
	if !rci.IsEnabled() {
		return false
	}

	// Use the upgrade handler to check if connection is marked for closing
	upgradeHandler := rci.clientIntegrator.GetUpgradeHandler()
	return upgradeHandler.IsConnectionMarkedForClosing(conn)
}

// ShouldRedirectBlockingConnection checks if a blocking connection should be redirected
func (rci *RedisClientIntegration) ShouldRedirectBlockingConnection(conn *pool.Conn) (bool, string) {
	if !rci.IsEnabled() {
		return false, ""
	}

	// Check client integrator's atomic state for pool-level redirection
	if rci.clientIntegrator.IsMoving() {
		return true, rci.clientIntegrator.GetNewEndpoint()
	}

	// Check specific connection state
	upgradeHandler := rci.clientIntegrator.GetUpgradeHandler()
	return upgradeHandler.ShouldRedirectBlockingConnection(conn, rci.clientIntegrator)
}

// CleanupConnection removes tracking for a connection (called when connection is closed)
func (rci *RedisClientIntegration) CleanupConnection(conn *pool.Conn) {
	if rci.IsEnabled() {
		rci.connectionStateTracker.CleanupConnection(conn)
	}
}

// CleanupPool removed - no pool state to clean up

// Close shuts down the Redis client integration
func (rci *RedisClientIntegration) Close() error {
	rci.mu.Lock()
	defer rci.mu.Unlock()

	var firstErr error

	// Close all components
	if err := rci.clientIntegrator.Close(); err != nil && firstErr == nil {
		firstErr = err
	}

	// poolRedirector removed in simplified implementation

	if err := rci.connectionStateTracker.Close(); err != nil && firstErr == nil {
		firstErr = err
	}

	rci.enabled = false

	return firstErr
}

// GetConfig returns the current configuration
func (rci *RedisClientIntegration) GetConfig() *HitlessUpgradeConfig {
	rci.mu.RLock()
	defer rci.mu.RUnlock()

	// Return a copy to prevent modification
	configCopy := *rci.config
	return &configCopy
}

// UpdateConfig updates the configuration
func (rci *RedisClientIntegration) UpdateConfig(config *HitlessUpgradeConfig) error {
	if config == nil {
		return fmt.Errorf("config cannot be nil")
	}

	rci.mu.Lock()
	defer rci.mu.Unlock()

	// Start with defaults for any zero values
	defaults := DefaultHitlessUpgradeConfig()

	// Ensure all fields are set with defaults if they are zero values
	enabled := config.Enabled
	transitionTimeout := config.TransitionTimeout
	cleanupInterval := config.CleanupInterval

	// Apply defaults for zero values
	if transitionTimeout == 0 {
		transitionTimeout = defaults.TransitionTimeout
	}
	if cleanupInterval == 0 {
		cleanupInterval = defaults.CleanupInterval
	}

	// Create properly configured config
	finalConfig := &HitlessUpgradeConfig{
		Enabled:           enabled,
		TransitionTimeout: transitionTimeout,
		CleanupInterval:   cleanupInterval,
	}

	rci.config = finalConfig
	rci.enabled = finalConfig.Enabled

	return nil
}
