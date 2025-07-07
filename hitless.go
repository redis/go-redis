package redis

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/hitless"
	"github.com/redis/go-redis/v9/internal/pool"
)

// HitlessUpgradeConfig provides configuration for hitless upgrades
type HitlessUpgradeConfig struct {
	// Enabled controls whether hitless upgrades are active
	Enabled bool

	// TransitionTimeout is the increased timeout for connections during transitions
	// (MIGRATING/FAILING_OVER). This should be longer than normal operation timeouts
	// to account for the time needed to complete the transition.
	// Default: 60 seconds
	TransitionTimeout time.Duration

	// CleanupInterval controls how often expired states are cleaned up
	// Default: 30 seconds
	CleanupInterval time.Duration
}

// DefaultHitlessUpgradeConfig returns the default configuration for hitless upgrades
func DefaultHitlessUpgradeConfig() *HitlessUpgradeConfig {
	return &HitlessUpgradeConfig{
		Enabled:           true,
		TransitionTimeout: 60 * time.Second, // Longer timeout for transitioning connections
		CleanupInterval:   30 * time.Second, // How often to clean up expired states
	}
}

// HitlessUpgradeStatistics provides statistics about ongoing upgrade operations
type HitlessUpgradeStatistics struct {
	ActiveConnections      int       // Total connections in transition
	IsMoving               bool      // Whether pool is currently moving
	MigratingConnections   int       // Connections in MIGRATING state
	FailingOverConnections int       // Connections in FAILING_OVER state
	Timestamp              time.Time // When these statistics were collected
}

// HitlessUpgradeStatus provides detailed status of all ongoing upgrades
type HitlessUpgradeStatus struct {
	ConnectionStates map[interface{}]interface{}
	IsMoving         bool
	NewEndpoint      string
	Timestamp        time.Time
}

// HitlessIntegration provides the interface for hitless upgrade functionality
type HitlessIntegration interface {
	// IsEnabled returns whether hitless upgrades are currently enabled
	IsEnabled() bool

	// EnableHitlessUpgrades enables hitless upgrade functionality
	EnableHitlessUpgrades()

	// DisableHitlessUpgrades disables hitless upgrade functionality
	DisableHitlessUpgrades()

	// GetConnectionTimeout returns the appropriate timeout for a connection
	// If the connection is transitioning, returns the longer TransitionTimeout
	GetConnectionTimeout(conn interface{}, defaultTimeout time.Duration) time.Duration

	// GetConnectionTimeouts returns both read and write timeouts for a connection
	// If the connection is transitioning, returns increased timeouts
	GetConnectionTimeouts(conn interface{}, defaultReadTimeout, defaultWriteTimeout time.Duration) (time.Duration, time.Duration)

	// MarkConnectionAsBlocking marks a connection as having blocking commands
	MarkConnectionAsBlocking(conn interface{}, isBlocking bool)

	// IsConnectionMarkedForClosing checks if a connection should be closed
	IsConnectionMarkedForClosing(conn interface{}) bool

	// ShouldRedirectBlockingConnection checks if a blocking connection should be redirected
	ShouldRedirectBlockingConnection(conn interface{}) (bool, string)

	// GetUpgradeStatistics returns current upgrade statistics
	GetUpgradeStatistics() *HitlessUpgradeStatistics

	// GetUpgradeStatus returns detailed upgrade status
	GetUpgradeStatus() *HitlessUpgradeStatus

	// UpdateConfig updates the hitless upgrade configuration
	UpdateConfig(config *HitlessUpgradeConfig) error

	// GetConfig returns the current configuration
	GetConfig() *HitlessUpgradeConfig

	// Close shuts down the hitless integration
	Close() error
}

// hitlessIntegrationImpl implements the HitlessIntegration interface
type hitlessIntegrationImpl struct {
	integration *hitless.RedisClientIntegration
	mu          sync.RWMutex
}

// newHitlessIntegration creates a new hitless integration instance
func newHitlessIntegration(config *HitlessUpgradeConfig) *hitlessIntegrationImpl {
	if config == nil {
		config = DefaultHitlessUpgradeConfig()
	}

	// Convert to internal config format
	internalConfig := &hitless.HitlessUpgradeConfig{
		Enabled:           config.Enabled,
		TransitionTimeout: config.TransitionTimeout,
		CleanupInterval:   config.CleanupInterval,
	}

	integration := hitless.NewRedisClientIntegration(internalConfig, 3*time.Second, 3*time.Second)

	return &hitlessIntegrationImpl{
		integration: integration,
	}
}

// newHitlessIntegrationWithTimeouts creates a new hitless integration instance with timeout configuration
func newHitlessIntegrationWithTimeouts(config *HitlessUpgradeConfig, defaultReadTimeout, defaultWriteTimeout time.Duration) *hitlessIntegrationImpl {
	// Start with defaults
	defaults := DefaultHitlessUpgradeConfig()

	// If config is nil, use all defaults
	if config == nil {
		config = defaults
	}

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

	// Convert to internal config format with all fields properly set
	internalConfig := &hitless.HitlessUpgradeConfig{
		Enabled:           enabled,
		TransitionTimeout: transitionTimeout,
		CleanupInterval:   cleanupInterval,
	}

	integration := hitless.NewRedisClientIntegration(internalConfig, defaultReadTimeout, defaultWriteTimeout)

	return &hitlessIntegrationImpl{
		integration: integration,
	}
}

// IsEnabled returns whether hitless upgrades are currently enabled
func (h *hitlessIntegrationImpl) IsEnabled() bool {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.integration.IsEnabled()
}

// EnableHitlessUpgrades enables hitless upgrade functionality
func (h *hitlessIntegrationImpl) EnableHitlessUpgrades() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.integration.EnableHitlessUpgrades()
}

// DisableHitlessUpgrades disables hitless upgrade functionality
func (h *hitlessIntegrationImpl) DisableHitlessUpgrades() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.integration.DisableHitlessUpgrades()
}

// GetConnectionTimeout returns the appropriate timeout for a connection
func (h *hitlessIntegrationImpl) GetConnectionTimeout(conn interface{}, defaultTimeout time.Duration) time.Duration {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// Convert interface{} to *pool.Conn
	if poolConn, ok := conn.(*pool.Conn); ok {
		return h.integration.GetConnectionTimeout(poolConn, defaultTimeout)
	}

	// If not a pool connection, return default timeout
	return defaultTimeout
}

// GetConnectionTimeouts returns both read and write timeouts for a connection
func (h *hitlessIntegrationImpl) GetConnectionTimeouts(conn interface{}, defaultReadTimeout, defaultWriteTimeout time.Duration) (time.Duration, time.Duration) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// Convert interface{} to *pool.Conn
	if poolConn, ok := conn.(*pool.Conn); ok {
		return h.integration.GetConnectionTimeouts(poolConn, defaultReadTimeout, defaultWriteTimeout)
	}

	// If not a pool connection, return default timeouts
	return defaultReadTimeout, defaultWriteTimeout
}

// MarkConnectionAsBlocking marks a connection as having blocking commands
func (h *hitlessIntegrationImpl) MarkConnectionAsBlocking(conn interface{}, isBlocking bool) {
	h.mu.Lock()
	defer h.mu.Unlock()

	// Convert interface{} to *pool.Conn
	if poolConn, ok := conn.(*pool.Conn); ok {
		h.integration.MarkConnectionAsBlocking(poolConn, isBlocking)
	}
}

// IsConnectionMarkedForClosing checks if a connection should be closed
func (h *hitlessIntegrationImpl) IsConnectionMarkedForClosing(conn interface{}) bool {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// Convert interface{} to *pool.Conn
	if poolConn, ok := conn.(*pool.Conn); ok {
		return h.integration.IsConnectionMarkedForClosing(poolConn)
	}

	return false
}

// ShouldRedirectBlockingConnection checks if a blocking connection should be redirected
func (h *hitlessIntegrationImpl) ShouldRedirectBlockingConnection(conn interface{}) (bool, string) {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// Convert interface{} to *pool.Conn (can be nil for checking pool state)
	var poolConn *pool.Conn
	if conn != nil {
		if pc, ok := conn.(*pool.Conn); ok {
			poolConn = pc
		}
	}

	return h.integration.ShouldRedirectBlockingConnection(poolConn)
}

// GetUpgradeStatistics returns current upgrade statistics
func (h *hitlessIntegrationImpl) GetUpgradeStatistics() *HitlessUpgradeStatistics {
	h.mu.RLock()
	defer h.mu.RUnlock()

	stats := h.integration.GetUpgradeStatistics()
	if stats == nil {
		return &HitlessUpgradeStatistics{Timestamp: time.Now()}
	}

	return &HitlessUpgradeStatistics{
		ActiveConnections:      stats.ActiveConnections,
		IsMoving:               stats.IsMoving,
		MigratingConnections:   stats.MigratingConnections,
		FailingOverConnections: stats.FailingOverConnections,
		Timestamp:              stats.Timestamp,
	}
}

// GetUpgradeStatus returns detailed upgrade status
func (h *hitlessIntegrationImpl) GetUpgradeStatus() *HitlessUpgradeStatus {
	h.mu.RLock()
	defer h.mu.RUnlock()

	status := h.integration.GetUpgradeStatus()
	if status == nil {
		return &HitlessUpgradeStatus{
			ConnectionStates: make(map[interface{}]interface{}),
			IsMoving:         false,
			NewEndpoint:      "",
			Timestamp:        time.Now(),
		}
	}

	return &HitlessUpgradeStatus{
		ConnectionStates: convertToInterfaceMap(status.ConnectionStates),
		IsMoving:         status.IsMoving,
		NewEndpoint:      status.NewEndpoint,
		Timestamp:        status.Timestamp,
	}
}

// UpdateConfig updates the hitless upgrade configuration
func (h *hitlessIntegrationImpl) UpdateConfig(config *HitlessUpgradeConfig) error {
	if config == nil {
		return fmt.Errorf("config cannot be nil")
	}

	h.mu.Lock()
	defer h.mu.Unlock()

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

	// Convert to internal config format with all fields properly set
	internalConfig := &hitless.HitlessUpgradeConfig{
		Enabled:           enabled,
		TransitionTimeout: transitionTimeout,
		CleanupInterval:   cleanupInterval,
	}

	return h.integration.UpdateConfig(internalConfig)
}

// GetConfig returns the current configuration
func (h *hitlessIntegrationImpl) GetConfig() *HitlessUpgradeConfig {
	h.mu.RLock()
	defer h.mu.RUnlock()

	internalConfig := h.integration.GetConfig()
	if internalConfig == nil {
		return DefaultHitlessUpgradeConfig()
	}

	return &HitlessUpgradeConfig{
		Enabled:           internalConfig.Enabled,
		TransitionTimeout: internalConfig.TransitionTimeout,
		CleanupInterval:   internalConfig.CleanupInterval,
	}
}

// Close shuts down the hitless integration
func (h *hitlessIntegrationImpl) Close() error {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.integration.Close()
}

// getInternalIntegration returns the internal integration for use by Redis clients
func (h *hitlessIntegrationImpl) getInternalIntegration() *hitless.RedisClientIntegration {
	h.mu.RLock()
	defer h.mu.RUnlock()
	return h.integration
}

// ClientTimeoutProvider interface for extracting timeout configuration from client options
type ClientTimeoutProvider interface {
	GetReadTimeout() time.Duration
	GetWriteTimeout() time.Duration
}

// optionsTimeoutProvider implements ClientTimeoutProvider for Options struct
type optionsTimeoutProvider struct {
	readTimeout  time.Duration
	writeTimeout time.Duration
}

func (p *optionsTimeoutProvider) GetReadTimeout() time.Duration {
	return p.readTimeout
}

func (p *optionsTimeoutProvider) GetWriteTimeout() time.Duration {
	return p.writeTimeout
}

// newOptionsTimeoutProvider creates a timeout provider from Options
func newOptionsTimeoutProvider(readTimeout, writeTimeout time.Duration) ClientTimeoutProvider {
	return &optionsTimeoutProvider{
		readTimeout:  readTimeout,
		writeTimeout: writeTimeout,
	}
}

// initializeHitlessIntegration initializes hitless integration for a client
func initializeHitlessIntegration(client interface{}, config *HitlessUpgradeConfig, timeoutProvider ClientTimeoutProvider) (*hitlessIntegrationImpl, error) {
	if config == nil || !config.Enabled {
		return nil, nil
	}

	// Extract timeout configuration from client options
	defaultReadTimeout := timeoutProvider.GetReadTimeout()
	defaultWriteTimeout := timeoutProvider.GetWriteTimeout()

	// Create hitless integration - each client gets its own instance
	integration := newHitlessIntegrationWithTimeouts(config, defaultReadTimeout, defaultWriteTimeout)

	// Push notification handlers are registered directly by the client
	// No separate registration needed in simplified implementation

	internal.Logger.Printf(context.Background(), "hitless: initialized hitless upgrades for client")

	return integration, nil
}

// convertToInterfaceMap converts a typed map to interface{} map for public API
func convertToInterfaceMap(input map[*pool.Conn]*hitless.ConnectionState) map[interface{}]interface{} {
	result := make(map[interface{}]interface{})
	for k, v := range input {
		result[k] = v
	}
	return result
}
