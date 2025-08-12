package hitless

import (
	"net"
	"runtime"
	"time"

	"github.com/redis/go-redis/v9/internal/util"
)

// MaintNotificationsMode represents the maintenance notifications mode
type MaintNotificationsMode string

// Constants for maintenance push notifications modes
const (
	MaintNotificationsDisabled MaintNotificationsMode = "disabled" // Client doesn't send CLIENT MAINT_NOTIFICATIONS ON command
	MaintNotificationsEnabled  MaintNotificationsMode = "enabled"  // Client forcefully sends command, interrupts connection on error
	MaintNotificationsAuto     MaintNotificationsMode = "auto"     // Client tries to send command, disables feature on error
)

// IsValid returns true if the maintenance notifications mode is valid
func (m MaintNotificationsMode) IsValid() bool {
	switch m {
	case MaintNotificationsDisabled, MaintNotificationsEnabled, MaintNotificationsAuto:
		return true
	default:
		return false
	}
}

// String returns the string representation of the mode
func (m MaintNotificationsMode) String() string {
	return string(m)
}

// EndpointType represents the type of endpoint to request in MOVING notifications
type EndpointType string

// Constants for endpoint types
const (
	EndpointTypeAuto         EndpointType = "auto"          // Auto-detect based on connection
	EndpointTypeInternalIP   EndpointType = "internal-ip"   // Internal IP address
	EndpointTypeInternalFQDN EndpointType = "internal-fqdn" // Internal FQDN
	EndpointTypeExternalIP   EndpointType = "external-ip"   // External IP address
	EndpointTypeExternalFQDN EndpointType = "external-fqdn" // External FQDN
	EndpointTypeNone         EndpointType = "none"          // No endpoint (reconnect with current config)
)

// IsValid returns true if the endpoint type is valid
func (e EndpointType) IsValid() bool {
	switch e {
	case EndpointTypeAuto, EndpointTypeInternalIP, EndpointTypeInternalFQDN,
		EndpointTypeExternalIP, EndpointTypeExternalFQDN, EndpointTypeNone:
		return true
	default:
		return false
	}
}

// String returns the string representation of the endpoint type
func (e EndpointType) String() string {
	return string(e)
}

// Config provides configuration options for hitless upgrades.
type Config struct {
	// Enabled controls how client maintenance notifications are handled.
	// Valid values: MaintNotificationsDisabled, MaintNotificationsEnabled, MaintNotificationsAuto
	// Default: MaintNotificationsAuto
	Enabled MaintNotificationsMode

	// EndpointType specifies the type of endpoint to request in MOVING notifications.
	// Valid values: EndpointTypeAuto, EndpointTypeInternalIP, EndpointTypeInternalFQDN,
	//               EndpointTypeExternalIP, EndpointTypeExternalFQDN, EndpointTypeNone
	// Default: EndpointTypeAuto
	EndpointType EndpointType

	// RelaxedTimeout is the concrete timeout value to use during
	// MIGRATING/FAILING_OVER states to accommodate increased latency.
	// This applies to both read and write timeouts.
	// Default: 30 seconds
	RelaxedTimeout time.Duration

	// HandoffTimeout is the maximum time to wait for connection handoff to complete.
	// If handoff takes longer than this, the old connection will be forcibly closed.
	// Default: 15 seconds (matches server-side eviction timeout)
	HandoffTimeout time.Duration

	// MinWorkers is the minimum number of worker goroutines for processing handoff requests.
	// The processor starts with this number of workers and scales down to this level when idle.
	// If zero, defaults to max(1, PoolSize/25) to be proportional to connection pool size.
	//
	// Default: max(1, PoolSize/25)
	MinWorkers int

	// MaxWorkers is the maximum number of worker goroutines for processing handoff requests.
	// The processor will scale up to this number when under load.
	// If zero, defaults to max(MinWorkers*4, PoolSize/5) to handle bursts effectively.
	//
	// Default: max(MinWorkers*4, PoolSize/5)
	MaxWorkers int

	// HandoffQueueSize is the size of the buffered channel used to queue handoff requests.
	// If the queue is full, new handoff requests will be rejected.
	//
	// Default: 10x max workers, but never more than pool size
	HandoffQueueSize int

	// PostHandoffRelaxedDuration is how long to keep relaxed timeouts on the new connection
	// after a handoff completes. This provides additional resilience during cluster transitions.
	// Default: 2 * RelaxedTimeout
	PostHandoffRelaxedDuration time.Duration

	// ScaleDownDelay is the delay before checking if workers should be scaled down.
	// This prevents expensive checks on every handoff completion and avoids rapid scaling cycles.
	// Default: 2 seconds
	ScaleDownDelay time.Duration

	// LogLevel controls the verbosity of hitless upgrade logging.
	// 0 = errors only, 1 = warnings, 2 = info, 3 = debug
	// Default: 1 (warnings)
	LogLevel int

	// Connection Handoff Configuration
	// MaxHandoffRetries is the maximum number of times to retry a failed handoff.
	// After this many retries, the connection will be removed from the pool.
	// Default: 3
	MaxHandoffRetries int

	// HandoffQueueTimeout is the maximum time to wait when queuing a handoff request.
	// If the queue is full and this timeout expires, the handoff will be abandoned.
	// Default: 5 seconds
	HandoffQueueTimeout time.Duration

	// HandoffRetryDelay is the delay between handoff retry attempts.
	// This prevents rapid retry loops and gives the system time to recover.
	// Default: 1 second
	HandoffRetryDelay time.Duration

	// Worker Scaling Configuration
	// WorkerScaleDownDelay is how long to wait before scaling down workers after load decreases.
	// This prevents rapid scaling cycles and maintains responsiveness for burst loads.
	// Default: 30 seconds
	WorkerScaleDownDelay time.Duration

	// WorkerScaleUpDelay is how long to wait before scaling up workers when queue depth increases.
	// This prevents over-scaling for temporary spikes.
	// Default: 5 seconds
	WorkerScaleUpDelay time.Duration

	// WorkerIdleTimeout is how long a worker can be idle before being considered for scale-down.
	// Workers idle longer than this may be terminated during scale-down operations.
	// Default: 60 seconds
	WorkerIdleTimeout time.Duration

	// Connection Validation Configuration
	// ConnectionValidationTimeout is the timeout for validating new connections during handoff.
	// This includes connection establishment and basic health checks.
	// Default: 2 seconds
	ConnectionValidationTimeout time.Duration

	// ConnectionHealthCheckInterval is how often to perform health checks on pooled connections.
	// This helps detect and remove stale connections proactively.
	// Default: 10 seconds
	ConnectionHealthCheckInterval time.Duration

	// Operation Tracking Configuration
	// OperationCleanupInterval is how often to clean up expired MOVING operations.
	// This prevents memory leaks from operations that never complete.
	// Default: 5 minutes
	OperationCleanupInterval time.Duration

	// MaxActiveOperations is the maximum number of concurrent MOVING operations to track.
	// This prevents unbounded memory growth under extreme load.
	// Default: 10000
	MaxActiveOperations int

	// Notification Processing Configuration
	// NotificationBufferSize is the size of the buffer for incoming push notifications.
	// Larger buffers can handle burst notification loads better.
	// Default: 1000
	NotificationBufferSize int

	// NotificationTimeout is the timeout for processing individual push notifications.
	// This prevents hanging on malformed or problematic notifications.
	// Default: 1 second
	NotificationTimeout time.Duration
}

func (c *Config) IsEnabled() bool {
	return c != nil && c.Enabled != MaintNotificationsDisabled
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Enabled:                    MaintNotificationsAuto, // Enable by default for Redis Cloud
		EndpointType:               EndpointTypeAuto,       // Auto-detect based on connection
		RelaxedTimeout:             30 * time.Second,
		HandoffTimeout:             15 * time.Second,
		MinWorkers:                 0, // Auto-calculated based on pool size
		MaxWorkers:                 0, // Auto-calculated based on pool size
		HandoffQueueSize:           0, // Auto-calculated based on max workers
		PostHandoffRelaxedDuration: 0, // Auto-calculated based on relaxed timeout
		ScaleDownDelay:             2 * time.Second,
		LogLevel:                   1,

		// Connection Handoff Configuration
		MaxHandoffRetries:         3,
		HandoffQueueTimeout:       5 * time.Second,
		HandoffRetryDelay:         1 * time.Second,

		// Worker Scaling Configuration
		WorkerScaleDownDelay:      30 * time.Second,
		WorkerScaleUpDelay:        5 * time.Second,
		WorkerIdleTimeout:         60 * time.Second,

		// Connection Validation Configuration
		ConnectionValidationTimeout:    2 * time.Second,
		ConnectionHealthCheckInterval:  10 * time.Second,

		// Operation Tracking Configuration
		OperationCleanupInterval: 5 * time.Minute,
		MaxActiveOperations:      10000,

		// Notification Processing Configuration
		NotificationBufferSize:   1000,
		NotificationTimeout:      1 * time.Second,
	}
}

// Validate checks if the configuration is valid.
func (c *Config) Validate() error {
	if c.RelaxedTimeout <= 0 {
		return ErrInvalidRelaxedTimeout
	}
	if c.HandoffTimeout <= 0 {
		return ErrInvalidHandoffTimeout
	}
	// Validate worker configuration
	// Allow 0 for auto-calculation, but negative values are invalid
	if c.MinWorkers < 0 {
		return ErrInvalidHandoffWorkers
	}
	if c.MinWorkers > 0 && c.MaxWorkers > 0 && c.MaxWorkers < c.MinWorkers {
		return ErrInvalidWorkerRange
	}
	// HandoffQueueSize validation - allow 0 for auto-calculation
	if c.HandoffQueueSize < 0 {
		return ErrInvalidHandoffQueueSize
	}
	if c.PostHandoffRelaxedDuration < 0 {
		return ErrInvalidPostHandoffRelaxedDuration
	}
	if c.LogLevel < 0 || c.LogLevel > 3 {
		return ErrInvalidLogLevel
	}

	// Validate Enabled (maintenance notifications mode)
	if !c.Enabled.IsValid() {
		return ErrInvalidMaintNotifications
	}

	// Validate EndpointType
	if !c.EndpointType.IsValid() {
		return ErrInvalidEndpointType
	}

	// Validate new configuration fields
	if c.MaxHandoffRetries < 1 || c.MaxHandoffRetries > 10 {
		return ErrInvalidHandoffRetries
	}

	if c.HandoffQueueTimeout <= 0 || c.HandoffQueueTimeout > time.Minute {
		return ErrInvalidHandoffQueueTimeout
	}

	if c.HandoffRetryDelay < 0 || c.HandoffRetryDelay > 10*time.Second {
		return ErrInvalidHandoffRetryDelay
	}

	if c.WorkerScaleDownDelay < 0 || c.WorkerScaleDownDelay > 5*time.Minute {
		return ErrInvalidWorkerScaleDownDelay
	}

	if c.WorkerScaleUpDelay < 0 || c.WorkerScaleUpDelay > time.Minute {
		return ErrInvalidWorkerScaleUpDelay
	}

	if c.WorkerIdleTimeout < 0 || c.WorkerIdleTimeout > 10*time.Minute {
		return ErrInvalidWorkerIdleTimeout
	}

	if c.ConnectionValidationTimeout <= 0 || c.ConnectionValidationTimeout > 30*time.Second {
		return ErrInvalidConnectionValidationTimeout
	}

	if c.ConnectionHealthCheckInterval < 0 || c.ConnectionHealthCheckInterval > time.Hour {
		return ErrInvalidConnectionHealthCheckInterval
	}

	if c.OperationCleanupInterval <= 0 || c.OperationCleanupInterval > time.Hour {
		return ErrInvalidOperationCleanupInterval
	}

	if c.MaxActiveOperations < 100 || c.MaxActiveOperations > 100000 {
		return ErrInvalidMaxActiveOperations
	}

	if c.NotificationBufferSize < 10 || c.NotificationBufferSize > 10000 {
		return ErrInvalidNotificationBufferSize
	}

	if c.NotificationTimeout <= 0 || c.NotificationTimeout > 30*time.Second {
		return ErrInvalidNotificationTimeout
	}

	return nil
}

// ProductionConfig returns a Config optimized for production environments.
// This configuration is tuned for high-load scenarios with enhanced reliability.
func ProductionConfig() *Config {
	return &Config{
		// Basic settings
		Enabled:                    MaintNotificationsEnabled,
		EndpointType:               EndpointTypeAuto,
		RelaxedTimeout:             45 * time.Second,
		HandoffTimeout:             20 * time.Second,
		PostHandoffRelaxedDuration: 15 * time.Second,

		// Enhanced performance settings for production
		MaxHandoffRetries:         5,
		HandoffQueueTimeout:       10 * time.Second,
		HandoffRetryDelay:         2 * time.Second,

		// Worker scaling for high load
		MinWorkers:                8,
		MaxWorkers:                64,
		HandoffQueueSize:          2000,
		WorkerScaleDownDelay:      60 * time.Second,
		WorkerScaleUpDelay:        3 * time.Second,
		WorkerIdleTimeout:         120 * time.Second,

		// Connection management
		ConnectionValidationTimeout:    3 * time.Second,
		ConnectionHealthCheckInterval:  30 * time.Second,

		// Operation management for high throughput
		OperationCleanupInterval: 2 * time.Minute,
		MaxActiveOperations:      50000,

		// Monitoring and observability
		LogLevel:                  1, // Warnings and errors
		NotificationBufferSize:    5000,
		NotificationTimeout:       2 * time.Second,

		// Existing fields
		ScaleDownDelay: 2 * time.Second,
	}
}

// DevelopmentConfig returns a Config optimized for development environments.
// This configuration prioritizes debugging and has conservative resource usage.
func DevelopmentConfig() *Config {
	return &Config{
		// Basic settings
		Enabled:                    MaintNotificationsAuto,
		EndpointType:               EndpointTypeAuto,
		RelaxedTimeout:             10 * time.Second,
		HandoffTimeout:             5 * time.Second,
		PostHandoffRelaxedDuration: 3 * time.Second,

		// Conservative performance settings
		MaxHandoffRetries:         3,
		HandoffQueueTimeout:       5 * time.Second,
		HandoffRetryDelay:         1 * time.Second,

		// Minimal worker scaling
		MinWorkers:                2,
		MaxWorkers:                8,
		HandoffQueueSize:          100,
		WorkerScaleDownDelay:      30 * time.Second,
		WorkerScaleUpDelay:        5 * time.Second,
		WorkerIdleTimeout:         60 * time.Second,

		// Connection management
		ConnectionValidationTimeout:    2 * time.Second,
		ConnectionHealthCheckInterval:  10 * time.Second,

		// Frequent cleanup for testing
		OperationCleanupInterval: 30 * time.Second,
		MaxActiveOperations:      1000,

		// Verbose logging for debugging
		LogLevel:                  3, // Debug level
		NotificationBufferSize:    100,
		NotificationTimeout:       1 * time.Second,

		// Existing fields
		ScaleDownDelay: 2 * time.Second,
	}
}

// ApplyDefaults applies default values to any zero-value fields in the configuration.
// This ensures that partially configured structs get sensible defaults for missing fields.
func (c *Config) ApplyDefaults() *Config {
	return c.ApplyDefaultsWithPoolSize(0)
}

// ApplyDefaultsWithPoolSize applies default values to any zero-value fields in the configuration,
// using the provided pool size to calculate worker defaults.
// This ensures that partially configured structs get sensible defaults for missing fields.
func (c *Config) ApplyDefaultsWithPoolSize(poolSize int) *Config {
	if c == nil {
		return DefaultConfig().ApplyDefaultsWithPoolSize(poolSize)
	}

	defaults := DefaultConfig()
	result := &Config{}

	// Apply defaults for enum fields (empty/zero means not set)
	if c.Enabled == "" {
		result.Enabled = defaults.Enabled
	} else {
		result.Enabled = c.Enabled
	}

	if c.EndpointType == "" {
		result.EndpointType = defaults.EndpointType
	} else {
		result.EndpointType = c.EndpointType
	}

	// Apply defaults for duration fields (zero means not set)
	if c.RelaxedTimeout <= 0 {
		result.RelaxedTimeout = defaults.RelaxedTimeout
	} else {
		result.RelaxedTimeout = c.RelaxedTimeout
	}

	if c.HandoffTimeout <= 0 {
		result.HandoffTimeout = defaults.HandoffTimeout
	} else {
		result.HandoffTimeout = c.HandoffTimeout
	}

	// Apply defaults for integer fields (zero means not set)
	if c.HandoffQueueSize <= 0 {
		result.HandoffQueueSize = defaults.HandoffQueueSize
	} else {
		result.HandoffQueueSize = c.HandoffQueueSize
	}

	// Copy worker configuration
	result.MinWorkers = c.MinWorkers
	result.MaxWorkers = c.MaxWorkers

	// Apply worker defaults based on pool size
	result.applyWorkerDefaults(poolSize)

	// Apply queue size defaults based on max workers, capped by pool size
	if c.HandoffQueueSize <= 0 {
		// Queue size: 10x max workers, but never more than pool size
		workerBasedSize := result.MaxWorkers * 10
		result.HandoffQueueSize = util.Min(workerBasedSize, poolSize)
	} else {
		result.HandoffQueueSize = c.HandoffQueueSize
	}

	if c.PostHandoffRelaxedDuration <= 0 {
		result.PostHandoffRelaxedDuration = result.RelaxedTimeout * 2
	} else {
		result.PostHandoffRelaxedDuration = c.PostHandoffRelaxedDuration
	}

	if c.ScaleDownDelay <= 0 {
		result.ScaleDownDelay = defaults.ScaleDownDelay
	} else {
		result.ScaleDownDelay = c.ScaleDownDelay
	}

	// LogLevel: 0 is a valid value (errors only), so we need to check if it was explicitly set
	// We'll use the provided value as-is, since 0 is valid
	result.LogLevel = c.LogLevel

	// Apply defaults for new configuration fields
	if c.MaxHandoffRetries <= 0 {
		result.MaxHandoffRetries = defaults.MaxHandoffRetries
	} else {
		result.MaxHandoffRetries = c.MaxHandoffRetries
	}

	if c.HandoffQueueTimeout <= 0 {
		result.HandoffQueueTimeout = defaults.HandoffQueueTimeout
	} else {
		result.HandoffQueueTimeout = c.HandoffQueueTimeout
	}

	if c.HandoffRetryDelay < 0 {
		result.HandoffRetryDelay = defaults.HandoffRetryDelay
	} else {
		result.HandoffRetryDelay = c.HandoffRetryDelay
	}

	if c.WorkerScaleDownDelay < 0 {
		result.WorkerScaleDownDelay = defaults.WorkerScaleDownDelay
	} else {
		result.WorkerScaleDownDelay = c.WorkerScaleDownDelay
	}

	if c.WorkerScaleUpDelay < 0 {
		result.WorkerScaleUpDelay = defaults.WorkerScaleUpDelay
	} else {
		result.WorkerScaleUpDelay = c.WorkerScaleUpDelay
	}

	if c.WorkerIdleTimeout < 0 {
		result.WorkerIdleTimeout = defaults.WorkerIdleTimeout
	} else {
		result.WorkerIdleTimeout = c.WorkerIdleTimeout
	}

	if c.ConnectionValidationTimeout <= 0 {
		result.ConnectionValidationTimeout = defaults.ConnectionValidationTimeout
	} else {
		result.ConnectionValidationTimeout = c.ConnectionValidationTimeout
	}

	if c.ConnectionHealthCheckInterval < 0 {
		result.ConnectionHealthCheckInterval = defaults.ConnectionHealthCheckInterval
	} else {
		result.ConnectionHealthCheckInterval = c.ConnectionHealthCheckInterval
	}

	if c.OperationCleanupInterval <= 0 {
		result.OperationCleanupInterval = defaults.OperationCleanupInterval
	} else {
		result.OperationCleanupInterval = c.OperationCleanupInterval
	}

	if c.MaxActiveOperations <= 0 {
		result.MaxActiveOperations = defaults.MaxActiveOperations
	} else {
		result.MaxActiveOperations = c.MaxActiveOperations
	}

	if c.NotificationBufferSize <= 0 {
		result.NotificationBufferSize = defaults.NotificationBufferSize
	} else {
		result.NotificationBufferSize = c.NotificationBufferSize
	}

	if c.NotificationTimeout <= 0 {
		result.NotificationTimeout = defaults.NotificationTimeout
	} else {
		result.NotificationTimeout = c.NotificationTimeout
	}

	return result
}

// Clone creates a deep copy of the configuration.
func (c *Config) Clone() *Config {
	if c == nil {
		return DefaultConfig()
	}

	return &Config{
		Enabled:                    c.Enabled,
		EndpointType:               c.EndpointType,
		RelaxedTimeout:             c.RelaxedTimeout,
		HandoffTimeout:             c.HandoffTimeout,
		MinWorkers:                 c.MinWorkers,
		MaxWorkers:                 c.MaxWorkers,
		HandoffQueueSize:           c.HandoffQueueSize,
		PostHandoffRelaxedDuration: c.PostHandoffRelaxedDuration,
		ScaleDownDelay:             c.ScaleDownDelay,
		LogLevel:                   c.LogLevel,

		// New configuration fields
		MaxHandoffRetries:                c.MaxHandoffRetries,
		HandoffQueueTimeout:              c.HandoffQueueTimeout,
		HandoffRetryDelay:                c.HandoffRetryDelay,
		WorkerScaleDownDelay:             c.WorkerScaleDownDelay,
		WorkerScaleUpDelay:               c.WorkerScaleUpDelay,
		WorkerIdleTimeout:                c.WorkerIdleTimeout,
		ConnectionValidationTimeout:      c.ConnectionValidationTimeout,
		ConnectionHealthCheckInterval:    c.ConnectionHealthCheckInterval,
		OperationCleanupInterval:         c.OperationCleanupInterval,
		MaxActiveOperations:              c.MaxActiveOperations,
		NotificationBufferSize:           c.NotificationBufferSize,
		NotificationTimeout:              c.NotificationTimeout,
	}
}

// applyWorkerDefaults calculates and applies worker defaults based on pool size
func (c *Config) applyWorkerDefaults(poolSize int) {
	// Calculate defaults based on pool size
	if poolSize <= 0 {
		poolSize = 10 * runtime.GOMAXPROCS(0)
	}

	// MinWorkers: max(1, poolSize/25) - conservative baseline
	if c.MinWorkers == 0 {
		c.MinWorkers = util.Max(1, poolSize/25)
	}

	// MaxWorkers: max(MinWorkers*4, poolSize/5) - handle bursts effectively
	if c.MaxWorkers == 0 {
		c.MaxWorkers = util.Max(c.MinWorkers*4, poolSize/5)
	}

	// Ensure MaxWorkers >= MinWorkers
	if c.MaxWorkers < c.MinWorkers {
		c.MaxWorkers = c.MinWorkers
	}
}

// DetectEndpointType automatically detects the appropriate endpoint type
// based on the connection address and TLS configuration.
func DetectEndpointType(addr string, tlsEnabled bool) EndpointType {
	// Parse the address to determine if it's an IP or hostname
	isPrivate := isPrivateIP(addr)

	var endpointType EndpointType

	if tlsEnabled {
		// TLS requires FQDN for certificate validation
		if isPrivate {
			endpointType = EndpointTypeInternalFQDN
		} else {
			endpointType = EndpointTypeExternalFQDN
		}
	} else {
		// No TLS, can use IP addresses
		if isPrivate {
			endpointType = EndpointTypeInternalIP
		} else {
			endpointType = EndpointTypeExternalIP
		}
	}

	return endpointType
}

// isPrivateIP checks if the given address is in a private IP range.
func isPrivateIP(addr string) bool {
	// Extract host from "host:port" format
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr // Assume no port
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return false // Not an IP address (likely hostname)
	}

	// Check for private/loopback ranges
	return ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast()
}
