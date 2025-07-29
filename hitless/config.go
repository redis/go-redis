package hitless

import (
	"time"
)

// Config provides configuration options for hitless upgrades.
type Config struct {
	// Enabled controls whether hitless upgrades are enabled.
	// Requires RESP3 protocol for push notifications.
	// Default: false
	Enabled bool

	// EndpointType specifies the type of endpoint to request in MOVING notifications.
	// Valid values: "internal-ip", "internal-fqdn", "external-ip", "external-fqdn", "none"
	// If empty, the client will auto-detect based on connection settings.
	// Default: "" (auto-detect)
	EndpointType string

	// RelaxedTimeout is the concrete timeout value to use during
	// MIGRATING/FAILING_OVER states to accommodate increased latency.
	// This applies to both read and write timeouts.
	// Default: 30 seconds
	RelaxedTimeout time.Duration

	// RelaxedTimeoutDuration controls how long the relaxed timeout remains active
	// for ALL connections in the pool during MOVING operations.
	// If zero, defaults to the time specified in the MOVING command.
	// Default: 30 seconds
	RelaxedTimeoutDuration time.Duration

	// HandoffTimeout is the maximum time to wait for connection handoff to complete.
	// If handoff takes longer than this, the old connection will be forcibly closed.
	// Default: 15 seconds (matches server-side eviction timeout)
	HandoffTimeout time.Duration

	// MinWorkers is the minimum number of worker goroutines for processing handoff requests.
	// The processor starts with this number of workers and scales down to this level when idle.
	// If zero, defaults to max(1, PoolSize/20) to be proportional to connection pool size.
	// Default: 0 (auto-calculated)
	MinWorkers int

	// MaxWorkers is the maximum number of worker goroutines for processing handoff requests.
	// The processor will scale up to this number when under load.
	// If zero, defaults to max(MinWorkers*4, PoolSize/5) to handle bursts effectively.
	// Default: 0 (auto-calculated)
	MaxWorkers int

	// HandoffQueueSize is the size of the buffered channel used to queue handoff requests.
	// If the queue is full, new handoff requests will be rejected.
	// Default: 100
	HandoffQueueSize int

	// PostHandoffRelaxedDuration is how long to keep relaxed timeouts on the new connection
	// after a handoff completes. This provides additional resilience during cluster transitions.
	// Default: 10 seconds
	PostHandoffRelaxedDuration time.Duration

	// ScaleDownDelay is the delay before checking if workers should be scaled down.
	// This prevents expensive checks on every handoff completion and avoids rapid scaling cycles.
	// Default: 2 seconds
	ScaleDownDelay time.Duration

	// LogLevel controls the verbosity of hitless upgrade logging.
	// 0 = errors only, 1 = warnings, 2 = info, 3 = debug
	// Default: 1 (warnings)
	LogLevel int
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() *Config {
	return &Config{
		Enabled:                false,
		EndpointType:           "", // Auto-detect
		RelaxedTimeout:         30 * time.Second,
		RelaxedTimeoutDuration: 30 * time.Second,
		HandoffTimeout:            15 * time.Second,
		MinWorkers:               0, // Auto-calculated based on pool size
		MaxWorkers:               0, // Auto-calculated based on pool size
		HandoffQueueSize:          0, // Auto-calculated based on max workers
		PostHandoffRelaxedDuration: 10 * time.Second,
		ScaleDownDelay:            2 * time.Second,
		LogLevel:                  1,
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
	// Check raw values before defaults are applied
	if c.MinWorkers <= 0 {
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
	
	validEndpointTypes := map[string]bool{
		"":              true, // Auto-detect
		"internal-ip":   true,
		"internal-fqdn": true,
		"external-ip":   true,
		"external-fqdn": true,
		"none":          true,
	}
	
	if !validEndpointTypes[c.EndpointType] {
		return ErrInvalidEndpointType
	}
	
	return nil
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
	result := &Config{
		Enabled:      c.Enabled, // boolean, no default needed
		EndpointType: c.EndpointType, // string, empty is valid (auto-detect)
	}

	// Apply defaults for duration fields (zero means not set)
	if c.RelaxedTimeout <= 0 {
		result.RelaxedTimeout = defaults.RelaxedTimeout
	} else {
		result.RelaxedTimeout = c.RelaxedTimeout
	}

	if c.RelaxedTimeoutDuration <= 0 {
		result.RelaxedTimeoutDuration = defaults.RelaxedTimeoutDuration
	} else {
		result.RelaxedTimeoutDuration = c.RelaxedTimeoutDuration
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
		result.HandoffQueueSize = min(workerBasedSize, poolSize)
	} else {
		result.HandoffQueueSize = c.HandoffQueueSize
	}

	if c.PostHandoffRelaxedDuration <= 0 {
		result.PostHandoffRelaxedDuration = defaults.PostHandoffRelaxedDuration
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
		RelaxedTimeoutDuration:     c.RelaxedTimeoutDuration,
		HandoffTimeout:             c.HandoffTimeout,
		MinWorkers:                 c.MinWorkers,
		MaxWorkers:                 c.MaxWorkers,
		HandoffQueueSize:           c.HandoffQueueSize,
		PostHandoffRelaxedDuration: c.PostHandoffRelaxedDuration,
		ScaleDownDelay:             c.ScaleDownDelay,
		LogLevel:                   c.LogLevel,
	}
}

// applyWorkerDefaults calculates and applies worker defaults based on pool size
func (c *Config) applyWorkerDefaults(poolSize int) {
	// Calculate defaults based on pool size
	if poolSize <= 0 {
		poolSize = 100 // Default assumption if pool size unknown
	}

	// MinWorkers: max(1, poolSize/20) - conservative baseline
	if c.MinWorkers == 0 {
		c.MinWorkers = max(1, poolSize/20)
	}

	// MaxWorkers: max(MinWorkers*4, poolSize/5) - handle bursts effectively
	if c.MaxWorkers == 0 {
		c.MaxWorkers = max(c.MinWorkers*4, poolSize/5)
	}

	// Ensure MaxWorkers >= MinWorkers
	if c.MaxWorkers < c.MinWorkers {
		c.MaxWorkers = c.MinWorkers
	}
}

// max returns the maximum of two integers
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// min returns the minimum of two integers
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// EndpointTypeConfig represents the endpoint type configuration for CLIENT MAINT_NOTIFICATIONS.
type EndpointTypeConfig struct {
	// Type is the endpoint type to request
	Type string
	
	// AutoDetected indicates if this was auto-detected from connection settings
	AutoDetected bool
}

// DetectEndpointType automatically detects the appropriate endpoint type
// based on the connection address and TLS configuration.
func DetectEndpointType(addr string, tlsEnabled bool) EndpointTypeConfig {
	// Parse the address to determine if it's an IP or hostname
	isPrivate := isPrivateIP(addr)

	var endpointType string

	if tlsEnabled {
		// TLS requires FQDN for certificate validation
		if isPrivate {
			endpointType = "internal-fqdn"
		} else {
			endpointType = "external-fqdn"
		}
	} else {
		// No TLS, can use IP addresses
		if isPrivate {
			endpointType = "internal-ip"
		} else {
			endpointType = "external-ip"
		}
	}

	return EndpointTypeConfig{
		Type:         endpointType,
		AutoDetected: true,
	}
}



// isPrivateIP checks if the given address is in a private IP range.
func isPrivateIP(addr string) bool {
	// Simplified check for common private IP ranges
	// 10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16
	// This is a simplified implementation; a full implementation would parse the IP properly
	if len(addr) >= 3 && addr[:3] == "10." {
		return true
	}
	if len(addr) >= 8 && addr[:8] == "192.168." {
		return true
	}
	if len(addr) >= 7 && addr[:7] == "172.16." {
		return true
	}
	return false
}
