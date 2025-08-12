package hitless

import (
	"time"
)

// ExampleCustomConfig shows how to create a custom hitless configuration
func ExampleCustomConfig() *Config {
	return &Config{
		Enabled:                MaintNotificationsEnabled,
		EndpointType:           EndpointTypeInternalIP,
		RelaxedTimeout:         45 * time.Second,
		HandoffTimeout:         20 * time.Second,
		MinWorkers:             5,   // Minimum workers for baseline processing
		MaxWorkers:             20,  // Maximum workers for high-throughput scenarios
		HandoffQueueSize:       200, // Larger queue for burst handling
		LogLevel:               2,   // Info level logging
	}
}

// ExampleLowResourceConfig shows a configuration for resource-constrained environments
func ExampleLowResourceConfig() *Config {
	return &Config{
		Enabled:                MaintNotificationsEnabled,
		EndpointType:           EndpointTypeInternalIP,
		RelaxedTimeout:         20 * time.Second,
		HandoffTimeout:         10 * time.Second,
		MinWorkers:             1,  // Minimum workers to save resources
		MaxWorkers:             3,  // Low maximum for resource-constrained environments
		HandoffQueueSize:       25, // Smaller queue
		LogLevel:               1,  // Warning level logging only
	}
}

// ExampleHighThroughputConfig shows a configuration for high-throughput scenarios
func ExampleHighThroughputConfig() *Config {
	return &Config{
		Enabled:                MaintNotificationsEnabled,
		EndpointType:           EndpointTypeInternalIP,
		RelaxedTimeout:         60 * time.Second,
		HandoffTimeout:         30 * time.Second,
		MinWorkers:             10,  // High baseline for consistent performance
		MaxWorkers:             30,  // Many workers for parallel processing
		HandoffQueueSize:       500, // Large queue for burst handling
		LogLevel:               3,   // Debug level logging for monitoring
	}
}

// ExamplePartialConfig shows how partial configuration works with automatic defaults
func ExamplePartialConfig() *Config {
	// Only specify the fields you want to customize
	// Other fields will automatically get default values when ApplyDefaults() is called
	return &Config{
		Enabled:    MaintNotificationsEnabled,
		MinWorkers: 3,  // Custom minimum worker count
		MaxWorkers: 15, // Custom maximum worker count
		LogLevel:   2,  // Info level logging
		// HandoffQueueSize will get default value (100)
		// RelaxedTimeout will get default value (30s)
		// HandoffTimeout will get default value (15s)
		// etc.
	}
}

// ExampleMinimalConfig shows the most minimal configuration
func ExampleMinimalConfig() *Config {
	// Just enable hitless upgrades, everything else gets defaults
	return &Config{
		Enabled: MaintNotificationsEnabled,
		// All other fields will get default values automatically
	}
}
