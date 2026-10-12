package e2e

import (
	"os"
	"time"
)

// TestMode represents the type of test environment
type TestMode int

const (
	// TestModeProxyMock uses the local proxy mock for fast testing
	TestModeProxyMock TestMode = iota
	// TestModeRealFaultInjector uses the real fault injector with actual Redis Enterprise
	TestModeRealFaultInjector
)

// TestModeConfig holds configuration for a specific test mode
type TestModeConfig struct {
	Mode TestMode

	// Timing configuration
	NotificationDelay        time.Duration // How long to wait after injecting a notification
	ActionWaitTimeout        time.Duration // How long to wait for fault injector actions
	ActionPollInterval       time.Duration // How often to poll for action status
	DatabaseReadyDelay       time.Duration // How long to wait for database to be ready
	ConnectionEstablishDelay time.Duration // How long to wait for connections to establish

	// Test behavior configuration
	MaxClients           int  // Maximum number of clients to create (proxy mock should use 1)
	SkipMultiClientTests bool // Whether to skip tests that require multiple clients
}

// GetTestMode detects the current test mode based on environment variables
func GetTestMode() TestMode {
	// If REDIS_ENDPOINTS_CONFIG_PATH is set, we're using real fault injector
	if os.Getenv("REDIS_ENDPOINTS_CONFIG_PATH") != "" {
		return TestModeRealFaultInjector
	}

	// If FAULT_INJECTOR_URL is set, we're using real fault injector
	if os.Getenv("FAULT_INJECTOR_URL") != "" {
		return TestModeRealFaultInjector
	}

	// Otherwise, we're using proxy mock
	return TestModeProxyMock
}

// GetTestModeConfig returns the configuration for the current test mode
func GetTestModeConfig() *TestModeConfig {
	mode := GetTestMode()

	switch mode {
	case TestModeProxyMock:
		return &TestModeConfig{
			Mode:                     TestModeProxyMock,
			NotificationDelay:        1 * time.Second,
			ActionWaitTimeout:        10 * time.Second,
			ActionPollInterval:       500 * time.Millisecond,
			DatabaseReadyDelay:       1 * time.Second,
			ConnectionEstablishDelay: 500 * time.Millisecond,
			MaxClients:               1,
			SkipMultiClientTests:     true,
		}

	case TestModeRealFaultInjector:
		return &TestModeConfig{
			Mode:                     TestModeRealFaultInjector,
			NotificationDelay:        30 * time.Second,
			ActionWaitTimeout:        5 * time.Minute, // Real fault injector can take up to 5 minutes
			ActionPollInterval:       500 * time.Millisecond,
			DatabaseReadyDelay:       10 * time.Second,
			ConnectionEstablishDelay: 2 * time.Second,
			MaxClients:               3,
			SkipMultiClientTests:     false,
		}

	default:
		// Default to proxy mock for safety
		return &TestModeConfig{
			Mode:                     TestModeProxyMock,
			NotificationDelay:        1 * time.Second,
			ActionWaitTimeout:        10 * time.Second,
			ActionPollInterval:       500 * time.Millisecond,
			DatabaseReadyDelay:       1 * time.Second,
			ConnectionEstablishDelay: 500 * time.Millisecond,
			MaxClients:               1,
			SkipMultiClientTests:     true,
		}
	}
}

// IsProxyMock returns true if running in proxy mock mode
func (c *TestModeConfig) IsProxyMock() bool {
	return c.Mode == TestModeProxyMock
}

// IsRealFaultInjector returns true if running with real fault injector
func (c *TestModeConfig) IsRealFaultInjector() bool {
	return c.Mode == TestModeRealFaultInjector
}

// String returns a human-readable name for the test mode
func (m TestMode) String() string {
	switch m {
	case TestModeProxyMock:
		return "ProxyMock"
	case TestModeRealFaultInjector:
		return "RealFaultInjector"
	default:
		return "Unknown"
	}
}
