// Package e2e provides end-to-end testing scenarios for the maintenance notifications system.
//
// This package contains comprehensive test scenarios that validate the maintenance notifications
// functionality in realistic environments. The tests are designed to work with Redis Enterprise
// clusters and require specific environment configuration.
//
// Environment Variables:
//   - E2E_SCENARIO_TESTS: Set to "true" to enable scenario tests
//   - REDIS_ENDPOINTS_CONFIG_PATH: Path to endpoints configuration file
//   - FAULT_INJECTION_API_URL: URL for fault injection API (optional)
//
// Test Scenarios:
//   - Basic Push Notifications: Core functionality testing
//   - Endpoint Types: Different endpoint resolution strategies
//   - Timeout Configurations: Various timeout strategies
//   - TLS Configurations: Different TLS setups
//   - Stress Testing: Extreme load and concurrent operations
//
// Note: Maintenance notifications are currently supported only in standalone Redis clients.
// Cluster clients (ClusterClient, FailoverClient, etc.) do not yet support this functionality.
package e2e
