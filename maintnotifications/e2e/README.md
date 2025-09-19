# Maintenance Notifications End-to-End Testing Framework

This directory contains a comprehensive Go-based end-to-end testing framework for Redis maintenance notifications functionality. It provides scenario-based testing with fault injection capabilities and is designed for testing Redis Enterprise maintenance operations.

## ⚠️ **Important Note**
**Maintenance notifications are currently supported only in standalone Redis clients.** Cluster clients (ClusterClient, FailoverClient, etc.) do not yet support maintenance notifications functionality.

## Overview

The E2E testing framework validates that Redis clients can handle maintenance notifications and continue operations during Redis Enterprise maintenance operations, failovers, and upgrades. It tests the maintenance notifications functionality through realistic scenarios including push notifications, connection handoffs, and timeout handling.

## Framework Components

### 1. Fault Injector (`fault_injector.go`, `fault_injector_server.go`)
- HTTP API for triggering Redis Enterprise maintenance operations
- Support for migrate and bind actions, cluster failovers, and node operations
- Real-time action monitoring and status tracking
- Integration with Redis Enterprise infrastructure

### 2. Configuration Parser (`config_parser.go`)
- JSON-based configuration for multiple Redis deployment types
- Support for standalone, cluster, sentinel, and enterprise Redis configurations
- Client factory pattern with automatic client lifecycle management
- Environment variable integration and validation

### 3. Scenario Tests
- **Push Notifications** (`scenario_push_notifications_test.go`): Tests MOVING, MIGRATING, MIGRATED notifications
- **Endpoint Types** (`scenario_endpoint_types_test.go`): Tests different endpoint resolution strategies
- **Timeout Configurations** (`scenario_timeout_configs_test.go`): Tests various timeout strategies
- **TLS Configurations** (`scenario_tls_configs_test.go`): Tests different TLS configurations
- **Stress Testing** (`scenario_stress_test.go`): Tests system behavior under extreme load

### 4. Infrastructure Support
- Docker Compose setup for local testing
- Fault injector server for maintenance operation simulation
- Comprehensive logging and diagnostics
- Notification tracking and analysis tools

## Configuration Management

The framework uses JSON-based configuration to support multiple Redis deployment types:

### Supported Deployment Types

- **Standalone Redis**: Single instance configurations
- **Redis Cluster**: Multi-node cluster configurations
- **Redis Enterprise**: Enterprise cluster configurations with BDB IDs
- **TLS/SSL**: Secure connection configurations

### Configuration File Format

```json
{
  "enterprise-cluster": {
    "bdb_id": 1,
    "username": "default",
    "password": "enterprise-password",
    "tls": false,
    "endpoints": ["redis://redis-enterprise.example.com:12000"]
  },
  "oss-cluster": {
    "username": "default",
    "password": "cluster-password",
    "tls": false,
    "endpoints": [
      "redis://localhost:7001",
      "redis://localhost:7002",
      "redis://localhost:7003"
    ]
  }
}
```

### Client Factory Usage

```go
// Create client factory from configuration
factory, err := CreateTestClientFactory("enterprise-cluster")
if err != nil {
    t.Fatalf("Failed to create client factory: %v", err)
}
defer factory.DestroyAll()

// Create Redis client with hitless upgrade configuration
client, err := factory.Create("test-client", &CreateClientOptions{
    Protocol: 3,
    HitlessUpgradeConfig: &hitless.Config{
        Mode:           hitless.ModeEnabled,
        HandoffTimeout: 30 * time.Second,
        RelaxedTimeout: 10 * time.Second,
        MaxWorkers:     20,
    },
})
```

## Available Scenario Tests

The framework includes comprehensive scenario tests that validate different aspects of hitless upgrades:

### 1. Push Notifications (`scenario_push_notifications_test.go`)
Tests Redis Enterprise push notification handling:
- **MOVING**: Slot movement notifications
- **MIGRATING**: Data migration in progress
- **MIGRATED**: Migration completion
- **Cluster Client Support**: Multi-node notification handling

### 2. Connection Handoff (`scenario_connection_handoff_test.go`)
Tests connection establishment and traffic resumption:
- **New Connection Establishment**: Verifies new connections during handoff
- **Traffic Resumption**: Validates continuous operations during maintenance
- **TLS Connection Handoff**: Tests secure connection handoffs
- **Endpoint Type Testing**: Tests different endpoint configurations

### 3. Timeout Handling (`scenario_timeout_during_notifications_test.go`)
Tests timeout behavior during maintenance:
- **Timeout Relaxation**: Verifies timeouts are relaxed during maintenance
- **Timeout Restoration**: Ensures normal timeouts are restored after maintenance
- **Error Classification**: Tests proper timeout error handling

### Writing Custom Scenario Tests

Create new scenario test files following the pattern `scenario_*.go`:

```go


package e2e

import (
    "context"
    "os"
    "testing"
    "time"

    "github.com/redis/go-redis/v9/hitless"
)

func TestCustomScenario(t *testing.T) {
    if os.Getenv("E2E_SCENARIO_TESTS") != "true" {
        t.Skip("Scenario tests require E2E_SCENARIO_TESTS=true")
    }

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
    defer cancel()

    // Create client factory from configuration
    factory, err := CreateTestClientFactory("enterprise-cluster")
    if err != nil {
        t.Skipf("Enterprise cluster not available: %v", err)
    }
    defer factory.DestroyAll()

    // Create fault injector
    faultInjector, err := CreateTestFaultInjector()
    if err != nil {
        t.Fatalf("Failed to create fault injector: %v", err)
    }

    // Create Redis client with hitless upgrades
    client, err := factory.Create("custom-scenario-client", &CreateClientOptions{
        Protocol: 3,
        HitlessUpgradeConfig: &hitless.Config{
            Mode:           hitless.ModeEnabled,
            HandoffTimeout: 30 * time.Second,
            RelaxedTimeout: 10 * time.Second,
            MaxWorkers:     20,
        },
    })
    if err != nil {
        t.Fatalf("Failed to create client: %v", err)
    }

    // Your scenario test logic here...
    // - Start background operations
    // - Trigger maintenance operations via fault injector
    // - Verify hitless behavior
    // - Validate recovery and consistency
}
```

## Environment Variables

### Required Configuration
- `REDIS_ENDPOINTS_CONFIG_PATH`: Path to the Redis endpoints configuration file
- `E2E_SCENARIO_TESTS`: Set to "true" to enable scenario tests

### Optional Configuration
- `FAULT_INJECTION_API_URL`: URL of the fault injector server (default: http://localhost:8080)

### Example Setup
```bash
export REDIS_ENDPOINTS_CONFIG_PATH="/path/to/endpoints.json"
export FAULT_INJECTION_API_URL="http://localhost:8080"
export E2E_SCENARIO_TESTS="true"
```

## Running Tests

### Prerequisites
- Redis Enterprise cluster or compatible Redis setup
- Go 1.19+ with build tags support
- Access to fault injection infrastructure (for maintenance operations)

### Basic Test Execution
```bash
# Set required environment variables
export REDIS_ENDPOINTS_CONFIG_PATH="/path/to/endpoints.json"
export E2E_SCENARIO_TESTS="true"

# Run all scenario tests
go test ./maintnotifications/e2e/... -v -timeout 2h

# Run specific scenario tests
go test ./maintnotifications/e2e/ -run TestPushNotifications -v
go test ./maintnotifications/e2e/ -run TestEndpointTypesPushNotifications -v
go test ./maintnotifications/e2e/ -run TestTimeoutConfigurationsPushNotifications -v
go test ./maintnotifications/e2e/ -run TestTLSConfigurationsPushNotifications -v
go test ./maintnotifications/e2e/ -run TestStressPushNotifications -v
```

### Test Infrastructure Requirements

#### Redis Enterprise Setup
- Redis Enterprise cluster with maintenance notifications enabled
- RESP3 protocol support
- Push notification capabilities
- Fault injection API access

#### Local Development Setup
- Docker Compose environment (see `docker-compose.yml`)
- Fault injector server
- Network simulation capabilities (optional)
