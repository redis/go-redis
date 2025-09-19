# E2E Test Scenarios for Push Notifications

This directory contains comprehensive end-to-end test scenarios for Redis push notifications and maintenance notifications functionality. Each scenario tests different aspects of the system under various conditions.

## ⚠️ **Important Note**
**Maintenance notifications are currently supported only in standalone Redis clients.** Cluster clients (ClusterClient, FailoverClient, etc.) do not yet support maintenance notifications functionality.

## Introduction

To run those tests you would need a fault injector service, please review the client and feel free to implement your
fault injector of choice. Those tests are tailored for Redis Enterprise, but can be adapted to other Redis distributions where
a fault injector is available.

Once you have fault injector service up and running, you can execute the tests by running the `run-e2e-tests.sh` script.
there are three environment variables that need to be set before running the tests:

- `REDIS_ENDPOINTS_CONFIG_PATH`: Path to Redis endpoints configuration
- `FAULT_INJECTION_API_URL`: URL of the fault injector server
- `E2E_SCENARIO_TESTS`: Set to `true` to enable scenario tests

## Test Scenarios Overview

### 1. Basic Push Notifications (`scenario_push_notifications_test.go`)
**Original template scenario**
- **Purpose**: Basic functionality test for Redis Enterprise push notifications
- **Features Tested**: FAILING_OVER, FAILED_OVER, MIGRATING, MIGRATED, MOVING notifications
- **Configuration**: Standard enterprise cluster setup
- **Duration**: ~10 minutes
- **Key Validations**: 
  - All notification types received
  - Timeout behavior (relaxed/unrelaxed)
  - Handoff success rates
  - Connection pool management

### 2. Endpoint Types Scenario (`scenario_endpoint_types_test.go`)
**Different endpoint resolution strategies**
- **Purpose**: Test push notifications with different endpoint types
- **Features Tested**: ExternalIP, InternalIP, InternalFQDN, ExternalFQDN endpoint types
- **Configuration**: Standard setup with varying endpoint types
- **Duration**: ~20 minutes (multiple sub-tests)
- **Key Validations**:
  - Functionality with each endpoint type
  - Proper endpoint resolution
  - Notification delivery consistency
  - Handoff behavior per endpoint type

### 3. Timeout Configurations Scenario (`scenario_timeout_configs_test.go`)
**Various timeout strategies**
- **Purpose**: Test different timeout configurations and their impact
- **Features Tested**: Conservative, Aggressive, HighLatency timeouts
- **Configuration**:
  - Conservative: 60s handoff, 20s relaxed, 5s post-handoff
  - Aggressive: 5s handoff, 3s relaxed, 1s post-handoff
  - HighLatency: 90s handoff, 30s relaxed, 10m post-handoff
- **Duration**: ~25 minutes (3 sub-tests)
- **Key Validations**:
  - Timeout behavior matches configuration
  - Recovery times appropriate for each strategy
  - Error rates correlate with timeout aggressiveness

### 4. TLS Configurations Scenario (`scenario_tls_configs_test.go`)
**Security and encryption testing framework**
- **Purpose**: Test push notifications with different TLS configurations
- **Features Tested**: NoTLS, TLSInsecure, TLSSecure, TLSMinimal, TLSStrict
- **Configuration**: Framework for testing various TLS settings (TLS config handled at connection level)
- **Duration**: ~20 minutes (multiple sub-tests)
- **Key Validations**:
  - Functionality with each TLS configuration
  - Performance impact of encryption
  - Certificate handling (where applicable)
  - Security compliance
- **Note**: TLS configuration is handled at the Redis connection config level, not client options level

### 5. Stress Test Scenario (`scenario_stress_test.go`)
**Extreme load and concurrent operations**
- **Purpose**: Test system limits and behavior under extreme stress
- **Features Tested**: Maximum concurrent operations, multiple clients
- **Configuration**:
  - 4 clients with 150 pool size each
  - 200 max connections per client
  - 50 workers, 1000 queue size
  - Concurrent failover/migration actions
- **Duration**: ~30 minutes
- **Key Validations**:
  - System stability under extreme load
  - Error rates within stress limits (<20%)
  - Resource utilization and limits
  - Concurrent fault injection handling


## Running the Scenarios

### Prerequisites
- Set environment variable: `E2E_SCENARIO_TESTS=true`
- Redis Enterprise cluster available
- Fault injection service available
- Appropriate network access and permissions
- **Note**: Tests use standalone Redis clients only (cluster clients not supported)

### Individual Scenario Execution
```bash
# Run a specific scenario
E2E_SCENARIO_TESTS=true go test -v ./maintnotifications/e2e -run TestEndpointTypesPushNotifications

# Run with timeout
E2E_SCENARIO_TESTS=true go test -v -timeout 30m ./maintnotifications/e2e -run TestStressPushNotifications
```

### All Scenarios Execution
```bash
./scripts/run-e2e-tests.sh
```
## Expected Outcomes

### Success Criteria
- All notifications received and processed correctly
- Error rates within acceptable limits for each scenario
- No notification processing errors
- Proper timeout behavior
- Successful handoffs
- Connection pool management within limits

### Performance Benchmarks
- **Basic**: >1000 operations, <1% errors
- **Stress**: >10000 operations, <20% errors
- **Others**: Functionality over performance

## Troubleshooting

### Common Issues
1. **Enterprise cluster not available**: Most scenarios require Redis Enterprise
2. **Fault injector unavailable**: Some scenarios need fault injection service
3. **Network timeouts**: Increase test timeouts for slow networks
4. **TLS certificate issues**: Some TLS scenarios may fail without proper certs
5. **Resource limits**: Stress scenarios may hit system limits

### Debug Options
- Enable detailed logging in scenarios
- Use `dump = true` to see full log analysis
- Check pool statistics for connection issues
- Monitor client resources during stress tests