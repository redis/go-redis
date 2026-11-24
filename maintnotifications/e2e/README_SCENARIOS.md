# E2E Test Scenarios for Push Notifications

This directory contains comprehensive end-to-end test scenarios for Redis push notifications and maintenance notifications functionality. Each scenario tests different aspects of the system under various conditions.

## ⚠️ **Important Note**
**Maintenance notifications are currently supported only in standalone Redis clients.** Cluster clients (ClusterClient, FailoverClient, etc.) do not yet support maintenance notifications functionality.

## Introduction

These tests support two modes:

### 1. Mock Proxy Mode (Default)
Uses a local Docker-based proxy ([cae-resp-proxy](https://github.com/redis-developer/cae-resp-proxy)) to simulate Redis Enterprise behavior. This mode:
- Runs entirely locally without external dependencies
- Provides fast feedback for development
- Simulates cluster topology changes
- Supports SMIGRATING and SMIGRATED notifications

To run in mock proxy mode:
```bash
make test.e2e
```

### 2. Real Fault Injector Mode
Uses a real Redis Enterprise fault injector service for comprehensive testing. This mode:
- Tests against actual Redis Enterprise clusters
- Validates real-world scenarios
- Requires external fault injector setup

To run with a real fault injector, set these environment variables:
- `REDIS_ENDPOINTS_CONFIG_PATH`: Path to Redis endpoints configuration
- `FAULT_INJECTION_API_URL`: URL of the fault injector server
- `E2E_SCENARIO_TESTS`: Set to `true` to enable scenario tests

Then run:
```bash
./scripts/run-e2e-tests.sh
```

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
- **Duration**: ~5 minutes 
- **Key Validations**:
  - Functionality with each endpoint type
  - Proper endpoint resolution
  - Notification delivery consistency
  - Handoff behavior per endpoint type

### 3. Unified Injector Scenarios (`scenario_unified_injector_test.go`)
**Mock proxy-based notification testing**
- **Purpose**: Test SMIGRATING and SMIGRATED notifications with simulated cluster topology changes
- **Features Tested**:
  - SMIGRATING notifications (slot migration in progress)
  - SMIGRATED notifications (slot migration completed)
  - Cluster topology changes (node swap simulation)
  - Complex multi-step migration scenarios
- **Configuration**: Uses local Docker proxy (cae-resp-proxy) with 4 nodes
- **Duration**: ~10 seconds
- **Key Validations**:
  - Notification delivery and parsing
  - Cluster state reload callbacks
  - Client resilience during migrations
  - Topology change handling
- **Topology Simulation**:
  - Starts with 4 proxy nodes (17000-17003)
  - Initially exposes 3 nodes in CLUSTER SLOTS (17000, 17001, 17002)
  - On SMIGRATED, swaps node 2 for node 3 (simulates node replacement)
  - Verifies client continues to function after topology change

### 4. Database Management Scenario (`scenario_database_management_test.go`)
**Dynamic database creation and deletion**
- **Purpose**: Test database lifecycle management via fault injector
- **Features Tested**: CREATE_DATABASE, DELETE_DATABASE endpoints
- **Configuration**: Various database configurations (simple, with modules, clustered)
- **Duration**: ~10 minutes
- **Key Validations**:
  - Database creation with different configurations
  - Database creation with Redis modules (ReJSON, search, timeseries, bf)
  - Database deletion
  - Complete lifecycle (create → use → delete)
  - Configuration validation

See [DATABASE_MANAGEMENT.md](DATABASE_MANAGEMENT.md) for detailed documentation on database management endpoints.

### 4. Timeout Configurations Scenario (`scenario_timeout_configs_test.go`)
**Various timeout strategies**
- **Purpose**: Test different timeout configurations and their impact
- **Features Tested**: Conservative, Aggressive, HighLatency timeouts
- **Configuration**:
  - Conservative: 60s handoff, 20s relaxed, 5s post-handoff
  - Aggressive: 5s handoff, 3s relaxed, 1s post-handoff
  - HighLatency: 90s handoff, 30s relaxed, 10m post-handoff
- **Duration**: ~10 minutes (3 sub-tests)
- **Key Validations**:
  - Timeout behavior matches configuration
  - Recovery times appropriate for each strategy
  - Error rates correlate with timeout aggressiveness

### 5. TLS Configurations Scenario (`scenario_tls_configs_test.go`)
**Security and encryption testing framework**
- **Purpose**: Test push notifications with different TLS configurations
- **Features Tested**: NoTLS, TLSInsecure, TLSSecure, TLSMinimal, TLSStrict
- **Configuration**: Framework for testing various TLS settings (TLS config handled at connection level)
- **Duration**: ~10 minutes (multiple sub-tests)
- **Key Validations**:
  - Functionality with each TLS configuration
  - Performance impact of encryption
  - Certificate handling (where applicable)
  - Security compliance
- **Note**: TLS configuration is handled at the Redis connection config level, not client options level

### 6. Stress Test Scenario (`scenario_stress_test.go`)
**Extreme load and concurrent operations**
- **Purpose**: Test system limits and behavior under extreme stress
- **Features Tested**: Maximum concurrent operations, multiple clients
- **Configuration**:
  - 4 clients with 150 pool size each
  - 200 max connections per client
  - 50 workers, 1000 queue size
  - Concurrent failover/migration actions
- **Duration**: ~15 minutes
- **Key Validations**:
  - System stability under extreme load
  - Error rates within stress limits (<20%)
  - Resource utilization and limits
  - Concurrent fault injection handling


### 7. Topology Changes, With and Without SCH
(`scenario_topology_standalone_test.go`, `scenario_cluster_without_sch_test.go`)
- **Purpose**: Check that clients recover from topology changes and data movement
  both with Smart Client Handoff (SCH, maintenance notifications) and without it.
- **Standalone**: `TestStandaloneTopologyChange` runs each effect of the fault
  injector's `/topology-change-standalone` route (`data_movement_conn_drop`,
  `data_movement_no_conn_drop`, `conn_drop`, `dns_resolution_change`) in two modes.
  `with_sch` keeps the default client (Mode Auto); `without_sch` disables
  maintenance notifications. A regular write loop, a subscription and a blocking
  `BLPOP` run across the change and must work again on the same client. Set
  `E2E_STANDALONE_ALL_TRIGGERS=true` to run every trigger instead of the first.
- **Cluster**: `TestClusterSlotMigrate_AllEffects` is the with-SCH test.
  `TestClusterSlotMigrate_WithoutSCH` runs one trigger per slot-migrate effect with
  notifications disabled: the client only has MOVED/ASK handling and its own
  topology reloads to follow the move, and no seeded key may be lost.
- **Data checks** (`data_verify_test.go`): the standalone test, the cluster
  without-SCH test and `TestClusterSlotMigrate_AllEffects` seed a data set with many
  commands before the fault (GET/SET with and without expiry, HSET/HGET/HGETALL,
  MSET/MGET, INCRBY/DECRBY, TTL/PERSIST, sets, lists, sorted sets, DEL) and read
  it back exactly afterwards. Mixed traffic that runs through the fault records
  which writes were acknowledged, and none of them may be lost.
  `TestDataVerifierAgainstRedis` shows each check fails on the matching corruption.
- **Both modes check the label**: a without-SCH client must have no
  maintenance-notification manager, so "without SCH" cannot be true by accident.
- **Real fault injector only**: the proxy mock injects notifications and moves no
  data. `TestStandaloneScenarioHarnessAgainstFakeFI` checks the standalone harness
  against a fake fault injector (see the test for the two environment variables).

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