# Database Management with Fault Injector

This document describes how to use the fault injector's database management endpoints to create and delete Redis databases during E2E testing.

## Overview

The fault injector now supports two new endpoints for database management:

1. **CREATE_DATABASE** - Create a new Redis database with custom configuration
2. **DELETE_DATABASE** - Delete an existing Redis database

These endpoints are useful for E2E tests that need to dynamically create and destroy databases as part of their test scenarios.

## Action Types

### CREATE_DATABASE

Creates a new Redis database with the specified configuration.

**Parameters:**
- `cluster_index` (int): The index of the cluster where the database should be created
- `database_config` (object): The database configuration (see structure below)

**Raises:**
- `CreateDatabaseException`: When database creation fails

### DELETE_DATABASE

Deletes an existing Redis database.

**Parameters:**
- `cluster_index` (int): The index of the cluster containing the database
- `bdb_id` (int): The database ID to delete

**Raises:**
- `DeleteDatabaseException`: When database deletion fails

## Database Configuration Structure

The `database_config` object supports the following fields:

```go
type DatabaseConfig struct {
    Name                            string                   `json:"name"`
    Port                            int                      `json:"port"`
    MemorySize                      int64                    `json:"memory_size"`
    Replication                     bool                     `json:"replication"`
    EvictionPolicy                  string                   `json:"eviction_policy"`
    Sharding                        bool                     `json:"sharding"`
    AutoUpgrade                     bool                     `json:"auto_upgrade"`
    ShardsCount                     int                      `json:"shards_count"`
    ModuleList                      []DatabaseModule         `json:"module_list,omitempty"`
    OSSCluster                      bool                     `json:"oss_cluster"`
    OSSClusterAPIPreferredIPType    string                   `json:"oss_cluster_api_preferred_ip_type,omitempty"`
    ProxyPolicy                     string                   `json:"proxy_policy,omitempty"`
    ShardsPlacement                 string                   `json:"shards_placement,omitempty"`
    ShardKeyRegex                   []ShardKeyRegexPattern   `json:"shard_key_regex,omitempty"`
}

type DatabaseModule struct {
    ModuleArgs string `json:"module_args"`
    ModuleName string `json:"module_name"`
}

type ShardKeyRegexPattern struct {
    Regex string `json:"regex"`
}
```

### Example Configuration

#### Simple Database

```json
{
  "name": "simple-db",
  "port": 12000,
  "memory_size": 268435456,
  "replication": false,
  "eviction_policy": "noeviction",
  "sharding": false,
  "auto_upgrade": true,
  "shards_count": 1,
  "oss_cluster": false
}
```

#### Clustered Database with Modules

```json
{
  "name": "ioredis-cluster",
  "port": 11112,
  "memory_size": 1273741824,
  "replication": true,
  "eviction_policy": "noeviction",
  "sharding": true,
  "auto_upgrade": true,
  "shards_count": 3,
  "module_list": [
    {
      "module_args": "",
      "module_name": "ReJSON"
    },
    {
      "module_args": "",
      "module_name": "search"
    },
    {
      "module_args": "",
      "module_name": "timeseries"
    },
    {
      "module_args": "",
      "module_name": "bf"
    }
  ],
  "oss_cluster": true,
  "oss_cluster_api_preferred_ip_type": "external",
  "proxy_policy": "all-master-shards",
  "shards_placement": "sparse",
  "shard_key_regex": [
    {
      "regex": ".*\\{(?<tag>.*)\\}.*"
    },
    {
      "regex": "(?<tag>.*)"
    }
  ]
}
```

## Usage Examples

### Example 1: Create a Simple Database

```go
ctx := context.Background()
faultInjector := NewFaultInjectorClient("http://127.0.0.1:20324")

dbConfig := DatabaseConfig{
    Name:           "test-db",
    Port:           12000,
    MemorySize:     268435456, // 256MB
    Replication:    false,
    EvictionPolicy: "noeviction",
    Sharding:       false,
    AutoUpgrade:    true,
    ShardsCount:    1,
    OSSCluster:     false,
}

resp, err := faultInjector.CreateDatabase(ctx, 0, dbConfig)
if err != nil {
    log.Fatalf("Failed to create database: %v", err)
}

// Wait for creation to complete
status, err := faultInjector.WaitForAction(ctx, resp.ActionID,
    WithMaxWaitTime(5*time.Minute))
if err != nil {
    log.Fatalf("Failed to wait for action: %v", err)
}

if status.Status == StatusSuccess {
    log.Println("Database created successfully!")
}
```

### Example 2: Create a Database with Modules

```go
dbConfig := DatabaseConfig{
    Name:           "modules-db",
    Port:           12001,
    MemorySize:     536870912, // 512MB
    Replication:    true,
    EvictionPolicy: "noeviction",
    Sharding:       true,
    AutoUpgrade:    true,
    ShardsCount:    3,
    ModuleList: []DatabaseModule{
        {ModuleArgs: "", ModuleName: "ReJSON"},
        {ModuleArgs: "", ModuleName: "search"},
    },
    OSSCluster:                   true,
    OSSClusterAPIPreferredIPType: "external",
    ProxyPolicy:                  "all-master-shards",
    ShardsPlacement:              "sparse",
}

resp, err := faultInjector.CreateDatabase(ctx, 0, dbConfig)
// ... handle response
```

### Example 3: Create Database Using a Map

```go
dbConfigMap := map[string]any{
    "name":            "map-db",
    "port":            12002,
    "memory_size":     268435456,
    "replication":     false,
    "eviction_policy": "volatile-lru",
    "sharding":        false,
    "auto_upgrade":    true,
    "shards_count":    1,
    "oss_cluster":     false,
}

resp, err := faultInjector.CreateDatabaseFromMap(ctx, 0, dbConfigMap)
// ... handle response
```

### Example 4: Delete a Database

```go
clusterIndex := 0
bdbID := 1

resp, err := faultInjector.DeleteDatabase(ctx, clusterIndex, bdbID)
if err != nil {
    log.Fatalf("Failed to delete database: %v", err)
}

status, err := faultInjector.WaitForAction(ctx, resp.ActionID,
    WithMaxWaitTime(2*time.Minute))
if err != nil {
    log.Fatalf("Failed to wait for action: %v", err)
}

if status.Status == StatusSuccess {
    log.Println("Database deleted successfully!")
}
```

### Example 5: Complete Lifecycle (Create and Delete)

```go
// Create database
dbConfig := DatabaseConfig{
    Name:           "temp-db",
    Port:           13000,
    MemorySize:     268435456,
    Replication:    false,
    EvictionPolicy: "noeviction",
    Sharding:       false,
    AutoUpgrade:    true,
    ShardsCount:    1,
    OSSCluster:     false,
}

createResp, err := faultInjector.CreateDatabase(ctx, 0, dbConfig)
if err != nil {
    log.Fatalf("Failed to create database: %v", err)
}

createStatus, err := faultInjector.WaitForAction(ctx, createResp.ActionID,
    WithMaxWaitTime(5*time.Minute))
if err != nil || createStatus.Status != StatusSuccess {
    log.Fatalf("Database creation failed")
}

// Extract bdb_id from output
var bdbID int
if id, ok := createStatus.Output["bdb_id"].(float64); ok {
    bdbID = int(id)
}

// Use the database for testing...
time.Sleep(10 * time.Second)

// Delete the database
deleteResp, err := faultInjector.DeleteDatabase(ctx, 0, bdbID)
if err != nil {
    log.Fatalf("Failed to delete database: %v", err)
}

deleteStatus, err := faultInjector.WaitForAction(ctx, deleteResp.ActionID,
    WithMaxWaitTime(2*time.Minute))
if err != nil || deleteStatus.Status != StatusSuccess {
    log.Fatalf("Database deletion failed")
}

log.Println("Database lifecycle completed successfully!")
```

## Available Methods

The `FaultInjectorClient` provides the following methods for database management:

### CreateDatabase

```go
func (c *FaultInjectorClient) CreateDatabase(
    ctx context.Context,
    clusterIndex int,
    databaseConfig DatabaseConfig,
) (*ActionResponse, error)
```

Creates a new database using a structured `DatabaseConfig` object.

### CreateDatabaseFromMap

```go
func (c *FaultInjectorClient) CreateDatabaseFromMap(
    ctx context.Context,
    clusterIndex int,
    databaseConfig map[string]any,
) (*ActionResponse, error)
```

Creates a new database using a flexible map configuration. Useful when you need to pass custom or dynamic configurations.

### DeleteDatabase

```go
func (c *FaultInjectorClient) DeleteDatabase(
    ctx context.Context,
    clusterIndex int,
    bdbID int,
) (*ActionResponse, error)
```

Deletes an existing database by its ID.

## Testing

To run the database management E2E tests:

```bash
# Run all database management tests
go test -tags=e2e -v ./maintnotifications/e2e/ -run TestDatabase

# Run specific test
go test -tags=e2e -v ./maintnotifications/e2e/ -run TestDatabaseLifecycle
```

## Notes

- Database creation can take several minutes depending on the configuration
- Always use `WaitForAction` to ensure the operation completes before proceeding
- The `bdb_id` returned in the creation output should be used for deletion
- Deleting a non-existent database will result in a failed action status
- Memory sizes are specified in bytes (e.g., 268435456 = 256MB)
- Port numbers should be unique and not conflict with existing databases

## Common Eviction Policies

- `noeviction` - Return errors when memory limit is reached
- `allkeys-lru` - Evict any key using LRU algorithm
- `volatile-lru` - Evict keys with TTL using LRU algorithm
- `allkeys-random` - Evict random keys
- `volatile-random` - Evict random keys with TTL
- `volatile-ttl` - Evict keys with TTL, shortest TTL first

## Common Proxy Policies

- `all-master-shards` - Route to all master shards
- `all-nodes` - Route to all nodes
- `single-shard` - Route to a single shard

