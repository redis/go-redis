# Redis Cluster MGET Example

This example demonstrates how to use the Redis Cluster client with the `MGET` command to retrieve multiple keys efficiently.

## Overview

The example shows:
- Creating a Redis Cluster client
- Setting 10 keys with individual `SET` commands
- Retrieving all 10 keys in a single operation using `MGET`
- Validating that the retrieved values match the expected values
- Cleaning up by deleting the test keys

## Prerequisites

You need a running Redis Cluster. The example expects cluster nodes at:
- `localhost:7000`
- `localhost:7001`
- `localhost:7002`

### Setting up a Redis Cluster (using Docker)

If you don't have a Redis Cluster running, you can use the docker-compose setup from the repository root:

```bash
# From the go-redis repository root
docker compose --profile cluster up -d
```

This will start a Redis Cluster with nodes on ports 16600-16605.

If using the docker-compose cluster, update the `Addrs` in `main.go` to:
```go
Addrs: []string{
    "localhost:16600",
    "localhost:16601",
    "localhost:16602",
},
```

## Running the Example

```bash
go run main.go
```

## Expected Output

```
✓ Connected to Redis cluster

=== Setting 10 keys ===
✓ SET key0 = value0
✓ SET key1 = value1
✓ SET key2 = value2
✓ SET key3 = value3
✓ SET key4 = value4
✓ SET key5 = value5
✓ SET key6 = value6
✓ SET key7 = value7
✓ SET key8 = value8
✓ SET key9 = value9

=== Retrieving keys with MGET ===

=== Validating MGET results ===
✓ key0: value0
✓ key1: value1
✓ key2: value2
✓ key3: value3
✓ key4: value4
✓ key5: value5
✓ key6: value6
✓ key7: value7
✓ key8: value8
✓ key9: value9

=== Summary ===
✓ All values retrieved successfully and match expected values!

=== Cleaning up ===
✓ Cleanup complete
```

## Key Concepts

### MGET Command

`MGET` (Multiple GET) is a Redis command that retrieves the values of multiple keys in a single operation. This is more efficient than executing multiple individual `GET` commands.

**Syntax:**
```go
result, err := rdb.MGet(ctx, key1, key2, key3, ...).Result()
```

**Returns:**
- A slice of `any` values
- Each value corresponds to a key in the same order
- `nil` is returned for keys that don't exist

### Cluster Client

The `ClusterClient` automatically handles:
- Distributing keys across cluster nodes based on hash slots
- Following cluster redirects
- Maintaining connections to all cluster nodes
- Retrying operations on cluster topology changes

For `MGET` operations in a cluster, the client may need to split the request across multiple nodes if the keys map to different hash slots.

## Code Highlights

```go
// Create cluster client
rdb := redis.NewClusterClient(&redis.ClusterOptions{
    Addrs: []string{
        "localhost:7000",
        "localhost:7001",
        "localhost:7002",
    },
})

// Set individual keys
for i := 0; i < 10; i++ {
    err := rdb.Set(ctx, fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i), 0).Err()
    // handle error
}

// Retrieve all keys with MGET
result, err := rdb.MGet(ctx, keys...).Result()

// Validate results
for i, val := range result {
    actualValue, ok := val.(string)
    // validate actualValue matches expected
}
```

## Learn More

- [Redis MGET Documentation](https://redis.io/commands/mget/)
- [Redis Cluster Specification](https://redis.io/topics/cluster-spec)
- [go-redis Documentation](https://redis.uptrace.dev/)

