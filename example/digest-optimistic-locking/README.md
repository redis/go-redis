# Redis Digest & Optimistic Locking Example

This example demonstrates how to use Redis DIGEST command and digest-based optimistic locking with go-redis.

## What is Redis DIGEST?

The DIGEST command (Redis 8.4+) returns a 64-bit xxh3 hash of a key's value. This hash can be used for:

- **Optimistic locking**: Update values only if they haven't changed
- **Change detection**: Detect if a value was modified
- **Conditional operations**: Delete or update based on expected content

## Features Demonstrated

1. **Basic Digest Usage**: Get digest from Redis and verify with client-side calculation
2. **Optimistic Locking with SetIFDEQ**: Update only if digest matches (value unchanged)
3. **Change Detection with SetIFDNE**: Update only if digest differs (value changed)
4. **Conditional Delete**: Delete only if digest matches expected value
5. **Client-Side Digest Generation**: Calculate digests without fetching from Redis

## Requirements

- Redis 8.4+ (for DIGEST command support)
- Go 1.18+

## Installation

```bash
cd example/digest-optimistic-locking
go mod tidy
```

## Running the Example

```bash
# Make sure Redis 8.4+ is running on localhost:6379
redis-server

# In another terminal, run the example
go run .
```

## Expected Output

```
=== Redis Digest & Optimistic Locking Example ===

1. Basic Digest Usage
---------------------
Key: user:1000:name
Value: Alice
Digest: 7234567890123456789 (0x6478a1b2c3d4e5f6)
Client-calculated digest: 7234567890123456789 (0x6478a1b2c3d4e5f6)
✓ Digests match!

2. Optimistic Locking with SetIFDEQ
------------------------------------
Initial value: 100
Current digest: 0x1234567890abcdef
✓ Update successful! New value: 150
✓ Correctly rejected update with wrong digest

3. Detecting Changes with SetIFDNE
-----------------------------------
Initial value: v1.0.0
Old digest: 0xabcdef1234567890
✓ Value changed! Updated to: v2.0.0
✓ Correctly rejected: current value matches the digest

4. Conditional Delete with DelExArgs
-------------------------------------
Created session: session:abc123
Expected digest: 0x9876543210fedcba
✓ Correctly refused to delete (wrong digest)
✓ Successfully deleted with correct digest
✓ Session deleted

5. Client-Side Digest Generation
---------------------------------
Current price: $29.99
Expected digest (calculated client-side): 0xfedcba0987654321
✓ Price updated successfully to $24.99

Binary data example:
Binary data digest: 0x1122334455667788
✓ Binary digest matches!

=== All examples completed successfully! ===
```

## How It Works

### Digest Calculation

Redis uses the **xxh3** hashing algorithm. To calculate digests client-side, use `github.com/zeebo/xxh3`:

```go
import "github.com/zeebo/xxh3"

// For strings
digest := xxh3.HashString("myvalue")

// For binary data
digest := xxh3.Hash([]byte{0x01, 0x02, 0x03})
```

### Optimistic Locking Pattern

```go
// 1. Read current value and get its digest
currentValue := rdb.Get(ctx, "key").Val()
currentDigest := rdb.Digest(ctx, "key").Val()

// 2. Perform business logic
newValue := processValue(currentValue)

// 3. Update only if value hasn't changed
result := rdb.SetIFDEQ(ctx, "key", newValue, currentDigest, 0)
if result.Err() == redis.Nil {
    // Value was modified by another client - retry or handle conflict
}
```

### Client-Side Digest (No Extra Round Trip)

```go
// If you know the expected current value, calculate digest client-side
expectedValue := "100"
expectedDigest := xxh3.HashString(expectedValue)

// Update without fetching digest from Redis first
result := rdb.SetIFDEQ(ctx, "counter", "150", expectedDigest, 0)
```

## Use Cases

### 1. Distributed Counter with Conflict Detection

```go
// Multiple clients can safely update a counter
currentValue := rdb.Get(ctx, "counter").Val()
currentDigest := rdb.Digest(ctx, "counter").Val()

newValue := incrementCounter(currentValue)

// Only succeeds if no other client modified it
if rdb.SetIFDEQ(ctx, "counter", newValue, currentDigest, 0).Err() == redis.Nil {
    // Retry with new value
}
```

### 2. Session Management

```go
// Delete session only if it contains expected data
sessionData := "user:1234:active"
expectedDigest := xxh3.HashString(sessionData)

deleted := rdb.DelExArgs(ctx, "session:xyz", redis.DelExArgs{
    Mode:        "IFDEQ",
    MatchDigest: expectedDigest,
}).Val()
```

### 3. Configuration Updates

```go
// Update config only if it changed
oldConfig := loadOldConfig()
oldDigest := xxh3.HashString(oldConfig)

newConfig := loadNewConfig()

// Only update if config actually changed
result := rdb.SetIFDNE(ctx, "config", newConfig, oldDigest, 0)
if result.Err() != redis.Nil {
    fmt.Println("Config updated!")
}
```

## Advantages Over WATCH/MULTI/EXEC

- **Simpler**: Single command instead of transaction
- **Faster**: No transaction overhead
- **Client-side digest**: Can calculate expected digest without fetching from Redis
- **Works with any command**: Not limited to transactions

## Learn More

- [Redis DIGEST command](https://redis.io/commands/digest/)
- [Redis SET command with IFDEQ/IFDNE](https://redis.io/commands/set/)
- [xxh3 hashing algorithm](https://github.com/Cyan4973/xxHash)
- [github.com/zeebo/xxh3](https://github.com/zeebo/xxh3)

## Comparison: XXH3 vs XXH64

**Note**: Redis uses **XXH3**, not XXH64. If you have `github.com/cespare/xxhash/v2` in your project, it implements XXH64 which produces **different hash values**. You must use `github.com/zeebo/xxh3` for Redis DIGEST operations.

See [XXHASH_LIBRARY_COMPARISON.md](../../XXHASH_LIBRARY_COMPARISON.md) for detailed comparison.

