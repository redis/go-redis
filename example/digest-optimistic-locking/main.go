package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/zeebo/xxh3"
)

func main() {
	ctx := context.Background()

	// Connect to Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	// Ping to verify connection
	if err := rdb.Ping(ctx).Err(); err != nil {
		fmt.Printf("Failed to connect to Redis: %v\n", err)
		return
	}

	fmt.Println("=== Redis Digest & Optimistic Locking Example ===")
	fmt.Println()

	// Example 1: Basic Digest Usage
	fmt.Println("1. Basic Digest Usage")
	fmt.Println("---------------------")
	basicDigestExample(ctx, rdb)
	fmt.Println()

	// Example 2: Optimistic Locking with SetIFDEQ
	fmt.Println("2. Optimistic Locking with SetIFDEQ")
	fmt.Println("------------------------------------")
	optimisticLockingExample(ctx, rdb)
	fmt.Println()

	// Example 3: Detecting Changes with SetIFDNE
	fmt.Println("3. Detecting Changes with SetIFDNE")
	fmt.Println("-----------------------------------")
	detectChangesExample(ctx, rdb)
	fmt.Println()

	// Example 4: Conditional Delete with DelExArgs
	fmt.Println("4. Conditional Delete with DelExArgs")
	fmt.Println("-------------------------------------")
	conditionalDeleteExample(ctx, rdb)
	fmt.Println()

	// Example 5: Client-Side Digest Generation
	fmt.Println("5. Client-Side Digest Generation")
	fmt.Println("---------------------------------")
	clientSideDigestExample(ctx, rdb)
	fmt.Println()

	fmt.Println("=== All examples completed successfully! ===")
}

// basicDigestExample demonstrates getting a digest from Redis
func basicDigestExample(ctx context.Context, rdb *redis.Client) {
	// Set a value
	key := "user:1000:name"
	value := "Alice"
	rdb.Set(ctx, key, value, 0)

	// Get the digest
	digest := rdb.Digest(ctx, key).Val()

	fmt.Printf("Key: %s\n", key)
	fmt.Printf("Value: %s\n", value)
	fmt.Printf("Digest: %d (0x%016x)\n", digest, digest)

	// Verify with client-side calculation
	clientDigest := xxh3.HashString(value)
	fmt.Printf("Client-calculated digest: %d (0x%016x)\n", clientDigest, clientDigest)

	if digest == clientDigest {
		fmt.Println("✓ Digests match!")
	}
}

// optimisticLockingExample demonstrates using SetIFDEQ for optimistic locking
func optimisticLockingExample(ctx context.Context, rdb *redis.Client) {
	key := "counter"

	// Initial value
	rdb.Set(ctx, key, "100", 0)
	fmt.Printf("Initial value: %s\n", rdb.Get(ctx, key).Val())

	// Get current digest
	currentDigest := rdb.Digest(ctx, key).Val()
	fmt.Printf("Current digest: 0x%016x\n", currentDigest)

	// Simulate some processing time
	time.Sleep(100 * time.Millisecond)

	// Try to update only if value hasn't changed (digest matches)
	newValue := "150"
	result := rdb.SetIFDEQ(ctx, key, newValue, currentDigest, 0)

	if result.Err() == redis.Nil {
		fmt.Println("✗ Update failed: value was modified by another client")
	} else if result.Err() != nil {
		fmt.Printf("✗ Error: %v\n", result.Err())
	} else {
		fmt.Printf("✓ Update successful! New value: %s\n", rdb.Get(ctx, key).Val())
	}

	// Try again with wrong digest (simulating concurrent modification)
	wrongDigest := uint64(12345)
	result = rdb.SetIFDEQ(ctx, key, "200", wrongDigest, 0)

	if result.Err() == redis.Nil {
		fmt.Println("✓ Correctly rejected update with wrong digest")
	}
}

// detectChangesExample demonstrates using SetIFDNE to detect if a value changed
func detectChangesExample(ctx context.Context, rdb *redis.Client) {
	key := "config:version"

	// Set initial value
	oldValue := "v1.0.0"
	rdb.Set(ctx, key, oldValue, 0)
	fmt.Printf("Initial value: %s\n", oldValue)

	// Calculate digest of a DIFFERENT value (what we expect it NOT to be)
	unwantedValue := "v0.9.0"
	unwantedDigest := xxh3.HashString(unwantedValue)
	fmt.Printf("Unwanted value digest: 0x%016x\n", unwantedDigest)

	// Update to new value only if current value is NOT the unwanted value
	// (i.e., only if digest does NOT match unwantedDigest)
	newValue := "v2.0.0"
	result := rdb.SetIFDNE(ctx, key, newValue, unwantedDigest, 0)

	if result.Err() == redis.Nil {
		fmt.Println("✗ Current value matches unwanted value (digest matches)")
	} else if result.Err() != nil {
		fmt.Printf("✗ Error: %v\n", result.Err())
	} else {
		fmt.Printf("✓ Current value is different from unwanted value! Updated to: %s\n", rdb.Get(ctx, key).Val())
	}

	// Try to update again, but this time the digest matches current value (should fail)
	currentDigest := rdb.Digest(ctx, key).Val()
	result = rdb.SetIFDNE(ctx, key, "v3.0.0", currentDigest, 0)

	if result.Err() == redis.Nil {
		fmt.Println("✓ Correctly rejected: current value matches the digest (IFDNE failed)")
	}
}

// conditionalDeleteExample demonstrates using DelExArgs with digest
func conditionalDeleteExample(ctx context.Context, rdb *redis.Client) {
	key := "session:abc123"
	value := "user_data_here"

	// Set a value
	rdb.Set(ctx, key, value, 0)
	fmt.Printf("Created session: %s\n", key)

	// Calculate expected digest
	expectedDigest := xxh3.HashString(value)
	fmt.Printf("Expected digest: 0x%016x\n", expectedDigest)

	// Try to delete with wrong digest (should fail)
	wrongDigest := uint64(99999)
	deleted := rdb.DelExArgs(ctx, key, redis.DelExArgs{
		Mode:        "IFDEQ",
		MatchDigest: wrongDigest,
	}).Val()

	if deleted == 0 {
		fmt.Println("✓ Correctly refused to delete (wrong digest)")
	}

	// Delete with correct digest (should succeed)
	deleted = rdb.DelExArgs(ctx, key, redis.DelExArgs{
		Mode:        "IFDEQ",
		MatchDigest: expectedDigest,
	}).Val()

	if deleted == 1 {
		fmt.Println("✓ Successfully deleted with correct digest")
	}

	// Verify deletion
	exists := rdb.Exists(ctx, key).Val()
	if exists == 0 {
		fmt.Println("✓ Session deleted")
	}
}

// clientSideDigestExample demonstrates calculating digests without fetching from Redis
func clientSideDigestExample(ctx context.Context, rdb *redis.Client) {
	key := "product:1001:price"

	// Scenario: We know the expected current value
	expectedCurrentValue := "29.99"
	newValue := "24.99"

	// Set initial value
	rdb.Set(ctx, key, expectedCurrentValue, 0)
	fmt.Printf("Current price: $%s\n", expectedCurrentValue)

	// Calculate digest client-side (no need to fetch from Redis!)
	expectedDigest := xxh3.HashString(expectedCurrentValue)
	fmt.Printf("Expected digest (calculated client-side): 0x%016x\n", expectedDigest)

	// Update price only if it matches our expectation
	result := rdb.SetIFDEQ(ctx, key, newValue, expectedDigest, 0)

	if result.Err() == redis.Nil {
		fmt.Println("✗ Price was already changed by someone else")
		actualValue := rdb.Get(ctx, key).Val()
		fmt.Printf("  Actual current price: $%s\n", actualValue)
	} else if result.Err() != nil {
		fmt.Printf("✗ Error: %v\n", result.Err())
	} else {
		fmt.Printf("✓ Price updated successfully to $%s\n", newValue)
	}

	// Demonstrate with binary data
	fmt.Println("\nBinary data example:")
	binaryKey := "image:thumbnail"
	binaryData := []byte{0xFF, 0xD8, 0xFF, 0xE0} // JPEG header

	rdb.Set(ctx, binaryKey, binaryData, 0)

	// Calculate digest for binary data
	binaryDigest := xxh3.Hash(binaryData)
	fmt.Printf("Binary data digest: 0x%016x\n", binaryDigest)

	// Verify it matches Redis
	redisDigest := rdb.Digest(ctx, binaryKey).Val()
	if binaryDigest == redisDigest {
		fmt.Println("✓ Binary digest matches!")
	}
}

