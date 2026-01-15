package redis_test

import (
	"context"
	"os"
	"strconv"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9"
)

func init() {
	// Initialize RedisVersion from environment variable for regular Go tests
	// (Ginkgo tests initialize this in BeforeSuite)
	if version := os.Getenv("REDIS_VERSION"); version != "" {
		if v, err := strconv.ParseFloat(strings.Trim(version, "\""), 64); err == nil && v > 0 {
			RedisVersion = v
		}
	}
}

// skipIfRedisBelow84 checks if Redis version is below 8.4 and skips the test if so
func skipIfRedisBelow84(t *testing.T) {
	if RedisVersion < 8.4 {
		t.Skipf("Skipping test: Redis version %.1f < 8.4 (DIGEST command requires Redis 8.4+)", RedisVersion)
	}
}

// TestDigestBasic validates that the Digest command returns a uint64 value
func TestDigestBasic(t *testing.T) {
	skipIfRedisBelow84(t)

	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	client.Del(ctx, "digest-test-key")

	// Set a value
	err := client.Set(ctx, "digest-test-key", "testvalue", 0).Err()
	if err != nil {
		t.Fatalf("Failed to set value: %v", err)
	}

	// Get digest
	digestCmd := client.Digest(ctx, "digest-test-key")
	if err := digestCmd.Err(); err != nil {
		t.Fatalf("Failed to get digest: %v", err)
	}

	digest := digestCmd.Val()
	if digest == 0 {
		t.Error("Digest should not be zero for non-empty value")
	}

	t.Logf("Digest for 'testvalue': %d (0x%016x)", digest, digest)

	// Verify same value produces same digest
	digest2 := client.Digest(ctx, "digest-test-key").Val()
	if digest != digest2 {
		t.Errorf("Same value should produce same digest: %d != %d", digest, digest2)
	}

	client.Del(ctx, "digest-test-key")
}

// TestSetIFDEQWithDigest validates the SetIFDEQ command works with digests
func TestSetIFDEQWithDigest(t *testing.T) {
	skipIfRedisBelow84(t)

	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	client.Del(ctx, "cas-test-key")

	// Set initial value
	initialValue := "initial-value"
	err := client.Set(ctx, "cas-test-key", initialValue, 0).Err()
	if err != nil {
		t.Fatalf("Failed to set initial value: %v", err)
	}

	// Get current digest
	correctDigest := client.Digest(ctx, "cas-test-key").Val()
	wrongDigest := uint64(12345) // arbitrary wrong digest

	// Test 1: SetIFDEQ with correct digest should succeed
	result := client.SetIFDEQ(ctx, "cas-test-key", "new-value", correctDigest, 0)
	if err := result.Err(); err != nil {
		t.Errorf("SetIFDEQ with correct digest failed: %v", err)
	} else {
		t.Logf("✓ SetIFDEQ with correct digest succeeded")
	}

	// Verify value was updated
	val, err := client.Get(ctx, "cas-test-key").Result()
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}
	if val != "new-value" {
		t.Errorf("Value not updated: got %q, want %q", val, "new-value")
	}

	// Test 2: SetIFDEQ with wrong digest should fail
	result = client.SetIFDEQ(ctx, "cas-test-key", "another-value", wrongDigest, 0)
	if result.Err() != redis.Nil {
		t.Errorf("SetIFDEQ with wrong digest should return redis.Nil, got: %v", result.Err())
	} else {
		t.Logf("✓ SetIFDEQ with wrong digest correctly failed")
	}

	// Verify value was NOT updated
	val, err = client.Get(ctx, "cas-test-key").Result()
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}
	if val != "new-value" {
		t.Errorf("Value should not have changed: got %q, want %q", val, "new-value")
	}

	client.Del(ctx, "cas-test-key")
}

// TestSetIFDNEWithDigest validates the SetIFDNE command works with digests
func TestSetIFDNEWithDigest(t *testing.T) {
	skipIfRedisBelow84(t)

	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	client.Del(ctx, "cad-test-key")

	// Set initial value
	initialValue := "initial-value"
	err := client.Set(ctx, "cad-test-key", initialValue, 0).Err()
	if err != nil {
		t.Fatalf("Failed to set initial value: %v", err)
	}

	// Use an arbitrary different digest
	wrongDigest := uint64(99999) // arbitrary different digest

	// Test 1: SetIFDNE with different digest should succeed
	result := client.SetIFDNE(ctx, "cad-test-key", "new-value", wrongDigest, 0)
	if err := result.Err(); err != nil {
		t.Errorf("SetIFDNE with different digest failed: %v", err)
	} else {
		t.Logf("✓ SetIFDNE with different digest succeeded")
	}

	// Verify value was updated
	val, err := client.Get(ctx, "cad-test-key").Result()
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}
	if val != "new-value" {
		t.Errorf("Value not updated: got %q, want %q", val, "new-value")
	}

	// Test 2: SetIFDNE with matching digest should fail
	newDigest := client.Digest(ctx, "cad-test-key").Val()
	result = client.SetIFDNE(ctx, "cad-test-key", "another-value", newDigest, 0)
	if result.Err() != redis.Nil {
		t.Errorf("SetIFDNE with matching digest should return redis.Nil, got: %v", result.Err())
	} else {
		t.Logf("✓ SetIFDNE with matching digest correctly failed")
	}

	// Verify value was NOT updated
	val, err = client.Get(ctx, "cad-test-key").Result()
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}
	if val != "new-value" {
		t.Errorf("Value should not have changed: got %q, want %q", val, "new-value")
	}

	client.Del(ctx, "cad-test-key")
}

// TestDelExArgsWithDigest validates DelExArgs works with digest matching
func TestDelExArgsWithDigest(t *testing.T) {
	skipIfRedisBelow84(t)

	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	client.Del(ctx, "del-test-key")

	// Set a value
	value := "delete-me"
	err := client.Set(ctx, "del-test-key", value, 0).Err()
	if err != nil {
		t.Fatalf("Failed to set value: %v", err)
	}

	// Get correct digest
	correctDigest := client.Digest(ctx, "del-test-key").Val()
	wrongDigest := uint64(54321)

	// Test 1: Delete with wrong digest should fail
	deleted := client.DelExArgs(ctx, "del-test-key", redis.DelExArgs{
		Mode:        "IFDEQ",
		MatchDigest: wrongDigest,
	}).Val()

	if deleted != 0 {
		t.Errorf("Delete with wrong digest should not delete: got %d deletions", deleted)
	} else {
		t.Logf("✓ DelExArgs with wrong digest correctly refused to delete")
	}

	// Verify key still exists
	exists := client.Exists(ctx, "del-test-key").Val()
	if exists != 1 {
		t.Errorf("Key should still exist after failed delete")
	}

	// Test 2: Delete with correct digest should succeed
	deleted = client.DelExArgs(ctx, "del-test-key", redis.DelExArgs{
		Mode:        "IFDEQ",
		MatchDigest: correctDigest,
	}).Val()

	if deleted != 1 {
		t.Errorf("Delete with correct digest should delete: got %d deletions", deleted)
	} else {
		t.Logf("✓ DelExArgs with correct digest successfully deleted")
	}

	// Verify key was deleted
	exists = client.Exists(ctx, "del-test-key").Val()
	if exists != 0 {
		t.Errorf("Key should not exist after successful delete")
	}
}
