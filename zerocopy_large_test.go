package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestZeroCopyLargeData tests zero-copy operations with large data payloads.
// These tests use a standalone Redis client with extended timeouts and don't
// depend on the Ginkgo test suite's BeforeSuite cluster setup.
//
// Note: Redis has a hard limit of int32_max - 1 (~2GB) for bulk string length,
// regardless of the proto-max-bulk-len configuration. This is because Redis
// internally uses a signed 32-bit integer for bulk string lengths.

func TestZeroCopyLargeData_500MB(t *testing.T) {
	const dataSize = 500 * 1024 * 1024 // 500MB

	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		ReadTimeout:  5 * time.Minute,
		WriteTimeout: 5 * time.Minute,
	})
	defer client.Close()

	// Verify connection
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("Failed to connect to Redis: %v", err)
	}

	t.Log("Creating 500MB test data...")
	hugeData := make([]byte, dataSize)
	for i := range hugeData {
		hugeData[i] = byte(i % 256)
	}

	// Set using SetFromBuffer
	t.Log("Writing 500MB to Redis using SetFromBuffer...")
	err := client.SetFromBuffer(ctx, "zerocopy_500mb_key", hugeData).Err()
	if err != nil {
		t.Fatalf("SetFromBuffer failed: %v", err)
	}
	t.Log("500MB written successfully")

	// Get using GetToBuffer
	t.Log("Reading 500MB from Redis using GetToBuffer...")
	readBuf := make([]byte, dataSize+1000)
	cmd := client.GetToBuffer(ctx, "zerocopy_500mb_key", readBuf)
	if cmd.Err() != nil {
		t.Fatalf("GetToBuffer failed: %v", cmd.Err())
	}

	n := cmd.Val()
	if n != dataSize {
		t.Fatalf("Expected %d bytes, got %d", dataSize, n)
	}

	// Verify data integrity
	retrievedData := cmd.Bytes()
	if len(retrievedData) != dataSize {
		t.Fatalf("Expected %d bytes in result, got %d", dataSize, len(retrievedData))
	}

	// Spot checks
	for i := 0; i < 1000; i++ {
		if retrievedData[i] != hugeData[i] {
			t.Fatalf("Mismatch at beginning, index %d", i)
		}
	}

	midPoint := dataSize / 2
	for i := 0; i < 1000; i++ {
		if retrievedData[midPoint+i] != hugeData[midPoint+i] {
			t.Fatalf("Mismatch at middle, index %d", midPoint+i)
		}
	}

	for i := 0; i < 1000; i++ {
		idx := dataSize - 1000 + i
		if retrievedData[idx] != hugeData[idx] {
			t.Fatalf("Mismatch at end, index %d", idx)
		}
	}

	// Cleanup
	client.Del(ctx, "zerocopy_500mb_key")

	t.Log("500MB zero-copy round-trip test PASSED!")
}

func TestZeroCopyLargeData_1GB(t *testing.T) {
	const dataSize = 1 * 1024 * 1024 * 1024 // 1GB

	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		ReadTimeout:  10 * time.Minute,
		WriteTimeout: 10 * time.Minute,
		PoolSize:     1,
	})
	defer client.Close()

	// Verify connection
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("Failed to connect to Redis: %v", err)
	}

	// Configure Redis for large bulk strings
	if err := client.ConfigSet(ctx, "proto-max-bulk-len", "2147483647").Err(); err != nil {
		t.Fatalf("Failed to set proto-max-bulk-len: %v", err)
	}
	if err := client.ConfigSet(ctx, "client-query-buffer-limit", "2147483647").Err(); err != nil {
		t.Fatalf("Failed to set client-query-buffer-limit: %v", err)
	}

	t.Log("Creating 1GB test data...")
	hugeData := make([]byte, dataSize)
	for i := range hugeData {
		hugeData[i] = byte(i % 256)
	}

	// Set using SetFromBuffer
	t.Log("Writing 1GB to Redis using SetFromBuffer...")
	err := client.SetFromBuffer(ctx, "zerocopy_1gb_key", hugeData).Err()
	if err != nil {
		t.Fatalf("SetFromBuffer failed: %v", err)
	}
	t.Log("1GB written successfully")

	// Get using GetToBuffer
	t.Log("Reading 1GB from Redis using GetToBuffer...")
	readBuf := make([]byte, dataSize+1000)
	cmd := client.GetToBuffer(ctx, "zerocopy_1gb_key", readBuf)
	if cmd.Err() != nil {
		t.Fatalf("GetToBuffer failed: %v", cmd.Err())
	}

	n := cmd.Val()
	if n != dataSize {
		t.Fatalf("Expected %d bytes, got %d", dataSize, n)
	}

	// Verify data integrity at 256MB intervals
	retrievedData := cmd.Bytes()
	for i := 0; i < 4; i++ {
		offset := i * 256 * 1024 * 1024
		for j := 0; j < 1000; j++ {
			if retrievedData[offset+j] != hugeData[offset+j] {
				t.Fatalf("Mismatch at %dMB, index %d", i*256, offset+j)
			}
		}
		t.Logf("Verified %dMB boundary", i*256)
	}

	// Cleanup
	client.Del(ctx, "zerocopy_1gb_key")

	t.Log("1GB zero-copy round-trip test PASSED!")
}

func TestZeroCopyLargeData_MaxSize(t *testing.T) {
	// Test at exact theoretical limit (verified working)
	// Redis closes connection when total RESP message exceeds int32 max
	// RESP overhead for this size = 15 bytes ($2147483632\r\n + \r\n)
	// Total = 2147483632 + 15 = 2147483647 = int32 max exactly
	const dataSize = 2147483632 // Maximum working size

	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:            "localhost:6379",
		ReadTimeout:     15 * time.Minute,
		WriteTimeout:    15 * time.Minute,
		PoolSize:        1, // Single connection to avoid pooling issues
		MinIdleConns:    1, // Keep the connection alive
		ConnMaxIdleTime: 30 * time.Minute,
	})
	defer client.Close()

	// Verify connection and check protocol
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("Failed to connect to Redis: %v", err)
	}

	// Check which protocol we're using
	info := client.Info(ctx, "server")
	t.Logf("Server info (first 200 chars): %s...", info.Val()[:min(200, len(info.Val()))])

	// Need to configure Redis for large bulk strings
	if err := client.ConfigSet(ctx, "proto-max-bulk-len", "2147483647").Err(); err != nil {
		t.Logf("Warning: Could not set proto-max-bulk-len: %v", err)
		t.Log("Skipping max size test - requires proto-max-bulk-len >= 2GB")
		t.Skip()
	}

	if err := client.ConfigSet(ctx, "client-query-buffer-limit", "2147483647").Err(); err != nil {
		t.Logf("Warning: Could not set client-query-buffer-limit: %v", err)
	}

	t.Logf("Testing with %d bytes (~2GB)", dataSize)

	t.Log("Creating ~2GB test data...")
	hugeData := make([]byte, dataSize)
	for i := range hugeData {
		hugeData[i] = byte(i % 256)
	}

	// Set using SetFromBuffer
	t.Log("Writing ~2GB to Redis using SetFromBuffer...")
	err := client.SetFromBuffer(ctx, "zerocopy_max_key", hugeData).Err()
	if err != nil {
		t.Fatalf("SetFromBuffer failed: %v", err)
	}
	t.Log("~2GB written successfully")

	// Check connection status with a simple command
	strLen := client.StrLen(ctx, "zerocopy_max_key")
	if strLen.Err() != nil {
		t.Fatalf("StrLen after SetFromBuffer failed: %v", strLen.Err())
	}
	t.Logf("Key length after SET: %d", strLen.Val())

	// Get using GetToBuffer
	t.Log("Reading ~2GB from Redis using GetToBuffer...")
	readBuf := make([]byte, dataSize+1000)
	cmd := client.GetToBuffer(ctx, "zerocopy_max_key", readBuf)
	if cmd.Err() != nil {
		t.Fatalf("GetToBuffer failed: %v", cmd.Err())
	}

	n := cmd.Val()
	if n != dataSize {
		t.Fatalf("Expected %d bytes, got %d", dataSize, n)
	}

	// Sample verification at intervals
	retrievedData := cmd.Bytes()
	checkInterval := 256 * 1024 * 1024 // 256MB
	numChecks := dataSize / checkInterval
	for i := 0; i < numChecks; i++ {
		offset := i * checkInterval
		for j := 0; j < 1000; j++ {
			if retrievedData[offset+j] != hugeData[offset+j] {
				t.Fatalf("Mismatch at %dMB, index %d", i*256, offset+j)
			}
		}
		t.Logf("Verified %dMB boundary", i*256)
	}

	// Cleanup
	client.Del(ctx, "zerocopy_max_key")

	t.Log("Max size (~2GB) zero-copy round-trip test PASSED!")
}

