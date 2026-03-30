package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestZeroCopyLargeData_1MB tests zero-copy operations with a 1MB payload.
// This uses a standalone Redis client and doesn't depend on the Ginkgo test
// suite's BeforeSuite cluster setup.
func TestZeroCopyLargeData_1MB(t *testing.T) {
	const dataSize = 1024 * 1024 // 1MB

	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		ReadTimeout:  time.Minute,
		WriteTimeout: time.Minute,
	})
	defer client.Close()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("Failed to connect to Redis: %v", err)
	}

	data := make([]byte, dataSize)
	for i := range data {
		data[i] = byte(i % 256)
	}

	// Set using SetFromBuffer
	err := client.SetFromBuffer(ctx, "zerocopy_1mb_key", data).Err()
	if err != nil {
		t.Fatalf("SetFromBuffer failed: %v", err)
	}

	// Get using GetToBuffer
	readBuf := make([]byte, dataSize+100)
	cmd := client.GetToBuffer(ctx, "zerocopy_1mb_key", readBuf)
	if cmd.Err() != nil {
		t.Fatalf("GetToBuffer failed: %v", cmd.Err())
	}

	n := cmd.Val()
	if n != dataSize {
		t.Fatalf("Expected %d bytes, got %d", dataSize, n)
	}

	// Full data integrity check
	retrieved := cmd.Bytes()
	if len(retrieved) != dataSize {
		t.Fatalf("Expected %d bytes in result, got %d", dataSize, len(retrieved))
	}
	for i := range data {
		if data[i] != retrieved[i] {
			t.Fatalf("Data mismatch at byte %d: expected %d, got %d", i, data[i], retrieved[i])
		}
	}

	// Cleanup
	client.Del(ctx, "zerocopy_1mb_key")
}

