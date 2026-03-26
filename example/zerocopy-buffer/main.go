// This example demonstrates the zero-copy buffer operations GetToBuffer and
// SetFromBuffer. These methods allow reading and writing Redis values directly
// from/to user-provided byte buffers, avoiding intermediate allocations.
//
// This is especially useful when working with large values (MB+) where
// reducing memory allocations and copies is important.
//
// Prerequisites:
//   - A running Redis server on localhost:6379
//
// Run:
//
//	go run .
package main

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	// Clean up example keys on exit.
	defer rdb.Del(ctx, "zerocopy:greeting", "zerocopy:largeblob")

	// --- Example 1: Basic SetFromBuffer / GetToBuffer round-trip ---
	fmt.Println("=== Example 1: Basic round-trip ===")

	// Write data from a buffer directly to Redis.
	writeData := []byte("Hello, zero-copy world!")
	if err := rdb.SetFromBuffer(ctx, "zerocopy:greeting", writeData).Err(); err != nil {
		log.Fatalf("SetFromBuffer failed: %v", err)
	}
	fmt.Printf("SET zerocopy:greeting (%d bytes)\n", len(writeData))

	// Read data from Redis directly into a pre-allocated buffer.
	readBuf := make([]byte, 100)
	cmd := rdb.GetToBuffer(ctx, "zerocopy:greeting", readBuf)
	if err := cmd.Err(); err != nil {
		log.Fatalf("GetToBuffer failed: %v", err)
	}

	n := cmd.Val() // number of bytes read
	fmt.Printf("GET zerocopy:greeting -> %d bytes: %q\n", n, string(cmd.Bytes()))

	// --- Example 2: Large binary data (1 MB) ---
	fmt.Println("\n=== Example 2: Large binary data (1 MB) ===")

	const blobSize = 1 * 1024 * 1024 // 1 MB
	blob := make([]byte, blobSize)
	if _, err := rand.Read(blob); err != nil {
		log.Fatalf("failed to generate random data: %v", err)
	}

	// Write 1 MB directly from the buffer — no intermediate string conversion.
	if err := rdb.SetFromBuffer(ctx, "zerocopy:largeblob", blob).Err(); err != nil {
		log.Fatalf("SetFromBuffer (1MB) failed: %v", err)
	}
	fmt.Printf("SET zerocopy:largeblob (%d bytes)\n", blobSize)

	// Read 1 MB directly into a pre-allocated buffer — no extra allocations.
	largeBuf := make([]byte, blobSize+64) // slightly larger than needed
	cmd = rdb.GetToBuffer(ctx, "zerocopy:largeblob", largeBuf)
	if err := cmd.Err(); err != nil {
		log.Fatalf("GetToBuffer (1MB) failed: %v", err)
	}
	fmt.Printf("GET zerocopy:largeblob -> %d bytes\n", cmd.Val())

	// Verify data integrity.
	retrieved := cmd.Bytes()
	if len(retrieved) != blobSize {
		log.Fatalf("size mismatch: expected %d, got %d", blobSize, len(retrieved))
	}
	for i := range blob {
		if blob[i] != retrieved[i] {
			log.Fatalf("data mismatch at byte %d", i)
		}
	}
	fmt.Println("Data integrity verified ✓")

	// --- Example 3: Handling non-existent keys ---
	fmt.Println("\n=== Example 3: Handling non-existent keys ===")

	buf := make([]byte, 64)
	cmd = rdb.GetToBuffer(ctx, "zerocopy:nonexistent", buf)
	if cmd.Err() == redis.Nil {
		fmt.Println("Key does not exist (redis.Nil) — as expected")
	} else if cmd.Err() != nil {
		log.Fatalf("unexpected error: %v", cmd.Err())
	}

	// --- Example 4: Buffer reuse across multiple reads ---
	fmt.Println("\n=== Example 4: Buffer reuse ===")

	// You can reuse the same buffer for multiple reads to avoid allocations.
	reuseBuf := make([]byte, 256)

	for i := 0; i < 3; i++ {
		key := fmt.Sprintf("zerocopy:reuse:%d", i)
		value := fmt.Sprintf("value-%d", i)

		_ = rdb.SetFromBuffer(ctx, key, []byte(value)).Err()
		defer rdb.Del(ctx, key)

		cmd = rdb.GetToBuffer(ctx, key, reuseBuf)
		if err := cmd.Err(); err != nil {
			log.Fatalf("GetToBuffer failed for %s: %v", key, err)
		}
		fmt.Printf("  %s -> %q (%d bytes)\n", key, string(cmd.Bytes()), cmd.Val())
	}

	fmt.Println("\nDone!")
}

