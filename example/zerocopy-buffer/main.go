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

	// --- Example 4: Packing multiple values into a single buffer ---
	fmt.Println("\n=== Example 4: Packing multiple values into one buffer ===")

	// We have 4 values, each gets a 7-byte slot. Total buffer = 4 * 7 = 28 bytes.
	// The last value ("s!") is only 2 bytes — this demonstrates that GetToBuffer
	// works correctly when the value is smaller than the allocated segment.
	const slotSize = 7
	values := []string{"hello!!", "world!!", "go-redi", "s!"}
	keys := make([]string, len(values))
	bigBuf := make([]byte, len(values)*slotSize) // 28 bytes

	// Write all keys.
	for i, v := range values {
		keys[i] = fmt.Sprintf("zerocopy:pack:%d", i)
		_ = rdb.SetFromBuffer(ctx, keys[i], []byte(v)).Err()
		defer rdb.Del(ctx, keys[i])
	}

	// Read each value into its 7-byte slot within the big buffer.
	// Track actual bytes read per slot to handle values shorter than slotSize.
	bytesRead := make([]int, len(values))
	cmds := make([]*redis.ZeroCopyStringCmd, len(values))
	for i, key := range keys {
		segment := bigBuf[i*slotSize : (i+1)*slotSize]
		cmds[i] = rdb.GetToBuffer(ctx, key, segment)
		if err := cmds[i].Err(); err != nil {
			log.Fatalf("GetToBuffer failed for %s: %v", key, err)
		}
		bytesRead[i] = cmds[i].Val()
		fmt.Printf("  %s -> read %d bytes into bigBuf[%d:%d]\n",
			key, cmds[i].Val(), i*slotSize, i*slotSize+cmds[i].Val())
	}

	// Print the full buffer and each slot.
	fmt.Printf("\nbigBuf (%d bytes): %q\n", len(bigBuf), string(bigBuf))
	for i := range values {
		start := i * slotSize
		end := start + bytesRead[i]
		fmt.Printf("  [%d:%d] = %q (%d/%d bytes used)\n",
			start, end, string(bigBuf[start:end]), bytesRead[i], slotSize)
	}

	// --- Example 5: Reusing the big buffer for a smaller read ---
	fmt.Println("\n=== Example 5: Reusing the big buffer ===")

	// The same 28-byte bigBuf can be reused for a single, smaller read.
	// Only the first cmd.Val() bytes are overwritten — the rest of the
	// buffer retains its previous content, so always use cmd.Bytes() or
	// bigBuf[:cmd.Val()] to get the actual data.
	cmd = rdb.GetToBuffer(ctx, "zerocopy:pack:3", bigBuf)
	if err := cmd.Err(); err != nil {
		log.Fatalf("GetToBuffer failed: %v", err)
	}

	fmt.Printf("  Read %d bytes into bigBuf (cap %d)\n", cmd.Val(), len(bigBuf))
	fmt.Printf("  cmd.Bytes() = %q\n", string(cmd.Bytes()))
	fmt.Printf("  Full bigBuf = %q  (stale data after byte %d)\n", string(bigBuf), cmd.Val())
	fmt.Printf("\n     ! IMPORTANT: This is reusing the old buffer, so an older cmd (which holds a reference to the buffer) will have its results corrupted. \n")
	fmt.Printf("     ! For example, the previous read of 'zerocopy:pack:0' is now corrupted:\n\n")
	for i, cmd := range cmds {
		fmt.Printf("         zerocopy:pack:%d == cmds[%d].Bytes() = %q\n", i, i, string(cmd.Bytes()))
	}

	fmt.Println("\nDone!")
}
