// This example demonstrates the zero-copy buffer operations GetToBuffer and
// SetFromBuffer. These methods allow reading and writing Redis values directly
// from/to user-provided byte buffers, avoiding intermediate allocations.
//
// It also shows how these zero-copy commands work with pipelining for
// batching multiple operations in a single round-trip, including reading
// multiple keys into a single shared buffer.
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

	// --- Example 6: SetFromBuffer + GetToBuffer in a pipeline ---
	fmt.Println("\n=== Example 6: SetFromBuffer + GetToBuffer in a pipeline ===")

	// Both SetFromBuffer and GetToBuffer work in pipelines. All commands
	// are sent in one round-trip and replies are read back sequentially.
	//
	// IMPORTANT: Because GetToBuffer (ZeroCopyStringCmd) has NoRetry() == true,
	// the entire pipeline will NOT be retried on failure. This prevents
	// data corruption from partial reads into the buffer.

	pipe := rdb.Pipeline()

	// Queue two SetFromBuffer commands — these write directly from our buffers.
	data1 := []byte("pipeline-value-one")
	data2 := []byte("pipeline-value-two")
	setCmd1 := pipe.SetFromBuffer(ctx, "zerocopy:pipe:1", data1)
	setCmd2 := pipe.SetFromBuffer(ctx, "zerocopy:pipe:2", data2)

	// Queue two GetToBuffer commands — each reads into its own pre-allocated buffer.
	readBuf1 := make([]byte, 64)
	readBuf2 := make([]byte, 64)
	getCmd1 := pipe.GetToBuffer(ctx, "zerocopy:pipe:1", readBuf1)
	getCmd2 := pipe.GetToBuffer(ctx, "zerocopy:pipe:2", readBuf2)

	// Execute all 4 commands in a single round-trip.
	_, err := pipe.Exec(ctx)
	if err != nil {
		log.Fatalf("pipeline exec failed: %v", err)
	}
	defer rdb.Del(ctx, "zerocopy:pipe:1", "zerocopy:pipe:2")

	fmt.Printf("SET pipe:1 -> %v\n", setCmd1.Val())
	fmt.Printf("SET pipe:2 -> %v\n", setCmd2.Val())
	fmt.Printf("GET pipe:1 -> %d bytes: %q\n", getCmd1.Val(), string(getCmd1.Bytes()))
	fmt.Printf("GET pipe:2 -> %d bytes: %q\n", getCmd2.Val(), string(getCmd2.Bytes()))

	// --- Example 7: Pipeline GET of multiple keys into a single shared buffer ---
	fmt.Println("\n=== Example 7: Pipeline GET into a single shared buffer ===")

	// This demonstrates reading multiple keys in a single pipeline round-trip,
	// with each value landing in its own slot within one pre-allocated buffer.
	// This is useful when you know the max size of each value upfront and want
	// to minimize both allocations and round-trips.

	pipelineKeys := []string{"zerocopy:pipe:slot:0", "zerocopy:pipe:slot:1", "zerocopy:pipe:slot:2"}
	pipelineValues := []string{"alpha", "beta!", "gamma"}
	const pipeSlotSize = 8

	// Write all keys in a pipeline using SetFromBuffer.
	pipe = rdb.Pipeline()
	for i, v := range pipelineValues {
		pipe.SetFromBuffer(ctx, pipelineKeys[i], []byte(v))
	}
	_, err = pipe.Exec(ctx)
	if err != nil {
		log.Fatalf("pipeline SET exec failed: %v", err)
	}
	for _, k := range pipelineKeys {
		defer rdb.Del(ctx, k)
	}

	// Now GET all keys in a single pipeline, reading each value into a slot
	// of one shared buffer. The buffer is divided into equal-sized slots.
	sharedBuf := make([]byte, len(pipelineKeys)*pipeSlotSize)
	getCmds := make([]*redis.ZeroCopyStringCmd, len(pipelineKeys))

	pipe = rdb.Pipeline()
	for i, key := range pipelineKeys {
		slot := sharedBuf[i*pipeSlotSize : (i+1)*pipeSlotSize]
		getCmds[i] = pipe.GetToBuffer(ctx, key, slot)
	}

	// Single round-trip for all GETs.
	_, err = pipe.Exec(ctx)
	if err != nil {
		log.Fatalf("pipeline GET exec failed: %v", err)
	}

	fmt.Printf("Shared buffer (%d bytes): %q\n", len(sharedBuf), string(sharedBuf))
	for i, cmd := range getCmds {
		start := i * pipeSlotSize
		end := start + cmd.Val()
		fmt.Printf("  %s -> slot [%d:%d] = %q (%d/%d bytes used)\n",
			pipelineKeys[i], start, end, string(sharedBuf[start:end]), cmd.Val(), pipeSlotSize)
	}

	fmt.Println("\nDone!")
}
