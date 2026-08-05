package main

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
)

// runPipelineExamples shows GetToBuffer and SetFromBuffer inside pipelines,
// including reading several keys into slots of one shared buffer in a single
// round-trip.
func runPipelineExamples(ctx context.Context, rdb *redis.Client) {
	// --- Example 6: SetFromBuffer + GetToBuffer in a pipeline ---
	fmt.Println("\n=== Example 6: SetFromBuffer + GetToBuffer in a pipeline ===")

	// Both SetFromBuffer and GetToBuffer work in pipelines. All commands
	// are sent in one round-trip and replies are read back sequentially.
	//
	// IMPORTANT: because GetToBuffer (ZeroCopyStringCmd) has NoRetry() == true,
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
	if _, err := pipe.Exec(ctx); err != nil {
		log.Fatalf("pipeline exec failed: %v", err)
	}
	defer rdb.Del(ctx, "zerocopy:pipe:1", "zerocopy:pipe:2")

	fmt.Printf("SET pipe:1 -> %v\n", setCmd1.Val())
	fmt.Printf("SET pipe:2 -> %v\n", setCmd2.Val())
	fmt.Printf("GET pipe:1 -> %d bytes: %q\n", getCmd1.Val(), string(getCmd1.Bytes()))
	fmt.Printf("GET pipe:2 -> %d bytes: %q\n", getCmd2.Val(), string(getCmd2.Bytes()))

	// --- Example 7: Pipeline GET of multiple keys into a single shared buffer ---
	fmt.Println("\n=== Example 7: Pipeline GET into a single shared buffer ===")

	// Reading multiple keys in a single pipeline round-trip, with each value
	// landing in its own slot of one pre-allocated buffer. Useful when the
	// caller knows the max size of each value and wants to minimize both
	// allocations and round-trips.

	pipelineKeys := []string{
		"zerocopy:pipe:slot:0",
		"zerocopy:pipe:slot:1",
		"zerocopy:pipe:slot:2",
	}
	pipelineValues := []string{"alpha", "beta!", "gamma"}
	const pipeSlotSize = 8

	// Write all keys in a pipeline using SetFromBuffer.
	pipe = rdb.Pipeline()
	for i, v := range pipelineValues {
		pipe.SetFromBuffer(ctx, pipelineKeys[i], []byte(v))
	}
	if _, err := pipe.Exec(ctx); err != nil {
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
	if _, err := pipe.Exec(ctx); err != nil {
		log.Fatalf("pipeline GET exec failed: %v", err)
	}

	fmt.Printf("Shared buffer (%d bytes): %q\n", len(sharedBuf), string(sharedBuf))
	for i, cmd := range getCmds {
		start := i * pipeSlotSize
		end := start + cmd.Val()
		fmt.Printf("  %s -> slot [%d:%d] = %q (%d/%d bytes used)\n",
			pipelineKeys[i], start, end, string(sharedBuf[start:end]), cmd.Val(), pipeSlotSize)
	}
}
