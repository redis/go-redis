package main

import (
	"context"
	"fmt"
	"log"

	"github.com/redis/go-redis/v9"
)

// runPackingExamples shows how to pack multiple GET results into one buffer
// and the hazard of reusing a buffer that earlier *ZeroCopyStringCmd values
// still reference.
func runPackingExamples(ctx context.Context, rdb *redis.Client) {
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
	cmd := rdb.GetToBuffer(ctx, "zerocopy:pack:3", bigBuf)
	if err := cmd.Err(); err != nil {
		log.Fatalf("GetToBuffer failed: %v", err)
	}

	fmt.Printf("  Read %d bytes into bigBuf (cap %d)\n", cmd.Val(), len(bigBuf))
	fmt.Printf("  cmd.Bytes() = %q\n", string(cmd.Bytes()))
	fmt.Printf("  Full bigBuf = %q  (stale data after byte %d)\n", string(bigBuf), cmd.Val())
	fmt.Println()
	fmt.Println("     ! IMPORTANT: this reuses the previous buffer, so any earlier *ZeroCopyStringCmd")
	fmt.Println("     ! that still references it now sees corrupted data:")
	fmt.Println()
	for i, c := range cmds {
		fmt.Printf("         zerocopy:pack:%d == cmds[%d].Bytes() = %q\n", i, i, string(c.Bytes()))
	}
}
