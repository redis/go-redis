package redis_test

// Validation the ship script's Ginkgo gate did NOT provide: that gate needs the
// docker compose stack (6390) and failed in BeforeSuite on master and on this
// change alike, so zero tests ran. This exercises the co-allocated GET/SET
// argument buffers against a plain local server instead.
//
// What this proves: every SET form still round-trips. The inline buffer is five
// slots, SET's longest form, and appending past it must fall back to the heap
// rather than corrupt or truncate the args.
// The allocation count itself is NOT asserted here: the exported constructors
// take variadic args and still allocate the slice, so measuring them proves
// nothing about the co-allocated path, and the unexported inline constructors
// need an internal test. The figures live in the commit message and the
// benchmark findings instead.

import (
	"context"
	"testing"
	"time"

	redis "github.com/redis/go-redis/v9"
)

func TestInlineArgsSetGetRoundTrip(t *testing.T) {
	ctx := context.Background()
	cl := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer cl.Close()
	if err := cl.Ping(ctx).Err(); err != nil {
		t.Skipf("no local redis: %v", err)
	}

	// Each case pushes a different number of args through the 5-slot SET
	// buffer; the KeepTTL and expiry forms are the ones that use the tail
	// slots, and SetArgs goes past them onto the heap.
	t.Run("plain", func(t *testing.T) {
		if err := cl.Set(ctx, "ia:plain", "v1", 0).Err(); err != nil {
			t.Fatal(err)
		}
		if got, err := cl.Get(ctx, "ia:plain").Result(); err != nil || got != "v1" {
			t.Fatalf("got %q err %v, want v1", got, err)
		}
	})

	t.Run("with expiry", func(t *testing.T) {
		if err := cl.Set(ctx, "ia:exp", "v2", time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
		if got, err := cl.Get(ctx, "ia:exp").Result(); err != nil || got != "v2" {
			t.Fatalf("got %q err %v, want v2", got, err)
		}
		ttl, err := cl.TTL(ctx, "ia:exp").Result()
		if err != nil || ttl <= 0 {
			t.Fatalf("expiry did not survive the inline buffer: ttl %v err %v", ttl, err)
		}
	})

	t.Run("keepttl", func(t *testing.T) {
		if err := cl.Set(ctx, "ia:keep", "v3", time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
		if err := cl.Set(ctx, "ia:keep", "v4", redis.KeepTTL).Err(); err != nil {
			t.Fatal(err)
		}
		if got, _ := cl.Get(ctx, "ia:keep").Result(); got != "v4" {
			t.Fatalf("got %q, want v4", got)
		}
		if ttl, _ := cl.TTL(ctx, "ia:keep").Result(); ttl <= 0 {
			t.Fatalf("KeepTTL lost the ttl: %v", ttl)
		}
	})

	// Past the inline buffer: must grow onto the heap, not truncate.
	t.Run("setargs beyond the buffer", func(t *testing.T) {
		err := cl.SetArgs(ctx, "ia:args", "v5", redis.SetArgs{
			TTL:     time.Minute,
			Mode:    "NX",
			KeepTTL: false,
			Get:     false,
		}).Err()
		if err != nil && err != redis.Nil {
			t.Fatal(err)
		}
		if got, _ := cl.Get(ctx, "ia:args").Result(); got != "v5" {
			t.Fatalf("got %q, want v5", got)
		}
	})

	// A value long enough to rule out any buffer-size confusion.
	t.Run("large value", func(t *testing.T) {
		big := make([]byte, 64*1024)
		for i := range big {
			big[i] = byte('a' + i%26)
		}
		if err := cl.Set(ctx, "ia:big", big, 0).Err(); err != nil {
			t.Fatal(err)
		}
		got, err := cl.Get(ctx, "ia:big").Bytes()
		if err != nil {
			t.Fatal(err)
		}
		if len(got) != len(big) || string(got) != string(big) {
			t.Fatalf("large value round-trip differs: got %d bytes, want %d", len(got), len(big))
		}
	})

	cl.Del(ctx, "ia:plain", "ia:exp", "ia:keep", "ia:args", "ia:big")
}
