package redis_test

// With N full-duplex engines on one client, every reply must
// still be the reply to its own command. Mis-correlation is the failure mode
// that matters -- N engines means N independent FIFO streams, and a command
// routed to engine A whose reply is read off engine B would corrupt data
// silently rather than error.
//
// Requires a local server on 127.0.0.1:6379.

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"

	redis "github.com/redis/go-redis/v9"
)

func TestFDShardsReplyCorrelation(t *testing.T) {
	ctx := context.Background()
	cl := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer cl.Close()
	if err := cl.Ping(ctx).Err(); err != nil {
		t.Skipf("no local redis: %v", err)
	}

	shards := 8
	if v := os.Getenv("FD_SHARDS"); v != "" {
		// Overridable so the same test can run at ONE engine, which is how the
		// silent half-duplex fallback was first caught: 1 passed, 8 failed.
		shards, _ = strconv.Atoi(v)
	}
	ap, err := cl.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{
		FullDuplex:           true,
		NumShards:            shards,
		MaxConcurrentBatches: 1,
	})
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	defer ap.Close()

	// Each key's value encodes the key, so a reply landing on the wrong command
	// is caught rather than merely suspected.
	const keys = 2000
	for i := 0; i < keys; i++ {
		k := fmt.Sprintf("fdshards:{%d}", i)
		if err := ap.Set(ctx, k, fmt.Sprintf("val-%d", i), 0).Err(); err != nil {
			t.Fatalf("set %d: %v", i, err)
		}
	}

	var wg sync.WaitGroup
	errs := make(chan error, keys)
	for w := 0; w < 64; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := w; i < keys; i += 64 {
				k := fmt.Sprintf("fdshards:{%d}", i)
				want := fmt.Sprintf("val-%d", i)
				got, err := ap.Get(ctx, k).Result()
				if err != nil {
					errs <- fmt.Errorf("get %s: %w", k, err)
					return
				}
				if got != want {
					errs <- fmt.Errorf("MIS-CORRELATED: %s got %q want %q", k, got, want)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	close(errs)
	n := 0
	for e := range errs {
		if n < 5 {
			t.Error(e)
		}
		n++
	}
	if n > 0 {
		t.Fatalf("%d correlation failures across %d keys", n, keys)
	}

	// Per-caller ordering on the deferred face: write then read the SAME key
	// without waiting in between. Key routing must put both on one engine.
	for i := 0; i < 500; i++ {
		k := fmt.Sprintf("fdshards:order:{%d}", i)
		setF := ap.Set(ctx, k, "second", 0)
		getF := ap.Get(ctx, k)
		if err := setF.Err(); err != nil {
			t.Fatalf("ordered set: %v", err)
		}
		got, err := getF.Result()
		if err != nil {
			t.Fatalf("ordered get: %v", err)
		}
		if got != "second" {
			t.Fatalf("ORDER VIOLATION at %s: got %q, want the value written just before it", k, got)
		}
	}
	t.Logf("ok: %d keys correlated, 500 same-key write-then-read ordered, %d engines", keys, shards)
}

// The two-sited rule: NumShards>1 under FullDuplex must actually ENGAGE full
// duplex, not fall back to half-duplex shards. Config() reports the EFFECTIVE
// state, so a fallback surfaces as FullDuplex:false.
func TestFDShardsKeepsFullDuplexOn(t *testing.T) {
	ctx := context.Background()
	cl := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer cl.Close()
	if err := cl.Ping(ctx).Err(); err != nil {
		t.Skipf("no local redis: %v", err)
	}
	ap, err := cl.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{
		FullDuplex:           true,
		NumShards:            8,
		MaxConcurrentBatches: 1,
	})
	if err != nil {
		t.Fatalf("construct: %v", err)
	}
	defer ap.Close()
	if got := ap.Config(); !got.FullDuplex {
		t.Fatal("FullDuplex reported false with NumShards=8: the engine fell back " +
			"to half-duplex shards, which are unordered")
	}
}

// More engines than the pipeline pool can hold must fail at construction rather
// than leave the surplus spilling to the main pool for the client's lifetime.
func TestFDShardsRejectsMoreEnginesThanPool(t *testing.T) {
	ctx := context.Background()
	cl := redis.NewClient(&redis.Options{
		Addr:             "127.0.0.1:6379",
		PipelinePoolSize: 4,
	})
	defer cl.Close()
	if err := cl.Ping(ctx).Err(); err != nil {
		t.Skipf("no local redis: %v", err)
	}
	_, err := cl.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{
		FullDuplex:           true,
		NumShards:            8,
		MaxConcurrentBatches: 1,
	})
	if err == nil {
		t.Fatal("expected NumShards=8 with PipelinePoolSize=4 to be rejected")
	}
	if !strings.Contains(err.Error(), "pipeline pool") {
		t.Fatalf("error should name the pipeline pool, got: %v", err)
	}
}
