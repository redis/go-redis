package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestBRPopContextCancellation verifies that a blocking BRPop with infinite timeout
// respects ctx.Done() and returns promptly with context.Canceled.
func TestBRPopContextCancellation(t *testing.T) {
	opt := redis.Options{
		Addr:                 ":6379",
		ReadTimeout:          -1, // block indefinitely for reads
		WriteTimeout:         -1,
		ContextTimeoutEnabled: true,
	}
	rdb := redis.NewClient(&opt)
	t.Cleanup(func() { _ = rdb.Close() })

	key := "brpop-cancel-key"
	_ = rdb.Del(context.Background(), key).Err()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := rdb.BRPop(ctx, 0, key).Result()
		done <- err
	}()

	// Ensure BRPop is blocked
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if err == nil || err != context.Canceled {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("BRPop did not return after context cancellation")
	}
}
