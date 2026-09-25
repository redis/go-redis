package redis_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestRingRetriesAfterExecutionError(t *testing.T) {
	errSecondAttempt := errors.New("redis: reached second attempt")

	var attempts int
	options := &redis.RingOptions{
		Addrs:              map[string]string{"shard1": "127.0.0.1:0"},
		HeartbeatFrequency: time.Hour,
		MaxRetries:         1,
		MinRetryBackoff:    time.Millisecond,
		MaxRetryBackoff:    time.Millisecond,
	}
	ring := redis.NewRing(options)
	defer ring.Close()

	err := ring.ForEachShard(context.Background(), func(_ context.Context, shard *redis.Client) error {
		shard.AddHook(&hook{
			processHook: func(next redis.ProcessHook) redis.ProcessHook {
				return func(ctx context.Context, cmd redis.Cmder) error {
					attempts++
					if attempts == 1 {
						return io.EOF
					}
					if err := cmd.Err(); err != nil {
						return err
					}
					return errSecondAttempt
				}
			},
		})
		return nil
	})
	if err != nil {
		t.Fatalf("ForEachShard: %v", err)
	}

	cmd := redis.NewStatusCmd(context.Background(), "ping")
	err = ring.Process(context.Background(), cmd)
	if attempts != 2 {
		t.Fatalf("hook attempts = %d, want 2; err = %v", attempts, err)
	}
	if !errors.Is(err, errSecondAttempt) {
		t.Fatalf("Process err = %v, want second-attempt error", err)
	}
}
