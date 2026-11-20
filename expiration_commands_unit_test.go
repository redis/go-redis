package redis_test

import (
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestMillisecondExpirationTimestamps(t *testing.T) {
	client := redis.NewClient(&redis.Options{})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()

	tests := []struct {
		name string
		time time.Time
		want int64
	}{
		{"after nanosecond range", time.Date(2270, time.January, 2, 3, 4, 5, 123000000, time.UTC), 9467204645123},
		{"before nanosecond range", time.Date(1670, time.January, 2, 3, 4, 5, 123000000, time.UTC), -9466923354877},
		{"millisecond precision", time.Date(2030, time.January, 2, 3, 4, 5, 123456789, time.UTC), 1893553445123},
		{"before epoch", time.Date(1969, time.December, 31, 23, 59, 59, 999500000, time.UTC), -1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pipe := client.Pipeline()
			commands := []redis.Cmder{
				pipe.PExpireAt(ctx, "key", tt.time),
				pipe.HPExpireAt(ctx, "hash", tt.time, "field"),
				pipe.HPExpireAtWithArgs(ctx, "hash", tt.time, redis.HExpireArgs{NX: true}, "field"),
			}
			for _, cmd := range commands {
				if got := cmd.Args()[2]; got != tt.want {
					t.Errorf("%v timestamp = %v, want %d", cmd.Args(), got, tt.want)
				}
			}
		})
	}
}
