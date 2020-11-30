package redis

import (
	"context"
	"time"
)

var (
	ctx = context.Background()
	rdb *Client
)

func init() {
	rdb = NewClient(&Options{
		Addr:         ":6379",
		DialTimeout:  10 * time.Second,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		PoolSize:     10,
		PoolTimeout:  10 * time.Second,
	})
}

func Fuzz(data []byte) int {
	array_len := len(data)
	if array_len < 4 {
		return -1
	}
	max_iter := int(uint(data[0]))
	for i := 0; i < max_iter && i < array_len; i++ {
		n := i % array_len
		if n == 0 {
			_ = rdb.Set(ctx, string(data[i:]), string(data[i:]), 0).Err()
		} else if n == 1 {
			_, _ = rdb.Get(ctx, string(data[i:])).Result()
		} else if n == 2 {
			_, _ = rdb.Incr(ctx, string(data[i:])).Result()
		} else if n == 3 {
			var cursor uint64
			_, _, _ = rdb.Scan(ctx, cursor, string(data[i:]), 10).Result()
		}
	}
	return 1
}
