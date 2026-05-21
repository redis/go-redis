//go:build gofuzz
// +build gofuzz

package fuzz

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	minDataLength     = 4
	redisAddr         = ":6379"
	dialTimeout       = 10 * time.Second
	readTimeout       = 10 * time.Second
	writeTimeout      = 10 * time.Second
	poolSize          = 10
	poolTimeout       = 10 * time.Second
	scanCount         = 10
	maxIterPercentage = 256 // Use first byte as percentage of data length
)

var (
	ctx = context.Background()
	rdb *redis.Client
)

type redisOperation func(key, value string)

func init() {
	rdb = redis.NewClient(&redis.Options{
		Addr:         redisAddr,
		DialTimeout:  dialTimeout,
		ReadTimeout:  readTimeout,
		WriteTimeout: writeTimeout,
		PoolSize:     poolSize,
		PoolTimeout:  poolTimeout,
	})
}

func Fuzz(data []byte) int {
	if len(data) < minDataLength {
		return -1
	}

	maxIter := (int(data[0]) * len(data)) / maxIterPercentage
	if maxIter == 0 {
		maxIter = 1 // Ensure at least one iteration
	}

	operations := []redisOperation{
		func(key, value string) { rdb.Set(ctx, key, value, 0).Err() },
		func(key, value string) { rdb.Get(ctx, key).Result() },
		func(key, value string) { rdb.Incr(ctx, key).Result() },
		func(key, value string) {
			var cursor uint64
			rdb.Scan(ctx, cursor, key, scanCount).Result()
		},
	}

	dataStr := string(data)

	for i := 0; i < maxIter && i < len(data); i++ {
		start := i % len(data)
		end := (i + 1) % len(data)
		if end <= start {
			end = len(data)
		}

		key := dataStr[start:end]
		opIndex := i % len(operations)

		operations[opIndex](key, dataStr)
	}

	return 1
}
