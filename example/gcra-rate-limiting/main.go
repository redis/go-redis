package main

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()

	// Clean up any existing keys
	_ = rdb.FlushDB(ctx).Err()

	fmt.Println("=== GCRA Rate Limiting Examples ===")

	// Example 1: Basic rate limiting
	fmt.Println("Example 1: Basic rate limiting (10 requests/second, burst of 5)")
	basicRateLimiting(ctx, rdb)

	// Example 2: Handling rate limit exceeded
	fmt.Println("Example 2: Handling rate limit exceeded")
	handleRateLimitExceeded(ctx, rdb)

	// Example 3: Weighted requests
	fmt.Println("Example 3: Weighted requests (different costs)")
	weightedRequests(ctx, rdb)

	// Example 4: Multiple users with independent limits
	fmt.Println("Example 4: Multiple users with independent limits")
	multipleUsers(ctx, rdb)
}

func basicRateLimiting(ctx context.Context, rdb *redis.Client) {
	key := "api:basic"

	// Allow 10 requests per second with a burst of 5
	// This means up to 6 requests (burst + 1) can be made immediately
	result, err := rdb.GCRA(ctx, key, 5, 10, time.Second).Result()
	if err != nil {
		panic(err)
	}

	fmt.Printf("  Request allowed: %v\n", result.Limited == 0)
	fmt.Printf("  Max requests: %d\n", result.MaxRequests)
	fmt.Printf("  Available requests: %d\n", result.AvailableRequests)
	fmt.Printf("  Full burst available after: %d seconds\n", result.FullBurstAfter)
}

func handleRateLimitExceeded(ctx context.Context, rdb *redis.Client) {
	key := "api:limited"

	// Allow only 1 request per second with no burst
	for i := 1; i <= 3; i++ {
		result, err := rdb.GCRA(ctx, key, 0, 1, time.Second).Result()
		if err != nil {
			panic(err)
		}

		if result.Limited == 1 {
			fmt.Printf("  Request %d: RATE LIMITED - retry after %d seconds\n", i, result.RetryAfter)
		} else {
			fmt.Printf("  Request %d: ALLOWED - %d requests remaining\n", i, result.AvailableRequests)
		}
	}
}

func weightedRequests(ctx context.Context, rdb *redis.Client) {
	key := "api:weighted"

	// Allow 10 requests per second with a burst of 10
	// First request: normal weight (1 token)
	result, err := rdb.GCRAWithArgs(ctx, key, &redis.GCRAArgs{
		MaxBurst:          10,
		RequestsPerPeriod: 10,
		Period:            time.Second,
		NumRequests:       1, // Normal request
	}).Result()
	if err != nil {
		panic(err)
	}
	fmt.Printf("  Normal request (cost=1): allowed=%v, available=%d\n",
		result.Limited == 0, result.AvailableRequests)

	// Second request: heavy weight (5 tokens)
	result, err = rdb.GCRAWithArgs(ctx, key, &redis.GCRAArgs{
		MaxBurst:          10,
		RequestsPerPeriod: 10,
		Period:            time.Second,
		NumRequests:       5, // Heavy request
	}).Result()
	if err != nil {
		panic(err)
	}
	fmt.Printf("  Heavy request (cost=5): allowed=%v, available=%d\n",
		result.Limited == 0, result.AvailableRequests)

	// Third request: another heavy weight (5 tokens)
	result, err = rdb.GCRAWithArgs(ctx, key, &redis.GCRAArgs{
		MaxBurst:          10,
		RequestsPerPeriod: 10,
		Period:            time.Second,
		NumRequests:       5, // Heavy request
	}).Result()
	if err != nil {
		panic(err)
	}
	fmt.Printf("  Heavy request (cost=5): allowed=%v, available=%d\n",
		result.Limited == 0, result.AvailableRequests)

	// Fourth request should be limited (would exceed capacity)
	result, err = rdb.GCRAWithArgs(ctx, key, &redis.GCRAArgs{
		MaxBurst:          10,
		RequestsPerPeriod: 10,
		Period:            time.Second,
		NumRequests:       1,
	}).Result()
	if err != nil {
		panic(err)
	}
	fmt.Printf("  Normal request (cost=1): allowed=%v, retry_after=%d\n",
		result.Limited == 0, result.RetryAfter)
}

func multipleUsers(ctx context.Context, rdb *redis.Client) {
	// Each user has their own rate limit
	users := []string{"user:alice", "user:bob", "user:charlie"}

	for _, userKey := range users {
		// Allow 5 requests per second with burst of 2
		result, err := rdb.GCRA(ctx, userKey, 2, 5, time.Second).Result()
		if err != nil {
			panic(err)
		}

		fmt.Printf("  %s: allowed=%v, available=%d/%d\n",
			userKey,
			result.Limited == 0,
			result.AvailableRequests,
			result.MaxRequests)
	}

	// Make multiple requests for one user
	fmt.Println(" Making 4 requests for user:alice:")
	for i := 1; i <= 4; i++ {
		result, err := rdb.GCRA(ctx, "user:alice", 2, 5, time.Second).Result()
		if err != nil {
			panic(err)
		}

		if result.Limited == 1 {
			fmt.Printf("    Request %d: RATE LIMITED\n", i)
		} else {
			fmt.Printf("    Request %d: ALLOWED (available: %d)\n", i, result.AvailableRequests)
		}
	}

	// Other users are unaffected
	result, err := rdb.GCRA(ctx, "user:bob", 2, 5, time.Second).Result()
	if err != nil {
		panic(err)
	}
	fmt.Printf("\n  user:bob still has %d requests available\n", result.AvailableRequests)
}
