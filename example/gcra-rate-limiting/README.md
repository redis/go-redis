# GCRA Rate Limiting Example

This example demonstrates how to use the GCRA (Generic Cell Rate Algorithm) command for rate limiting in Redis.

## What is GCRA?

GCRA is a rate limiting algorithm that allows a sustained rate of requests with occasional bursts. It's commonly used for API rate limiting because it:

- Provides smooth rate limiting without hard cutoffs
- Allows bursts while maintaining an average rate
- Is efficient and atomic (single Redis command)

## Prerequisites

- Redis 8.8 or later (GCRA command support)
- Go 1.24 or later

## Running the Example

```bash
# Start Redis
redis-server

# Run the example
go run main.go
```

## API Overview

### Basic Usage

```go
// Allow 10 requests per second with a burst of 5
result, err := rdb.GCRA(ctx, "user:123", 5, 10, time.Second).Result()
if err != nil {
    panic(err)
}

if result.Limited == 1 {
    fmt.Printf("Rate limited. Retry after %d seconds\n", result.RetryAfter)
} else {
    fmt.Printf("Request allowed. %d requests available\n", result.AvailableRequests)
}
```

### With Custom Request Cost (Weight)

```go
// Heavy request that costs 5 tokens instead of 1
result, err := rdb.GCRAWithArgs(ctx, "user:123", &redis.GCRAArgs{
    MaxBurst:          10,
    RequestsPerPeriod: 100,
    Period:            time.Second,
    NumRequests:       5, // This request costs 5 tokens
}).Result()
```

## Parameters

| Parameter | Description |
|-----------|-------------|
| `key` | Unique identifier for this rate limit (e.g., `user:123`, `api:endpoint`) |
| `maxBurst` | Maximum extra requests allowed in a burst (on top of sustained rate) |
| `requestsPerPeriod` | Number of requests allowed per period at sustained rate |
| `period` | Time period for the rate limit (e.g., `time.Second`, `time.Minute`) |
| `numRequests` | Cost/weight of this request (default: 1) |

## Response Fields

| Field | Description |
|-------|-------------|
| `Limited` | 0 if allowed, 1 if rate limited |
| `MaxRequests` | Maximum requests allowed (always `maxBurst + 1`) |
| `AvailableRequests` | Number of requests available immediately |
| `RetryAfter` | Seconds until retry is allowed (-1 if not limited) |
| `FullBurstAfter` | Seconds until full burst capacity is restored |

## Use Cases

1. **API Rate Limiting**: Limit requests per user/IP
2. **Resource Protection**: Prevent abuse of expensive operations
3. **Fair Usage**: Ensure equitable access across users
4. **Cost Control**: Limit usage of paid external APIs

