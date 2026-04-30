package redis

import (
	"context"
	"time"
)

// RateLimitCmdable defines rate limiting commands.
type RateLimitCmdable interface {
	GCRA(ctx context.Context, key string, maxBurst int64, tokensPerPeriod int64, period time.Duration) *GCRACmd
	GCRAWithArgs(ctx context.Context, key string, args *GCRAArgs) *GCRACmd
}

// GCRAArgs represents the arguments for the GCRA command.
type GCRAArgs struct {
	// MaxBurst is the maximum number of tokens allowed as a burst (in addition to the sustained rate).
	// Min: 0
	MaxBurst int64

	// TokensPerPeriod is the number of tokens allowed per Period.
	// Min: 1
	TokensPerPeriod int64

	// Period is the period used for calculating the sustained rate.
	// Min: 1 second
	Period time.Duration

	// Tokens is the cost (or weight) of this rate-limiting request.
	// A higher cost drains the allowance faster.
	// Default: 1
	Tokens int64
}

// GCRAResult represents the result of a GCRA rate limiting check.
type GCRAResult struct {
	// Limited indicates whether the request is rate limited (0 = allowed, 1 = limited).
	Limited int64

	// MaxTokens is the maximum number of tokens allowed.
	// Always equal to MaxBurst + 1.
	MaxTokens int64

	// AvailableTokens is the number of tokens available immediately.
	AvailableTokens int64

	// RetryAfter is the number of seconds after which the caller should retry.
	// Always returns -1 if the request isn't limited.
	RetryAfter int64

	// FullBurstAfter is the number of seconds after which a full burst will be allowed.
	FullBurstAfter int64
}

// GCRA performs rate limiting using the Generic Cell Rate Algorithm (GCRA).
//
// The GCRA algorithm allows tokensPerPeriod tokens per period at a sustained rate,
// with a minimum spacing (emission interval) of period/tokensPerPeriod between each request.
// The maxBurst parameter allows for occasional spikes by granting up to maxBurst additional
// tokens to be consumed at once.
//
// Parameters:
//   - key: key related to specific rate limiting case
//   - maxBurst: maximum number of tokens allowed as a burst (in addition to the sustained rate). Min: 0
//   - tokensPerPeriod: number of tokens allowed per period. Min: 1
//   - period: period used for calculating the sustained rate. Min: 1 second
//
// Returns a GCRACmd containing the rate limiting result.
//
// Example:
//
//	// Allow 10 tokens per second with a burst of 5
//	result, err := client.GCRA(ctx, "user:123", 5, 10, time.Second).Result()
//	if err != nil {
//	    panic(err)
//	}
//	if result.Limited == 1 {
//	    fmt.Printf("Rate limited. Retry after %d seconds\n", result.RetryAfter)
//	} else {
//	    fmt.Printf("Request allowed. %d tokens available\n", result.AvailableTokens)
//	}
//
// Redis 8.8+. See https://redis.io/commands/gcra/
func (c cmdable) GCRA(ctx context.Context, key string, maxBurst int64, tokensPerPeriod int64, period time.Duration) *GCRACmd {
	return c.GCRAWithArgs(ctx, key, &GCRAArgs{
		MaxBurst:        maxBurst,
		TokensPerPeriod: tokensPerPeriod,
		Period:          period,
		Tokens:          1,
	})
}

// GCRAWithArgs performs rate limiting using the Generic Cell Rate Algorithm (GCRA) with additional options.
//
// This function allows specifying the cost (weight) of the request via Tokens.
// A higher cost drains the allowance faster.
//
// Parameters:
//   - key: key related to specific rate limiting case
//   - args: GCRA arguments including MaxBurst, TokensPerPeriod, Period, and optional Tokens
//
// Returns a GCRACmd containing the rate limiting result.
//
// Example:
//
//	// Allow 10 tokens per second with a burst of 5, consuming 2 tokens per request
//	result, err := client.GCRAWithArgs(ctx, "user:123", &redis.GCRAArgs{
//	    MaxBurst:        5,
//	    TokensPerPeriod: 10,
//	    Period:          time.Second,
//	    Tokens:          2,
//	}).Result()
//	if err != nil {
//	    panic(err)
//	}
//	if result.Limited == 1 {
//	    fmt.Printf("Rate limited. Retry after %d seconds\n", result.RetryAfter)
//	}
//
// Redis 8.8+. See https://redis.io/commands/gcra/
func (c cmdable) GCRAWithArgs(ctx context.Context, key string, args *GCRAArgs) *GCRACmd {
	cmdArgs := make([]interface{}, 0, 7)
	cmdArgs = append(cmdArgs, "gcra", key, args.MaxBurst, args.TokensPerPeriod)

	// Convert period to seconds as a float
	periodSeconds := float64(args.Period) / float64(time.Second)
	cmdArgs = append(cmdArgs, periodSeconds)

	// Add TOKENS if specified and not default
	if args.Tokens > 1 {
		cmdArgs = append(cmdArgs, "TOKENS", args.Tokens)
	}

	cmd := NewGCRACmd(ctx, cmdArgs...)
	_ = c(ctx, cmd)
	return cmd
}
