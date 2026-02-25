package redis

import (
	"context"
	"time"
)

// StringCmdableV10 defines the v10 API for string commands
// Regular commands return (value, error) directly instead of *Cmd objects
type StringCmdableV10 interface {
	GetV10(ctx context.Context, key string) (string, error)
	SetV10(ctx context.Context, key string, value interface{}, expiration time.Duration) (string, error)
	IncrV10(ctx context.Context, key string) (int64, error)
	IncrByV10(ctx context.Context, key string, value int64) (int64, error)
	DecrV10(ctx context.Context, key string) (int64, error)
	DecrByV10(ctx context.Context, key string, decrement int64) (int64, error)
	AppendV10(ctx context.Context, key, value string) (int64, error)
	GetRangeV10(ctx context.Context, key string, start, end int64) (string, error)
	GetDelV10(ctx context.Context, key string) (string, error)
	GetExV10(ctx context.Context, key string, expiration time.Duration) (string, error)
	SetNXV10(ctx context.Context, key string, value interface{}, expiration time.Duration) (bool, error)
	SetXXV10(ctx context.Context, key string, value interface{}, expiration time.Duration) (bool, error)
	SetRangeV10(ctx context.Context, key string, offset int64, value string) (int64, error)
	StrLenV10(ctx context.Context, key string) (int64, error)
	MGetV10(ctx context.Context, keys ...string) ([]interface{}, error)
	MSetV10(ctx context.Context, values ...interface{}) (string, error)
	MSetNXV10(ctx context.Context, values ...interface{}) (bool, error)
	DigestV10(ctx context.Context, key string) (uint64, error)
}

// GetV10 Redis `GET key` command. It returns redis.Nil error when key does not exist.
// This is the v10 API that returns (value, error) directly instead of *StringCmd.
func (c cmdable) GetV10(ctx context.Context, key string) (string, error) {
	// Create Cmd internally (not exposed to user)
	cmd := NewStringCmd(ctx, "get", key)
	
	// Process it (existing internal logic unchanged)
	err := c(ctx, cmd)
	
	// Extract and return value directly
	return cmd.Val(), err
}

// SetV10 Redis `SET key value [expiration]` command.
// Use expiration for `SETEx`-like behavior.
//
// Zero expiration means the key has no expiration time.
// KeepTTL is a Redis KEEPTTL option to keep existing TTL, it requires your redis-server version >= 6.0,
// otherwise you will receive an error: (error) ERR syntax error.
//
// This is the v10 API that returns (status, error) directly instead of *StatusCmd.
func (c cmdable) SetV10(ctx context.Context, key string, value interface{}, expiration time.Duration) (string, error) {
	args := make([]interface{}, 3, 5)
	args[0] = "set"
	args[1] = key
	args[2] = value
	if expiration > 0 {
		if usePrecise(expiration) {
			args = append(args, "px", formatMs(ctx, expiration))
		} else {
			args = append(args, "ex", formatSec(ctx, expiration))
		}
	} else if expiration == KeepTTL {
		args = append(args, "keepttl")
	}

	cmd := NewStatusCmd(ctx, args...)
	err := c(ctx, cmd)
	return cmd.Val(), err
}

// IncrV10 Redis `INCR key` command.
// This is the v10 API that returns (value, error) directly instead of *IntCmd.
func (c cmdable) IncrV10(ctx context.Context, key string) (int64, error) {
	cmd := NewIntCmd(ctx, "incr", key)
	err := c(ctx, cmd)
	return cmd.Val(), err
}

// IncrByV10 Redis `INCRBY key increment` command.
// This is the v10 API that returns (value, error) directly instead of *IntCmd.
func (c cmdable) IncrByV10(ctx context.Context, key string, value int64) (int64, error) {
	cmd := NewIntCmd(ctx, "incrby", key, value)
	err := c(ctx, cmd)
	return cmd.Val(), err
}

// DecrV10 Redis `DECR key` command.
// This is the v10 API that returns (value, error) directly instead of *IntCmd.
func (c cmdable) DecrV10(ctx context.Context, key string) (int64, error) {
	cmd := NewIntCmd(ctx, "decr", key)
	err := c(ctx, cmd)
	return cmd.Val(), err
}

// DecrByV10 Redis `DECRBY key decrement` command.
// This is the v10 API that returns (value, error) directly instead of *IntCmd.
func (c cmdable) DecrByV10(ctx context.Context, key string, decrement int64) (int64, error) {
	cmd := NewIntCmd(ctx, "decrby", key, decrement)
	err := c(ctx, cmd)
	return cmd.Val(), err
}

// AppendV10 Redis `APPEND key value` command.
// This is the v10 API that returns (length, error) directly instead of *IntCmd.
func (c cmdable) AppendV10(ctx context.Context, key, value string) (int64, error) {
	cmd := NewIntCmd(ctx, "append", key, value)
	err := c(ctx, cmd)
	return cmd.Val(), err
}

// GetRangeV10 Redis `GETRANGE key start end` command.
// This is the v10 API that returns (value, error) directly instead of *StringCmd.
func (c cmdable) GetRangeV10(ctx context.Context, key string, start, end int64) (string, error) {
	cmd := NewStringCmd(ctx, "getrange", key, start, end)
	err := c(ctx, cmd)
	return cmd.Val(), err
}

// GetDelV10 Redis `GETDEL key` command (redis-server version >= 6.2.0).
// This is the v10 API that returns (value, error) directly instead of *StringCmd.
func (c cmdable) GetDelV10(ctx context.Context, key string) (string, error) {
	cmd := NewStringCmd(ctx, "getdel", key)
	err := c(ctx, cmd)
	return cmd.Val(), err
}

// GetExV10 Redis `GETEX key [EX seconds|PX milliseconds|PERSIST]` command.
// An expiration of zero removes the TTL associated with the key (i.e. GETEX key persist).
// Requires Redis >= 6.2.0.
// This is the v10 API that returns (value, error) directly instead of *StringCmd.
func (c cmdable) GetExV10(ctx context.Context, key string, expiration time.Duration) (string, error) {
	args := make([]interface{}, 0, 4)
	args = append(args, "getex", key)
	if expiration > 0 {
		if usePrecise(expiration) {
			args = append(args, "px", formatMs(ctx, expiration))
		} else {
			args = append(args, "ex", formatSec(ctx, expiration))
		}
	} else if expiration == 0 {
		args = append(args, "persist")
	}

	cmd := NewStringCmd(ctx, args...)
	err := c(ctx, cmd)
	return cmd.Val(), err
}

// SetNXV10 sets the value of a key only if the key does not exist.
//
// Zero expiration means the key has no expiration time.
// KeepTTL is a Redis KEEPTTL option to keep existing TTL, it requires your redis-server version >= 6.0,
// otherwise you will receive an error: (error) ERR syntax error.
//
// This is the v10 API that returns (wasSet, error) directly instead of *BoolCmd.
func (c cmdable) SetNXV10(ctx context.Context, key string, value interface{}, expiration time.Duration) (bool, error) {
	var cmd *BoolCmd
	switch expiration {
	case 0:
		// Use old `SETNX` to support old Redis versions.
		cmd = NewBoolCmd(ctx, "setnx", key, value)
	case KeepTTL:
		cmd = NewBoolCmd(ctx, "set", key, value, "keepttl", "nx")
	default:
		if usePrecise(expiration) {
			cmd = NewBoolCmd(ctx, "set", key, value, "px", formatMs(ctx, expiration), "nx")
		} else {
			cmd = NewBoolCmd(ctx, "set", key, value, "ex", formatSec(ctx, expiration), "nx")
		}
	}

	err := c(ctx, cmd)
	return cmd.Val(), err
}

// SetXXV10 sets the value of a key only if the key already exists.
//
// Zero expiration means the key has no expiration time.
// KeepTTL is a Redis KEEPTTL option to keep existing TTL, it requires your redis-server version >= 6.0,
// otherwise you will receive an error: (error) ERR syntax error.
//
// This is the v10 API that returns (wasSet, error) directly instead of *BoolCmd.
func (c cmdable) SetXXV10(ctx context.Context, key string, value interface{}, expiration time.Duration) (bool, error) {
	var cmd *BoolCmd
	switch expiration {
	case 0:
		cmd = NewBoolCmd(ctx, "set", key, value, "xx")
	case KeepTTL:
		cmd = NewBoolCmd(ctx, "set", key, value, "keepttl", "xx")
	default:
		if usePrecise(expiration) {
			cmd = NewBoolCmd(ctx, "set", key, value, "px", formatMs(ctx, expiration), "xx")
		} else {
			cmd = NewBoolCmd(ctx, "set", key, value, "ex", formatSec(ctx, expiration), "xx")
		}
	}

	err := c(ctx, cmd)
	return cmd.Val(), err
}

// SetRangeV10 Redis `SETRANGE key offset value` command.
// This is the v10 API that returns (length, error) directly instead of *IntCmd.
func (c cmdable) SetRangeV10(ctx context.Context, key string, offset int64, value string) (int64, error) {
	cmd := NewIntCmd(ctx, "setrange", key, offset, value)
	err := c(ctx, cmd)
	return cmd.Val(), err
}

// StrLenV10 Redis `STRLEN key` command.
// This is the v10 API that returns (length, error) directly instead of *IntCmd.
func (c cmdable) StrLenV10(ctx context.Context, key string) (int64, error) {
	cmd := NewIntCmd(ctx, "strlen", key)
	err := c(ctx, cmd)
	return cmd.Val(), err
}

// MGetV10 Redis `MGET key [key ...]` command.
// This is the v10 API that returns (values, error) directly instead of *SliceCmd.
func (c cmdable) MGetV10(ctx context.Context, keys ...string) ([]interface{}, error) {
	args := make([]interface{}, 1+len(keys))
	args[0] = "mget"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := NewSliceCmd(ctx, args...)
	err := c(ctx, cmd)
	return cmd.Val(), err
}

// MSetV10 Redis `MSET key value [key value ...]` command.
// This is the v10 API that returns (status, error) directly instead of *StatusCmd.
func (c cmdable) MSetV10(ctx context.Context, values ...interface{}) (string, error) {
	args := make([]interface{}, 1, 1+len(values))
	args[0] = "mset"
	args = appendArgs(args, values)
	cmd := NewStatusCmd(ctx, args...)
	err := c(ctx, cmd)
	return cmd.Val(), err
}

// MSetNXV10 Redis `MSETNX key value [key value ...]` command.
// This is the v10 API that returns (wasSet, error) directly instead of *BoolCmd.
func (c cmdable) MSetNXV10(ctx context.Context, values ...interface{}) (bool, error) {
	args := make([]interface{}, 1, 1+len(values))
	args[0] = "msetnx"
	args = appendArgs(args, values)
	cmd := NewBoolCmd(ctx, args...)
	err := c(ctx, cmd)
	return cmd.Val(), err
}

// DigestV10 returns the xxh3 hash (uint64) of the specified key's value.
//
// The digest is a 64-bit xxh3 hash that can be used for optimistic locking
// with SetIFDEQ, SetIFDNE, and DelExArgs commands.
//
// For examples of client-side digest generation and usage patterns, see:
// example/digest-optimistic-locking/
//
// Redis 8.4+. See https://redis.io/commands/digest/
//
// NOTE Digest is still experimental
// it's signature and behaviour may change
//
// This is the v10 API that returns (digest, error) directly instead of *DigestCmd.
func (c cmdable) DigestV10(ctx context.Context, key string) (uint64, error) {
	cmd := NewDigestCmd(ctx, "digest", key)
	err := c(ctx, cmd)
	return cmd.Val(), err
}

