package redis

import (
	"context"
	"strings"
	"time"
)

type ListCmdable interface {
	BLPop(ctx context.Context, timeout time.Duration, keys ...string) *StringSliceCmd
	BLMPop(ctx context.Context, timeout time.Duration, direction string, count int64, keys ...string) *KeyValuesCmd
	BRPop(ctx context.Context, timeout time.Duration, keys ...string) *StringSliceCmd
	BRPopLPush(ctx context.Context, source, destination string, timeout time.Duration) *StringCmd
	LIndex(ctx context.Context, key string, index int64) *StringCmd
	LInsert(ctx context.Context, key, op string, pivot, value any) *IntCmd
	LInsertBefore(ctx context.Context, key string, pivot, value any) *IntCmd
	LInsertAfter(ctx context.Context, key string, pivot, value any) *IntCmd
	LLen(ctx context.Context, key string) *IntCmd
	LMPop(ctx context.Context, direction string, count int64, keys ...string) *KeyValuesCmd
	LPop(ctx context.Context, key string) *StringCmd
	LPopCount(ctx context.Context, key string, count int) *StringSliceCmd
	LPos(ctx context.Context, key string, value string, args LPosArgs) *IntCmd
	LPosCount(ctx context.Context, key string, value string, count int64, args LPosArgs) *IntSliceCmd
	LPush(ctx context.Context, key string, values ...any) *IntCmd
	LPushX(ctx context.Context, key string, values ...any) *IntCmd
	LRange(ctx context.Context, key string, start, stop int64) *StringSliceCmd
	LRem(ctx context.Context, key string, count int64, value any) *IntCmd
	LSet(ctx context.Context, key string, index int64, value any) *StatusCmd
	LTrim(ctx context.Context, key string, start, stop int64) *StatusCmd
	RPop(ctx context.Context, key string) *StringCmd
	RPopCount(ctx context.Context, key string, count int) *StringSliceCmd
	RPopLPush(ctx context.Context, source, destination string) *StringCmd
	RPush(ctx context.Context, key string, values ...any) *IntCmd
	RPushX(ctx context.Context, key string, values ...any) *IntCmd
	LMove(ctx context.Context, source, destination, srcpos, destpos string) *StringCmd
	BLMove(ctx context.Context, source, destination, srcpos, destpos string, timeout time.Duration) *StringCmd
}

func (c cmdable) BLPop(ctx context.Context, timeout time.Duration, keys ...string) *StringSliceCmd {
	args := make([]any, 1+len(keys)+1)
	args[0] = "blpop"
	for i, key := range keys {
		args[1+i] = key
	}
	args[len(args)-1] = formatSec(ctx, timeout)
	cmd := NewStringSliceCmd(ctx, args...)
	cmd.setReadTimeout(timeout)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BLMPop(ctx context.Context, timeout time.Duration, direction string, count int64, keys ...string) *KeyValuesCmd {
	args := make([]any, 3+len(keys), 6+len(keys))
	args[0] = "blmpop"
	args[1] = formatSec(ctx, timeout)
	args[2] = len(keys)
	for i, key := range keys {
		args[3+i] = key
	}
	args = append(args, strings.ToLower(direction), "count", count)
	cmd := NewKeyValuesCmd(ctx, args...)
	cmd.setReadTimeout(timeout)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BRPop(ctx context.Context, timeout time.Duration, keys ...string) *StringSliceCmd {
	args := make([]any, 1+len(keys)+1)
	args[0] = "brpop"
	for i, key := range keys {
		args[1+i] = key
	}
	args[len(keys)+1] = formatSec(ctx, timeout)
	cmd := NewStringSliceCmd(ctx, args...)
	cmd.setReadTimeout(timeout)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BRPopLPush(ctx context.Context, source, destination string, timeout time.Duration) *StringCmd {
	cmd := NewStringCmd(
		ctx,
		"brpoplpush",
		source,
		destination,
		formatSec(ctx, timeout),
	)
	cmd.setReadTimeout(timeout)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LIndex(ctx context.Context, key string, index int64) *StringCmd {
	cmd := NewStringCmd(ctx, "lindex", key, index)
	_ = c(ctx, cmd)
	return cmd
}

// LMPop Pops one or more elements from the first non-empty list key from the list of provided key names.
// direction: left or right, count: > 0
// example: client.LMPop(ctx, "left", 3, "key1", "key2")
func (c cmdable) LMPop(ctx context.Context, direction string, count int64, keys ...string) *KeyValuesCmd {
	args := make([]any, 2+len(keys), 5+len(keys))
	args[0] = "lmpop"
	args[1] = len(keys)
	for i, key := range keys {
		args[2+i] = key
	}
	args = append(args, strings.ToLower(direction), "count", count)
	cmd := NewKeyValuesCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LInsert(ctx context.Context, key, op string, pivot, value any) *IntCmd {
	cmd := NewIntCmd(ctx, "linsert", key, op, pivot, value)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LInsertBefore(ctx context.Context, key string, pivot, value any) *IntCmd {
	cmd := NewIntCmd(ctx, "linsert", key, "before", pivot, value)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LInsertAfter(ctx context.Context, key string, pivot, value any) *IntCmd {
	cmd := NewIntCmd(ctx, "linsert", key, "after", pivot, value)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LLen(ctx context.Context, key string) *IntCmd {
	cmd := NewIntCmd(ctx, "llen", key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LPop(ctx context.Context, key string) *StringCmd {
	cmd := NewStringCmd(ctx, "lpop", key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LPopCount(ctx context.Context, key string, count int) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "lpop", key, count)
	_ = c(ctx, cmd)
	return cmd
}

type LPosArgs struct {
	Rank, MaxLen int64
}

func (c cmdable) LPos(ctx context.Context, key string, value string, a LPosArgs) *IntCmd {
	args := []any{"lpos", key, value}
	if a.Rank != 0 {
		args = append(args, "rank", a.Rank)
	}
	if a.MaxLen != 0 {
		args = append(args, "maxlen", a.MaxLen)
	}

	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LPosCount(ctx context.Context, key string, value string, count int64, a LPosArgs) *IntSliceCmd {
	args := []any{"lpos", key, value, "count", count}
	if a.Rank != 0 {
		args = append(args, "rank", a.Rank)
	}
	if a.MaxLen != 0 {
		args = append(args, "maxlen", a.MaxLen)
	}
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LPush(ctx context.Context, key string, values ...any) *IntCmd {
	args := make([]any, 2, 2+len(values))
	args[0] = "lpush"
	args[1] = key
	args = appendArgs(args, values)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LPushX(ctx context.Context, key string, values ...any) *IntCmd {
	args := make([]any, 2, 2+len(values))
	args[0] = "lpushx"
	args[1] = key
	args = appendArgs(args, values)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LRange(ctx context.Context, key string, start, stop int64) *StringSliceCmd {
	cmd := NewStringSliceCmd(
		ctx,
		"lrange",
		key,
		start,
		stop,
	)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LRem(ctx context.Context, key string, count int64, value any) *IntCmd {
	cmd := NewIntCmd(ctx, "lrem", key, count, value)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LSet(ctx context.Context, key string, index int64, value any) *StatusCmd {
	cmd := NewStatusCmd(ctx, "lset", key, index, value)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LTrim(ctx context.Context, key string, start, stop int64) *StatusCmd {
	cmd := NewStatusCmd(
		ctx,
		"ltrim",
		key,
		start,
		stop,
	)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) RPop(ctx context.Context, key string) *StringCmd {
	cmd := NewStringCmd(ctx, "rpop", key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) RPopCount(ctx context.Context, key string, count int) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "rpop", key, count)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) RPopLPush(ctx context.Context, source, destination string) *StringCmd {
	cmd := NewStringCmd(ctx, "rpoplpush", source, destination)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) RPush(ctx context.Context, key string, values ...any) *IntCmd {
	args := make([]any, 2, 2+len(values))
	args[0] = "rpush"
	args[1] = key
	args = appendArgs(args, values)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) RPushX(ctx context.Context, key string, values ...any) *IntCmd {
	args := make([]any, 2, 2+len(values))
	args[0] = "rpushx"
	args[1] = key
	args = appendArgs(args, values)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) LMove(ctx context.Context, source, destination, srcpos, destpos string) *StringCmd {
	cmd := NewStringCmd(ctx, "lmove", source, destination, srcpos, destpos)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BLMove(
	ctx context.Context, source, destination, srcpos, destpos string, timeout time.Duration,
) *StringCmd {
	cmd := NewStringCmd(ctx, "blmove", source, destination, srcpos, destpos, formatSec(ctx, timeout))
	cmd.setReadTimeout(timeout)
	_ = c(ctx, cmd)
	return cmd
}
