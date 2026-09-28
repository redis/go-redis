package redis

import "context"

type BlessFlag string

const (
	BlessNoEvict BlessFlag = "NO-EVICT"
)

type BlessCmdable interface {
	BlessSet(ctx context.Context, key string, flag BlessFlag) *IntCmd
	BlessGet(ctx context.Context, key string) *StringSliceCmd
	BlessClear(ctx context.Context, key string, flag BlessFlag) *IntCmd
	BlessScan(ctx context.Context, cursor uint64, flag BlessFlag, count int64) *ScanCmd
}

func (c cmdable) BlessSet(ctx context.Context, key string, flag BlessFlag) *IntCmd {
	args := []any{"bless", "set", key, string(flag)}
	cmd := NewIntCmd(ctx, args...)
	cmd.SetFirstKeyPos(2)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BlessGet(ctx context.Context, key string) *StringSliceCmd {
	args := []any{"bless", "get", key}
	cmd := NewStringSliceCmd(ctx, args...)
	cmd.SetFirstKeyPos(2)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BlessClear(ctx context.Context, key string, flag BlessFlag) *IntCmd {
	args := []any{"bless", "clear", key, string(flag)}
	cmd := NewIntCmd(ctx, args...)
	cmd.SetFirstKeyPos(2)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BlessScan(ctx context.Context, cursor uint64, flag BlessFlag, count int64) *ScanCmd {
	args := []any{"bless", "scan", cursor, string(flag)}
	if count > 0 {
		args = append(args, "count", count)
	}
	cmd := NewScanCmd(ctx, c, args...)
	_ = c(ctx, cmd)
	return cmd
}
