package redis

import "context"

func (c cmdable) Do(ctx context.Context, args ...interface{}) *Cmd {
	cmd := NewCmd(ctx, args)
	_ = c(ctx, cmd)
	return cmd
}
