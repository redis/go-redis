package redis

import (
	"context"
)

type SearchCmdable interface {
}

func (c cmdable) FT_List(ctx context.Context) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "FT._LIST")
	_ = c(ctx, cmd)
	return cmd
}
