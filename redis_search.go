package redis

import (
	"context"
)

type SearchCmdable interface {
}

type FTCreateOptions struct {
}

type FTDropIndexOptions struct {
	DeleteDocs bool
}

// TODO - consider remove due to temporary command
func (c cmdable) FT_List(ctx context.Context) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "FT._LIST")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) FTAliasAdd(ctx context.Context, alias string, index string) *StatusCmd {
	args := []interface{}{"FT.ALIASADD", alias, index}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) FTAliasDel(ctx context.Context, alias string) *StatusCmd {
	cmd := NewStatusCmd(ctx, "FT.ALIASDEL", alias)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) FTAliasUpdate(ctx context.Context, alias string, index string) *StatusCmd {
	cmd := NewStatusCmd(ctx, "FT.ALIASUPDATE", alias, index)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) FTAlter(ctx context.Context, index string, skipInitalScan bool, definition []interface{}) *StatusCmd {
	args := []interface{}{"FT.ALTER", index}
	if skipInitalScan {
		args = append(args, "SKIPINITIALSCAN")
	}
	args = append(args, "SCHEMA", "ADD")
	args = append(args, definition...)
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) FTConfigSet(ctx context.Context, option string, value interface{}) *StatusCmd {
	cmd := NewStatusCmd(ctx, "FT.CONFIG", "SET", option, value)
	_ = c(ctx, cmd)
	return cmd
}

// func (c cmdable) FTCreate(ctx context.Context, index string, schema string, ) *StatusCmd {
// 	args := []interface{}{"FT.CREATE", index, "SCHEMA", schema}
// 	cmd := NewStatusCmd(ctx, args...)
// 	_ = c(ctx, cmd)
// 	return cmd
// }

func (c cmdable) FTCursorDel(ctx context.Context, index string, cursorId int) *StatusCmd {
	cmd := NewStatusCmd(ctx, "FT.CURSOR", "DEL", index, cursorId)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) FTDictAdd(ctx context.Context, dict string, term []interface{}) *IntCmd {
	args := []interface{}{"FT.DICTADD", dict}
	args = append(args, term...)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) FTDictDel(ctx context.Context, dict string, term []interface{}) *IntCmd {
	args := []interface{}{"FT.DICTDEL", dict}
	args = append(args, term...)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) FTDictDump(ctx context.Context, dict string) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "FT.DICTDUMP", dict)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) FTDropIndex(ctx context.Context, index string) *StatusCmd {
	args := []interface{}{"FT.DROPINDEX", index}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) FTDropIndexWithOptions(ctx context.Context, index string, options *FTDropIndexOptions) *StatusCmd {
	args := []interface{}{"FT.DROPINDEX", index}
	if options != nil {
		if options.DeleteDocs {
			args = append(args, "DD")
		}
	}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}
