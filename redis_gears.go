package redis

import (
	"context"
	"fmt"
	"strings"
)

type gearsCmdable interface {
	TFunctionLoad(ctx context.Context, lib string) *StatusCmd
	TFunctionLoadArgs(ctx context.Context, lib string, options *TFunctionLoadOptions) *StatusCmd
	TFunctionDelete(ctx context.Context, libName string) *StatusCmd
	TFunctionList(ctx context.Context) *MapStringInterfaceSliceCmd
	TFunctionListArgs(ctx context.Context, options *TFunctionListOptions) *MapStringInterfaceSliceCmd
	TFCall(ctx context.Context, libName string, funcName string, numKeys int) *Cmd
	TFCallArgs(ctx context.Context, libName string, funcName string, numKeys int, options *TFCallOptions) *Cmd
}
type TFunctionLoadOptions struct {
	Replace bool
	Config  string
}

type TFunctionListOptions struct {
	Withcode bool
	Verbose  int
	Library  string
}

type TFCallOptions struct {
	Keys      []string
	Arguments []string
}

func (c cmdable) TFunctionLoad(ctx context.Context, lib string) *StatusCmd {
	args := []interface{}{"TFUNCTION", "LOAD", lib}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) TFunctionLoadArgs(ctx context.Context, lib string, options *TFunctionLoadOptions) *StatusCmd {
	args := []interface{}{"TFUNCTION", "LOAD"}
	if options != nil {
		if options.Replace {
			args = append(args, "REPLACE")
		}
		if options.Config != "" {
			args = append(args, "CONFIG", options.Config)
		}
	}
	args = append(args, lib)
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) TFunctionDelete(ctx context.Context, libName string) *StatusCmd {
	args := []interface{}{"TFUNCTION", "DELETE", libName}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) TFunctionList(ctx context.Context) *MapStringInterfaceSliceCmd {
	args := []interface{}{"TFUNCTION", "LIST"}
	cmd := NewMapStringInterfaceSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) TFunctionListArgs(ctx context.Context, options *TFunctionListOptions) *MapStringInterfaceSliceCmd {
	args := []interface{}{"TFUNCTION", "LIST"}
	if options != nil {
		if options.Withcode {
			args = append(args, "WITHCODE")
		}
		if options.Verbose != 0 {
			v := strings.Repeat("v", options.Verbose)
			args = append(args, v)
		}
		if options.Library != "" {
			args = append(args, "LIBRARY", options.Library)

		}
	}
	cmd := NewMapStringInterfaceSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) TFCall(ctx context.Context, libName string, funcName string, numKeys int) *Cmd {
	lf := fmt.Sprintf("%s.%s", libName, funcName)
	args := []interface{}{"TFCALL", lf, numKeys}
	cmd := NewCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) TFCallArgs(ctx context.Context, libName string, funcName string, numKeys int, options *TFCallOptions) *Cmd {
	lf := fmt.Sprintf("%s.%s", libName, funcName)
	args := []interface{}{"TFCALL", lf, numKeys}
	if options != nil {
		if options.Keys != nil {
			for _, key := range options.Keys {

				args = append(args, key)
			}
		}
		if options.Arguments != nil {
			for _, key := range options.Arguments {

				args = append(args, key)
			}
		}
	}
	cmd := NewCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) TFCallASync(ctx context.Context, libName string, funcName string, numKeys int) *Cmd {
	lf := fmt.Sprintf("%s.%s", libName, funcName)
	args := []interface{}{"TFCALLASYNC", lf, numKeys}
	cmd := NewCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) TFCallASyncArgs(ctx context.Context, libName string, funcName string, numKeys int, options *TFCallOptions) *Cmd {
	lf := fmt.Sprintf("%s.%s", libName, funcName)
	args := []interface{}{"TFCALLASYNC", lf, numKeys}
	if options != nil {
		if options.Keys != nil {
			for _, key := range options.Keys {

				args = append(args, key)
			}
		}
		if options.Arguments != nil {
			for _, key := range options.Arguments {

				args = append(args, key)
			}
		}
	}
	cmd := NewCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}
