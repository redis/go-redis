package redis

import (
	"context"
)

// HotKeysCmdable is the interface for Redis HOTKEYS commands.
type HotKeysCmdable interface {
	HotKeysStart(ctx context.Context, args *HotKeysStartArgs) *StatusCmd
	HotKeysStop(ctx context.Context) *StatusCmd
	HotKeysReset(ctx context.Context) *StatusCmd
	HotKeysGet(ctx context.Context) *HotKeysCmd
}

// HotKeysStartArgs contains the arguments for the HOTKEYS START command.
type HotKeysStartArgs struct {
	CPU      bool    // Track CPU consumption metrics
	NET      bool    // Track network utilization metrics
	Count    int64   // Number of top keys to track per metric
	Duration int64   // Auto-stop collection after duration seconds (0 for manual stop)
	Sample   int64   // Sample ratio: 1/ratio probability (1 = no sampling)
	Slots    []int64 // Specific hash slots to track
}

// HotKeysStart starts collecting hotkeys data.
// If no metrics are specified (CPU and NET both false), defaults to tracking both.
func (c cmdable) HotKeysStart(ctx context.Context, args *HotKeysStartArgs) *StatusCmd {
	cmdArgs := make([]interface{}, 0, 16)
	cmdArgs = append(cmdArgs, "hotkeys", "start")

	metricsCount := 0
	if args.CPU {
		metricsCount++
	}
	if args.NET {
		metricsCount++
	}
	if metricsCount == 0 {
		metricsCount = 2
		args.CPU = true
		args.NET = true
	}

	cmdArgs = append(cmdArgs, "metrics", metricsCount)
	if args.CPU {
		cmdArgs = append(cmdArgs, "cpu")
	}
	if args.NET {
		cmdArgs = append(cmdArgs, "net")
	}

	if args.Count > 0 {
		cmdArgs = append(cmdArgs, "count", args.Count)
	}

	if args.Duration > 0 {
		cmdArgs = append(cmdArgs, "duration", args.Duration)
	}

	if args.Sample > 0 {
		cmdArgs = append(cmdArgs, "sample", args.Sample)
	}

	if len(args.Slots) > 0 {
		cmdArgs = append(cmdArgs, "slots", len(args.Slots))
		for _, slot := range args.Slots {
			cmdArgs = append(cmdArgs, slot)
		}
	}

	cmd := NewStatusCmd(ctx, cmdArgs...)
	_ = c(ctx, cmd)
	return cmd
}

// HotKeysStop stops the ongoing hotkeys collection session.
func (c cmdable) HotKeysStop(ctx context.Context) *StatusCmd {
	cmd := NewStatusCmd(ctx, "hotkeys", "stop")
	_ = c(ctx, cmd)
	return cmd
}

// HotKeysReset discards the last hotkeys collection session results.
// Returns an error if tracking is currently active.
func (c cmdable) HotKeysReset(ctx context.Context) *StatusCmd {
	cmd := NewStatusCmd(ctx, "hotkeys", "reset")
	_ = c(ctx, cmd)
	return cmd
}

// HotKeysGet retrieves the results of the ongoing or last hotkeys collection session.
func (c cmdable) HotKeysGet(ctx context.Context) *HotKeysCmd {
	cmd := NewHotKeysCmd(ctx, "hotkeys", "get")
	_ = c(ctx, cmd)
	return cmd
}
