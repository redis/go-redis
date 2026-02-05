package redis

import (
	"context"
	"strings"
)

// HotKeysMetric represents the metrics that can be tracked by the HOTKEYS command.
type HotKeysMetric string

const (
	// HotKeysMetricCPU tracks CPU time spent on the key (in microseconds).
	HotKeysMetricCPU HotKeysMetric = "CPU"
	// HotKeysMetricNET tracks network bytes used by the key (ingress + egress + replication).
	HotKeysMetricNET HotKeysMetric = "NET"
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
	// Metrics to track. At least one must be specified.
	Metrics []HotKeysMetric
	// Count is the number of top keys to report.
	// Default: 10, Min: 10, Max: 64
	Count int64
	// Duration is the auto-stop tracking after this many seconds.
	// Default: 0 (no auto-stop)
	Duration int64
	// Sample is the sample ratio - track keys with probability 1/sample.
	// Default: 1 (track every key), Min: 1
	Sample int64
	// Slots specifies specific hash slots to track.
	// All specified slots must be hosted by the receiving node.
	// If not specified, all slots are tracked.
	Slots []int64
}

// HotKeysStart starts collecting hotkeys data.
// At least one metric must be specified in args.Metrics.
func (c cmdable) HotKeysStart(ctx context.Context, args *HotKeysStartArgs) *StatusCmd {
	cmdArgs := make([]interface{}, 0, 16)
	cmdArgs = append(cmdArgs, "hotkeys", "start")

	// Metrics are required - at least one must be specified
	if len(args.Metrics) > 0 {
		cmdArgs = append(cmdArgs, "metrics", len(args.Metrics))
		for _, metric := range args.Metrics {
			cmdArgs = append(cmdArgs, strings.ToLower(string(metric)))
		}
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
