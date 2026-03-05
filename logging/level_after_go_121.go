//go:build go1.21

package logging

// The purpose of this file is to provide an implementation of isLevelEnabled
// that uses [slog.Logger.Enabled] method when available.

import (
	"context"
	"log/slog"
)

// isLevelEnabled checks whether the given logging level is enabled
// for the provided logger. If the logger is of type [slog.Logger],
// it uses its [slog.Logger.Enabled] method to determine whether
// the level is enabled.
func isLevelEnabled(ctx context.Context, logger LoggerWithLevelI, level LogLevelT) bool {
	sl, ok := logger.(slogEnabler)
	if !ok {
		// unknown logger type, fall back to legacy logger
		return legacyLoggerWithLevel.Enabled(ctx, level)
	}

	// map our [LogLevelT] to [slog.Level]
	// TODO(ccoVeille): simplify in v10 align when levels will be aligned with slog.Level
	slogLevel, ok := levelMap[level]
	if !ok {
		// unknown level, assume enabled
		return true
	}
	return sl.Enabled(ctx, slogLevel)

}

// TODO(ccoVeille): simplify in v10 align when levels will be aligned with slog.Level
var levelMap = map[LogLevelT]slog.Level{
	LogLevelDebug: slog.LevelDebug,
	LogLevelInfo:  slog.LevelInfo,
	LogLevelWarn:  slog.LevelWarn,
	LogLevelError: slog.LevelError,
}

type slogEnabler interface {
	Enabled(ctx context.Context, level slog.Level) bool
}

// Verify that [slog.Logger] implements [slogEnabler]
var _ slogEnabler = (*slog.Logger)(nil)
