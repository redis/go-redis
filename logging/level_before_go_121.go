//go:build !go1.21

package logging

// The purpose of this file is to provide an implementation of isLevelEnabled
// when [slog.Logger.Enabled] method is not available (before Go 1.21).

import "context"

// isLevelEnabled checks whether the given logging level is enabled
// for the provided logger. Since before Go 1.21 we don't have
// [slog.Logger.Enabled], we always fall back to the legacy logger.
func isLevelEnabled(ctx context.Context, logger LoggerWithLevelI, level LogLevelT) bool {
	// unknown logger type, fall back to legacy logger
	return legacyLoggerWithLevel.Enabled(ctx, level)
}
