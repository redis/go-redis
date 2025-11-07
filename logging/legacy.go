package logging

import (
	"context"

	"github.com/redis/go-redis/v9/internal"
)

// legacyLoggerAdapter is a logger that implements [LoggerWithLevel] interface
// using the global [internal.Logger] and [internal.LogLevel] variables.
type legacyLoggerAdapter struct{}

var _ LoggerWithLevel = (*legacyLoggerAdapter)(nil)

// structuredToPrintf converts a structured log message and key-value pairs into something a Printf-style logger can understand.
func (l *legacyLoggerAdapter) structuredToPrintf(msg string, v ...any) (string, []any) {
	format := msg
	var args []any

	for i := 0; i < len(v); i += 2 {
		if i+1 >= len(v) {
			break
		}
		format += " %v=%v"
		args = append(args, v[i], v[i+1])
	}

	return format, args
}

func (l legacyLoggerAdapter) Errorf(ctx context.Context, format string, v ...any) {
	internal.Logger.Printf(ctx, format, v...)
}

func (l *legacyLoggerAdapter) ErrorContext(ctx context.Context, msg string, args ...any) {
	format, v := l.structuredToPrintf(msg, args...)
	l.Errorf(ctx, format, v...)
}

func (l *legacyLoggerAdapter) WarnContext(ctx context.Context, msg string, args ...any) {
	format, v := l.structuredToPrintf(msg, args...)
	l.Warnf(ctx, format, v...)
}

func (l *legacyLoggerAdapter) Warnf(ctx context.Context, format string, v ...any) {
	if !internal.LogLevel.WarnOrAbove() {
		// Skip logging
		return
	}
	internal.Logger.Printf(ctx, format, v...)
}

func (l *legacyLoggerAdapter) InfoContext(ctx context.Context, msg string, args ...any) {
	format, v := l.structuredToPrintf(msg, args...)
	l.Infof(ctx, format, v...)
}

func (l *legacyLoggerAdapter) Infof(ctx context.Context, format string, v ...any) {
	if !internal.LogLevel.InfoOrAbove() {
		// Skip logging
		return
	}
	internal.Logger.Printf(ctx, format, v...)
}

func (l *legacyLoggerAdapter) DebugContext(ctx context.Context, msg string, args ...any) {
	format, v := l.structuredToPrintf(msg, args...)
	l.Debugf(ctx, format, v...)
}

func (l *legacyLoggerAdapter) Debugf(ctx context.Context, format string, v ...any) {
	if !internal.LogLevel.DebugOrAbove() {
		// Skip logging
		return
	}
	internal.Logger.Printf(ctx, format, v...)
}

func (l *legacyLoggerAdapter) Enabled(ctx context.Context, level LogLevelT) bool {
	switch level {
	case LogLevelDebug:
		return internal.LogLevel.DebugOrAbove()
	case LogLevelWarn:
		return internal.LogLevel.WarnOrAbove()
	case LogLevelInfo:
		return internal.LogLevel.InfoOrAbove()
	}
	return true
}

var legacyLoggerWithLevel = &legacyLoggerAdapter{}
