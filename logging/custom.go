package logging

import (
	"context"
	"fmt"
)

// LoggerWrapper is a slog.Logger wrapper that implements the Lgr interface.
type LoggerWrapper struct {
	logger        LoggerWithLevelI
	loggerLevel   *LogLevelT
	printfAdapter PrintfAdapter
}

func NewLoggerWrapper(logger LoggerWithLevelI, opts ...LoggerWrapperOption) *LoggerWrapper {
	cl := &LoggerWrapper{
		logger: logger,
	}
	for _, opt := range opts {
		opt(cl)
	}
	return cl
}

type LoggerWrapperOption func(*LoggerWrapper)

func WithPrintfAdapter(adapter PrintfAdapter) LoggerWrapperOption {
	return func(cl *LoggerWrapper) {
		cl.printfAdapter = adapter
	}
}

func WithLoggerLevel(level LogLevelT) LoggerWrapperOption {
	return func(cl *LoggerWrapper) {
		cl.loggerLevel = &level
	}
}

// PrintfAdapter is a function that converts Printf-style log messages into structured log messages.
// It can be used to extract key-value pairs from the formatted message.
type PrintfAdapter func(ctx context.Context, format string, v ...any) (context.Context, string, []any)

// Error is a structured error level logging method with context and arguments.
func (cl *LoggerWrapper) Error(ctx context.Context, msg string, args ...any) {
	if cl == nil || cl.logger == nil {
		legacyLoggerWithLevel.Errorf(ctx, msg, args...)
		return
	}
	cl.logger.ErrorContext(ctx, msg, args...)
}

func (cl *LoggerWrapper) Errorf(ctx context.Context, format string, v ...any) {
	if cl == nil || cl.logger == nil {
		legacyLoggerWithLevel.Errorf(ctx, format, v...)
		return
	}
	cl.logger.ErrorContext(ctx, format, v...)
}

// Warn is a structured warning level logging method with context and arguments.
func (cl *LoggerWrapper) Warn(ctx context.Context, msg string, args ...any) {
	if cl == nil || cl.logger == nil {
		legacyLoggerWithLevel.Warnf(ctx, msg, args...)
		return
	}
	cl.logger.WarnContext(ctx, msg, args...)
}

func (cl *LoggerWrapper) Warnf(ctx context.Context, format string, v ...any) {
	if cl == nil || cl.logger == nil {
		legacyLoggerWithLevel.Warnf(ctx, format, v...)
		return
	}
	cl.logger.WarnContext(cl.printfToStructured(ctx, format, v...))
}

// Info is a structured info level logging method with context and arguments.
func (cl *LoggerWrapper) Info(ctx context.Context, msg string, args ...any) {
	if cl == nil || cl.logger == nil {
		legacyLoggerWithLevel.Infof(ctx, msg, args...)
		return
	}
	cl.logger.InfoContext(ctx, msg, args...)
}

// Debug is a structured debug level logging method with context and arguments.
func (cl *LoggerWrapper) Debug(ctx context.Context, msg string, args ...any) {
	if cl == nil || cl.logger == nil {
		legacyLoggerWithLevel.Debugf(ctx, msg, args...)
		return
	}
	cl.logger.DebugContext(ctx, msg, args...)
}

func (cl *LoggerWrapper) Infof(ctx context.Context, format string, v ...any) {
	if cl == nil || cl.logger == nil {
		legacyLoggerWithLevel.Infof(ctx, format, v...)
		return
	}

	cl.logger.InfoContext(cl.printfToStructured(ctx, format, v...))
}

func (cl *LoggerWrapper) Debugf(ctx context.Context, format string, v ...any) {
	if cl == nil || cl.logger == nil {
		legacyLoggerWithLevel.Debugf(ctx, format, v...)
		return
	}
	cl.logger.DebugContext(cl.printfToStructured(ctx, format, v...))
}

func (cl *LoggerWrapper) printfToStructured(ctx context.Context, format string, v ...any) (context.Context, string, []any) {
	if cl != nil && cl.printfAdapter != nil {
		return cl.printfAdapter(ctx, format, v...)
	}
	return ctx, fmt.Sprintf(format, v...), nil
}

func (cl *LoggerWrapper) Enabled(ctx context.Context, level LogLevelT) bool {
	if cl != nil && cl.loggerLevel != nil {
		return level >= *cl.loggerLevel
	}

	return legacyLoggerWithLevel.Enabled(ctx, level)
}

// LoggerWithLevelI is a logger interface with leveled logging methods.
//
// [slog.Logger] from the standard library satisfies this interface.
type LoggerWithLevelI interface {
	// InfoContext logs an info level message
	InfoContext(ctx context.Context, format string, v ...any)

	// WarnContext logs a warning level message
	WarnContext(ctx context.Context, format string, v ...any)

	// Debugf logs a debug level message
	DebugContext(ctx context.Context, format string, v ...any)

	// Errorf logs an error level message
	ErrorContext(ctx context.Context, format string, v ...any)
}
