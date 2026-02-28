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
	cl, ok := logger.(*LoggerWrapper)
	if !ok {
		cl = &LoggerWrapper{
			logger: logger,
		}
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

// ErrorContext is a structured error level logging method with context and arguments.
func (cl *LoggerWrapper) ErrorContext(ctx context.Context, msg string, args ...any) {
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
	ctx, msg, args := cl.printfToStructured(ctx, format, v...)
	cl.logger.ErrorContext(ctx, msg, args...)
}

// WarnContext is a structured warning level logging method with context and arguments.
func (cl *LoggerWrapper) WarnContext(ctx context.Context, msg string, args ...any) {
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
	ctx, msg, args := cl.printfToStructured(ctx, format, v...)
	cl.logger.WarnContext(ctx, msg, args...)
}

// InfoContext is a structured info level logging method with context and arguments.
func (cl *LoggerWrapper) InfoContext(ctx context.Context, msg string, args ...any) {
	if cl == nil || cl.logger == nil {
		legacyLoggerWithLevel.Infof(ctx, msg, args...)
		return
	}
	cl.logger.InfoContext(ctx, msg, args...)
}

// DebugContext is a structured debug level logging method with context and arguments.
func (cl *LoggerWrapper) DebugContext(ctx context.Context, msg string, args ...any) {
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

	ctx, msg, args := cl.printfToStructured(ctx, format, v...)
	cl.logger.InfoContext(ctx, msg, args...)
}

func (cl *LoggerWrapper) Debugf(ctx context.Context, format string, v ...any) {
	if cl == nil || cl.logger == nil {
		legacyLoggerWithLevel.Debugf(ctx, format, v...)
		return
	}
	ctx, msg, args := cl.printfToStructured(ctx, format, v...)
	cl.logger.DebugContext(ctx, msg, args...)
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

	// delegate to a method that use go:build tags to determine how to check level
	return isLevelEnabled(ctx, cl.logger, level)
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

	//TODO(ndyakov): add Enabled when Go 1.21 is min supported
}

// Verify that LoggerWrapper implements LoggerWithLevelI
var _ LoggerWithLevelI = (*LoggerWrapper)(nil)
