package internal

import (
	"context"
	"fmt"
	"log"
	"os"
)

// TODO (ned): Revisit logging
// Add more standardized approach with log levels and configurability

type Logging interface {
	Printf(ctx context.Context, format string, v ...interface{})
}

type DefaultLogger struct {
	log *log.Logger
}

func (l *DefaultLogger) Printf(ctx context.Context, format string, v ...interface{}) {
	_ = l.log.Output(2, fmt.Sprintf(format, v...))
}

func NewDefaultLogger() Logging {
	return &DefaultLogger{
		log: log.New(os.Stderr, "redis: ", log.LstdFlags|log.Lshortfile),
	}
}

// Logger calls Output to print to the stderr.
// Arguments are handled in the manner of fmt.Print.
var Logger Logging = NewDefaultLogger()

var LogLevel LogLevelT = LogLevelError

// LogLevelT represents the logging level
type LogLevelT int

// Log level constants for the entire go-redis library
const (
	LogLevelError LogLevelT = iota // 0 - errors only
	LogLevelWarn                   // 1 - warnings and errors
	LogLevelInfo                   // 2 - info, warnings, and errors
	LogLevelDebug                  // 3 - debug, info, warnings, and errors
)

// String returns the string representation of the log level
func (l LogLevelT) String() string {
	switch l {
	case LogLevelError:
		return "ERROR"
	case LogLevelWarn:
		return "WARN"
	case LogLevelInfo:
		return "INFO"
	case LogLevelDebug:
		return "DEBUG"
	default:
		return "UNKNOWN"
	}
}

// IsValid returns true if the log level is valid
func (l LogLevelT) IsValid() bool {
	return l >= LogLevelError && l <= LogLevelDebug
}

func (l LogLevelT) WarnOrAbove() bool {
	return l >= LogLevelWarn
}

func (l LogLevelT) InfoOrAbove() bool {
	return l >= LogLevelInfo
}

func (l LogLevelT) DebugOrAbove() bool {
	return l >= LogLevelDebug
}

// LoggerWithLevel is a logger interface with leveled logging methods.
//
// This interface can be implemented by custom loggers to provide leveled logging.
type LoggerWithLevel interface {
	// Infof logs an info level message
	Infof(ctx context.Context, format string, v ...interface{})

	// Warnf logs a warning level message
	Warnf(ctx context.Context, format string, v ...interface{})

	// Debugf logs a debug level message
	Debugf(ctx context.Context, format string, v ...interface{})

	// Errorf logs an error level message
	Errorf(ctx context.Context, format string, v ...interface{})

	// Enabled reports whether the given log level is enabled in the logger
	Enabled(ctx context.Context, level LogLevelT) bool
}

// legacyLoggerAdapter is a logger that implements LoggerWithLevel interface
// using the global [Logger] and [LogLevel] variables.
type legacyLoggerAdapter struct{}

func (l *legacyLoggerAdapter) Infof(ctx context.Context, format string, v ...interface{}) {
	if LogLevel.InfoOrAbove() {
		Logger.Printf(ctx, format, v...)
	}
}

func (l *legacyLoggerAdapter) Warnf(ctx context.Context, format string, v ...interface{}) {
	if LogLevel.WarnOrAbove() {
		Logger.Printf(ctx, format, v...)
	}
}

func (l *legacyLoggerAdapter) Debugf(ctx context.Context, format string, v ...interface{}) {
	if LogLevel.DebugOrAbove() {
		Logger.Printf(ctx, format, v...)
	}
}

func (l legacyLoggerAdapter) Errorf(ctx context.Context, format string, v ...interface{}) {
	Logger.Printf(ctx, format, v...)
}

func (l legacyLoggerAdapter) Enabled(_ context.Context, level LogLevelT) bool {
	switch level {
	case LogLevelWarn:
		return LogLevel.WarnOrAbove()
	case LogLevelInfo:
		return LogLevel.InfoOrAbove()
	case LogLevelDebug:
		return LogLevel.DebugOrAbove()
	case LogLevelError:
		fallthrough
	default:
		return true
	}
}

var LegacyLoggerWithLevel LoggerWithLevel = &legacyLoggerAdapter{}
