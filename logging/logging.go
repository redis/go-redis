// Package logging provides logging level constants and utilities for the go-redis library.
// This package centralizes logging configuration to ensure consistency across all components.
package logging

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9/internal"
)

type LogLevelT = internal.LogLevelT

const (
	LogLevelError = internal.LogLevelError
	LogLevelWarn  = internal.LogLevelWarn
	LogLevelInfo  = internal.LogLevelInfo
	LogLevelDebug = internal.LogLevelDebug
)

// VoidLogger is a logger that does nothing.
// Used to disable logging and thus speed up the library.
type VoidLogger struct{}

func (v *VoidLogger) Printf(_ context.Context, _ string, _ ...interface{}) {
	// do nothing
}

// Disable disables logging by setting the internal logger to a void logger.
// This can be used to speed up the library if logging is not needed.
// It will override any custom logger that was set before and set the VoidLogger.
func Disable() {
	internal.Logger = &VoidLogger{}
}

// Enable enables logging by setting the internal logger to the default logger.
// This is the default behavior.
// You can use redis.SetLogger to set a custom logger.
//
// NOTE: This function is not thread-safe.
// It will override any custom logger that was set before and set the DefaultLogger.
func Enable() {
	internal.Logger = internal.NewDefaultLogger()
}

// SetLogLevel sets the log level for the library.
func SetLogLevel(logLevel LogLevelT) {
	internal.LogLevel = logLevel
}

// NewBlacklistLogger returns a new logger that filters out messages containing any of the substrings.
// This can be used to filter out messages containing sensitive information.
func NewBlacklistLogger(substr []string) internal.Logging {
	l := internal.NewDefaultLogger()
	return &filterLogger{logger: l, substr: substr, blacklist: true}
}

// NewWhitelistLogger returns a new logger that only logs messages containing any of the substrings.
// This can be used to only log messages related to specific commands or patterns.
func NewWhitelistLogger(substr []string) internal.Logging {
	l := internal.NewDefaultLogger()
	return &filterLogger{logger: l, substr: substr, blacklist: false}
}

type filterLogger struct {
	logger    internal.Logging
	blacklist bool
	substr    []string
}

func (l *filterLogger) Printf(ctx context.Context, format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	found := false
	for _, substr := range l.substr {
		if strings.Contains(msg, substr) {
			found = true
			if l.blacklist {
				return
			}
		}
	}
	// whitelist, only log if one of the substrings is present
	if !l.blacklist && !found {
		return
	}
	if l.logger != nil {
		l.logger.Printf(ctx, format, v...)
		return
	}
}

// Lgr is a logger interface with leveled logging methods.
// It is implemented by LoggerWrapper and legacyLoggerAdapter.
// If you would like to use `slog.Logger` from the standard library, you can use LoggerWrapper.
//
//		logger := slog.New(slog.NewTextHandler(os.Stderr))
//	    db := redis.NewClient(&redis.Options{
//			Logger: logging.NewLoggerWrapper(logger),
//		})
//
// This will NOT handle all logging at the moment, singe there is still a global logger in use.
// that will be deprecated in the future.
type Lgr interface {
	Errorf(ctx context.Context, format string, v ...any)
	Warnf(ctx context.Context, format string, v ...any)
	Infof(ctx context.Context, format string, v ...any)
	Debugf(ctx context.Context, format string, v ...any)
	Enabled(ctx context.Context, level LogLevelT) bool
}
