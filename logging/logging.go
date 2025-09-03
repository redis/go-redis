// Package logging provides logging level constants and utilities for the go-redis library.
// This package centralizes logging configuration to ensure consistency across all components.
package logging

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9/internal"
)

// LogLevel represents the logging level
type LogLevel int

// Log level constants for the entire go-redis library
const (
	LogLevelError LogLevel = iota // 0 - errors only
	LogLevelWarn                  // 1 - warnings and errors
	LogLevelInfo                  // 2 - info, warnings, and errors
	LogLevelDebug                 // 3 - debug, info, warnings, and errors
)

// String returns the string representation of the log level
func (l LogLevel) String() string {
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
func (l LogLevel) IsValid() bool {
	return l >= LogLevelError && l <= LogLevelDebug
}

func (l LogLevel) WarnOrAbove() bool {
	return l >= LogLevelWarn
}

func (l LogLevel) InfoOrAbove() bool {
	return l >= LogLevelInfo
}

func (l LogLevel) DebugOrAbove() bool {
	return l >= LogLevelDebug
}

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
