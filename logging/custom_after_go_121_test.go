//go:build go1.21

package logging

// The purpose of this file is to provide tests for [LoggerWrapper] with [slog.Logger].
// These tests require Go 1.21 or above because they use [slog.Logger] which was not available
// before Go 1.21.

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"testing"
)

// validation that [slog.Logger] implements [LoggerWithLevelI]
var _ LoggerWithLevelI = &slog.Logger{}

var _ *LoggerWrapper = NewLoggerWrapper(&slog.Logger{})

func TestLoggerWrapper_slog(t *testing.T) {

	ctx := context.Background()

	t.Run("Debug", func(t *testing.T) {
		wrapped, buf := NewTestLogger(t)
		wrapped.DebugContext(ctx, "debug message", "foo", "bar")

		checkLog(t, buf, map[string]any{
			"level": "DEBUG",
			"msg":   "debug message",
			"foo":   "bar",
		})
	})

	t.Run("Info", func(t *testing.T) {
		wrapped, buf := NewTestLogger(t)
		wrapped.InfoContext(ctx, "info message", "foo", "bar")

		checkLog(t, buf, map[string]any{
			"level": "INFO",
			"msg":   "info message",
			"foo":   "bar",
		})
	})
	t.Run("Warn", func(t *testing.T) {
		wrapped, buf := NewTestLogger(t)
		wrapped.WarnContext(ctx, "warn message", "foo", "bar")

		checkLog(t, buf, map[string]any{
			"level": "WARN",
			"msg":   "warn message",
			"foo":   "bar",
		})
	})
	t.Run("Error", func(t *testing.T) {
		wrapped, buf := NewTestLogger(t)
		wrapped.ErrorContext(ctx, "error message", "foo", "bar")

		checkLog(t, buf, map[string]any{
			"level": "ERROR",
			"msg":   "error message",
			"foo":   "bar",
		})
	})

	t.Run("Errorf", func(t *testing.T) {
		wrapped, buf := NewTestLogger(t)
		wrapped.Errorf(ctx, "%d is the answer to %s", 42, "everything")

		checkLog(t, buf, map[string]any{
			"level": "ERROR",
			"msg":   "42 is the answer to everything",
		})
	})

	t.Run("Infof", func(t *testing.T) {
		wrapped, buf := NewTestLogger(t)
		wrapped.Infof(ctx, "%d is the answer to %s", 42, "everything")

		checkLog(t, buf, map[string]any{
			"level": "INFO",
			"msg":   "42 is the answer to everything",
		})
	})

	t.Run("Warnf", func(t *testing.T) {
		wrapped, buf := NewTestLogger(t)
		wrapped.Warnf(ctx, "%d is the answer to %s", 42, "everything")

		checkLog(t, buf, map[string]any{
			"level": "WARN",
			"msg":   "42 is the answer to everything",
		})
	})

	t.Run("Debugf", func(t *testing.T) {
		wrapped, buf := NewTestLogger(t)
		wrapped.Debugf(ctx, "%d is the answer to %s", 42, "everything")

		checkLog(t, buf, map[string]any{
			"level": "DEBUG",
			"msg":   "42 is the answer to everything",
		})
	})

	t.Run("Insufficient loglevel: default", func(t *testing.T) {
		wrapped, buf := NewTestLoggerWithLevel(t, nil)

		wrapped.DebugContext(ctx, "debug message", "foo", "bar")
		if buf.Len() != 0 {
			t.Errorf("expected no log message, got %s", buf.String())
		}
	})

	t.Run("Insufficient loglevel: error", func(t *testing.T) {
		wrapped, buf := NewTestLoggerWithLevel(t, slog.LevelError)
		wrapped.DebugContext(ctx, "debug message", "foo", "bar")
		wrapped.WarnContext(ctx, "warn message", "foo", "bar")
		wrapped.InfoContext(ctx, "info message", "foo", "bar")
		if buf.Len() != 0 {
			t.Errorf("expected no log message, got %s", buf.String())
		}
		wrapped.ErrorContext(ctx, "error message", "foo", "bar")

		checkLog(t, buf, map[string]any{
			"level": "ERROR",
			"msg":   "error message",
			"foo":   "bar",
		})
	})

	t.Run("Enabled loglevel: debug", func(t *testing.T) {

		wrapped, buf := NewTestLoggerWithLevel(t, slog.LevelInfo)
		if wrapped.Enabled(ctx, LogLevelDebug) {
			t.Errorf("expected debug level to be disabled")
		}
		if !wrapped.Enabled(ctx, LogLevelInfo) {
			t.Errorf("expected info level to be enabled")
		}
		if !wrapped.Enabled(ctx, LogLevelWarn) {
			t.Errorf("expected warn level to be enabled")
		}

		if !wrapped.Enabled(ctx, LogLevelError) {
			t.Errorf("expected error level to be enabled")
		}

		wrapped.DebugContext(ctx, "debug message", "foo", "bar")
		if buf.Len() != 0 {
			t.Errorf("expected no log message, got %s", buf.String())
		}

		wrapped.InfoContext(ctx, "info message", "foo", "bar")
		checkLog(t, buf, map[string]any{
			"level": "INFO",
			"msg":   "info message",
			"foo":   "bar",
		})
	})
}

func NewTestLogger(t *testing.T) (*LoggerWrapper, *bytes.Buffer) {
	return NewTestLoggerWithLevel(t, slog.LevelDebug)
}

func NewTestLoggerWithLevel(t *testing.T, level slog.Leveler) (*LoggerWrapper, *bytes.Buffer) {
	var buf bytes.Buffer
	wrapped := NewLoggerWrapper(slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: level})))
	return wrapped, &buf
}

func checkLog(t *testing.T, buf *bytes.Buffer, attrs map[string]any) {
	t.Helper()

	res := buf.Bytes()

	var m map[string]any
	err := json.Unmarshal(res, &m)
	if err != nil {
		t.Fatalf("failed to unmarshal log message: %v", err)
	}

	delete(m, "time") // remove time for testing purposes

	if len(m) != len(attrs) {
		t.Errorf("expected %d attributes, got %d", len(attrs), len(m))
	}

	for k, expected := range attrs {
		v, ok := m[k]
		if !ok {
			t.Errorf("expected log to have key %s", k)
			continue
		}
		if v != expected {
			t.Errorf("expected %s to be %v, got %v", k, expected, v)
		}
	}

	if t.Failed() {
		t.Logf("log message: %s", res)
		t.Log("time is ignored in comparison")
	}
}

func ExampleNewLoggerWrapper_slog_logger() {
	// assuming you have a context
	ctx := context.Background()

	// assuming you have a slogLogger
	slogLogger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	// Create a LoggerWrapper with the custom logger
	redisLogger := NewLoggerWrapper(slogLogger)

	// Use the wrapped logger with Printf-style logging
	redisLogger.Infof(ctx, "This is an info message: %s", "hello world")

	// Use the wrapped logger with structured logging
	redisLogger.ErrorContext(ctx, "This is an error message", "key", "value")
}
