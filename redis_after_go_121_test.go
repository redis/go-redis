//go:build go1.21

package redis_test

// The purpose of this file is to provide an implementation of isLevelEnabled
// that uses [slog.Logger.Enabled] method when available.

import (
	"context"
	"log/slog"
	"os"
	"strings"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/logging"
)

// You can pass a custom logger that implements [logging.LoggerWithLevelI] interface
// to the Redis [Client]. Here's an example using slog.Logger.
func ExampleNewClient_with_slog_logger() {
	// assuming you have a context
	ctx := context.Background()

	// assuming you have a logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	// Create a Redis client with the custom logger
	rdb := redis.NewClient(&redis.Options{
		Addr:   ":6379",
		Logger: logger,
	})

	// Use the Redis client
	_ = rdb.Set(ctx, "key", "value", 0)
}

// You can also wrap your slog.Logger with [logging.NewLoggerWrapper] to
// customize its behavior when used with Redis client. Here's an example:
func ExampleNewClient_with_logger_wrapper() {
	// assuming you have a context
	ctx := context.Background()

	// assuming you have a logger
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelDebug,
	}))

	// Wrap the slog.Logger with LoggerWrapper
	wrappedLogger := logging.NewLoggerWrapper(
		logger,
		logging.WithLoggerLevel(logging.LogLevelDebug), // set minimum log level to Debug
		logging.WithPrintfAdapter(
			func(ctx context.Context, format string, v ...any) (context.Context, string, []any) {

				if after, ok := strings.CutPrefix(format, "redis:"); ok {
					// adjust the format string to remove "redis:" prefix when present
					// some log messages from go-redis have this prefix
					format = after
				}

				// Here is an example of customizing the log format:
				// if any of the arguments is an error, add "ERROR:" prefix to the format
				// you can customize this logic as needed
				//
				// for example, you might want to inject the `err` value into the context,
				// if you have a slog.Logger that supports it.
				for i := range v {
					if _, ok := v[i].(error); ok {
						format = "ERROR: " + format
						break
					}
				}

				// add a prefix to indicate these are Redis logs
				format = "redis-logger: " + format

				return ctx, format, v
			},
		),
	)

	// Create a Redis client with the wrapped logger
	rdb := redis.NewClient(&redis.Options{
		Addr:   ":6379",
		Logger: wrappedLogger,
	})

	// Use the Redis client
	_ = rdb.Set(ctx, "key", "value", 0)
}

// You can use your own logger that satisfies [logging.LoggerWithLevelI] interface. Here's an example:
func ExampleNewClient_with_custom_logger() {
	// assuming you have your own logger that implements [logging.LoggerWithLevelI]
	var myLogger logging.LoggerWithLevelI

	// Create a Redis client with the custom logger
	rdb := redis.NewClient(&redis.Options{
		Addr:   ":6379",
		Logger: myLogger,
	})

	// Use the Redis client
	_ = rdb.Set(ctx, "key", "value", 0)
}
