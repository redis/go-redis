package redisotel

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/redis/go-redis/v9"
)

// TestWithExcludedCommands tests the new O(1) command exclusion feature.
func TestWithExcludedCommands(t *testing.T) {
	t.Run("exclude multiple commands with O(1) lookup", func(t *testing.T) {
		provider := sdktrace.NewTracerProvider()
		hook := newTracingHook(
			"",
			WithTracerProvider(provider),
			WithExcludedCommands("PING", "INFO", "SELECT"),
		)
		ctx, span := provider.Tracer("redis-test").Start(context.Background(), "redis-test")
		defer span.End()

		tests := []struct {
			name         string
			cmdName      string
			shouldFilter bool
		}{
			// Commands that should be excluded
			{"ping lowercase should be excluded", "ping", true},
			{"PING uppercase should be excluded", "PING", true},
			{"info lowercase should be excluded", "info", true},
			{"INFO uppercase should be excluded", "INFO", true},
			{"select lowercase should be excluded", "select", true},
			{"SELECT uppercase should be excluded", "SELECT", true},
			// Commands that should not be excluded
			{"get command should not be excluded", "get", false},
			{"set command should not be excluded", "set", false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				cmd := redis.NewCmd(ctx, tt.cmdName)
				processHook := hook.ProcessHook(func(ctx context.Context, cmd redis.Cmder) error {
					innerSpan := trace.SpanFromContext(ctx).(sdktrace.ReadOnlySpan)
					if tt.shouldFilter {
						// Filtered commands should use parent span, not create new span
						if innerSpan.Name() != "redis-test" {
							t.Errorf("command %q should be filtered out, got span name %q", tt.cmdName, innerSpan.Name())
						}
					} else {
						// Should create new span with command name
						if innerSpan.Name() != tt.cmdName {
							t.Fatalf("%s command should not be filtered", tt.cmdName)
						}
					}
					return nil
				})
				err := processHook(ctx, cmd)
				if err != nil {
					t.Fatal(err)
				}
			})
		}
	})

	t.Run("multiple calls to WithExcludedCommands should accumulate", func(t *testing.T) {
		provider := sdktrace.NewTracerProvider()
		hook := newTracingHook(
			"",
			WithTracerProvider(provider),
			WithExcludedCommands("PING", "INFO"),
			WithExcludedCommands("SELECT", "CONFIG"),
		)
		ctx, span := provider.Tracer("redis-test").Start(context.Background(), "redis-test")
		defer span.End()

		tests := []struct {
			cmdName      string
			shouldFilter bool
		}{
			{"PING", true},   // From first call
			{"INFO", true},   // From first call
			{"SELECT", true}, // From second call
			{"CONFIG", true}, // From second call
			{"GET", false},   // Not excluded
		}

		for _, tt := range tests {
			t.Run(fmt.Sprintf("command_%s", tt.cmdName), func(t *testing.T) {
				cmd := redis.NewCmd(ctx, tt.cmdName)
				processHook := hook.ProcessHook(func(ctx context.Context, cmd redis.Cmder) error {
					innerSpan := trace.SpanFromContext(ctx).(sdktrace.ReadOnlySpan)
					if tt.shouldFilter {
						// Filtered commands should use parent span
						if innerSpan.Name() != "redis-test" {
							t.Errorf("command %q should be filtered out, got span name %q", tt.cmdName, innerSpan.Name())
						}
					} else {
						// Should create new span with command name (normalized to lowercase)
						expectedSpanName := strings.ToLower(tt.cmdName)
						if innerSpan.Name() != expectedSpanName {
							t.Errorf("command %q should not be filtered, got span name %q, expected %q", tt.cmdName, innerSpan.Name(), expectedSpanName)
						}
					}
					return nil
				})
				err := processHook(ctx, cmd)
				if err != nil {
					t.Fatal(err)
				}
			})
		}
	})
}

// TestUnifiedFilteringAPI tests the new unified filtering API.
func TestUnifiedFilteringAPI(t *testing.T) {
	t.Run("process filter with custom logic", func(t *testing.T) {
		provider := sdktrace.NewTracerProvider()
		hook := newTracingHook(
			"",
			WithTracerProvider(provider),
			WithProcessFilter(func(cmd redis.Cmder) bool {
				// Filter out commands that start with "dangerous"
				return len(cmd.Name()) > 9 && cmd.Name()[:9] == "dangerous"
			}),
		)
		ctx, span := provider.Tracer("redis-test").Start(context.Background(), "redis-test")
		defer span.End()

		tests := []struct {
			name         string
			cmdName      string
			shouldFilter bool
		}{
			{"dangerous command should be filtered", "dangerous_cmd", true},
			{"safe command should not be filtered", "safe_cmd", false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				cmd := redis.NewCmd(ctx, tt.cmdName)
				processHook := hook.ProcessHook(func(ctx context.Context, cmd redis.Cmder) error {
					innerSpan := trace.SpanFromContext(ctx).(sdktrace.ReadOnlySpan)
					if tt.shouldFilter {
						if innerSpan.Name() != "redis-test" {
							t.Fatalf("%s command should be filtered out", tt.cmdName)
						}
					} else {
						if innerSpan.Name() != tt.cmdName {
							t.Fatalf("%s command should not be filtered", tt.cmdName)
						}
					}
					return nil
				})
				err := processHook(ctx, cmd)
				if err != nil {
					t.Fatal(err)
				}
			})
		}
	})

	t.Run("process pipeline filter with custom logic", func(t *testing.T) {
		provider := sdktrace.NewTracerProvider()
		hook := newTracingHook(
			"",
			WithTracerProvider(provider),
			WithProcessPipelineFilter(func(cmds []redis.Cmder) bool {
				// Filter pipelines that contain more than 5 commands
				return len(cmds) > 5
			}),
		)
		ctx, span := provider.Tracer("redis-test").Start(context.TODO(), "redis-test")
		defer span.End()

		tests := []struct {
			name         string
			cmdCount     int
			shouldFilter bool
		}{
			{"small pipeline should not be filtered", 3, false},
			{"large pipeline should be filtered", 7, true},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				cmds := make([]redis.Cmder, tt.cmdCount)
				for i := 0; i < tt.cmdCount; i++ {
					cmds[i] = redis.NewCmd(ctx, "ping")
				}

				processPipelineHook := hook.ProcessPipelineHook(func(ctx context.Context, cmds []redis.Cmder) error {
					innerSpan := trace.SpanFromContext(ctx).(sdktrace.ReadOnlySpan)
					if tt.shouldFilter {
						if innerSpan.Name() != "redis-test" {
							t.Fatalf("large pipeline should be filtered out")
						}
					} else {
						if innerSpan.Name() != "redis.pipeline ping" {
							t.Fatalf("small pipeline should not be filtered")
						}
					}
					return nil
				})
				err := processPipelineHook(ctx, cmds)
				if err != nil {
					t.Fatal(err)
				}
			})
		}
	})

	t.Run("dial filter with custom logic", func(t *testing.T) {
		provider := sdktrace.NewTracerProvider()
		hook := newTracingHook(
			"",
			WithTracerProvider(provider),
			WithDialFilterFunc(func(network, addr string) bool {
				// Filter connections to localhost
				return addr == "localhost:6379"
			}),
		)
		ctx, span := provider.Tracer("redis-test").Start(context.TODO(), "redis-test")
		defer span.End()

		tests := []struct {
			name         string
			addr         string
			shouldFilter bool
		}{
			{"localhost should be filtered", "localhost:6379", true},
			{"remote should not be filtered", "remote:6379", false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				dialHook := hook.DialHook(func(ctx context.Context, network, addr string) (conn net.Conn, err error) {
					innerSpan := trace.SpanFromContext(ctx).(sdktrace.ReadOnlySpan)
					if tt.shouldFilter {
						if innerSpan.Name() != "redis-test" {
							t.Fatalf("localhost dial should be filtered out")
						}
					} else {
						if innerSpan.Name() != "redis.dial" {
							t.Fatalf("remote dial should not be filtered")
						}
					}
					return nil, nil
				})
				_, err := dialHook(ctx, "tcp", tt.addr)
				if err != nil {
					t.Fatal(err)
				}
			})
		}
	})
}

// TestCombinedApproach tests that ExcludedCommands fast path works with custom filters
func TestCombinedApproach(t *testing.T) {
	t.Run("excluded commands take precedence over custom filters", func(t *testing.T) {
		provider := sdktrace.NewTracerProvider()
		hook := newTracingHook(
			"",
			WithTracerProvider(provider),
			WithExcludedCommands("PING"),
			WithProcessFilter(func(cmd redis.Cmder) bool {
				// This filter would normally allow PING, but excluded commands take precedence
				return false // never filter
			}),
		)
		ctx, span := provider.Tracer("redis-test").Start(context.TODO(), "redis-test")
		cmd := redis.NewCmd(ctx, "ping")
		defer span.End()

		processHook := hook.ProcessHook(func(ctx context.Context, cmd redis.Cmder) error {
			innerSpan := trace.SpanFromContext(ctx).(sdktrace.ReadOnlySpan)
			if innerSpan.Name() != "redis-test" {
				t.Fatalf("PING should be excluded despite ProcessFilter saying otherwise")
			}
			return nil
		})
		err := processHook(ctx, cmd)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("custom filter applied when command not in exclusion set", func(t *testing.T) {
		provider := sdktrace.NewTracerProvider()
		hook := newTracingHook(
			"",
			WithTracerProvider(provider),
			WithExcludedCommands("PING"),
			WithProcessFilter(func(cmd redis.Cmder) bool {
				// Filter get commands
				return cmd.Name() == "get"
			}),
		)
		ctx, span := provider.Tracer("redis-test").Start(context.TODO(), "redis-test")
		defer span.End()

		tests := []struct {
			name         string
			cmdName      string
			shouldFilter bool
		}{
			{"ping excluded by set", "ping", true},
			{"get filtered by custom filter", "get", true},
			{"set not filtered", "set", false},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				cmd := redis.NewCmd(ctx, tt.cmdName)
				processHook := hook.ProcessHook(func(ctx context.Context, cmd redis.Cmder) error {
					innerSpan := trace.SpanFromContext(ctx).(sdktrace.ReadOnlySpan)
					if tt.shouldFilter {
						if innerSpan.Name() != "redis-test" {
							t.Fatalf("%s command should be filtered out", tt.cmdName)
						}
					} else {
						if innerSpan.Name() != tt.cmdName {
							t.Fatalf("%s command should not be filtered", tt.cmdName)
						}
					}
					return nil
				})
				err := processHook(ctx, cmd)
				if err != nil {
					t.Fatal(err)
				}
			})
		}
	})
}

// TestBackwardCompatibility ensures existing APIs still work
func TestBackwardCompatibility(t *testing.T) {
	t.Run("legacy WithCommandFilter still works", func(t *testing.T) {
		provider := sdktrace.NewTracerProvider()
		hook := newTracingHook(
			"",
			WithTracerProvider(provider),
			WithCommandFilter(func(cmd redis.Cmder) bool {
				return cmd.Name() == "ping"
			}),
		)
		ctx, span := provider.Tracer("redis-test").Start(context.TODO(), "redis-test")
		cmd := redis.NewCmd(ctx, "ping")
		defer span.End()

		processHook := hook.ProcessHook(func(ctx context.Context, cmd redis.Cmder) error {
			innerSpan := trace.SpanFromContext(ctx).(sdktrace.ReadOnlySpan)
			if innerSpan.Name() != "redis-test" {
				t.Fatalf("ping should be filtered by legacy filter")
			}
			return nil
		})
		err := processHook(ctx, cmd)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("legacy WithCommandsFilter still works", func(t *testing.T) {
		provider := sdktrace.NewTracerProvider()
		hook := newTracingHook(
			"",
			WithTracerProvider(provider),
			WithCommandsFilter(func(cmds []redis.Cmder) bool {
				return len(cmds) > 1
			}),
		)
		ctx, span := provider.Tracer("redis-test").Start(context.TODO(), "redis-test")
		cmds := []redis.Cmder{
			redis.NewCmd(ctx, "ping"),
			redis.NewCmd(ctx, "ping"),
		}
		defer span.End()

		processPipelineHook := hook.ProcessPipelineHook(func(ctx context.Context, cmds []redis.Cmder) error {
			innerSpan := trace.SpanFromContext(ctx).(sdktrace.ReadOnlySpan)
			if innerSpan.Name() != "redis-test" {
				t.Fatalf("multi-command pipeline should be filtered")
			}
			return nil
		})
		err := processPipelineHook(ctx, cmds)
		if err != nil {
			t.Fatal(err)
		}
	})

	t.Run("legacy WithDialFilter still works", func(t *testing.T) {
		provider := sdktrace.NewTracerProvider()
		hook := newTracingHook(
			"",
			WithTracerProvider(provider),
			WithDialFilter(true), // enable filtering
		)
		ctx, span := provider.Tracer("redis-test").Start(context.TODO(), "redis-test")
		defer span.End()

		dialHook := hook.DialHook(func(ctx context.Context, network, addr string) (conn net.Conn, err error) {
			innerSpan := trace.SpanFromContext(ctx).(sdktrace.ReadOnlySpan)
			if innerSpan.Name() != "redis-test" {
				t.Fatalf("dial should be filtered when filterDial is true")
			}
			return nil, nil
		})
		_, err := dialHook(ctx, "tcp", "localhost:6379")
		if err != nil {
			t.Fatal(err)
		}
	})
}
