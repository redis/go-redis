package redisotel

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/redis/go-redis/v9"
)

type providerFunc func(name string, opts ...trace.TracerOption) trace.TracerProvider

func (fn providerFunc) TracerProvider(name string, opts ...trace.TracerOption) trace.TracerProvider {
	return fn(name, opts...)
}

func TestNewWithTracerProvider(t *testing.T) {
	invoked := false

	tp := providerFunc(func(name string, opts ...trace.TracerOption) trace.TracerProvider {
		invoked = true
		return otel.GetTracerProvider()
	})

	_ = newTracingHook("redis-hook", WithTracerProvider(tp.TracerProvider("redis-test")))

	if !invoked {
		t.Fatalf("did not call custom TraceProvider")
	}
}

func TestWithDBStatement(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	hook := newTracingHook(
		"",
		WithTracerProvider(provider),
		WithDBStatement(false),
	)
	ctx, span := provider.Tracer("redis-test").Start(context.TODO(), "redis-test")
	cmd := redis.NewCmd(ctx, "ping")
	defer span.End()

	processHook := hook.ProcessHook(func(ctx context.Context, cmd redis.Cmder) error {
		attrs := trace.SpanFromContext(ctx).(sdktrace.ReadOnlySpan).Attributes()
		for _, attr := range attrs {
			if attr.Key == semconv.DBStatementKey {
				t.Fatal("Attribute with db statement should not exist")
			}
		}
		return nil
	})
	err := processHook(ctx, cmd)
	if err != nil {
		t.Fatal(err)
	}
}

func TestTracingHook_DialHook(t *testing.T) {
	imsb := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(imsb))
	hook := newTracingHook(
		"redis://localhost:6379",
		WithTracerProvider(provider),
	)

	tests := []struct {
		name    string
		errTest error
	}{
		{"nil error", nil},
		{"test error", fmt.Errorf("test error")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer imsb.Reset()

			dialHook := hook.DialHook(func(ctx context.Context, network, addr string) (conn net.Conn, err error) {
				return nil, tt.errTest
			})
			if _, err := dialHook(context.Background(), "tcp", "localhost:6379"); err != tt.errTest {
				t.Fatal(err)
			}

			assertEqual(t, 1, len(imsb.GetSpans()))

			spanData := imsb.GetSpans()[0]
			assertEqual(t, instrumName, spanData.InstrumentationLibrary.Name)
			assertEqual(t, "redis.dial", spanData.Name)
			assertEqual(t, trace.SpanKindClient, spanData.SpanKind)
			assertAttributeContains(t, spanData.Attributes, semconv.DBSystemRedis)
			assertAttributeContains(t, spanData.Attributes, semconv.DBConnectionStringKey.String("redis://localhost:6379"))

			if tt.errTest == nil {
				assertEqual(t, 0, len(spanData.Events))
				assertEqual(t, codes.Unset, spanData.Status.Code)
				assertEqual(t, "", spanData.Status.Description)
				return
			}

			assertEqual(t, 1, len(spanData.Events))
			assertAttributeContains(t, spanData.Events[0].Attributes, semconv.ExceptionTypeKey.String("*errors.errorString"))
			assertAttributeContains(t, spanData.Events[0].Attributes, semconv.ExceptionMessageKey.String(tt.errTest.Error()))
			assertEqual(t, codes.Error, spanData.Status.Code)
			assertEqual(t, tt.errTest.Error(), spanData.Status.Description)
		})
	}
}

func TestTracingHook_ProcessHook(t *testing.T) {
	imsb := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(imsb))
	hook := newTracingHook(
		"redis://localhost:6379",
		WithTracerProvider(provider),
	)

	tests := []struct {
		name    string
		errTest error
	}{
		{"nil error", nil},
		{"test error", fmt.Errorf("test error")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer imsb.Reset()

			cmd := redis.NewCmd(context.Background(), "ping")
			processHook := hook.ProcessHook(func(ctx context.Context, cmd redis.Cmder) error {
				return tt.errTest
			})
			assertEqual(t, tt.errTest, processHook(context.Background(), cmd))
			assertEqual(t, 1, len(imsb.GetSpans()))

			spanData := imsb.GetSpans()[0]
			assertEqual(t, instrumName, spanData.InstrumentationLibrary.Name)
			assertEqual(t, "ping", spanData.Name)
			assertEqual(t, trace.SpanKindClient, spanData.SpanKind)
			assertAttributeContains(t, spanData.Attributes, semconv.DBSystemRedis)
			assertAttributeContains(t, spanData.Attributes, semconv.DBConnectionStringKey.String("redis://localhost:6379"))
			assertAttributeContains(t, spanData.Attributes, semconv.DBStatementKey.String("ping"))

			if tt.errTest == nil {
				assertEqual(t, 0, len(spanData.Events))
				assertEqual(t, codes.Unset, spanData.Status.Code)
				assertEqual(t, "", spanData.Status.Description)
				return
			}

			assertEqual(t, 1, len(spanData.Events))
			assertAttributeContains(t, spanData.Events[0].Attributes, semconv.ExceptionTypeKey.String("*errors.errorString"))
			assertAttributeContains(t, spanData.Events[0].Attributes, semconv.ExceptionMessageKey.String(tt.errTest.Error()))
			assertEqual(t, codes.Error, spanData.Status.Code)
			assertEqual(t, tt.errTest.Error(), spanData.Status.Description)
		})
	}
}

func TestTracingHook_ProcessPipelineHook(t *testing.T) {
	imsb := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(imsb))
	hook := newTracingHook(
		"redis://localhost:6379",
		WithTracerProvider(provider),
	)

	tests := []struct {
		name    string
		errTest error
	}{
		{"nil error", nil},
		{"test error", fmt.Errorf("test error")},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer imsb.Reset()

			cmds := []redis.Cmder{
				redis.NewCmd(context.Background(), "ping"),
				redis.NewCmd(context.Background(), "ping"),
			}
			processHook := hook.ProcessPipelineHook(func(ctx context.Context, cmds []redis.Cmder) error {
				return tt.errTest
			})
			assertEqual(t, tt.errTest, processHook(context.Background(), cmds))
			assertEqual(t, 1, len(imsb.GetSpans()))

			spanData := imsb.GetSpans()[0]
			assertEqual(t, instrumName, spanData.InstrumentationLibrary.Name)
			assertEqual(t, "redis.pipeline ping", spanData.Name)
			assertEqual(t, trace.SpanKindClient, spanData.SpanKind)
			assertAttributeContains(t, spanData.Attributes, semconv.DBSystemRedis)
			assertAttributeContains(t, spanData.Attributes, semconv.DBConnectionStringKey.String("redis://localhost:6379"))
			assertAttributeContains(t, spanData.Attributes, semconv.DBStatementKey.String("ping\nping"))

			if tt.errTest == nil {
				assertEqual(t, 0, len(spanData.Events))
				assertEqual(t, codes.Unset, spanData.Status.Code)
				assertEqual(t, "", spanData.Status.Description)
				return
			}

			assertEqual(t, 1, len(spanData.Events))
			assertAttributeContains(t, spanData.Events[0].Attributes, semconv.ExceptionTypeKey.String("*errors.errorString"))
			assertAttributeContains(t, spanData.Events[0].Attributes, semconv.ExceptionMessageKey.String(tt.errTest.Error()))
			assertEqual(t, codes.Error, spanData.Status.Code)
			assertEqual(t, tt.errTest.Error(), spanData.Status.Description)
		})
	}
}

func TestTracingHook_ProcessHook_LongCommand(t *testing.T) {
	imsb := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(imsb))
	hook := newTracingHook(
		"redis://localhost:6379",
		WithTracerProvider(provider),
	)
	longValue := strings.Repeat("a", 102400)

	tests := []struct {
		name     string
		cmd      redis.Cmder
		expected string
	}{
		{
			name:     "short command",
			cmd:      redis.NewCmd(context.Background(), "SET", "key", "value"),
			expected: "SET key value",
		},
		{
			name:     "set command with long key",
			cmd:      redis.NewCmd(context.Background(), "SET", longValue, "value"),
			expected: "SET " + longValue + " value",
		},
		{
			name:     "set command with long value",
			cmd:      redis.NewCmd(context.Background(), "SET", "key", longValue),
			expected: "SET key " + longValue,
		},
		{
			name:     "set command with long key and value",
			cmd:      redis.NewCmd(context.Background(), "SET", longValue, longValue),
			expected: "SET " + longValue + " " + longValue,
		},
		{
			name:     "short command with many arguments",
			cmd:      redis.NewCmd(context.Background(), "MSET", "key1", "value1", "key2", "value2", "key3", "value3", "key4", "value4", "key5", "value5"),
			expected: "MSET key1 value1 key2 value2 key3 value3 key4 value4 key5 value5",
		},
		{
			name:     "long command",
			cmd:      redis.NewCmd(context.Background(), longValue, "key", "value"),
			expected: longValue + " key value",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer imsb.Reset()

			processHook := hook.ProcessHook(func(ctx context.Context, cmd redis.Cmder) error {
				return nil
			})

			if err := processHook(context.Background(), tt.cmd); err != nil {
				t.Fatal(err)
			}

			assertEqual(t, 1, len(imsb.GetSpans()))

			spanData := imsb.GetSpans()[0]

			var dbStatement string
			for _, attr := range spanData.Attributes {
				if attr.Key == semconv.DBStatementKey {
					dbStatement = attr.Value.AsString()
					break
				}
			}

			if dbStatement != tt.expected {
				t.Errorf("Expected DB statement: %q\nGot: %q", tt.expected, dbStatement)
			}
		})
	}
}

func TestTracingHook_ProcessPipelineHook_LongCommands(t *testing.T) {
	imsb := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(imsb))
	hook := newTracingHook(
		"redis://localhost:6379",
		WithTracerProvider(provider),
	)

	tests := []struct {
		name     string
		cmds     []redis.Cmder
		expected string
	}{
		{
			name: "multiple short commands",
			cmds: []redis.Cmder{
				redis.NewCmd(context.Background(), "SET", "key1", "value1"),
				redis.NewCmd(context.Background(), "SET", "key2", "value2"),
			},
			expected: "SET key1 value1\nSET key2 value2",
		},
		{
			name: "multiple short commands with long key",
			cmds: []redis.Cmder{
				redis.NewCmd(context.Background(), "SET", strings.Repeat("a", 102400), "value1"),
				redis.NewCmd(context.Background(), "SET", strings.Repeat("b", 102400), "value2"),
			},
			expected: "SET " + strings.Repeat("a", 102400) + " value1\nSET " + strings.Repeat("b", 102400) + " value2",
		},
		{
			name: "multiple short commands with long value",
			cmds: []redis.Cmder{
				redis.NewCmd(context.Background(), "SET", "key1", strings.Repeat("a", 102400)),
				redis.NewCmd(context.Background(), "SET", "key2", strings.Repeat("b", 102400)),
			},
			expected: "SET key1 " + strings.Repeat("a", 102400) + "\nSET key2 " + strings.Repeat("b", 102400),
		},
		{
			name: "multiple short commands with long key and value",
			cmds: []redis.Cmder{
				redis.NewCmd(context.Background(), "SET", strings.Repeat("a", 102400), strings.Repeat("b", 102400)),
				redis.NewCmd(context.Background(), "SET", strings.Repeat("c", 102400), strings.Repeat("d", 102400)),
			},
			expected: "SET " + strings.Repeat("a", 102400) + " " + strings.Repeat("b", 102400) + "\nSET " + strings.Repeat("c", 102400) + " " + strings.Repeat("d", 102400),
		},
		{
			name: "multiple long commands",
			cmds: []redis.Cmder{
				redis.NewCmd(context.Background(), strings.Repeat("a", 102400), "key1", "value1"),
				redis.NewCmd(context.Background(), strings.Repeat("a", 102400), "key2", "value2"),
			},
			expected: strings.Repeat("a", 102400) + " key1 value1\n" + strings.Repeat("a", 102400) + " key2 value2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer imsb.Reset()

			processHook := hook.ProcessPipelineHook(func(ctx context.Context, cmds []redis.Cmder) error {
				return nil
			})

			if err := processHook(context.Background(), tt.cmds); err != nil {
				t.Fatal(err)
			}

			assertEqual(t, 1, len(imsb.GetSpans()))

			spanData := imsb.GetSpans()[0]

			var dbStatement string
			for _, attr := range spanData.Attributes {
				if attr.Key == semconv.DBStatementKey {
					dbStatement = attr.Value.AsString()
					break
				}
			}

			if dbStatement != tt.expected {
				t.Errorf("Expected DB statement:\n%q\nGot:\n%q", tt.expected, dbStatement)
			}
		})
	}
}

func assertEqual(t *testing.T, expected, actual interface{}) {
	t.Helper()
	if expected != actual {
		t.Fatalf("expected %v, got %v", expected, actual)
	}
}

func assertAttributeContains(t *testing.T, attrs []attribute.KeyValue, attr attribute.KeyValue) {
	t.Helper()
	for _, a := range attrs {
		if a == attr {
			return
		}
	}
	t.Fatalf("attribute %v not found", attr)
}
