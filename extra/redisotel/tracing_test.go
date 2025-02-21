package redisotel

import (
	"context"
	"fmt"
	"net"
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
