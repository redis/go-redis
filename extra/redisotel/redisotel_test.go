package redisotel

import (
	"context"
	"strings"
	"testing"

	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/redis/go-redis/v9"
)

type providerFunc func(name string, opts ...trace.TracerOption) trace.Tracer

func (fn providerFunc) Tracer(name string, opts ...trace.TracerOption) trace.Tracer {
	return fn(name, opts...)
}

func TestNewWithTracerProvider(t *testing.T) {
	invoked := false

	tp := providerFunc(func(name string, opts ...trace.TracerOption) trace.Tracer {
		invoked = true
		return otel.GetTracerProvider().Tracer(name, opts...)
	})

	_ = newTracingHook("", WithTracerProvider(tp))

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

func TestMaxString(t *testing.T) {
	src := strings.Repeat("a", 100)
	dst := maxString(src, 50)
	if dst != src[:50] {
		t.Fatal("failed to maxstring")
	}

	dst = maxString(src, 200)
	if dst != src {
		t.Fatal("failed to maxstring")
	}

	dst = maxString(src, 0)
	if dst != src {
		t.Fatal("failed to maxstring")
	}
}
