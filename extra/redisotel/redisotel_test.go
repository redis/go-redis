package redisotel

import (
	"context"
	"testing"

	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"

	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/go-redis/redis/v9"
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

func TestNewWithAttributes(t *testing.T) {
	provider := sdktrace.NewTracerProvider()
	hook := newTracingHook("", WithTracerProvider(provider), WithAttributes(semconv.NetPeerNameKey.String("localhost")))
	ctx, span := provider.Tracer("redis-test").Start(context.TODO(), "redis-test")
	cmd := redis.NewCmd(ctx, "ping")
	defer span.End()

	processHook := hook.ProcessHook(func(ctx context.Context, cmd redis.Cmder) error {
		attrs := trace.SpanFromContext(ctx).(sdktrace.ReadOnlySpan).Attributes()
		if !(attrs[0] == semconv.DBSystemRedis) {
			t.Fatalf("expected attrs[0] to be semconv.DBSystemRedis, got: %v", attrs[0])
		}
		if !(attrs[1] == semconv.NetPeerNameKey.String("localhost")) {
			t.Fatalf("expected attrs[1] to be semconv.NetPeerNameKey.String(\"localhost\"), got: %v", attrs[1])
		}
		if !(attrs[2] == semconv.DBStatementKey.String("ping")) {
			t.Fatalf("expected attrs[2] to be semconv.DBStatementKey.String(\"ping\"), got: %v", attrs[2])
		}
		return nil
	})
	err := processHook(ctx, cmd)
	if err != nil {
		t.Fatal(err)
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
