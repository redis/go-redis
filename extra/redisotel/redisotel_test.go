package redisotel_test

import (
	"testing"

	"github.com/go-redis/redis/extra/redisotel/v8"
	"github.com/go-redis/redis/v8"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/trace"
)

func TestNew(t *testing.T) {
	// this also functions as a compile-time test that the
	// TracingHook conforms to the Hook interface
	var _ redis.Hook = redisotel.NewTracingHook()
}

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

	_ = redisotel.NewTracingHook(redisotel.WithTracerProvider(tp))

	if !invoked {
		t.Fatalf("did not call custom TraceProvider")
	}
}
