package main

import (
	"context"

	"github.com/go-redis/redis/extra/redisotel/v8"
	"github.com/go-redis/redis/v8"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

var tracer = otel.Tracer("app_or_package_name")

func main() {
	exporter, err := stdout.NewExporter(stdout.WithPrettyPrint())
	if err != nil {
		panic(err)
	}

	provider := sdktrace.NewTracerProvider()
	provider.RegisterSpanProcessor(sdktrace.NewSimpleSpanProcessor(exporter))

	otel.SetTracerProvider(provider)

	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
	defer rdb.Close()

	rdb.AddHook(redisotel.NewTracingHook())

	ctx, span := tracer.Start(context.TODO(), "main")
	defer span.End()

	if err := rdb.Ping(ctx).Err(); err != nil {
		panic(err)
	}
}
