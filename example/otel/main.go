package main

import (
	"context"
	"sync"

	"github.com/go-redis/redis/extra/redisotel/v8"
	"github.com/go-redis/redis/v8"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

var tracer = otel.Tracer("redisexample")

func main() {
	ctx := context.Background()

	stop := configureOpentelemetry(ctx)
	defer stop()

	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
	rdb.AddHook(redisotel.TracingHook{})

	ctx, span := tracer.Start(ctx, "handleRequest")
	defer span.End()

	if err := handleRequest(ctx); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

func handleRequest(ctx context.Context) error {
	if err := rdb.Set(ctx, "First value", "value_1", 0).Err(); err != nil {
		return err
	}
	if err := rdb.Set(ctx, "Second value", "value_2", 0).Err(); err != nil {
		return err
	}

	var group sync.WaitGroup

	for i := 0; i < 20; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			val := rdb.Get(ctx, "Second value").Val()
			if val != "value_2" {
				panic(err)
			}
		}()
	}

	group.Wait()

	if err := rdb.Del(ctx, "First value").Err(); err != nil {
		return err
	}
	if err := rdb.Del(ctx, "Second value").Err(); err != nil {
		return err
	}

	return nil
}

func configureOpentelemetry(ctx context.Context) func() {
	provider := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(provider)

	exp, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		panic(err)
	}

	bsp := sdktrace.NewBatchSpanProcessor(exp)
	provider.RegisterSpanProcessor(bsp)

	return func(ctx context.Context) {
		if err := provider.Shutdown(ctx); err != nil {
			panic(err)
		}
	}
}
