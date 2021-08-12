package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redis/v8/extra/redisotel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func main() {
	ctx := context.Background()

	stop := runExporter(ctx)
	defer stop(ctx)

	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})

	rdb.AddHook(redisotel.TracingHook{})

	tracer := otel.Tracer("Example tracer")
	ctx, span := tracer.Start(ctx, "start-test-span")

	rdb.Set(ctx, "First value", "value_1", 0)

	rdb.Set(ctx, "Second value", "value_2", 0)

	var group sync.WaitGroup

	for i := 0; i < 20; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			val := rdb.Get(ctx, "Second value").Val()
			if val != "value_2" {
				log.Fatalf("val was not set. expected: %s but got: %s", "value_2", val)
			}
		}()
	}
	group.Wait()

	rdb.Del(ctx, "First value")
	rdb.Del(ctx, "Second value")

	// Wait some time to allow spans to export
	<-time.After(5 * time.Second)
	span.End()
}

func runExporter(ctx context.Context) func(context.Context) {
	provider := sdktrace.NewTracerProvider()
	otel.SetTracerProvider(provider)

	exp, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		log.Fatal(err)
	}

	bsp := sdktrace.NewBatchSpanProcessor(exp)
	provider.RegisterSpanProcessor(bsp)

	return func(ctx context.Context) {
		if err := provider.Shutdown(ctx); err != nil {
			log.Printf("Shutdown failed: %s", err)
		}
	}
}
