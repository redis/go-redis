package main

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/uptrace/uptrace-go/uptrace"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"

	"github.com/go-redis/redis/extra/redisotel/v9"
	"github.com/go-redis/redis/v9"
)

var tracer = otel.Tracer("github.com/go-redis/redis/example/otel")

func main() {
	ctx := context.Background()

	uptrace.ConfigureOpentelemetry(
		// copy your project DSN here or use UPTRACE_DSN env var
		uptrace.WithDSN("http://project2_secret_token@localhost:14317/2"),

		uptrace.WithServiceName("myservice"),
		uptrace.WithServiceVersion("v1.0.0"),
	)
	defer uptrace.Shutdown(ctx)

	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
	if err := redisotel.InstrumentTracing(rdb); err != nil {
		panic(err)
	}
	if err := redisotel.InstrumentMetrics(rdb); err != nil {
		panic(err)
	}

	for i := 0; i < 1e6; i++ {
		ctx, rootSpan := tracer.Start(ctx, "handleRequest")

		if err := handleRequest(ctx, rdb); err != nil {
			rootSpan.RecordError(err)
			rootSpan.SetStatus(codes.Error, err.Error())
		}

		rootSpan.End()

		if i == 0 {
			fmt.Printf("view trace: %s\n", uptrace.TraceURL(rootSpan))
		}

		time.Sleep(time.Second)
	}
}

func handleRequest(ctx context.Context, rdb *redis.Client) error {
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
				log.Printf("%q != %q", val, "value_2")
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
