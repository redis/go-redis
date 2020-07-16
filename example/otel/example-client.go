package main

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redis/v8/redisext"
	"go.opentelemetry.io/otel/api/global"
	meterStdout "go.opentelemetry.io/otel/exporters/metric/stdout"
	traceStdout "go.opentelemetry.io/otel/exporters/trace/stdout"
	"go.opentelemetry.io/otel/sdk/metric/controller/push"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

func main() {
	<-time.After(1 * time.Second)

	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})

	meterExporter, err := meterStdout.NewExportPipeline(meterStdout.Config{PrettyPrint: true},
		push.WithPeriod(1*time.Second))
	if err != nil {
		log.Fatal(err.Error())
	} else {
		global.SetMeterProvider(meterExporter.Provider())
	}

	traceExporter, err := traceStdout.NewExporter(traceStdout.Options{
		PrettyPrint: true,
	})
	if err != nil {
		log.Fatal(err.Error())
	}

	if tp, err := sdktrace.NewProvider(
		sdktrace.WithSyncer(traceExporter),
		sdktrace.WithConfig(sdktrace.Config{DefaultSampler: sdktrace.AlwaysSample()}),
	); err != nil {
		log.Fatal(err.Error())
	} else {
		global.SetTraceProvider(tp)
	}

	rdb.AddHook(redisext.OpenTelemetryHook{})

	ctx := context.Background()
	tracer := global.Tracer("Example tracer")
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
