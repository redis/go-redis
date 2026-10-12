// EXAMPLE: otel_metrics
// HIDE_START
package main

import (
	"context"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"

	redisotel "github.com/redis/go-redis/extra/redisotel-native/v9"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/sdk/metric"
)

// ExampleClient_otel_metrics demonstrates how to enable OpenTelemetry metrics
// for Redis operations and export them to an OTLP collector.
func main() {
	ctx := context.Background()

	// HIDE_END

	// STEP_START otel_exporter_setup
	// Create OTLP exporter that sends metrics to the collector
	// Default endpoint is localhost:4317 (gRPC)
	exporter, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithInsecure(), // Use insecure for local development
		// For production, configure TLS and authentication:
		// otlpmetricgrpc.WithEndpoint("your-collector:4317"),
		// otlpmetricgrpc.WithTLSCredentials(...),
	)
	if err != nil {
		log.Fatalf("Failed to create OTLP exporter: %v", err)
	}
	// STEP_END

	// STEP_START otel_meter_provider
	// Create meter provider with periodic reader
	// Metrics are exported every 10 seconds
	meterProvider := metric.NewMeterProvider(
		metric.WithReader(
			metric.NewPeriodicReader(exporter,
				metric.WithInterval(10*time.Second),
			),
		),
	)
	defer func() {
		if err := meterProvider.Shutdown(ctx); err != nil {
			log.Printf("Error shutting down meter provider: %v", err)
		}
	}()

	// Set the global meter provider
	otel.SetMeterProvider(meterProvider)
	// STEP_END

	// STEP_START redis_client_setup
	// Initialize OTel instrumentation BEFORE creating Redis clients
	otelInstance := redisotel.GetObservabilityInstance()
	config := redisotel.NewConfig().WithEnabled(true)
	if err := otelInstance.Init(config); err != nil {
		log.Fatalf("Failed to initialize OTel: %v", err)
	}
	defer otelInstance.Shutdown()

	// Create Redis client - automatically instrumented
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	defer rdb.Close()
	// STEP_END

	// STEP_START redis_operations
	// Execute Redis operations - metrics are automatically collected
	log.Println("Executing Redis operations...")
	var wg sync.WaitGroup
	wg.Add(50)
	for i := range 50 {
		go func(i int) {
			defer wg.Done()

			for j := range 10 {
				if err := rdb.Set(ctx, "key"+strconv.Itoa(i*10+j), "value", 0).Err(); err != nil {
					log.Printf("Error setting key: %v", err)
				}
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(400)))
			}
		}(i)
	}
	wg.Wait()

	wg.Add(10)
	for i := range 10 {
		go func(i int) {
			defer wg.Done()

			for j := range 10 {
				if err := rdb.Set(ctx, "key"+strconv.Itoa(i*10+j), "value", 0).Err(); err != nil {
					log.Printf("Error setting key: %v", err)
				}
				time.Sleep(time.Millisecond * time.Duration(rand.Intn(400)))
			}
		}(i)
	}
	wg.Wait()

	for j := range 10 {
		if err := rdb.Set(ctx, "key"+strconv.Itoa(j), "value", 0).Err(); err != nil {
			log.Printf("Error setting key: %v", err)
		}
		time.Sleep(time.Millisecond * time.Duration(rand.Intn(400)))
	}

	log.Println("Operations complete. Waiting for metrics to be exported...")

	// Wait for metrics to be exported
	time.Sleep(15 * time.Second)
	// STEP_END
}
