// Package redisotel provides native OpenTelemetry instrumentation for go-redis.
//
// This package implements the OpenTelemetry Semantic Conventions for database clients,
// providing metrics, traces, and logs for Redis operations.
//
// Basic Usage (with global MeterProvider):
//
//	import (
//	    "github.com/redis/go-redis/v9"
//	    redisotel "github.com/redis/go-redis/extra/redisotel-native/v9"
//	    "go.opentelemetry.io/otel"
//	)
//
//	func main() {
//	    // Initialize OpenTelemetry globally (meter provider, etc.)
//	    otel.SetMeterProvider(myMeterProvider)
//
//	    // Create Redis client
//	    rdb := redis.NewClient(&redis.Options{
//	        Addr: "localhost:6379",
//	        DB:   0,
//	    })
//
//	    // Initialize native OTel instrumentation (uses global MeterProvider)
//	    if err := redisotel.Init(rdb); err != nil {
//	        panic(err)
//	    }
//
//	    // Use the client normally - metrics are automatically recorded
//	    rdb.Set(ctx, "key", "value", 0)
//	}
//
// Advanced Usage (with custom MeterProvider):
//
//	// Pass a custom MeterProvider
//	if err := redisotel.Init(rdb, redisotel.WithMeterProvider(customProvider)); err != nil {
//	    panic(err)
//	}
package redisotel

import (
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

var (
	// Global singleton instance
	globalInstance     *metricsRecorder
	globalInstanceOnce sync.Once
	initErr            error
)

// Init initializes native OpenTelemetry instrumentation for the given Redis client.
// This function should be called once per application, typically during startup.
// Subsequent calls are no-ops and return nil.
//
// The function extracts configuration from the client (server address, port, database index)
// and registers a global metrics recorder.
//
// If no MeterProvider is provided via WithMeterProvider option, the global MeterProvider
// from otel.GetMeterProvider() will be used. Make sure to call otel.SetMeterProvider()
// before calling Init() if you want to use a custom provider.
//
// Example (using global MeterProvider):
//
//	otel.SetMeterProvider(myMeterProvider)
//	rdb := redis.NewClient(&redis.Options{
//	    Addr: "localhost:6379",
//	    DB:   0,
//	})
//	if err := redisotel.Init(rdb); err != nil {
//	    log.Fatal(err)
//	}
//
// Example (using custom MeterProvider):
//
//	if err := redisotel.Init(rdb, redisotel.WithMeterProvider(customProvider)); err != nil {
//	    log.Fatal(err)
//	}
func Init(client redis.UniversalClient, opts ...Option) error {
	globalInstanceOnce.Do(func() {
		initErr = initOnce(client, opts...)
	})
	return initErr
}

// initOnce performs the actual initialization (called once by sync.Once)
func initOnce(client redis.UniversalClient, opts ...Option) error {
	// Apply options
	cfg := defaultConfig()
	for _, opt := range opts {
		opt.apply(&cfg)
	}

	// Extract client configuration
	serverAddr, serverPort, dbIndex, err := extractClientConfig(client)
	if err != nil {
		return fmt.Errorf("failed to extract client config: %w", err)
	}

	// Get meter provider (use global if not provided)
	meterProvider := cfg.meterProvider
	if meterProvider == nil {
		meterProvider = otel.GetMeterProvider()
	}

	// Create meter
	meter := meterProvider.Meter(
		"github.com/redis/go-redis",
		metric.WithInstrumentationVersion(redis.Version()),
	)

	// Create histogram for operation duration
	operationDuration, err := meter.Float64Histogram(
		"db.client.operation.duration",
		metric.WithDescription("Duration of database client operations"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(cfg.histogramBuckets...),
	)
	if err != nil {
		return fmt.Errorf("failed to create operation duration histogram: %w", err)
	}

	// Create synchronous UpDownCounter for connection count
	connectionCount, err := meter.Int64UpDownCounter(
		"db.client.connection.count",
		metric.WithDescription("The number of connections that are currently in state described by the state attribute"),
		metric.WithUnit("{connection}"),
	)
	if err != nil {
		return fmt.Errorf("failed to create connection count metric: %w", err)
	}

	// Create recorder
	recorder := &metricsRecorder{
		operationDuration: operationDuration,
		connectionCount:   connectionCount,
		serverAddr:        serverAddr,
		serverPort:        serverPort,
		dbIndex:           dbIndex,
	}

	// Register global recorder
	redis.SetOTelRecorder(recorder)
	globalInstance = recorder

	return nil
}

// extractClientConfig extracts server address, port, and database index from a Redis client
func extractClientConfig(client redis.UniversalClient) (serverAddr, serverPort, dbIndex string, err error) {
	switch c := client.(type) {
	case *redis.Client:
		opts := c.Options()
		host, port := parseAddr(opts.Addr)
		return host, port, formatDBIndex(opts.DB), nil

	case *redis.ClusterClient:
		opts := c.Options()
		if len(opts.Addrs) > 0 {
			// Use first address for server.address attribute
			host, port := parseAddr(opts.Addrs[0])
			return host, port, "", nil
		}
		return "", "", "", fmt.Errorf("cluster client has no addresses")

	case *redis.Ring:
		opts := c.Options()
		if len(opts.Addrs) > 0 {
			// Use first address for server.address attribute
			for _, addr := range opts.Addrs {
				host, port := parseAddr(addr)
				return host, port, formatDBIndex(opts.DB), nil
			}
		}
		return "", "", "", fmt.Errorf("ring client has no addresses")

	default:
		return "", "", "", fmt.Errorf("unsupported client type: %T", client)
	}
}

// Shutdown cleans up resources (for testing purposes)
func Shutdown() {
	if globalInstance != nil {
		redis.SetOTelRecorder(nil)
		globalInstance = nil
	}
	// Reset the sync.Once so Init can be called again (useful for tests)
	globalInstanceOnce = sync.Once{}
	initErr = nil
}
