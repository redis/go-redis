package redisotel

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

const (
	stressTestDuration       = 30 * time.Second
	stressTestConcurrency    = 50
	stressTestMinDelay       = 10 * time.Millisecond
	stressTestMaxDelay       = 100 * time.Millisecond
	stressTestStatusInterval = 5 * time.Second
)

// TestMetricsUnderStress validates metrics recording under concurrent load.
func TestMetricsUnderStress(t *testing.T) {
	ctx := context.Background()
	testClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := testClient.Ping(ctx).Err(); err != nil {
		testClient.Close()
		t.Skip("Redis not available at localhost:6379")
	}
	testClient.Close()

	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	defer func() {
		_ = meterProvider.Shutdown(ctx)
	}()

	otel.SetMeterProvider(meterProvider)
	resetObservabilityForTest()

	otelInstance := GetObservabilityInstance()
	config := NewConfig().
		WithEnabled(true).
		WithMeterProvider(meterProvider).
		WithMetricGroups(MetricGroupAll)

	if err := otelInstance.Init(config); err != nil {
		t.Fatalf("Failed to initialize OTel: %v", err)
	}
	defer otelInstance.Shutdown()

	rdb := redis.NewClient(&redis.Options{
		Addr:         "localhost:6379",
		PoolSize:     stressTestConcurrency,
		MinIdleConns: 10,
	})
	defer rdb.Close()

	var opsCompleted atomic.Int64
	var opsErrors atomic.Int64
	startTime := time.Now()
	deadline := startTime.Add(stressTestDuration)

	statusTicker := time.NewTicker(stressTestStatusInterval)
	defer statusTicker.Stop()
	done := make(chan struct{})

	go func() {
		for {
			select {
			case <-statusTicker.C:
				elapsed := time.Since(startTime).Seconds()
				ops := opsCompleted.Load()
				t.Logf("[%.0fs] %d ops, %.1f ops/sec", elapsed, ops, float64(ops)/elapsed)
			case <-done:
				return
			}
		}
	}()

	var wg sync.WaitGroup
	for i := 0; i < stressTestConcurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for time.Now().Before(deadline) {
				key := fmt.Sprintf("stress_test_key_%d_%d", workerID, rand.Int63())
				value := fmt.Sprintf("value_%d", time.Now().UnixNano())

				if err := rdb.Set(ctx, key, value, time.Minute).Err(); err != nil {
					opsErrors.Add(1)
				} else {
					opsCompleted.Add(1)
				}

				if _, err := rdb.Get(ctx, key).Result(); err != nil && err != redis.Nil {
					opsErrors.Add(1)
				} else {
					opsCompleted.Add(1)
				}

				delay := stressTestMinDelay + time.Duration(rand.Int63n(int64(stressTestMaxDelay-stressTestMinDelay)))
				time.Sleep(delay)
			}
		}(i)
	}

	wg.Wait()
	close(done)

	totalOps := opsCompleted.Load()
	totalErrors := opsErrors.Load()
	elapsed := time.Since(startTime)
	t.Logf("Completed: %d ops in %v (%.1f ops/sec), %d errors",
		totalOps, elapsed.Round(time.Second), float64(totalOps)/elapsed.Seconds(), totalErrors)

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Failed to collect metrics: %v", err)
	}

	validateMetrics(t, rm)
}

func validateMetrics(t *testing.T, rm metricdata.ResourceMetrics) {
	metricsFound := make(map[string]bool)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			metricsFound[m.Name] = true
		}
	}

	required := []string{
		MetricConnectionCount,
		MetricConnectionCreateTime,
		MetricOperationDuration,
	}

	for _, name := range required {
		if !metricsFound[name] {
			t.Errorf("Required metric not found: %s", name)
		}
	}
}

func resetObservabilityForTest() {
	observabilityInstanceOnce = sync.Once{}
	observabilityInstance = nil
}

// TestTracingAndMetricsCompatibility verifies that redisotel (tracing) and
// redisotel-native (metrics) can be used together without conflicts.
// Tracing uses AddHook (per-client), metrics uses SetOTelRecorder (global).
func TestTracingAndMetricsCompatibility(t *testing.T) {
	ctx := context.Background()
	testClient := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	if err := testClient.Ping(ctx).Err(); err != nil {
		testClient.Close()
		t.Skip("Redis not available at localhost:6379")
	}
	testClient.Close()

	reader := metric.NewManualReader()
	meterProvider := metric.NewMeterProvider(metric.WithReader(reader))
	defer meterProvider.Shutdown(ctx)

	otel.SetMeterProvider(meterProvider)
	resetObservabilityForTest()

	otelInstance := GetObservabilityInstance()
	config := NewConfig().
		WithEnabled(true).
		WithMeterProvider(meterProvider).
		WithMetricGroups(MetricGroupAll)

	if err := otelInstance.Init(config); err != nil {
		t.Fatalf("Failed to initialize OTel metrics: %v", err)
	}
	defer otelInstance.Shutdown()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		PoolSize: 5,
	})
	defer rdb.Close()

	// In production, also call: redisotel.InstrumentTracing(rdb)

	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("compat-test-%d", i)
		if err := rdb.Set(ctx, key, "value", time.Minute).Err(); err != nil {
			t.Fatalf("SET failed: %v", err)
		}
		if _, err := rdb.Get(ctx, key).Result(); err != nil {
			t.Fatalf("GET failed: %v", err)
		}
	}

	var rm metricdata.ResourceMetrics
	if err := reader.Collect(ctx, &rm); err != nil {
		t.Fatalf("Failed to collect metrics: %v", err)
	}

	found := false
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == MetricOperationDuration {
				found = true
			}
		}
	}

	if !found {
		t.Error("Expected to find db.client.operation.duration metric")
	}
}
