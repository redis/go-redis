package redisotel

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
)

var instrumentationScope = instrumentation.Scope{
	Name:    instrumName,
	Version: "semver:" + redis.Version(),
}

func setupMetrics(conf *config) (*sdkmetric.ManualReader, *redis.Client) {
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	otel.SetMeterProvider(mp)

	rdb := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
	if conf.meter == nil {
		conf.meter = conf.mp.Meter(
			instrumName,
			metric.WithInstrumentationVersion("semver:"+redis.Version()),
		)
	}
	addMetricsHook(rdb, conf)
	return reader, rdb
}

func TestMetrics(t *testing.T) {
	reader, rdb := setupMetrics(newConfig())
	rdb.Get(context.Background(), "key")

	want := metricdata.ScopeMetrics{
		Scope: instrumentationScope,
		Metrics: []metricdata.Metrics{
			{
				Name:        "db.client.connections.create_time",
				Description: "The time it took to create a new connection.",
				Unit:        "ms",
				Data: metricdata.Histogram[float64]{
					Temporality: metricdata.CumulativeTemporality,
					DataPoints: []metricdata.HistogramDataPoint[float64]{
						{
							Attributes: attribute.NewSet(
								semconv.DBSystemRedis,
								attribute.String("status", "ok"),
							),
						},
					},
				},
			},
			{
				Name:        "db.client.connections.use_time",
				Description: "The time between borrowing a connection and returning it to the pool.",
				Unit:        "ms",
				Data: metricdata.Histogram[float64]{
					Temporality: metricdata.CumulativeTemporality,
					DataPoints: []metricdata.HistogramDataPoint[float64]{
						{
							Attributes: attribute.NewSet(
								semconv.DBSystemRedis,
								attribute.String("type", "command"),
								attribute.String("status", "ok"),
							),
						},
					},
				},
			},
		},
	}
	rm := metricdata.ResourceMetrics{}
	err := reader.Collect(context.Background(), &rm)
	assert.NoError(t, err)
	require.Len(t, rm.ScopeMetrics, 1)
	metricdatatest.AssertEqual(t, want, rm.ScopeMetrics[0], metricdatatest.IgnoreTimestamp(), metricdatatest.IgnoreValue())
}

func TestCustomAttributes(t *testing.T) {
	customAttrFn := func(ctx context.Context) []attribute.KeyValue {
		return []attribute.KeyValue{
			attribute.String("custom", "value"),
		}
	}
	config := newConfig(WithAttributesFunc(customAttrFn))
	reader, rdb := setupMetrics(config)

	rdb.Get(context.Background(), "key")

	want := metricdata.ScopeMetrics{
		Scope: instrumentationScope,
		Metrics: []metricdata.Metrics{
			{
				Name:        "db.client.connections.create_time",
				Description: "The time it took to create a new connection.",
				Unit:        "ms",
				Data: metricdata.Histogram[float64]{
					Temporality: metricdata.CumulativeTemporality,
					DataPoints: []metricdata.HistogramDataPoint[float64]{
						{
							Attributes: attribute.NewSet(
								semconv.DBSystemRedis,
								attribute.String("status", "ok"),
							),
						},
					},
				},
			},
			{
				Name:        "db.client.connections.use_time",
				Description: "The time between borrowing a connection and returning it to the pool.",
				Unit:        "ms",
				Data: metricdata.Histogram[float64]{
					Temporality: metricdata.CumulativeTemporality,
					DataPoints: []metricdata.HistogramDataPoint[float64]{
						{
							Attributes: attribute.NewSet(
								semconv.DBSystemRedis,
								attribute.String("type", "command"),
								attribute.String("status", "ok"),
								attribute.String("custom", "value"),
							),
						},
					},
				},
			},
		},
	}
	rm := metricdata.ResourceMetrics{}
	err := reader.Collect(context.Background(), &rm)
	assert.NoError(t, err)
	require.Len(t, rm.ScopeMetrics, 1)
	metricdatatest.AssertEqual(t, want, rm.ScopeMetrics[0], metricdatatest.IgnoreTimestamp(), metricdatatest.IgnoreValue())
}
