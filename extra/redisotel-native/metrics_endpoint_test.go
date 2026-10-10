package redisotel

import (
	"context"
	"net"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type endpointConnInfo struct {
	poolName string
}

func (c endpointConnInfo) PoolName() string     { return c.poolName }
func (c endpointConnInfo) RemoteAddr() net.Addr { return nil }

func TestConnectionMetricEndpoints(t *testing.T) {
	for _, tc := range []struct {
		name     string
		poolName string
		address  string
		port     string
	}{
		{"main", "localhost:6379_1", "localhost", ""},
		{"non-default port", "localhost:6380_2", "localhost", "6380"},
		{"pubsub", "localhost:6380_2_pubsub", "localhost", "6380"},
		{"pipeline", "localhost:6380_2_pipeline", "localhost", "6380"},
		{"IPv6", "[::1]:6380_3", "::1", "6380"},
		{"hostname with underscore", "redis_primary:6380_4", "redis_primary", "6380"},
		{"Unix socket", "/tmp/redis_socket.sock_5", "/tmp/redis_socket.sock", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			reader := metric.NewManualReader()
			provider := metric.NewMeterProvider(metric.WithReader(reader))
			t.Cleanup(func() { _ = provider.Shutdown(ctx) })
			meter := provider.Meter("test")
			count, err := meter.Int64UpDownCounter(MetricConnectionCount)
			if err != nil {
				t.Fatal(err)
			}
			pending, err := meter.Int64UpDownCounter(MetricConnectionPendingReqs)
			if err != nil {
				t.Fatal(err)
			}
			recorder := &metricsRecorder{connectionCount: count, connectionPendingReqs: pending}
			recorder.RecordConnectionCount(ctx, 1, endpointConnInfo{tc.poolName}, "idle", false)
			recorder.RecordPendingRequests(ctx, 1, nil, tc.poolName)
			var rm metricdata.ResourceMetrics
			if err := reader.Collect(ctx, &rm); err != nil {
				t.Fatal(err)
			}
			points := 0
			for _, scope := range rm.ScopeMetrics {
				for _, m := range scope.Metrics {
					for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
						points++
						for key, want := range map[string]string{
							AttrServerAddress:              tc.address,
							AttrDBClientConnectionPoolName: tc.poolName,
						} {
							value, ok := point.Attributes.Value(attribute.Key(key))
							if !ok || value.AsString() != want {
								t.Errorf("%s: %s = %v, want %q", m.Name, key, value, want)
							}
						}
						port, hasPort := point.Attributes.Value(attribute.Key(AttrServerPort))
						if hasPort != (tc.port != "") || (hasPort && port.AsString() != tc.port) {
							t.Errorf("%s: server.port = %v (present %v), want %q", m.Name, port, hasPort, tc.port)
						}
					}
				}
			}
			if points != 2 {
				t.Fatalf("got %d data points, want 2", points)
			}
		})
	}
}
