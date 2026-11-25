package redisotel

import (
	"testing"

	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
)

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
			recorder := &metricsRecorder{}
			check := func(metricName string, opt otelmetric.ObserveOption) {
				attrs := otelmetric.NewObserveConfig([]otelmetric.ObserveOption{opt}).Attributes()
				for key, want := range map[string]string{
					AttrServerAddress:              tc.address,
					AttrDBClientConnectionPoolName: tc.poolName,
				} {
					value, ok := attrs.Value(attribute.Key(key))
					if !ok || value.AsString() != want {
						t.Errorf("%s: %s = %v, want %q", metricName, key, value, want)
					}
				}
				port, hasPort := attrs.Value(attribute.Key(AttrServerPort))
				if hasPort != (tc.port != "") || (hasPort && port.AsString() != tc.port) {
					t.Errorf("%s: server.port = %v (present %v), want %q", metricName, port, hasPort, tc.port)
				}
			}
			check(MetricConnectionPendingReqs, recorder.attrSet(pendingRequestsAttrs(tc.poolName)))
			check(MetricConnectionCount, recorder.connCountAttrs(tc.poolName, "idle", false))
		})
	}
}
