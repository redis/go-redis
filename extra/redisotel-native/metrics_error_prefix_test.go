package redisotel

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type serverReplyErr string

func (e serverReplyErr) Error() string { return string(e) }
func (serverReplyErr) RedisError()     {}

// TestOperationDurationStatusCodeOnlyForServerErrors verifies
// db.response.status_code and the "redis:" error type are reported only for an
// error reply from the server, not for a client-side error whose text happens
// to start with an uppercase word (io.EOF).
func TestOperationDurationStatusCodeOnlyForServerErrors(t *testing.T) {
	for _, tc := range []struct {
		name       string
		err        error
		errorType  string
		category   string
		statusCode string // "" means the attribute is absent
	}{
		{"connection closed", io.EOF, "eof", "network", ""},
		{"wrapped connection closed", fmt.Errorf("read: %w", io.EOF), "read", "network", ""},
		{"server error", serverReplyErr("WRONGTYPE Operation against a key"), "redis:WRONGTYPE", "server", "WRONGTYPE"},
		{"wrapped server error", fmt.Errorf("hook: %w", serverReplyErr("ERR unknown command")), "redis:ERR", "server", "ERR"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			reader := metric.NewManualReader()
			provider := metric.NewMeterProvider(metric.WithReader(reader))
			t.Cleanup(func() { _ = provider.Shutdown(ctx) })
			histogram, err := provider.Meter("test").Float64Histogram(MetricOperationDuration)
			if err != nil {
				t.Fatal(err)
			}
			recorder := &metricsRecorder{operationDuration: histogram}

			recorder.RecordOperationDuration(ctx, time.Millisecond, redis.NewStringCmd(ctx, "get", "k"), 1, tc.err, nil, 0)

			var rm metricdata.ResourceMetrics
			if err := reader.Collect(ctx, &rm); err != nil {
				t.Fatal(err)
			}
			points := 0
			for _, sm := range rm.ScopeMetrics {
				for _, m := range sm.Metrics {
					for _, dp := range m.Data.(metricdata.Histogram[float64]).DataPoints {
						points++
						for key, want := range map[string]string{
							AttrErrorType:                 tc.errorType,
							AttrRedisClientErrorsCategory: tc.category,
						} {
							if value, _ := dp.Attributes.Value(attribute.Key(key)); value.AsString() != want {
								t.Errorf("%s = %q, want %q", key, value.AsString(), want)
							}
						}
						status, ok := dp.Attributes.Value(attribute.Key(AttrDBResponseStatusCode))
						if ok != (tc.statusCode != "") || status.AsString() != tc.statusCode {
							t.Errorf("%s = %q (present %v), want %q", AttrDBResponseStatusCode, status.AsString(), ok, tc.statusCode)
						}
					}
				}
			}
			if points != 1 {
				t.Fatalf("got %d data points, want 1", points)
			}
		})
	}
}
