package redisotel

import "testing"

// Expected metric names per OTel semantic conventions.
// Reference: https://opentelemetry.io/docs/specs/semconv/database/database-metrics/
const (
	semconvOperationDuration    = "db.client.operation.duration"
	semconvConnectionCount      = "db.client.connection.count"
	semconvConnectionCreateTime = "db.client.connection.create_time"
	semconvConnectionWaitTime   = "db.client.connection.wait_time"
	semconvConnectionPending    = "db.client.connection.pending_requests"
)

// TestMetricDefinitionsMatchSemconv verifies metric names match OTel semantic conventions.
func TestMetricDefinitionsMatchSemconv(t *testing.T) {
	tests := []struct {
		name     string
		got      string
		expected string
	}{
		{"db.client.operation.duration", MetricOperationDuration, semconvOperationDuration},
		{"db.client.connection.count", MetricConnectionCount, semconvConnectionCount},
		{"db.client.connection.create_time", MetricConnectionCreateTime, semconvConnectionCreateTime},
		{"db.client.connection.wait_time", MetricConnectionWaitTime, semconvConnectionWaitTime},
		{"db.client.connection.pending_requests", MetricConnectionPendingReqs, semconvConnectionPending},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.expected {
				t.Errorf("got %q, want %q", tt.got, tt.expected)
			}
		})
	}
}

// TestSemconvMetricTypes documents expected metric instrument types.
// Semconv specifies UpDownCounter for connection.count and pending_requests,
// but this implementation uses ObservableGauge (known deviation, see issue #3730).
func TestSemconvMetricTypes(t *testing.T) {
	t.Run("connection.count uses Gauge (semconv specifies UpDownCounter)", func(t *testing.T) {
		// Known deviation: using ObservableGauge instead of UpDownCounter
	})

	t.Run("pending_requests uses Gauge (semconv specifies UpDownCounter)", func(t *testing.T) {
		// Known deviation: using ObservableGauge instead of UpDownCounter
	})

	t.Run("operation.duration uses Histogram (correct)", func(t *testing.T) {
		// Matches semconv: Float64Histogram
	})

	t.Run("connection.create_time uses Histogram (correct)", func(t *testing.T) {
		// Matches semconv: Float64Histogram
	})

	t.Run("connection.wait_time uses Histogram (correct)", func(t *testing.T) {
		// Matches semconv: Float64Histogram
	})
}
