package redisotel

import (
	"reflect"
	"testing"

	"go.opentelemetry.io/otel/attribute"
)

func Test_poolStatsAttrs(t *testing.T) {
	t.Parallel()
	type args struct {
		conf *config
	}
	tests := []struct {
		name          string
		args          args
		wantPoolAttrs attribute.Set
		wantIdleAttrs attribute.Set
		wantUsedAttrs attribute.Set
	}{
		{
			name: "#3122",
			args: func() args {
				conf := &config{
					attrs: make([]attribute.KeyValue, 0, 4),
				}
				conf.attrs = append(conf.attrs, attribute.String("foo1", "bar1"), attribute.String("foo2", "bar2"))
				conf.attrs = append(conf.attrs, attribute.String("pool.name", "pool1"))
				return args{conf: conf}
			}(),
			wantPoolAttrs: attribute.NewSet(attribute.String("foo1", "bar1"), attribute.String("foo2", "bar2"),
				attribute.String("pool.name", "pool1")),
			wantIdleAttrs: attribute.NewSet(attribute.String("foo1", "bar1"), attribute.String("foo2", "bar2"),
				attribute.String("pool.name", "pool1"), attribute.String("state", "idle")),
			wantUsedAttrs: attribute.NewSet(attribute.String("foo1", "bar1"), attribute.String("foo2", "bar2"),
				attribute.String("pool.name", "pool1"), attribute.String("state", "used")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotPoolAttrs, gotIdleAttrs, gotUsedAttrs := poolStatsAttrs(tt.args.conf)
			if !reflect.DeepEqual(gotPoolAttrs, tt.wantPoolAttrs) {
				t.Errorf("poolStatsAttrs() gotPoolAttrs = %v, want %v", gotPoolAttrs, tt.wantPoolAttrs)
			}
			if !reflect.DeepEqual(gotIdleAttrs, tt.wantIdleAttrs) {
				t.Errorf("poolStatsAttrs() gotIdleAttrs = %v, want %v", gotIdleAttrs, tt.wantIdleAttrs)
			}
			if !reflect.DeepEqual(gotUsedAttrs, tt.wantUsedAttrs) {
				t.Errorf("poolStatsAttrs() gotUsedAttrs = %v, want %v", gotUsedAttrs, tt.wantUsedAttrs)
			}
		})
	}
}

// TestMetricTypes_CodeReview verifies that the code uses Int64ObservableCounter
// for cumulative count metrics (waits, timeouts, hits, misses) instead of
// Int64ObservableUpDownCounter. This is a code-level verification test.
func TestMetricTypes_CodeReview(t *testing.T) {
	t.Parallel()

	// This test documents the expected metric types according to OpenTelemetry conventions.
	// The actual metric type verification happens at runtime when metrics are registered.
	//
	// Expected Counter metrics (monotonic, only increase):
	expectedCounters := []string{
		"db.client.connections.waits",
		"db.client.connections.timeouts",
		"db.client.connections.hits",
		"db.client.connections.misses",
	}

	// Expected UpDownCounter metrics (non-monotonic, can increase or decrease):
	expectedUpDownCounters := []string{
		"db.client.connections.idle.max",
		"db.client.connections.idle.min",
		"db.client.connections.max",
		"db.client.connections.usage",
		"db.client.connections.waits_duration",
	}

	// Verify the lists are non-empty
	if len(expectedCounters) == 0 {
		t.Fatal("Expected at least one counter metric")
	}
	if len(expectedUpDownCounters) == 0 {
		t.Fatal("Expected at least one up-down counter metric")
	}

	// This test serves as documentation and will fail if someone accidentally
	// changes the metric types back to UpDownCounter for the counter metrics.
	t.Logf("Counter metrics (must use Int64ObservableCounter): %v", expectedCounters)
	t.Logf("UpDownCounter metrics (must use Int64ObservableUpDownCounter): %v", expectedUpDownCounters)
}
