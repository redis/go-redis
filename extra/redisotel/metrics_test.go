package redisotel

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/redis/go-redis/v9"
)

type testPoolStatsClient struct {
	stats *redis.PoolStats
}

func (c *testPoolStatsClient) Options() *redis.Options {
	return &redis.Options{PoolSize: 10}
}

func (c *testPoolStatsClient) PoolStats() *redis.PoolStats {
	return c.stats
}

func TestReportPoolStatsUsage(t *testing.T) {
	var reported []error
	previous := otel.GetErrorHandler()
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		reported = append(reported, err)
	}))
	t.Cleanup(func() { otel.SetErrorHandler(previous) })

	tests := []struct {
		name  string
		total uint32
		idle  uint32
	}{
		{name: "empty"},
		{name: "all idle", total: 3, idle: 3},
		{name: "all used", total: 3},
		{name: "mixed", total: 5, idle: 2},
		{name: "maximum total", total: math.MaxUint32, idle: 1},
		{name: "maximum idle", total: math.MaxUint32, idle: math.MaxUint32},
		{name: "idle exceeds zero", idle: 1},
		{name: "idle exceeds total", total: 1, idle: 3},
		{name: "maximum invalid idle", idle: math.MaxUint32},
	}
	for _, semconv := range []bool{false, true} {
		t.Run(fmt.Sprintf("semconv=%t", semconv), func(t *testing.T) {
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					ctx := context.Background()
					reader := sdkmetric.NewManualReader()
					mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
					t.Cleanup(func() { _ = mp.Shutdown(ctx) })

					client := &testPoolStatsClient{stats: &redis.PoolStats{
						TotalConns: tt.total,
						IdleConns:  tt.idle,
						Hits:       7,
					}}
					conf := newConfig(WithSemConvCompliantMetrics(semconv))
					conf.meter = mp.Meter("test")
					conf.attrs = append(conf.attrs, attribute.String("pool.name", "test-pool"))
					if _, err := reportPoolStats(client, conf); err != nil {
						t.Fatalf("reportPoolStats: %v", err)
					}

					// Invalid samples must not reuse an earlier observation or
					// prevent valid observations in later collections.
					for _, stats := range []*redis.PoolStats{
						{TotalConns: 4, IdleConns: 1, Hits: 7},
						client.stats,
						{TotalConns: 4, IdleConns: 1, Hits: 7},
					} {
						client.stats = stats
						invalid := client.stats.IdleConns > client.stats.TotalConns
						var rm metricdata.ResourceMetrics
						reported = nil
						if err := reader.Collect(ctx, &rm); err != nil {
							t.Fatalf("Collect: %v", err)
						}
						if invalid {
							want := fmt.Sprintf("redisotel: invalid pool stats: idle connections (%d) exceed total connections (%d)",
								client.stats.IdleConns, client.stats.TotalConns)
							if len(reported) != 1 || reported[0].Error() != want {
								t.Errorf("reported errors = %v, want [%q]", reported, want)
							}
						} else if len(reported) != 0 {
							t.Errorf("unexpected reported errors: %v", reported)
						}

						usage := make(map[string]int64)
						var hits int64
						for _, sm := range rm.ScopeMetrics {
							for _, m := range sm.Metrics {
								switch m.Name {
								case "db.client.connections.usage":
									sum, ok := m.Data.(metricdata.Sum[int64])
									if !ok {
										t.Fatalf("usage aggregation = %T, want Sum[int64]", m.Data)
									}
									if sum.IsMonotonic {
										t.Error("usage must remain a non-monotonic sum")
									}
									for _, point := range sum.DataPoints {
										pool, _ := point.Attributes.Value("pool.name")
										if pool.AsString() != "test-pool" {
											t.Errorf("pool.name = %v, want test-pool", pool)
										}
										state, _ := point.Attributes.Value("state")
										if _, exists := usage[state.AsString()]; exists {
											t.Errorf("duplicate usage state %q", state.AsString())
										}
										usage[state.AsString()] = point.Value
									}
								case "db.client.connections.hits":
									sum, ok := m.Data.(metricdata.Sum[int64])
									if !ok || len(sum.DataPoints) != 1 {
										t.Fatalf("unexpected hits aggregation: %#v", m.Data)
									}
									hits = sum.DataPoints[0].Value
								}
							}
						}
						wantUsage := make(map[string]int64)
						if !invalid {
							wantUsage["idle"] = int64(client.stats.IdleConns)
							wantUsage["used"] = int64(client.stats.TotalConns) - int64(client.stats.IdleConns)
						}
						if !reflect.DeepEqual(usage, wantUsage) {
							t.Errorf("usage = %v, want %v", usage, wantUsage)
						}
						if hits != 7 {
							t.Errorf("hits = %d, want 7 even when usage is invalid", hits)
						}
					}
				})
			}
		})
	}
}

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

// Test_poolStatsAttrs_race reproduces issue #3880: attribute.NewSet sorts and
// de-duplicates its input slice in place, so poolStatsAttrs mutates the shared
// conf.attrs backing array. That array is aliased by every metricsHook.attrs
// and read concurrently while MinIdleConns pre-warms connections in the
// background, producing a data race. Must be run with -race.
func Test_poolStatsAttrs_race(t *testing.T) {
	conf := newConfig()
	conf.attrs = append(conf.attrs,
		attribute.String("pool.name", "pool1"),
		attribute.String("foo1", "bar1"),
		attribute.String("foo2", "bar2"),
	)

	// A metricsHook aliases conf.attrs, mirroring addMetricsHook.
	mh := &metricsHook{attrs: conf.attrs}

	const n = 50
	var wg sync.WaitGroup
	wg.Add(2 * n)

	// Readers: mirror metricsHook.DialHook reading mh.attrs during a dial.
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			attrs := make([]attribute.KeyValue, 0, len(mh.attrs)+2)
			attrs = append(attrs, mh.attrs...)
			_ = attribute.NewSet(attrs...)
		}()
	}

	// Writers: registerClient calling poolStatsAttrs for each cluster node.
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			poolStatsAttrs(conf)
		}()
	}

	wg.Wait()
}
