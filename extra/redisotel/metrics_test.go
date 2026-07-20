package redisotel

import (
	"reflect"
	"sync"
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
