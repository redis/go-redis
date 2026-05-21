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
