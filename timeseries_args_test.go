package redis_test

import (
	"context"
	"slices"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestTimeSeriesSelectedLabelsArgs(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	t.Cleanup(func() { _ = client.Close() })
	pipeline := client.Pipeline()
	filters := []string{"team=example"}
	commands := []struct {
		name string
		args []interface{}
		run  func([]interface{}) *redis.MapStringSliceInterfaceCmd
	}{
		{"TS.MGET", []interface{}{"TS.MGET"}, func(labels []interface{}) *redis.MapStringSliceInterfaceCmd {
			return pipeline.TSMGetWithArgs(ctx, filters, &redis.TSMGetOptions{SelectedLabels: labels})
		}},
		{"TS.MRANGE", []interface{}{"TS.MRANGE", 0, 100}, func(labels []interface{}) *redis.MapStringSliceInterfaceCmd {
			return pipeline.TSMRangeWithArgs(ctx, 0, 100, filters, &redis.TSMRangeOptions{SelectedLabels: labels})
		}},
		{"TS.MREVRANGE", []interface{}{"TS.MREVRANGE", 0, 100}, func(labels []interface{}) *redis.MapStringSliceInterfaceCmd {
			return pipeline.TSMRevRangeWithArgs(ctx, 0, 100, filters, &redis.TSMRevRangeOptions{SelectedLabels: labels})
		}},
	}
	cases := []struct {
		name   string
		labels []interface{}
	}{
		{name: "nil"},
		{name: "empty", labels: []interface{}{}},
		{name: "nonempty", labels: []interface{}{"team", "region"}},
	}
	for _, command := range commands {
		for _, tc := range cases {
			t.Run(command.name+"/"+tc.name, func(t *testing.T) {
				want := slices.Clone(command.args)
				if len(tc.labels) > 0 {
					want = append(want, "SELECTED_LABELS", "team", "region")
				}
				want = append(want, "FILTER", filters[0])
				if got := command.run(tc.labels).Args(); !slices.Equal(got, want) {
					t.Errorf("Args() = %v, want %v", got, want)
				}
			})
		}
	}
}
