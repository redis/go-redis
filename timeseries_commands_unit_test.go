package redis

import (
	"context"
	"reflect"
	"testing"
)

// ON_DUPLICATE is a TS.ADD option; TS.CREATE has none and must not send it.
func TestTSAddOnDuplicate(t *testing.T) {
	ctx := context.Background()
	options := &TSOptions{DuplicatePolicy: "BLOCK", OnDuplicate: "LAST"}

	var captured Cmder
	c := captureCmdable(&captured)

	want := []interface{}{"TS.ADD", "series", 1000, float64(1), "DUPLICATE_POLICY", "BLOCK", "ON_DUPLICATE", "LAST"}
	if got := c.TSAddWithArgs(ctx, "series", 1000, 1, options).Args(); !reflect.DeepEqual(got, want) {
		t.Errorf("TSAddWithArgs args\n got: %#v\nwant: %#v", got, want)
	}

	want = []interface{}{"TS.CREATE", "series", "DUPLICATE_POLICY", "BLOCK"}
	if got := c.TSCreateWithArgs(ctx, "series", options).Args(); !reflect.DeepEqual(got, want) {
		t.Errorf("TSCreateWithArgs args\n got: %#v\nwant: %#v", got, want)
	}
}

func TestTSIgnoreBeforeLabels(t *testing.T) {
	ctx := context.Background()
	labels := map[string]string{"sensor": "temperature"}
	options := &TSOptions{IgnoreMaxTimeDiff: 5, IgnoreMaxValDiff: 10, Labels: labels}
	alterOptions := &TSAlterOptions{IgnoreMaxTimeDiff: 5, IgnoreMaxValDiff: 10, Labels: labels}
	incrDecrOptions := &TSIncrDecrOptions{IgnoreMaxTimeDiff: 5, IgnoreMaxValDiff: 10, Labels: labels}

	tests := []struct {
		name string
		call func(cmdable) Cmder
		want []interface{}
	}{
		{
			name: "add",
			call: func(c cmdable) Cmder { return c.TSAddWithArgs(ctx, "series", 1000, 1, options) },
			want: []interface{}{"TS.ADD", "series", 1000, float64(1)},
		},
		{
			name: "create",
			call: func(c cmdable) Cmder { return c.TSCreateWithArgs(ctx, "series", options) },
			want: []interface{}{"TS.CREATE", "series"},
		},
		{
			name: "alter",
			call: func(c cmdable) Cmder { return c.TSAlter(ctx, "series", alterOptions) },
			want: []interface{}{"TS.ALTER", "series"},
		},
		{
			name: "increment",
			call: func(c cmdable) Cmder { return c.TSIncrByWithArgs(ctx, "series", 1, incrDecrOptions) },
			want: []interface{}{"TS.INCRBY", "series", float64(1)},
		},
		{
			name: "decrement",
			call: func(c cmdable) Cmder { return c.TSDecrByWithArgs(ctx, "series", 1, incrDecrOptions) },
			want: []interface{}{"TS.DECRBY", "series", float64(1)},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var captured Cmder
			cmd := tt.call(captureCmdable(&captured))
			// LABELS consumes the remaining arguments as label/value pairs.
			want := append(tt.want, "IGNORE", int64(5), float64(10), "LABELS", "sensor", "temperature")
			if !reflect.DeepEqual(cmd.Args(), want) {
				t.Errorf("args mismatch\n got: %#v\nwant: %#v", cmd.Args(), want)
			}
		})
	}
}

func TestTSRangeFilterArgs(t *testing.T) {
	ctx := context.Background()
	labels := []string{"team=example"}
	commands := []struct {
		name   string
		call   func(cmdable, []int, []int) Cmder
		prefix []interface{}
		suffix []interface{}
	}{
		{
			name: "range",
			call: func(c cmdable, ts, values []int) Cmder {
				return c.TSRangeWithArgs(ctx, "series", 0, 100, &TSRangeOptions{FilterByTS: ts, FilterByValue: values})
			},
			prefix: []interface{}{"TS.RANGE", "series", 0, 100},
		},
		{
			name: "revrange",
			call: func(c cmdable, ts, values []int) Cmder {
				return c.TSRevRangeWithArgs(ctx, "series", 0, 100, &TSRevRangeOptions{FilterByTS: ts, FilterByValue: values})
			},
			prefix: []interface{}{"TS.REVRANGE", "series", 0, 100},
		},
		{
			name: "mrange",
			call: func(c cmdable, ts, values []int) Cmder {
				return c.TSMRangeWithArgs(ctx, 0, 100, labels, &TSMRangeOptions{FilterByTS: ts, FilterByValue: values})
			},
			prefix: []interface{}{"TS.MRANGE", 0, 100},
			suffix: []interface{}{"FILTER", "team=example"},
		},
		{
			name: "mrevrange",
			call: func(c cmdable, ts, values []int) Cmder {
				return c.TSMRevRangeWithArgs(ctx, 0, 100, labels, &TSMRevRangeOptions{FilterByTS: ts, FilterByValue: values})
			},
			prefix: []interface{}{"TS.MREVRANGE", 0, 100},
			suffix: []interface{}{"FILTER", "team=example"},
		},
	}
	tests := []struct {
		name   string
		ts     []int
		values []int
		want   []interface{}
	}{
		{name: "nil"},
		{name: "empty", ts: []int{}, values: []int{}},
		{name: "nonempty", ts: []int{10, 20}, values: []int{0, 5}, want: []interface{}{"FILTER_BY_TS", 10, 20, "FILTER_BY_VALUE", 0, 5}},
	}
	for _, command := range commands {
		for _, tt := range tests {
			t.Run(command.name+"/"+tt.name, func(t *testing.T) {
				var captured Cmder
				cmd := command.call(captureCmdable(&captured), tt.ts, tt.values)
				want := append([]interface{}{}, command.prefix...)
				want = append(want, tt.want...)
				want = append(want, command.suffix...)
				if !reflect.DeepEqual(cmd.Args(), want) {
					t.Errorf("args mismatch\n got: %#v\nwant: %#v", cmd.Args(), want)
				}
			})
		}
	}
}
