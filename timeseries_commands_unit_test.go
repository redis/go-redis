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
