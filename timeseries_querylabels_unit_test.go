package redis

import (
	"context"
	"reflect"
	"testing"
)

// TestTSQueryLabelsArgs pins the wire format of both TS.QUERYLABELS forms.
// The FILTER token must be emitted only when at least one filter expression
// is given (the server rejects a bare FILTER), filter expressions must be
// passed verbatim and in order, and the VALUES label must not be normalized.
func TestTSQueryLabelsArgs(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name string
		call func(c cmdable) Cmder
		want []interface{}
	}{
		{
			name: "labels_without_filter_omits_filter_token",
			call: func(c cmdable) Cmder { return c.TSQueryLabels(ctx, nil) },
			want: []interface{}{"TS.QUERYLABELS", "LABELS"},
		},
		{
			name: "labels_with_empty_filter_slice_omits_filter_token",
			call: func(c cmdable) Cmder { return c.TSQueryLabels(ctx, []string{}) },
			want: []interface{}{"TS.QUERYLABELS", "LABELS"},
		},
		{
			name: "labels_with_one_filter",
			call: func(c cmdable) Cmder { return c.TSQueryLabels(ctx, []string{"type=sensor"}) },
			want: []interface{}{"TS.QUERYLABELS", "LABELS", "FILTER", "type=sensor"},
		},
		{
			name: "labels_filters_verbatim_and_ordered",
			call: func(c cmdable) Cmder {
				return c.TSQueryLabels(ctx, []string{"type=sensor", "location!=", "l=(a,b)"})
			},
			want: []interface{}{"TS.QUERYLABELS", "LABELS", "FILTER", "type=sensor", "location!=", "l=(a,b)"},
		},
		{
			name: "values_without_filter_omits_filter_token",
			call: func(c cmdable) Cmder { return c.TSQueryLabelValues(ctx, "location", nil) },
			want: []interface{}{"TS.QUERYLABELS", "VALUES", "location"},
		},
		{
			name: "values_with_filters",
			call: func(c cmdable) Cmder {
				return c.TSQueryLabelValues(ctx, "location", []string{"type=sensor"})
			},
			want: []interface{}{"TS.QUERYLABELS", "VALUES", "location", "FILTER", "type=sensor"},
		},
		{
			name: "values_label_not_normalized",
			call: func(c cmdable) Cmder { return c.TSQueryLabelValues(ctx, " LoCaTiOn ", nil) },
			want: []interface{}{"TS.QUERYLABELS", "VALUES", " LoCaTiOn "},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var captured Cmder
			c := captureCmdable(&captured)
			cmd := tt.call(c)
			if cmd == nil {
				t.Fatalf("command builder returned nil")
			}
			if !reflect.DeepEqual(cmd.Args(), tt.want) {
				t.Errorf("args mismatch\n got: %#v\nwant: %#v", cmd.Args(), tt.want)
			}
		})
	}
}
