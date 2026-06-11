package redis

import (
	"context"
	"reflect"
	"testing"
)

// These tests assert the argument lists built by the XTRIM/XADD trimming
// commands without dispatching them to a server, with particular focus on
// the LIMIT clause semantics:
//   - limit == 0 omits the LIMIT clause (historical behavior);
//   - limit > 0 emits "LIMIT <limit>";
//   - limit < 0 (XTrimLimitDisabled) emits an explicit "LIMIT 0";
//   - exact ("=") trim commands never emit LIMIT.

func TestXTrim_LimitArgs(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name string
		call func(c cmdable) Cmder
		want []interface{}
	}{
		{
			name: "maxlen_approx_limit_omitted",
			call: func(c cmdable) Cmder { return c.XTrimMaxLenApprox(ctx, "stream", 100, 0) },
			want: []interface{}{"xtrim", "stream", "maxlen", "~", int64(100)},
		},
		{
			name: "maxlen_approx_positive_limit",
			call: func(c cmdable) Cmder { return c.XTrimMaxLenApprox(ctx, "stream", 100, 1000) },
			want: []interface{}{"xtrim", "stream", "maxlen", "~", int64(100), "limit", int64(1000)},
		},
		{
			name: "maxlen_approx_limit_disabled",
			call: func(c cmdable) Cmder { return c.XTrimMaxLenApprox(ctx, "stream", 100, XTrimLimitDisabled) },
			want: []interface{}{"xtrim", "stream", "maxlen", "~", int64(100), "limit", int64(0)},
		},
		{
			name: "minid_approx_limit_omitted",
			call: func(c cmdable) Cmder { return c.XTrimMinIDApprox(ctx, "stream", "4-0", 0) },
			want: []interface{}{"xtrim", "stream", "minid", "~", "4-0"},
		},
		{
			name: "minid_approx_positive_limit",
			call: func(c cmdable) Cmder { return c.XTrimMinIDApprox(ctx, "stream", "4-0", 42) },
			want: []interface{}{"xtrim", "stream", "minid", "~", "4-0", "limit", int64(42)},
		},
		{
			name: "minid_approx_limit_disabled",
			call: func(c cmdable) Cmder { return c.XTrimMinIDApprox(ctx, "stream", "4-0", XTrimLimitDisabled) },
			want: []interface{}{"xtrim", "stream", "minid", "~", "4-0", "limit", int64(0)},
		},
		{
			name: "maxlen_exact_never_emits_limit",
			call: func(c cmdable) Cmder { return c.XTrimMaxLen(ctx, "stream", 100) },
			want: []interface{}{"xtrim", "stream", "maxlen", "=", int64(100)},
		},
		{
			name: "minid_exact_never_emits_limit",
			call: func(c cmdable) Cmder { return c.XTrimMinID(ctx, "stream", "4-0") },
			want: []interface{}{"xtrim", "stream", "minid", "=", "4-0"},
		},
		{
			name: "maxlen_approx_mode_limit_omitted",
			call: func(c cmdable) Cmder { return c.XTrimMaxLenApproxMode(ctx, "stream", 100, 0, "KEEPREF") },
			want: []interface{}{"xtrim", "stream", "maxlen", "~", int64(100), "KEEPREF"},
		},
		{
			name: "maxlen_approx_mode_positive_limit",
			call: func(c cmdable) Cmder { return c.XTrimMaxLenApproxMode(ctx, "stream", 100, 1000, "KEEPREF") },
			want: []interface{}{"xtrim", "stream", "maxlen", "~", int64(100), "limit", int64(1000), "KEEPREF"},
		},
		{
			name: "maxlen_approx_mode_limit_disabled",
			call: func(c cmdable) Cmder {
				return c.XTrimMaxLenApproxMode(ctx, "stream", 100, XTrimLimitDisabled, "KEEPREF")
			},
			want: []interface{}{"xtrim", "stream", "maxlen", "~", int64(100), "limit", int64(0), "KEEPREF"},
		},
		{
			name: "minid_approx_mode_limit_disabled",
			call: func(c cmdable) Cmder {
				return c.XTrimMinIDApproxMode(ctx, "stream", "4-0", XTrimLimitDisabled, "DELREF")
			},
			want: []interface{}{"xtrim", "stream", "minid", "~", "4-0", "limit", int64(0), "DELREF"},
		},
		{
			name: "maxlen_exact_mode_never_emits_limit",
			call: func(c cmdable) Cmder { return c.XTrimMaxLenMode(ctx, "stream", 100, "KEEPREF") },
			want: []interface{}{"xtrim", "stream", "maxlen", "=", int64(100), "KEEPREF"},
		},
		{
			name: "minid_exact_mode_never_emits_limit",
			call: func(c cmdable) Cmder { return c.XTrimMinIDMode(ctx, "stream", "4-0", "DELREF") },
			want: []interface{}{"xtrim", "stream", "minid", "=", "4-0", "DELREF"},
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

func TestXAdd_TrimLimitArgs(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name string
		args *XAddArgs
		want []interface{}
	}{
		{
			name: "approx_limit_omitted",
			args: &XAddArgs{
				Stream: "stream",
				MaxLen: 100,
				Approx: true,
				ID:     "1-0",
				Values: []string{"k", "v"},
			},
			want: []interface{}{"xadd", "stream", "maxlen", "~", int64(100), "1-0", "k", "v"},
		},
		{
			name: "approx_positive_limit",
			args: &XAddArgs{
				Stream: "stream",
				MaxLen: 100,
				Approx: true,
				Limit:  1000,
				ID:     "1-0",
				Values: []string{"k", "v"},
			},
			want: []interface{}{"xadd", "stream", "maxlen", "~", int64(100), "limit", int64(1000), "1-0", "k", "v"},
		},
		{
			name: "approx_limit_disabled",
			args: &XAddArgs{
				Stream: "stream",
				MaxLen: 100,
				Approx: true,
				Limit:  XTrimLimitDisabled,
				ID:     "1-0",
				Values: []string{"k", "v"},
			},
			want: []interface{}{"xadd", "stream", "maxlen", "~", int64(100), "limit", int64(0), "1-0", "k", "v"},
		},
		{
			name: "minid_approx_limit_disabled",
			args: &XAddArgs{
				Stream: "stream",
				MinID:  "4-0",
				Approx: true,
				Limit:  XTrimLimitDisabled,
				ID:     "5-0",
				Values: []string{"k", "v"},
			},
			want: []interface{}{"xadd", "stream", "minid", "~", "4-0", "limit", int64(0), "5-0", "k", "v"},
		},
		{
			name: "exact_zero_limit_omitted",
			args: &XAddArgs{
				Stream: "stream",
				MaxLen: 100,
				ID:     "1-0",
				Values: []string{"k", "v"},
			},
			want: []interface{}{"xadd", "stream", "maxlen", "=", int64(100), "1-0", "k", "v"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var captured Cmder
			c := captureCmdable(&captured)
			cmd := c.XAdd(ctx, tt.args)
			if cmd == nil {
				t.Fatalf("XAdd returned nil")
			}
			if !reflect.DeepEqual(cmd.Args(), tt.want) {
				t.Errorf("args mismatch\n got: %#v\nwant: %#v", cmd.Args(), tt.want)
			}
		})
	}
}
