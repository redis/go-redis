package redis

import (
	"context"
	"reflect"
	"testing"
	"time"
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

func TestXRead_Args(t *testing.T) {
	tests := []struct {
		name       string
		args       XReadArgs
		want       []interface{}
		wantKeyPos int8
	}{
		{
			name: "keys_only_with_per_stream_ids",
			args: XReadArgs{
				Streams: []string{"k1", "k2"},
				IDs:     []string{"id1", "id2"},
				Block:   -1,
			},
			want:       []interface{}{"xread", "streams", "k1", "k2", "id1", "id2"},
			wantKeyPos: 2,
		},
		{
			name: "count_and_block_precede_streams",
			args: XReadArgs{
				Streams: []string{"k1", "k2"},
				IDs:     []string{"id1", "id2"},
				Count:   5,
				Block:   100 * time.Millisecond,
			},
			want:       []interface{}{"xread", "count", int64(5), "block", int64(100), "streams", "k1", "k2", "id1", "id2"},
			wantKeyPos: 6,
		},
		{
			name: "single_id_applied_to_every_stream",
			args: XReadArgs{
				Streams: []string{"k1", "k2"},
				ID:      "0",
				Block:   -1,
			},
			want:       []interface{}{"xread", "streams", "k1", "k2", "0", "0"},
			wantKeyPos: 2,
		},
		{
			name: "ids_take_precedence_over_id",
			args: XReadArgs{
				Streams: []string{"k1", "k2"},
				ID:      "$",
				IDs:     []string{"id1", "id2"},
				Block:   -1,
			},
			want:       []interface{}{"xread", "streams", "k1", "k2", "id1", "id2"},
			wantKeyPos: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var captured Cmder
			c := captureCmdable(&captured)
			cmd := c.XRead(context.Background(), &tt.args)
			if cmd == nil {
				t.Fatalf("XRead returned nil")
			}
			if cmd.Err() != nil {
				t.Fatalf("XRead returned error: %v", cmd.Err())
			}
			if !reflect.DeepEqual(cmd.Args(), tt.want) {
				t.Errorf("args mismatch\n got: %#v\nwant: %#v", cmd.Args(), tt.want)
			}
			if got := cmd.firstKeyPos(); got != tt.wantKeyPos {
				t.Errorf("firstKeyPos = %d, want %d", got, tt.wantKeyPos)
			}
		})
	}
}

func TestXRead_IDsLengthMismatch(t *testing.T) {
	var captured Cmder
	c := captureCmdable(&captured)
	cmd := c.XRead(context.Background(), &XReadArgs{
		Streams: []string{"k1", "k2"},
		IDs:     []string{"id1"},
		Block:   -1,
	})
	if cmd == nil {
		t.Fatalf("XRead returned nil")
	}
	if cmd.Err() == nil {
		t.Fatalf("expected error for mismatched IDs length, got nil")
	}
	if captured != nil {
		t.Errorf("command was dispatched despite mismatched IDs length: %#v", captured.Args())
	}
}

// These tests assert the argument lists built by XREAD for the MAXCOUNT and
// MAXSIZE options (Redis >= 8.10) without dispatching to a server:
//   - both default to unset and emit no token when zero;
//   - when set they are emitted in canonical order, after COUNT and before
//     BLOCK, and before the STREAMS keys;
//   - the first-key position (used for cluster routing) accounts for the new
//     tokens, since they appear before STREAMS.
func TestXRead_MaxArgs(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name    string
		args    *XReadArgs
		want    []interface{}
		wantPos int8
	}{
		{
			name: "no_max_options",
			args: &XReadArgs{Streams: []string{"s1", "0"}, Block: -1},
			want: []interface{}{"xread", "streams", "s1", "0"},
			// keyPos: 1 (xread) + 1 (streams) = 2
			wantPos: 2,
		},
		{
			name: "maxcount_only",
			args: &XReadArgs{Streams: []string{"s1", "s2", "0", "0"}, MaxCount: 80, Block: -1},
			want: []interface{}{"xread", "maxcount", int64(80), "streams", "s1", "s2", "0", "0"},
			// keyPos: 1 + 2 (maxcount) + 1 (streams) = 4
			wantPos: 4,
		},
		{
			name: "maxsize_only",
			args: &XReadArgs{Streams: []string{"s1", "0"}, MaxSize: 65536, Block: -1},
			want: []interface{}{"xread", "maxsize", int64(65536), "streams", "s1", "0"},
			// keyPos: 1 + 2 (maxsize) + 1 (streams) = 4
			wantPos: 4,
		},
		{
			name: "count_maxcount_maxsize_block",
			args: &XReadArgs{
				Streams:  []string{"s1", "s2", "0", "0"},
				Count:    50,
				MaxCount: 80,
				MaxSize:  65536,
				Block:    5 * time.Second,
			},
			want: []interface{}{
				"xread", "count", int64(50), "maxcount", int64(80), "maxsize", int64(65536),
				"block", int64(5000), "streams", "s1", "s2", "0", "0",
			},
			// keyPos: 1 + 2 (count) + 2 (maxcount) + 2 (maxsize) + 2 (block) + 1 (streams) = 10
			wantPos: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var captured Cmder
			c := captureCmdable(&captured)
			cmd := c.XRead(ctx, tt.args)
			if cmd == nil {
				t.Fatalf("XRead returned nil")
			}
			if !reflect.DeepEqual(cmd.Args(), tt.want) {
				t.Errorf("args mismatch\n got: %#v\nwant: %#v", cmd.Args(), tt.want)
			}
			if got := cmd.firstKeyPos(); got != tt.wantPos {
				t.Errorf("firstKeyPos mismatch: got %d, want %d", got, tt.wantPos)
			}
		})
	}
}

// These tests assert the argument lists built by XREADGROUP for the MAXCOUNT and
// MAXSIZE options (Redis >= 8.10), including their interaction with the existing
// NOACK and CLAIM options and the first-key position used for cluster routing.
func TestXReadGroup_MaxArgs(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name    string
		args    *XReadGroupArgs
		want    []interface{}
		wantPos int8
	}{
		{
			name: "no_max_options",
			args: &XReadGroupArgs{Group: "g", Consumer: "c", Streams: []string{"s1", ">"}, Block: -1},
			want: []interface{}{"xreadgroup", "group", "g", "c", "streams", "s1", ">"},
			// keyPos: 4 (group/consumer) + 1 (streams) = 5
			wantPos: 5,
		},
		{
			name: "maxcount_only",
			args: &XReadGroupArgs{Group: "g", Consumer: "c", Streams: []string{"s1", ">"}, MaxCount: 80, Block: -1},
			want: []interface{}{"xreadgroup", "group", "g", "c", "maxcount", int64(80), "streams", "s1", ">"},
			// keyPos: 4 + 2 (maxcount) + 1 (streams) = 7
			wantPos: 7,
		},
		{
			name: "all_options",
			args: &XReadGroupArgs{
				Group:    "g",
				Consumer: "c",
				Streams:  []string{"s1", "s2", ">", ">"},
				Count:    50,
				MaxCount: 80,
				MaxSize:  65536,
				Block:    5 * time.Second,
				NoAck:    true,
				Claim:    30 * time.Second,
			},
			want: []interface{}{
				"xreadgroup", "group", "g", "c", "count", int64(50), "maxcount", int64(80),
				"maxsize", int64(65536), "block", int64(5000), "noack", "claim", int64(30000),
				"streams", "s1", "s2", ">", ">",
			},
			// keyPos: 4 + 2 (count) + 2 (maxcount) + 2 (maxsize) + 2 (block) + 1 (noack) + 2 (claim) + 1 (streams) = 16
			wantPos: 16,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var captured Cmder
			c := captureCmdable(&captured)
			cmd := c.XReadGroup(ctx, tt.args)
			if cmd == nil {
				t.Fatalf("XReadGroup returned nil")
			}
			if !reflect.DeepEqual(cmd.Args(), tt.want) {
				t.Errorf("args mismatch\n got: %#v\nwant: %#v", cmd.Args(), tt.want)
			}
			if got := cmd.firstKeyPos(); got != tt.wantPos {
				t.Errorf("firstKeyPos mismatch: got %d, want %d", got, tt.wantPos)
			}
		})
	}
}
