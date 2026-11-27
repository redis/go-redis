package redis

import (
	"context"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

func TestClusterReplicas_Args(t *testing.T) {
	var captured Cmder
	c := captureCmdable(&captured)

	cmd := c.ClusterReplicas(context.Background(), "node-id")
	if err := cmd.Err(); err != nil {
		t.Fatalf("ClusterReplicas: %v", err)
	}
	want := []any{"cluster", "replicas", "node-id"}
	if captured == nil || !reflect.DeepEqual(captured.Args(), want) {
		t.Fatalf("ClusterReplicas sent %v, want %v", captured, want)
	}
}

// CLUSTER ADDSLOTS/DELSLOTS take a list of slots, and the *SlotsRange helpers
// expand an inclusive range into that list. A range the caller cannot have
// meant must be reported as an error: the length it implies is negative, so
// building the slot list used to panic. The slot numbers themselves are left
// for the server to judge.

func TestClusterAddSlotsRange_Args(t *testing.T) {
	tests := []struct {
		name string
		min  int
		max  int
		want []any
	}{
		{
			name: "range",
			min:  0,
			max:  3,
			want: []any{"cluster", "addslots", 0, 1, 2, 3},
		},
		{
			name: "single slot",
			min:  7,
			max:  7,
			want: []any{"cluster", "addslots", 7},
		},
		{
			name: "out of bounds is left to the server",
			min:  99999,
			max:  100000,
			want: []any{"cluster", "addslots", 99999, 100000},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var captured Cmder
			c := captureCmdable(&captured)

			cmd := c.ClusterAddSlotsRange(context.Background(), tc.min, tc.max)
			if cmd.Err() != nil {
				t.Fatalf("ClusterAddSlotsRange(%d, %d) err = %v, want nil", tc.min, tc.max, cmd.Err())
			}
			if !reflect.DeepEqual(captured.Args(), tc.want) {
				t.Errorf("args = %#v, want %#v", captured.Args(), tc.want)
			}
		})
	}
}

func TestClusterDelSlotsRange_Args(t *testing.T) {
	var captured Cmder
	c := captureCmdable(&captured)

	cmd := c.ClusterDelSlotsRange(context.Background(), 2, 4)
	if cmd.Err() != nil {
		t.Fatalf("ClusterDelSlotsRange err = %v, want nil", cmd.Err())
	}
	want := []any{"cluster", "delslots", 2, 3, 4}
	if !reflect.DeepEqual(captured.Args(), want) {
		t.Errorf("args = %#v, want %#v", captured.Args(), want)
	}
}

func TestClusterSlotStats_Args(t *testing.T) {
	ctx := context.Background()
	tests := []struct {
		name string
		call func(c cmdable)
		want []any
	}{
		{
			name: "slots range",
			call: func(c cmdable) { c.ClusterSlotStats(ctx, 0, 100) },
			want: []any{"cluster", "slot-stats", "slotsrange", 0, 100},
		},
		{
			name: "order by",
			call: func(c cmdable) {
				c.ClusterSlotStatsOrderBy(ctx, &ClusterSlotStatsOrderByArgs{Metric: "key-count"})
			},
			want: []any{"cluster", "slot-stats", "orderby", "key-count"},
		},
		{
			name: "order by with limit and asc",
			call: func(c cmdable) {
				c.ClusterSlotStatsOrderBy(ctx, &ClusterSlotStatsOrderByArgs{Metric: "cpu-usec", Limit: 5, Asc: true})
			},
			want: []any{"cluster", "slot-stats", "orderby", "cpu-usec", "limit", 5, "asc"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var captured Cmder
			tc.call(captureCmdable(&captured))
			if !reflect.DeepEqual(captured.Args(), tc.want) {
				t.Errorf("args = %#v, want %#v", captured.Args(), tc.want)
			}
		})
	}
}

// Replies of Redis 8.10.3 started with cluster-slot-stats-enabled yes, plus one
// field this client does not know.
func TestClusterSlotStatsCmd_readReply(t *testing.T) {
	want := []ClusterSlotStat{
		{Slot: 3300, KeyCount: 1, MemoryBytes: 20, CPUUsec: 92, NetworkBytesIn: 27, NetworkBytesOut: 32},
		{Slot: 0},
	}
	fields := "$9\r\nkey-count\r\n:1\r\n$12\r\nmemory-bytes\r\n:20\r\n$8\r\ncpu-usec\r\n:92\r\n" +
		"$16\r\nnetwork-bytes-in\r\n:27\r\n$17\r\nnetwork-bytes-out\r\n:32\r\n$6\r\nfuture\r\n*1\r\n:7\r\n"
	replies := map[string]string{
		"RESP2": "*2\r\n*2\r\n:3300\r\n*12\r\n" + fields + "*2\r\n:0\r\n*2\r\n$9\r\nkey-count\r\n:0\r\n",
		"RESP3": "*2\r\n*2\r\n:3300\r\n%6\r\n" + fields + "*2\r\n:0\r\n%1\r\n$9\r\nkey-count\r\n:0\r\n",
	}

	for name, reply := range replies {
		t.Run(name, func(t *testing.T) {
			cmd := NewClusterSlotStatsCmd(context.Background(), "cluster", "slot-stats", "slotsrange", 0, 16383)
			if err := cmd.readReply(proto.NewReader(strings.NewReader(reply))); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(cmd.Val(), want) {
				t.Fatalf("val = %+v, want %+v", cmd.Val(), want)
			}

			clone := cmd.Clone().(*ClusterSlotStatsCmd)
			clone.Val()[0].KeyCount = 99
			if cmd.Val()[0].KeyCount != 1 {
				t.Error("Clone shares its slice with the original")
			}
		})
	}
}

// Every shard orders and limits its own slots, so the merged reply has to be
// ordered and limited again.
func TestClusterSlotStats_Aggregate(t *testing.T) {
	c := newMetadataTestCluster(t, nil)
	ctx := context.Background()
	stat := func(slot, keys int64) ClusterSlotStat { return ClusterSlotStat{Slot: slot, KeyCount: keys} }
	// What two shards answer to ORDERBY key-count (DESC by default).
	desc := [][]ClusterSlotStat{
		{stat(9000, 5), stat(8000, 1), stat(8001, 1)},
		{stat(100, 7), stat(200, 5), stat(50, 0)},
	}
	asc := [][]ClusterSlotStat{
		{stat(8000, 1), stat(8001, 1), stat(9000, 5)},
		{stat(50, 0), stat(200, 5), stat(100, 7)},
	}

	tests := []struct {
		name   string
		args   []any
		shards [][]ClusterSlotStat
		want   []ClusterSlotStat
	}{
		{
			name:   "slots range is ordered by slot",
			args:   []any{"slotsrange", 0, 16383},
			shards: desc,
			want:   []ClusterSlotStat{stat(50, 0), stat(100, 7), stat(200, 5), stat(8000, 1), stat(8001, 1), stat(9000, 5)},
		},
		{
			name:   "order by is descending with ties by slot",
			args:   []any{"orderby", "key-count"},
			shards: desc,
			want:   []ClusterSlotStat{stat(100, 7), stat(200, 5), stat(9000, 5), stat(8000, 1), stat(8001, 1), stat(50, 0)},
		},
		{
			name:   "limit is applied to the merged reply",
			args:   []any{"ORDERBY", "key-count", "LIMIT", 2, "DESC"},
			shards: [][]ClusterSlotStat{desc[0][:2], desc[1][:2]},
			want:   []ClusterSlotStat{stat(100, 7), stat(200, 5)},
		},
		{
			name:   "ascending",
			args:   []any{"orderby", "key-count", "limit", 3, "asc"},
			shards: asc,
			want:   []ClusterSlotStat{stat(50, 0), stat(8000, 1), stat(8001, 1)},
		},
		{
			name:   "limit above the number of slots",
			args:   []any{"orderby", "key-count", "limit", 100},
			shards: [][]ClusterSlotStat{{stat(1, 1)}, nil},
			want:   []ClusterSlotStat{stat(1, 1)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd := NewClusterSlotStatsCmd(ctx, append([]any{"cluster", "slot-stats"}, tc.args...)...)
			var parts []Cmder
			for _, shard := range tc.shards {
				parts = append(parts, metadataTestResult(NewClusterSlotStatsCmd(ctx), shard))
			}
			decision := c.commandRoutingDecision(ctx, cmd)
			if err := c.aggregateResponses(cmd, parts, decision.policy, &decision); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(cmd.Val(), tc.want) {
				t.Fatalf("val = %+v, want %+v", cmd.Val(), tc.want)
			}
		})
	}

	t.Run("untyped command keeps one reply per shard", func(t *testing.T) {
		cmd := NewCmd(ctx, "cluster", "slot-stats", "orderby", "key-count")
		parts := []Cmder{NewCmdResult([]any{"a"}, nil), NewCmdResult([]any{"b"}, nil)}
		decision := c.commandRoutingDecision(ctx, cmd)
		if err := c.aggregateResponses(cmd, parts, decision.policy, &decision); err != nil {
			t.Fatal(err)
		}
		want := []any{[]any{"a"}, []any{"b"}}
		if !reflect.DeepEqual(cmd.Val(), want) {
			t.Fatalf("val = %#v, want %#v", cmd.Val(), want)
		}
	})
}

func TestClusterSlotsRange_InvalidRange(t *testing.T) {
	tests := []struct {
		name   string
		min    int
		max    int
		errSub string
	}{
		{name: "inverted", min: 5, max: 3, errSub: "max is below min"},
		{name: "empty", min: 5, max: 4, errSub: "max is below min"},
		{name: "size overflow", min: 0, max: math.MaxInt, errSub: "range is too large"},
		{name: "whole int range", min: math.MinInt, max: math.MaxInt, errSub: "range is too large"},
	}

	for _, tc := range tests {
		for _, add := range []bool{true, false} {
			name := tc.name
			if !add {
				name += " (delslots)"
			}
			t.Run(name, func(t *testing.T) {
				var captured Cmder
				c := captureCmdable(&captured)
				ctx := context.Background()

				var cmd *StatusCmd
				if add {
					cmd = c.ClusterAddSlotsRange(ctx, tc.min, tc.max)
				} else {
					cmd = c.ClusterDelSlotsRange(ctx, tc.min, tc.max)
				}

				if cmd.Err() == nil {
					t.Fatalf("range %d..%d: err = nil, want an error", tc.min, tc.max)
				}
				if !strings.Contains(cmd.Err().Error(), tc.errSub) {
					t.Errorf("range %d..%d: err = %q, want it to mention %q",
						tc.min, tc.max, cmd.Err(), tc.errSub)
				}
				if captured != nil {
					t.Errorf("range %d..%d: dispatched %v despite the error", tc.min, tc.max, captured.Args())
				}
			})
		}
	}
}
