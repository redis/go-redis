package redis

import (
	"context"
	"math"
	"reflect"
	"strings"
	"testing"
)

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
