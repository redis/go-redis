package redis

import (
	"context"
	"reflect"
	"testing"
	"time"
)

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
