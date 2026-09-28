package redis

import (
	"context"
	"reflect"
	"testing"
)

// These tests assert the argument lists and key-position hints built by the
// BLESS commands without dispatching them to a server, since a test Redis
// instance is not guaranteed to support the BLESS command family.

func TestBlessSet_Args(t *testing.T) {
	ctx := context.Background()

	var captured Cmder
	c := captureCmdable(&captured)
	c.BlessSet(ctx, "key1", BlessNoEvict)

	want := []any{"bless", "set", "key1", "NO-EVICT"}
	if !reflect.DeepEqual(captured.Args(), want) {
		t.Errorf("args = %#v, want %#v", captured.Args(), want)
	}
	if pos := captured.firstKeyPos(); pos != 2 {
		t.Errorf("firstKeyPos = %d, want 2", pos)
	}
}

func TestBlessGet_Args(t *testing.T) {
	ctx := context.Background()

	var captured Cmder
	c := captureCmdable(&captured)
	c.BlessGet(ctx, "key1")

	want := []any{"bless", "get", "key1"}
	if !reflect.DeepEqual(captured.Args(), want) {
		t.Errorf("args = %#v, want %#v", captured.Args(), want)
	}
	if pos := captured.firstKeyPos(); pos != 2 {
		t.Errorf("firstKeyPos = %d, want 2", pos)
	}
}

func TestBlessClear_Args(t *testing.T) {
	ctx := context.Background()

	var captured Cmder
	c := captureCmdable(&captured)
	c.BlessClear(ctx, "key1", BlessNoEvict)

	want := []any{"bless", "clear", "key1", "NO-EVICT"}
	if !reflect.DeepEqual(captured.Args(), want) {
		t.Errorf("args = %#v, want %#v", captured.Args(), want)
	}
	if pos := captured.firstKeyPos(); pos != 2 {
		t.Errorf("firstKeyPos = %d, want 2", pos)
	}
}

func TestBlessScan_Args(t *testing.T) {
	tests := []struct {
		name   string
		cursor uint64
		count  uint64
		want   []any
	}{
		{
			name:   "count_omitted",
			cursor: 0,
			count:  0,
			want:   []any{"bless", "scan", uint64(0), "NO-EVICT"},
		},
		{
			name:   "with_count",
			cursor: 10,
			count:  100,
			want:   []interface{}{"bless", "scan", uint64(10), "NO-EVICT", "count", uint64(100)},
		},
	}

	ctx := context.Background()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var captured Cmder
			c := captureCmdable(&captured)
			c.BlessScan(ctx, tt.cursor, BlessNoEvict, tt.count)

			if !reflect.DeepEqual(captured.Args(), tt.want) {
				t.Errorf("args = %#v, want %#v", captured.Args(), tt.want)
			}
		})
	}
}

// BlessScan has no key argument: it must route as keyless (via the static
// table) rather than hashing "scan" as a key, since it never calls
// SetFirstKeyPos.
func TestBlessIsKeyless(t *testing.T) {
	if _, ok := keylessCommands["bless"]; !ok {
		t.Error(`keylessCommands["bless"] missing: BlessScan would hash "scan" as a key`)
	}
}
