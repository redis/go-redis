package redis

import (
	"context"
	"reflect"
	"testing"
)

// A negative constructor hint must not cause an invalid argument access.
// Resolved metadata remains authoritative over the public key-position hint.
func TestCSCNegativeKeyPositionHint(t *testing.T) {
	ctx := context.Background()
	get := NewStringCmd(ctx, "get", "k")
	get.SetFirstKeyPos(-1)
	mget := NewSliceCmd(ctx, "mget", "a", "b")
	mget.SetFirstKeyPos(-3)
	for _, cmd := range []Cmder{get, mget} {
		meta, ok := cscCommandMetaFor(cmd)
		if !ok || !cscCanExtractRedisKeys(meta, cmd) {
			t.Fatalf("%s: constructor hint overrode valid metadata", cmd.Name())
		}
		keys := extractRedisKeys(cmd)
		cmd.SetFirstKeyPos(1)
		if want := extractRedisKeys(cmd); !reflect.DeepEqual(keys, want) {
			t.Errorf("%s: keys = %v, want %v", cmd.Name(), keys, want)
		}
		meta.firstKey = -1
		if cscCanExtractRedisKeys(meta, cmd) || keyArgOK(cmd, -1) {
			t.Errorf("%s: accepted a negative metadata key position", cmd.Name())
		}
	}
}
