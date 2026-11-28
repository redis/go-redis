package redis

import (
	"context"
	"testing"
)

// TestCSCKeyGateAllocatesNothing pins that the key check processCached runs
// on every cached read, hits included, does not allocate.
func TestCSCKeyGateAllocatesNothing(t *testing.T) {
	ctx := context.Background()
	// A no-op cmdable builds the typed commands without any I/O.
	c := cmdable(func(context.Context, Cmder) error { return nil })
	cmds := map[string]Cmder{
		"get":           c.Get(ctx, "k"),
		"mget":          c.MGet(ctx, "a", "b", "c"),
		"zinter":        c.ZInter(ctx, &ZStore{Keys: []string{"a", "b"}}),
		"lcs":           c.LCS(ctx, &LCSQuery{Key1: "a", Key2: "b"}),
		"sort_ro":       c.SortRO(ctx, "mylist", &Sort{Offset: 0, Count: 10, Alpha: true}),
		"sort_ro_bytes": NewCmd(ctx, "sort_ro", "mylist", []byte("LIMIT"), 0, 10, []byte("ALPHA")),
	}
	for name, cmd := range cmds {
		meta, ok := cscCommandMetaFor(cmd)
		if !ok || !cscCanExtractRedisKeys(meta, cmd) {
			t.Fatalf("%s: keys not renderable", name)
		}
		if n := testing.AllocsPerRun(100, func() {
			_ = cscCanExtractRedisKeys(meta, cmd)
		}); n != 0 {
			t.Errorf("%s: key gate allocates %v objects per call, want 0", name, n)
		}
	}
}
