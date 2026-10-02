package redis

import (
	"context"
	"testing"
)

// Raw forms (Do / NewCmd) of the commands whose key is not args[1] carry no
// declared key position, so the shared resolver must find the key itself.
// Without that it fell back to args[1]: raw XREAD hashed "streams" and raw
// LMPOP hashed its numkeys, which routes to the wrong Ring shard, the wrong
// full-duplex engine, or a cold cluster slot.
func TestRawCommandsRouteByTheirKey(t *testing.T) {
	ctx := context.Background()
	const K = "KEY_SENTINEL"
	cases := [][]interface{}{
		{"xread", "streams", K, "0"},
		{"XREAD", "COUNT", 1, "STREAMS", K, "0"},
		{"xread", "count", 1, "block", 0, "streams", K, "b", "0", "0"},
		{"xreadgroup", "group", "g", "c", "streams", K, ">"},
		{"xreadgroup", "group", "streams", "streams", "count", 1, "noack", "streams", K, ">"},
		{"object", "encoding", K},
		{"xinfo", "stream", K},
		{"xgroup", "create", K, "g", "$"},
		{"XGROUP", "CREATE", K, "g", "$", "MKSTREAM"},
		{"xgroup", "setid", K, "g", "0"},
		{"xgroup", "destroy", K, "g"},
		{"xgroup", "createconsumer", K, "g", "c"},
		{"xgroup", "delconsumer", K, "g", "c"},
		{"bitop", "and", K, "b"},
		{"lmpop", 1, K, "left"},
		{"zmpop", 1, K, "min"},
		{"sintercard", 1, K},
		{"zintercard", 2, K, "b"},
		{"zunion", 1, K},
		{"zinter", 2, K, "b", "withscores"},
		{"zdiff", 2, K, "b"},
		{"blmpop", 0, 1, K, "left"},
		{"bzmpop", 0, 1, K, "min"},
		{"migrate", "h", 1, K, 0, 1000},
		{"migrate", "h", 1, "", 0, 1000, "copy", "keys", K, "b"},
	}
	for _, args := range cases {
		cmd := NewCmd(ctx, args...)
		want := -1
		for i, a := range args {
			if s, ok := a.(string); ok && s == K {
				want = i
				break
			}
		}
		if pos := cmdFirstKeyPosWithInfo(cmd, nil); pos != want {
			t.Errorf("%v: resolver picks position %d, key is at %d", args, pos, want)
		}
	}

	// Subcommand help has no key.
	for _, args := range [][]interface{}{{"object", "help"}, {"xinfo", "help"}, {"xgroup", "help"}} {
		if pos := cmdFirstKeyPosWithInfo(NewCmd(ctx, args...), nil); pos != 0 {
			t.Errorf("%v: resolver picks position %d, want 0 (keyless)", args, pos)
		}
	}
}
