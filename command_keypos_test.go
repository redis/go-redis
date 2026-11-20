package redis

// Typed commands whose key is not args[1] must say where it is, because the
// shared resolver (cmdFirstKeyPosWithInfo) routes by it: Ring shard selection,
// cluster slot routing and full-duplex engine routing. Each case builds the
// command with a sentinel key and checks the resolver lands on it.

import (
	"context"
	"testing"
	"time"
)

func TestTypedCommandsRouteByTheirKey(t *testing.T) {
	ctx := context.Background()
	var got Cmder
	c := cmdable(func(_ context.Context, cmd Cmder) error { got = cmd; return nil })
	const K = "KEY_SENTINEL"
	cases := []struct {
		name string
		fn   func()
	}{
		{"ObjectEncoding", func() { c.ObjectEncoding(ctx, K) }},
		{"ObjectRefCount", func() { c.ObjectRefCount(ctx, K) }},
		{"ObjectIdleTime", func() { c.ObjectIdleTime(ctx, K) }},
		{"ObjectFreq", func() { c.ObjectFreq(ctx, K) }},
		{"MemoryUsage", func() { c.MemoryUsage(ctx, K) }},
		{"XInfoStream", func() { c.XInfoStream(ctx, K) }},
		{"XInfoStreamFull", func() { c.XInfoStreamFull(ctx, K, 0) }},
		{"XInfoGroups", func() { c.XInfoGroups(ctx, K) }},
		{"XInfoConsumers", func() { c.XInfoConsumers(ctx, K, "g") }},
		{"XGroupCreate", func() { c.XGroupCreate(ctx, K, "g", "$") }},
		{"XGroupCreateMkStream", func() { c.XGroupCreateMkStream(ctx, K, "g", "$") }},
		{"XGroupDestroy", func() { c.XGroupDestroy(ctx, K, "g") }},
		{"XGroupSetID", func() { c.XGroupSetID(ctx, K, "g", "$") }},
		{"XGroupCreateConsumer", func() { c.XGroupCreateConsumer(ctx, K, "g", "c") }},
		{"XGroupDelConsumer", func() { c.XGroupDelConsumer(ctx, K, "g", "c") }},
		{"XRead", func() { c.XRead(ctx, &XReadArgs{Streams: []string{K, "0"}}) }},
		{"XReadGroup", func() { c.XReadGroup(ctx, &XReadGroupArgs{Group: "g", Consumer: "c", Streams: []string{K, "0"}}) }},
		{"ZUnion", func() { c.ZUnion(ctx, ZStore{Keys: []string{K, "b"}}) }},
		{"ZInter", func() { c.ZInter(ctx, &ZStore{Keys: []string{K, "b"}}) }},
		{"ZDiff", func() { c.ZDiff(ctx, K, "b") }},
		{"ZInterCard", func() { c.ZInterCard(ctx, 0, K, "b") }},
		{"SInterCard", func() { c.SInterCard(ctx, 0, K, "b") }},
		{"LMPop", func() { c.LMPop(ctx, "left", 1, K) }},
		{"BLMPop", func() { c.BLMPop(ctx, 0, "left", 1, K) }},
		{"ZMPop", func() { c.ZMPop(ctx, "min", 1, K) }},
		{"BZMPop", func() { c.BZMPop(ctx, 0, "min", 1, K) }},
		{"BitOpAnd", func() { c.BitOpAnd(ctx, K, "b") }},
		{"Migrate", func() { c.Migrate(ctx, "h", "1", K, 0, time.Second) }},
		{"Eval", func() { c.Eval(ctx, "s", []string{K}) }},
		{"EvalSha", func() { c.EvalSha(ctx, "s", []string{K}) }},
		{"EvalRO", func() { c.EvalRO(ctx, "s", []string{K}) }},
		{"FCall", func() { c.FCall(ctx, "f", []string{K}) }},
		{"FCallRO", func() { c.FCallRo(ctx, "f", []string{K}) }},
		{"Sort", func() { c.Sort(ctx, K, &Sort{}) }},
		{"GeoRadius", func() { c.GeoRadius(ctx, K, 0, 0, &GeoRadiusQuery{Radius: 1}) }},
		{"XAutoClaim", func() { c.XAutoClaim(ctx, &XAutoClaimArgs{Stream: K, Group: "g", Consumer: "c", Start: "0"}) }},
		{"Copy", func() { c.Copy(ctx, K, "b", 0, false) }},
		{"LCS", func() { c.LCS(ctx, &LCSQuery{Key1: K, Key2: "b"}) }},
	}
	for _, tc := range cases {
		got = nil
		tc.fn()
		if got == nil {
			t.Errorf("%s: no command captured", tc.name)
			continue
		}
		args := got.Args()
		want := -1
		for i, a := range args {
			if s, ok := a.(string); ok && s == K {
				want = i
				break
			}
		}
		if pos := cmdFirstKeyPosWithInfo(got, nil); pos != want {
			at := "<none>"
			if pos > 0 && pos < len(args) {
				at = got.stringArg(pos)
			}
			t.Errorf("%s: resolver picks position %d (%q), key is at %d: %v", tc.name, pos, at, want, args)
		}
	}
}
