package redis

import (
	"context"
	"strconv"
	"testing"
)

// Routing-only microbenchmarks for PR #4093 perf work. No network: the client
// gets its topology from ClusterSlots and nothing is dialed. Every iteration
// builds fresh commands (as real callers do); BenchmarkPerfNewGet* measures that
// construction alone so it can be subtracted.

var perfKeys = func() []string {
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = "perf:key:" + strconv.Itoa(i)
	}
	return keys
}()

func newPerfClusterClient(b *testing.B) *ClusterClient {
	b.Helper()
	slots := []ClusterSlot{
		{Start: 0, End: 5460, Nodes: []ClusterNode{{Addr: "127.0.0.1:1"}, {Addr: "127.0.0.1:4"}}},
		{Start: 5461, End: 10922, Nodes: []ClusterNode{{Addr: "127.0.0.1:2"}, {Addr: "127.0.0.1:5"}}},
		{Start: 10923, End: 16383, Nodes: []ClusterNode{{Addr: "127.0.0.1:3"}, {Addr: "127.0.0.1:6"}}},
	}
	c := NewClusterClient(&ClusterOptions{
		ClusterSlots: func(context.Context) ([]ClusterSlot, error) { return slots, nil },
	})
	if _, err := c.state.Get(context.Background()); err != nil {
		b.Fatal(err)
	}
	return c
}

var perfSink Cmder

func BenchmarkPerfNewGet(b *testing.B) {
	ctx := context.Background()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		perfSink = NewStringCmd(ctx, "get", perfKeys[i&1023])
	}
}

func BenchmarkPerfRouteGet(b *testing.B) {
	c := newPerfClusterClient(b)
	defer c.Close()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := NewStringCmd(ctx, "get", perfKeys[i&1023])
		if _, err := perfRouteOne(c, ctx, cmd); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkPerfNewGet100(b *testing.B) {
	ctx := context.Background()
	cmds := make([]Cmder, 100)
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		for j := range cmds {
			cmds[j] = NewStringCmd(ctx, "get", perfKeys[(i*100+j)&1023])
		}
	}
}

func BenchmarkPerfMapCmds100(b *testing.B) {
	c := newPerfClusterClient(b)
	defer c.Close()
	ctx := context.Background()
	cmds := make([]Cmder, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range cmds {
			cmds[j] = NewStringCmd(ctx, "get", perfKeys[(i*100+j)&1023])
		}
		if err := c.mapCmdsByNode(ctx, newCmdsMap(), cmds); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPerfAPAdmission runs what AutoPipeliner.submit calls per command
// before queueing on the cluster face (routing hooks + shardFn, see perfAPAdmit).
func BenchmarkPerfAPAdmission(b *testing.B) {
	c := newPerfClusterClient(b)
	defer c.Close()
	ap, err := c.AutoPipelineWithOptions(&AutoPipelineOptions{MaxBatchSize: 512, MaxConcurrentBatches: 200, Unordered: true})
	if err != nil {
		b.Fatal(err)
	}
	defer ap.Close()
	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cmd := NewStringCmd(ctx, "get", perfKeys[i&1023])
		if err := perfAPAdmit(ap, ctx, cmd); err != nil {
			b.Fatal(err)
		}
		perfAPRelease(cmd)
	}
}

// BenchmarkPerfAPAdmitAndMap100 is AP admission for 100 commands followed by
// the flush-side node mapping of the same commands (the HD AP batch path).
func BenchmarkPerfAPAdmitAndMap100(b *testing.B) {
	c := newPerfClusterClient(b)
	defer c.Close()
	ap, err := c.AutoPipelineWithOptions(&AutoPipelineOptions{MaxBatchSize: 512, MaxConcurrentBatches: 200, Unordered: true})
	if err != nil {
		b.Fatal(err)
	}
	defer ap.Close()
	ctx := context.Background()
	cmds := make([]Cmder, 100)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := range cmds {
			cmd := NewStringCmd(ctx, "get", perfKeys[(i*100+j)&1023])
			if err := perfAPAdmit(ap, ctx, cmd); err != nil {
				b.Fatal(err)
			}
			cmds[j] = cmd
		}
		if err := c.mapCmdsByNode(ctx, newCmdsMap(), cmds); err != nil {
			b.Fatal(err)
		}
		for _, cmd := range cmds {
			perfAPRelease(cmd)
		}
	}
}
