package redis

import (
	"context"
	"fmt"
	"runtime"
	"testing"
)

// A member where full duplex cannot engage (no pipeline pool) diverts to
// MultiDB's normal path and is remembered: no child is built per command.
func TestMultiDBFullDuplexMemberWithoutFD(t *testing.T) {
	requireLocalRedis(t)
	noPool := MultiDBClientConfig{
		Options: &Options{Addr: ":6379", PipelinePoolSize: -1, MaxRetries: -1},
		Weight:  1,
	}
	mdb := newFDTestMultiDB(t, noPool)
	ctx := context.Background()
	ap, err := mdb.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	if ap.mdbFD == nil {
		t.Fatal("router not installed")
	}

	before := runtime.NumGoroutine()
	cmds := make([]*StatusCmd, 2000)
	for i := range cmds {
		cmds[i] = ap.Set(ctx, fmt.Sprintf("mdbfd:nofd:%d", i%50), i, 0)
	}
	for i, c := range cmds {
		if err := c.Err(); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	db := mdb.core.dbByID(0)
	ap.mdbFD.mu.RLock()
	_, remembered := ap.mdbFD.noFD[db]
	nChildren := len(ap.mdbFD.children)
	ap.mdbFD.mu.RUnlock()
	if !remembered {
		t.Fatal("member without FD not remembered")
	}
	if nChildren != 0 {
		t.Fatalf("%d children built for a member without FD", nChildren)
	}
	if after := runtime.NumGoroutine(); after > before+20 {
		t.Fatalf("goroutines grew from %d to %d: children leaking", before, after)
	}
}

// One caller's async writes to a key across a member death: the last write
// submitted must be the value that sticks (submit order holds across the
// reroute and the failover).
func TestMultiDBFullDuplexOrderAcrossMemberDeath(t *testing.T) {
	requireLocalRedis(t)
	proxy := startFDTestProxy(t, "127.0.0.1:6379")
	mdb := newFDTestMultiDB(t, member(proxy.addr(), 2), member(":6379", 1))
	ctx := context.Background()
	ap, err := mdb.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	key := "mdbfd:order"

	const n = 20000
	cmds := make([]*StatusCmd, 0, n)
	for i := 0; i < n; i++ {
		if i == n/4 {
			proxy.kill()
		}
		cmds = append(cmds, ap.Set(ctx, key, i, 0))
	}
	for i, c := range cmds {
		if err := c.Err(); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	direct := NewClient(&Options{Addr: ":6379"})
	defer direct.Close()
	got, err := direct.Get(ctx, key).Int()
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	if got != n-1 {
		t.Fatalf("final value = %d, want %d (the last write submitted): a write submitted earlier ran after a later one", got, n-1)
	}
}
