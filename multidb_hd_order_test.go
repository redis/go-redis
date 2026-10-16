package redis

import (
	"context"
	"testing"
)

// Reference for TestMultiDBFullDuplexOrderAcrossMemberDeath on the half-duplex
// async face.
func TestMultiDBHalfDuplexOrderAcrossMemberDeath(t *testing.T) {
	requireLocalRedis(t)
	proxy := startFDTestProxy(t, "127.0.0.1:6379")
	mdb := newFDTestMultiDB(t, member(proxy.addr(), 2), member(":6379", 1))
	ctx := context.Background()
	ap, err := mdb.AsyncAutoPipeline()
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	key := "mdbhd:order"
	const n = 20000
	cmds := make([]*StatusCmd, 0, n)
	for i := 0; i < n; i++ {
		if i == n/4 {
			proxy.kill()
		}
		cmds = append(cmds, ap.Set(ctx, key, i, 0))
	}
	failed := 0
	for _, c := range cmds {
		if c.Err() != nil {
			failed++
		}
	}
	direct := NewClient(&Options{Addr: ":6379"})
	defer direct.Close()
	got, err := direct.Get(ctx, key).Int()
	t.Logf("half-duplex: final=%d want=%d failed=%d err=%v", got, n-1, failed, err)
	if got != n-1 {
		t.Fatalf("final value = %d, want %d", got, n-1)
	}
}
