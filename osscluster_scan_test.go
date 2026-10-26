package redis

import (
	"context"
	"fmt"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

const clusterScanKeys = 600

// clusterScanFill writes clusterScanKeys string keys under a fresh prefix, checks
// that they span several masters, and deletes them when the test ends.
func clusterScanFill(t *testing.T, cc *ClusterClient, name string) (prefix string, want map[string]bool) {
	t.Helper()
	ctx := context.Background()
	prefix = fmt.Sprintf("scantest:%s:%s", name, strconv.FormatInt(time.Now().UnixNano(), 36))
	want = make(map[string]bool, clusterScanKeys)
	for i := 0; i < clusterScanKeys; i++ {
		k := fmt.Sprintf("%s:%d", prefix, i)
		if err := cc.Set(ctx, k, i, time.Minute).Err(); err != nil {
			t.Fatalf("SET %s: %v", k, err)
		}
		want[k] = true
	}
	t.Cleanup(func() {
		for k := range want {
			cc.Del(ctx, k)
		}
	})
	owners := map[string]bool{}
	for k := range want {
		owners[cc.cmdNode(ctx, k)] = true
	}
	if len(owners) < 2 {
		t.Fatalf("keys on %d master(s), want several", len(owners))
	}
	return prefix, want
}

// cmdNode is the address of the master that owns key (test helper).
func (c *ClusterClient) cmdNode(ctx context.Context, key string) string {
	n, err := c.MasterForKey(ctx, key)
	if err != nil {
		return ""
	}
	return n.Options().Addr
}

func clusterScanCollect(t *testing.T, it *ScanIterator) map[string]int {
	t.Helper()
	ctx := context.Background()
	got := map[string]int{}
	for it.Next(ctx) {
		got[it.Val()]++
		if len(got) > 2*clusterScanKeys {
			t.Fatal("the scan does not end")
		}
	}
	if err := it.Err(); err != nil {
		t.Fatalf("iterator: %v", err)
	}
	return got
}

func clusterScanExpectEvery(t *testing.T, got map[string]int, want map[string]bool) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("scan returned %d keys, want %d", len(got), len(want))
	}
	for k, n := range got {
		if !want[k] {
			t.Fatalf("scan returned unknown key %q", k)
		}
		if n != 1 {
			t.Fatalf("scan returned %q %d times", k, n)
		}
	}
}

// A SCAN iterator on a ClusterClient returns every key of every master once.
// Before the fix it sent each page to a random node with the previous node's
// cursor and stopped early without an error.
func TestClusterScanIteratorCoversEveryMaster(t *testing.T) {
	cc := dialClusterFDTest(t)
	t.Cleanup(func() { _ = cc.Close() })
	prefix, want := clusterScanFill(t, cc, "next")
	clusterScanExpectEvery(t, clusterScanCollect(t, cc.Scan(context.Background(), 0, prefix+":*", 50).Iterator()), want)
}

func TestClusterScanVals(t *testing.T) {
	cc := dialClusterFDTest(t)
	t.Cleanup(func() { _ = cc.Close() })
	ctx := context.Background()
	prefix, want := clusterScanFill(t, cc, "vals")
	got := map[string]int{}
	for k := range cc.Scan(ctx, 0, prefix+":*", 50).Iterator().Vals(ctx) {
		got[k]++
	}
	clusterScanExpectEvery(t, got, want)
}

func TestClusterScanTypeIterator(t *testing.T) {
	cc := dialClusterFDTest(t)
	t.Cleanup(func() { _ = cc.Close() })
	ctx := context.Background()
	prefix, _ := clusterScanFill(t, cc, "type")
	lists := map[string]bool{}
	for i := 0; i < clusterScanKeys; i++ {
		k := fmt.Sprintf("%s-list:%d", prefix, i)
		if err := cc.RPush(ctx, k, "x").Err(); err != nil {
			t.Fatal(err)
		}
		lists[k] = true
	}
	t.Cleanup(func() {
		for k := range lists {
			cc.Del(ctx, k)
		}
	})
	clusterScanExpectEvery(t, clusterScanCollect(t, cc.ScanType(ctx, 0, prefix+"*", 50, "list").Iterator()), lists)
}

// Every page runs inside the ClusterClient's process hooks.
func TestClusterScanRunsClusterHooksPerPage(t *testing.T) {
	cc := dialClusterFDTest(t)
	t.Cleanup(func() { _ = cc.Close() })
	ctx := context.Background()
	prefix, _ := clusterScanFill(t, cc, "hook")
	var pages atomic.Int64
	cc.AddHook(scanCountHook{&pages})
	state, err := cc.LoadState(ctx)
	if err != nil {
		t.Fatal(err)
	}
	clusterScanCollect(t, cc.Scan(ctx, 0, prefix+":*", 50).Iterator())
	if p := pages.Load(); p < int64(len(state.Masters)) {
		t.Fatalf("cluster hook saw %d SCAN pages, want at least one per master (%d)", p, len(state.Masters))
	}
}

type scanCountHook struct{ n *atomic.Int64 }

func (h scanCountHook) DialHook(next DialHook) DialHook { return next }
func (h scanCountHook) ProcessHook(next ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error {
		if cmd.Name() == "scan" {
			h.n.Add(1)
		}
		return next(ctx, cmd)
	}
}
func (h scanCountHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}

// Scan on a full-duplex autopipeliner over a ClusterClient covers every master.
func TestClusterScanFromFullDuplexAutoPipeliner(t *testing.T) {
	cc := dialClusterFDTest(t)
	t.Cleanup(func() { _ = cc.Close() })
	prefix, want := clusterScanFill(t, cc, "ap")
	ap, err := cc.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()
	clusterScanExpectEvery(t, clusterScanCollect(t, ap.Scan(context.Background(), 0, prefix+":*", 50).Iterator()), want)

	ap.Close()
	if err := ap.Scan(context.Background(), 0, prefix+":*", 50).Err(); err != ErrClosed {
		t.Fatalf("Scan after Close: err = %v, want ErrClosed", err)
	}
}

// A MATCH pattern with a hash tag still goes to the node that owns the slot.
func TestClusterScanHashTagMatch(t *testing.T) {
	cc := dialClusterFDTest(t)
	t.Cleanup(func() { _ = cc.Close() })
	ctx := context.Background()
	clusterScanFill(t, cc, "other")
	tag := fmt.Sprintf("{scantag-%d}", time.Now().UnixNano())
	want := map[string]bool{}
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("%s:%d", tag, i)
		if err := cc.Set(ctx, k, i, time.Minute).Err(); err != nil {
			t.Fatal(err)
		}
		want[k] = true
	}
	clusterScanExpectEvery(t, clusterScanCollect(t, cc.Scan(ctx, 0, tag+":*", 10).Iterator()), want)
}
