package redis

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// clusterFDTestAddrs is the local 3-master cluster the functional test targets.
// Override with GOREDIS_CLUSTER_FD_ADDRS (comma-separated) to point at another
// cluster (e.g. a real RE endpoint). The test self-skips when the cluster is not
// reachable, so it is safe to run in any environment.
var clusterFDTestAddrs = clusterFDTestAddrsFromEnv()

func clusterFDTestAddrsFromEnv() []string {
	if v := os.Getenv("GOREDIS_CLUSTER_FD_ADDRS"); v != "" {
		return strings.Split(v, ",")
	}
	return []string{"127.0.0.1:16600", "127.0.0.1:16601", "127.0.0.1:16602"}
}

func dialClusterFDTest(t *testing.T) *ClusterClient {
	t.Helper()
	cc := NewClusterClient(&ClusterOptions{Addrs: clusterFDTestAddrs})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := cc.Ping(ctx).Err(); err != nil {
		cc.Close()
		t.Skipf("local cluster not reachable at %v: %v", clusterFDTestAddrs, err)
	}
	// Force a topology load so state.Masters is populated for the assertions.
	if _, err := cc.state.ReloadOrGet(ctx); err != nil {
		cc.Close()
		t.Skipf("cluster state not loadable: %v", err)
	}
	return cc
}

// TestClusterFullDuplexEngagesPerNode is the correctness demo for native
// full-duplex on a ClusterClient: FD is reported active, one FD child engine
// runs per master, a mixed workload across all slots executes with zero errors,
// diverted commands (fan-out, blocking) still work, and Close returns cleanly.
func TestClusterFullDuplexEngagesPerNode(t *testing.T) {
	cc := dialClusterFDTest(t)
	defer cc.Close()

	ctx := context.Background()

	ap, err := cc.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipelineWithOptions: %v", err)
	}
	defer ap.Close()

	// (a) Config() reports FD active — the exact bit that was always false on a
	// ClusterClient before this change.
	if !ap.Config().FullDuplex {
		t.Fatalf("Config().FullDuplex = false, want true (cluster FD should be active)")
	}
	if ap.clusterFD == nil {
		t.Fatalf("ap.clusterFD is nil, want a router")
	}
	if got := ap.Config().NumShards; got != 1 {
		t.Fatalf("Config().NumShards = %d, want 1 (cluster FD forces a single flusherless shard)", got)
	}

	// (c) Mixed GET/SET across many keys → keys spread over all three masters'
	// slot ranges. Fire concurrently so the FD engines actually batch. Zero errors
	// and correct read-back proves per-node routing lands each key on its owner.
	const nKeys = 300
	var wg sync.WaitGroup
	errCh := make(chan error, nKeys)
	for i := 0; i < nKeys; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("cfd:{%d}:k", i)
			val := fmt.Sprintf("v%d", i)
			if err := ap.Set(ctx, key, val, 0).Err(); err != nil {
				errCh <- fmt.Errorf("SET %s: %w", key, err)
				return
			}
			got, err := ap.Get(ctx, key).Result()
			if err != nil {
				errCh <- fmt.Errorf("GET %s: %w", key, err)
				return
			}
			if got != val {
				errCh <- fmt.Errorf("GET %s = %q, want %q", key, got, val)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("workload error: %v", err)
	}

	// (b) One FD child engine per master, and every child actually engaged FD.
	state, err := cc.state.Get(ctx)
	if err != nil {
		t.Fatalf("state.Get: %v", err)
	}
	nMasters := len(state.Masters)
	ap.clusterFD.mu.RLock()
	nChildren := len(ap.clusterFD.children)
	var notFD, badBudget int
	for _, ch := range ap.clusterFD.children {
		if ch.fd == nil {
			notFD++
			continue
		}
		// Recovery budget must be the cluster's MaxRedirects, not the node client's
		// MaxRetries (-1) — otherwise carry-replay is disabled on this master.
		if ch.fd.retryBudget() != cc.opt.MaxRedirects {
			badBudget++
		}
	}
	ap.clusterFD.mu.RUnlock()
	if nChildren != nMasters {
		t.Errorf("clusterFD has %d children, want %d (one per master)", nChildren, nMasters)
	}
	if notFD != 0 {
		t.Errorf("%d/%d children did not engage FD (child.fd == nil)", notFD, nChildren)
	}
	if badBudget != 0 {
		t.Errorf("%d/%d children have retryBudget != cc.MaxRedirects (%d) — carry-replay would be misbudgeted",
			badBudget, nChildren, cc.opt.MaxRedirects)
	}
	t.Logf("cluster FD: %d masters, %d FD children engaged", nMasters, nChildren-notFD)

	// (d) Diversion still works: a fan-out command (DBSIZE) and a blocking command
	// (BLPOP with a short timeout) must not ride the FD pipe.
	if err := cc.DBSize(ctx).Err(); err != nil {
		t.Errorf("DBSize (fan-out) failed: %v", err)
	}
	blKey := "cfd:{bl}:list"
	if _, err := ap.BLPop(ctx, 100*time.Millisecond, blKey).Result(); err != nil && err != Nil {
		t.Errorf("BLPop (diverted) unexpected error: %v", err)
	}

	// (e) Close returns without hanging.
	done := make(chan error, 1)
	go func() { done <- ap.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("ap.Close returned error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("ap.Close did not return within 10s")
	}
}

// TestClusterFullDuplexReuseAfterClose covers the lifecycle the single-AP test
// cannot: closing the cluster autopipeliner closes the per-node FD children,
// which stay cached on each node.Client. A fresh autopipeliner on the SAME
// (still-open) ClusterClient must NOT hand back a closed child and fail every
// command with ErrClosed — the node getter rebuilds a live child and the router
// re-engages FD.
func TestClusterFullDuplexReuseAfterClose(t *testing.T) {
	cc := dialClusterFDTest(t)
	defer cc.Close()
	ctx := context.Background()

	ap1, err := cc.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("first AsyncAutoPipelineWithOptions: %v", err)
	}
	if err := ap1.Set(ctx, "cfd:{reuse}:k", "1", 0).Err(); err != nil {
		t.Fatalf("first SET: %v", err)
	}
	if err := ap1.Close(); err != nil {
		t.Fatalf("ap1.Close: %v", err)
	}

	// getOrCreateAutoPipeliner rebuilds on a closed cached instance, so this
	// returns a NEW parent (the ClusterClient itself was never closed).
	ap2, err := cc.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("second AsyncAutoPipelineWithOptions: %v", err)
	}
	defer ap2.Close()
	if ap2 == ap1 {
		t.Fatalf("expected a fresh autopipeliner after Close, got the closed one")
	}
	if !ap2.Config().FullDuplex {
		t.Fatalf("reused AP Config().FullDuplex = false, want true")
	}

	// The command that would have failed ErrClosed if a closed child were cached.
	got, err := func() (string, error) {
		if err := ap2.Set(ctx, "cfd:{reuse}:k", "2", 0).Err(); err != nil {
			return "", err
		}
		return ap2.Get(ctx, "cfd:{reuse}:k").Result()
	}()
	if err != nil {
		t.Fatalf("SET/GET on reused AP: %v", err)
	}
	if got != "2" {
		t.Fatalf("GET after reuse = %q, want %q", got, "2")
	}

	// FD re-engaged: a fresh live child, not the closed one.
	ap2.clusterFD.mu.RLock()
	var closedChildren int
	for _, ch := range ap2.clusterFD.children {
		if ch.fd == nil || ch.IsClosed() {
			closedChildren++
		}
	}
	nChildren := len(ap2.clusterFD.children)
	ap2.clusterFD.mu.RUnlock()
	if nChildren == 0 {
		t.Fatalf("reused AP has no FD children after traffic")
	}
	if closedChildren != 0 {
		t.Errorf("reused AP cached %d closed/non-FD children, want 0", closedChildren)
	}
}

// TestClusterFullDuplexChildCloseSelfHeals covers the case ReuseAfterClose does
// not: the ROUTER's parent AutoPipeliner stays open, but ONE node child gets
// closed underneath it — the shape of a cluster topology GC closing a node's
// client (and its cached FD child) while the rest of the cluster, and this
// router, are still very much in use. A command on that node must self-heal
// (rebuild the child, or divert to Process for a node that is truly gone)
// rather than surface the closed child's ErrClosed to the caller.
//
// This pins the observable CONTRACT, not the specific race a bot review
// flagged. Closing the child here happens strictly BEFORE the next submit, so
// getOrCreateChild's own pre-existing cached-but-closed rebuild
// (autopipeline_cluster_fd.go childFor) already satisfies this test on its
// own — the harder case (a submit that is already parked inside
// fdEngine.submit's backpressure select when the child closes, per
// autopipeline_fullduplex.go) is a true TOCTOU race with no deterministic
// repro; clusterFDRouter.submit's post-dispatch retry (same file) narrows
// that window but is not exercised as a distinct case here.
func TestClusterFullDuplexChildCloseSelfHeals(t *testing.T) {
	cc := dialClusterFDTest(t)
	defer cc.Close()
	ctx := context.Background()

	ap, err := cc.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipelineWithOptions: %v", err)
	}
	defer ap.Close()

	const key = "cfd:{childclose}:k"
	if err := ap.Set(ctx, key, "1", 0).Err(); err != nil {
		t.Fatalf("initial SET: %v", err)
	}

	// Grab the child that owns key's slot and close it DIRECTLY — simulating
	// topology GC tearing down a node child without touching the ClusterClient
	// or the router (unlike ReuseAfterClose, which closes the whole parent).
	ap.clusterFD.mu.RLock()
	var closedChild *AutoPipeliner
	for _, ch := range ap.clusterFD.children {
		closedChild = ch
		break
	}
	ap.clusterFD.mu.RUnlock()
	if closedChild == nil {
		t.Fatalf("no FD child cached after initial SET")
	}
	if err := closedChild.Close(); err != nil {
		t.Fatalf("closedChild.Close: %v", err)
	}

	// A command on the SAME key must still succeed — self-healed, not ErrClosed.
	if err := ap.Set(ctx, key, "2", 0).Err(); err != nil {
		t.Fatalf("SET after child close: %v (want self-heal, not ErrClosed)", err)
	}
	got, err := ap.Get(ctx, key).Result()
	if err != nil {
		t.Fatalf("GET after child close: %v", err)
	}
	if got != "2" {
		t.Fatalf("GET after child close = %q, want %q", got, "2")
	}

	// The router now caches a fresh, live child for that node — not the closed one.
	ap.clusterFD.mu.RLock()
	var sawFresh bool
	for _, ch := range ap.clusterFD.children {
		if ch != closedChild && !ch.IsClosed() {
			sawFresh = true
		}
	}
	ap.clusterFD.mu.RUnlock()
	if !sawFresh {
		t.Errorf("router did not rebuild a fresh child after the cached one closed")
	}
}

// nodeClientFor reports the *Client that owns key's slot in the current
// topology, without creating or touching any FD child (unlike childFor).
// Used by eviction tests to find a key that routes to a node different from
// one already cached.
func nodeClientFor(t *testing.T, ctx context.Context, r *clusterFDRouter, key string) (*Client, bool) {
	t.Helper()
	slot := r.cc.cmdSlot(NewStatusCmd(ctx, "get", key), -1)
	if slot < 0 {
		return nil, false
	}
	state, err := r.cc.state.Get(ctx)
	if err != nil {
		return nil, false
	}
	node, err := state.slotMasterNode(slot)
	if err != nil || node == nil {
		return nil, false
	}
	return node.Client, true
}

// TestClusterFullDuplexEvictsStaleChildOnRebuild pins that a closed node
// child for a DIFFERENT node than the one currently being (re)built gets
// swept out of clusterFDRouter.children. Topology GC can close a node client
// (and its cached child) while the router stays up; once the cluster stops
// routing to that *Client, getOrCreateChild is never called with it again, so
// nothing else would notice it went stale — without the sweep it (and its FD
// engine, submit channel, held connection) would sit retained in the map for
// the router's whole life (cursor bugbot on #4002).
func TestClusterFullDuplexEvictsStaleChildOnRebuild(t *testing.T) {
	cc := dialClusterFDTest(t)
	defer cc.Close()
	ctx := context.Background()

	ap, err := cc.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipelineWithOptions: %v", err)
	}
	defer ap.Close()

	// Seed one key to populate its node's child, then hunt for a second tag
	// that lands on a DIFFERENT node — a first-time build for that second
	// node is what forces getOrCreateChild's write-lock (sweep) path without
	// needing to touch the second node's own cache state.
	const seedKey = "cfd:{evictseed}:k"
	if err := ap.Set(ctx, seedKey, "1", 0).Err(); err != nil {
		t.Fatalf("seed SET: %v", err)
	}
	ap.clusterFD.mu.RLock()
	var staleClient *Client
	var staleChild *AutoPipeliner
	for c, ch := range ap.clusterFD.children {
		staleClient, staleChild = c, ch
	}
	ap.clusterFD.mu.RUnlock()
	if staleClient == nil {
		t.Fatal("no FD child cached after seed SET")
	}

	var secondKey string
	for i := 0; i < 50; i++ {
		k := fmt.Sprintf("cfd:{evicttarget%d}:k", i)
		nc, ok := nodeClientFor(t, ctx, ap.clusterFD, k)
		if ok && nc != staleClient {
			secondKey = k
			break
		}
	}
	if secondKey == "" {
		t.Skip("could not find a tag routing to a different master after 50 tries")
	}

	// Simulate topology GC: close the seed node's child directly, but leave
	// its (now stale) entry in the map exactly as a real close hook would.
	if err := staleChild.Close(); err != nil {
		t.Fatalf("staleChild.Close: %v", err)
	}

	// First-time build for the second node: write-lock path, sweep runs.
	if err := ap.Set(ctx, secondKey, "1", 0).Err(); err != nil {
		t.Fatalf("SET on second node: %v", err)
	}

	ap.clusterFD.mu.RLock()
	_, stillPresent := ap.clusterFD.children[staleClient]
	n := len(ap.clusterFD.children)
	ap.clusterFD.mu.RUnlock()
	if stillPresent {
		t.Error("stale closed child was not evicted from clusterFDRouter.children")
	}
	if n != 1 {
		t.Errorf("clusterFDRouter.children has %d entries, want 1 (only the second node's fresh child)", n)
	}
}

// TestClusterFullDuplexEvictsChildOnNodeClose pins that a node's entry is
// pruned from clusterFDRouter.children the moment the node's OWN *Client
// closes — with NO further submit or rebuild needed. The sweep in
// getOrCreateChild only runs when some OTHER node's cache miss takes the
// write-lock path; a cluster whose live traffic only ever hits
// already-cached nodes would never trigger it, so a node removed by
// topology GC would sit retained forever (cursor bugbot on #4002, a gap in
// the sweep-only fix).
func TestClusterFullDuplexEvictsChildOnNodeClose(t *testing.T) {
	cc := dialClusterFDTest(t)
	defer cc.Close()
	ctx := context.Background()

	ap, err := cc.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipelineWithOptions: %v", err)
	}
	defer ap.Close()

	if err := ap.Set(ctx, "cfd:{nodeclose}:k", "1", 0).Err(); err != nil {
		t.Fatalf("seed SET: %v", err)
	}

	ap.clusterFD.mu.RLock()
	var nc *Client
	for c := range ap.clusterFD.children {
		nc = c
	}
	ap.clusterFD.mu.RUnlock()
	if nc == nil {
		t.Fatal("no FD child cached after seed SET")
	}

	// Close the NODE CLIENT itself (not its cached child) — this is exactly
	// what clusterNodes.GC does to a removed node (osscluster.go).
	if err := nc.Close(); err != nil {
		t.Fatalf("nc.Close: %v", err)
	}

	ap.clusterFD.mu.RLock()
	_, stillPresent := ap.clusterFD.children[nc]
	ap.clusterFD.mu.RUnlock()
	if stillPresent {
		t.Error("node client's entry was not evicted immediately on its own Close")
	}
}

// TestClusterFullDuplexGatedOffForReplicaRouting asserts the construction gate:
// a ClusterClient configured for replica routing (ReadOnly / RouteByLatency /
// RouteRandomly) must NOT engage cluster FD, because the router only routes to
// the slot master (slotMasterNode) and would silently ignore those options,
// pinning reads to masters. It must fall back to the half-duplex flushers (which
// honor the ShardPicker) and report Config().FullDuplex == false — the honest
// effective state. Pure construction check: no live cluster required (the gate
// reads cc.opt synchronously, and the half-duplex AP dials nothing until a
// command is submitted).
// TestClusterFDRouterCloseUnregistersEvictHooks pins that closing a cluster FD
// router detaches the per-node evict hooks getOrCreateChild registered on each
// node client's onClose registry — for every node it ever registered on, not
// only those still in children (the stale-child sweep and the hook itself drop
// children entries while the registration stays live). Left registered, each
// hook kept the closed router, and through parent the AutoPipeliner and its
// clusterReprocess closure, reachable until the node client itself closed,
// accumulating one per autopipeliner create/close cycle per node (codex +
// cursor bugbot on #4002). No server: the registry and router state are driven
// directly, the way getOrCreateChild's publish step leaves them.
func TestClusterFDRouterCloseUnregistersEvictHooks(t *testing.T) {
	nodeA := NewClient(&Options{Addr: "localhost:1"}) // never dialed
	nodeB := NewClient(&Options{Addr: "localhost:1"})
	t.Cleanup(func() { _ = nodeA.Close(); _ = nodeB.Close() })

	r := &clusterFDRouter{children: make(map[*Client]*AutoPipeliner)}
	for _, nc := range []*Client{nodeA, nodeB} {
		nc := nc
		if !nc.onClose.register(clusterFDRouterEvictID(r, nc), func() error {
			r.mu.Lock()
			delete(r.children, nc)
			delete(r.evictHooks, nc)
			r.mu.Unlock()
			return nil
		}) {
			t.Fatal("register on a live node client must succeed")
		}
		if r.evictHooks == nil {
			r.evictHooks = make(map[*Client]struct{})
		}
		r.evictHooks[nc] = struct{}{}
	}
	// Neither node has a children entry (as after the sweep dropped them); the
	// registrations must be detached regardless.
	hasHook := func(nc *Client) bool {
		nc.onClose.mu.Lock()
		defer nc.onClose.mu.Unlock()
		_, ok := nc.onClose.hooks[clusterFDRouterEvictID(r, nc)]
		return ok
	}
	if !hasHook(nodeA) || !hasHook(nodeB) {
		t.Fatal("precondition: both node clients must hold this router's evict hook")
	}

	if err := r.close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	if hasHook(nodeA) || hasHook(nodeB) {
		t.Fatal("close must unregister the router's evict hook from every node client it registered on")
	}
	r.mu.Lock()
	tracked := len(r.evictHooks)
	r.mu.Unlock()
	if tracked != 0 {
		t.Fatalf("evictHooks still tracks %d node clients after close", tracked)
	}
}

func TestClusterFullDuplexGatedOffForReplicaRouting(t *testing.T) {
	addrs := []string{"127.0.0.1:16600"}
	cases := []struct {
		name string
		opt  *ClusterOptions
	}{
		{"ReadOnly", &ClusterOptions{Addrs: addrs, ReadOnly: true}},
		{"RouteByLatency", &ClusterOptions{Addrs: addrs, RouteByLatency: true}},
		{"RouteRandomly", &ClusterOptions{Addrs: addrs, RouteRandomly: true}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cc := NewClusterClient(tc.opt)
			defer cc.Close()
			ap, err := cc.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
			if err != nil {
				t.Fatalf("AsyncAutoPipelineWithOptions: %v", err)
			}
			defer ap.Close()
			if ap.Config().FullDuplex {
				t.Errorf("Config().FullDuplex = true, want false (replica routing must disable cluster FD)")
			}
			if ap.clusterFD != nil {
				t.Errorf("ap.clusterFD is non-nil, want nil (cluster FD must not engage under replica routing)")
			}
		})
	}
}

// TestClusterFullDuplexResolvesDefaultsOnParent asserts the effective-defaults
// contract: a default-constructed cluster-FD autopipeliner must report the
// resolved FD tuning defaults from Config(), not the raw zeros the user passed —
// the same guarantee the standalone FD path gives. The per-node children resolve
// these in newFDEngine; the parent must resolve them too so Config() is honest.
// Pure construction check: cluster FD engages on PipelinePoolSize>=0 (the default)
// without loading topology, so no live cluster is required.
func TestClusterFullDuplexResolvesDefaultsOnParent(t *testing.T) {
	cc := NewClusterClient(&ClusterOptions{Addrs: []string{"127.0.0.1:16600"}})
	defer cc.Close()

	ap, err := cc.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipelineWithOptions: %v", err)
	}
	defer ap.Close()

	cfg := ap.Config()
	if !cfg.FullDuplex || ap.clusterFD == nil {
		t.Fatalf("cluster FD did not engage (FullDuplex=%v, clusterFD==nil:%v); cannot check defaults",
			cfg.FullDuplex, ap.clusterFD == nil)
	}
	if cfg.FullDuplexWindow != fdDefaultWindow {
		t.Errorf("Config().FullDuplexWindow = %d, want %d (effective default)", cfg.FullDuplexWindow, fdDefaultWindow)
	}
	if cfg.FullDuplexIdleTimeout != fdDefaultIdle {
		t.Errorf("Config().FullDuplexIdleTimeout = %v, want %v (effective default)", cfg.FullDuplexIdleTimeout, fdDefaultIdle)
	}
	if cfg.FullDuplexMaxHold != fdDefaultMaxHold {
		t.Errorf("Config().FullDuplexMaxHold = %v, want %v (effective default)", cfg.FullDuplexMaxHold, fdDefaultMaxHold)
	}
}

// TestClusterFullDuplexRecoveryBudgetFromMaxRedirects asserts the connection-
// failure recovery budget wiring: a cluster node.Client normalizes MaxRetries to
// -1 (cluster retries live in MaxRedirects), which the FD carry-replay would read
// as "budget already spent" and fail every in-flight command on the first socket
// error. The router must seed each child with the ClusterClient's MaxRedirects
// instead. Pure construction check: the router is built at construction, so no
// live cluster is needed to verify the plumbed budget.
func TestClusterFullDuplexRecoveryBudgetFromMaxRedirects(t *testing.T) {
	cc := NewClusterClient(&ClusterOptions{Addrs: []string{"127.0.0.1:16600"}, MaxRedirects: 5})
	defer cc.Close()

	ap, err := cc.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipelineWithOptions: %v", err)
	}
	defer ap.Close()
	if ap.clusterFD == nil {
		t.Fatalf("cluster FD did not engage; cannot check the recovery budget")
	}
	if got := ap.clusterFD.childCfg.clusterRetryBudget; got != 5 {
		t.Errorf("child clusterRetryBudget = %d, want 5 (cc.opt.MaxRedirects); the node client's MaxRetries=-1 would disable carry-replay", got)
	}
}
