package redis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fdTestProxy forwards TCP to target until killed: a member that can die.
type fdTestProxy struct {
	ln     net.Listener
	target string
	mu     sync.Mutex
	conns  []net.Conn
}

func startFDTestProxy(t *testing.T, target string) *fdTestProxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p := &fdTestProxy{ln: ln, target: target}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			up, err := net.Dial("tcp", target)
			if err != nil {
				_ = c.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, c, up)
			p.mu.Unlock()
			go func() { _, _ = io.Copy(up, c); _ = up.Close() }()
			go func() { _, _ = io.Copy(c, up); _ = c.Close() }()
		}
	}()
	t.Cleanup(p.kill)
	return p
}

func (p *fdTestProxy) addr() string { return p.ln.Addr().String() }

// kill stops accepting and drops every live connection.
func (p *fdTestProxy) kill() {
	_ = p.ln.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
}

func requireLocalRedis(t *testing.T) {
	t.Helper()
	c := NewClient(&Options{Addr: ":6379"})
	defer c.Close()
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Skipf("no redis on :6379: %v", err)
	}
}

func newFDTestMultiDB(t *testing.T, members ...MultiDBClientConfig) *MultiDBClient {
	t.Helper()
	return newFDTestMultiDBRetries(t, 3, members...)
}

func newFDTestMultiDBRetries(t *testing.T, retries int, members ...MultiDBClientConfig) *MultiDBClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mdb, err := NewMultiDBClient(ctx, &MultiDBOptions{
		Clients:             members,
		HealthCheckInterval: 200 * time.Millisecond,
		HealthCheckTimeout:  100 * time.Millisecond,
		CircuitBreakerConfig: &MultiDBCircuitBreakerConfig{
			FailureThreshold: 3,
			SuccessThreshold: 1,
			GracePeriod:      time.Hour,
		},
		CommandRetries:       retries,
		AutoFallbackInterval: -1,
	})
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	t.Cleanup(func() { _ = mdb.Close() })
	return mdb
}

func member(addr string, weight float64) MultiDBClientConfig {
	return MultiDBClientConfig{
		Options: &Options{Addr: addr, DialTimeout: 200 * time.Millisecond, MaxRetries: -1},
		Weight:  weight,
	}
}

// FullDuplex engages on a MultiDBClient: the router serves the active
// member's FD child, and its outcomes reach MultiDB's accounting.
func TestMultiDBFullDuplexEngages(t *testing.T) {
	requireLocalRedis(t)
	mdb := newFDTestMultiDB(t, member(":6379", 2), member(":6379", 1))
	ctx := context.Background()

	for _, async := range []bool{false, true} {
		var ap *AutoPipeliner
		var err error
		if async {
			ap, err = mdb.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
		} else {
			ap, err = mdb.AutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
		}
		if err != nil {
			t.Fatalf("async=%v: %v", async, err)
		}
		if !ap.Config().FullDuplex || ap.mdbFD == nil {
			t.Fatalf("async=%v: full duplex not engaged (Config().FullDuplex=%v)", async, ap.Config().FullDuplex)
		}
		key := fmt.Sprintf("mdbfd:engage:%v", async)
		if err := ap.Set(ctx, key, "v", 0).Err(); err != nil {
			t.Fatalf("async=%v: Set: %v", async, err)
		}
		if got, err := ap.Get(ctx, key).Result(); err != nil || got != "v" {
			t.Fatalf("async=%v: Get = %q, %v", async, got, err)
		}
		child, _ := ap.mdbFD.childFor(mdb.core.dbByID(0))
		if child == nil || child.fd == nil {
			t.Fatalf("async=%v: active member has no FD child", async)
		}
	}
	if !mdb.core.successSinceFailover.Load() {
		t.Error("FD outcomes did not reach MultiDB accounting (successSinceFailover unset)")
	}
}

// A command whose active member is not closed-admitted (here: forced open)
// leaves the fast path and runs on MultiDB's normal path, which fails over.
func TestMultiDBFullDuplexDivertsWhenNotClosed(t *testing.T) {
	requireLocalRedis(t)
	mdb := newFDTestMultiDB(t, member(":6379", 2), member(":6379", 1))
	ctx := context.Background()
	ap, err := mdb.AutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AutoPipeline: %v", err)
	}

	mdb.core.dbByID(0).cb.ForceOpen()
	if err := ap.Set(ctx, "mdbfd:divert", "v", 0).Err(); err != nil {
		t.Fatalf("Set with the active member open: %v", err)
	}
	if got := mdb.ActiveDatabaseID(); got != 1 {
		t.Fatalf("active = %d, want 1 (the diverted command fails over)", got)
	}
}

// The active member dies under full-duplex traffic: in-flight and queued
// commands reroute through MultiDB to the other member, and new traffic
// follows the failover. No command fails and no reply is misaligned.
func TestMultiDBFullDuplexSurvivesMemberDeath(t *testing.T) {
	t.Run("CommandRetries=3", func(t *testing.T) { testFDMemberDeath(t, 3) })
	// Unbounded MultiDB retries must not leave the dead member's queue waiting
	// on lease retries: the child reroutes on the first failed lease.
	t.Run("CommandRetries=MaxInt", func(t *testing.T) { testFDMemberDeath(t, math.MaxInt) })
}

func testFDMemberDeath(t *testing.T, retries int) {
	requireLocalRedis(t)
	proxy := startFDTestProxy(t, "127.0.0.1:6379")
	mdb := newFDTestMultiDBRetries(t, retries, member(proxy.addr(), 2), member(":6379", 1))
	ctx := context.Background()
	ap, err := mdb.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	if ap.mdbFD == nil {
		t.Fatal("full duplex not engaged")
	}

	const workers = 8
	var stop atomic.Bool
	var ops atomic.Int64
	done := make(chan error, workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			key := fmt.Sprintf("mdbfd:death:%d", w)
			for n := 0; !stop.Load(); n++ {
				val := fmt.Sprintf("%d:%d", w, n)
				if err := ap.Set(ctx, key, val, 0).Err(); err != nil {
					done <- fmt.Errorf("worker %d set #%d: %w", w, n, err)
					return
				}
				got, err := ap.Get(ctx, key).Result()
				if err != nil {
					done <- fmt.Errorf("worker %d get #%d: %w", w, n, err)
					return
				}
				if got != val {
					done <- fmt.Errorf("worker %d get #%d: got %q want %q", w, n, got, val)
					return
				}
				ops.Add(1)
			}
			done <- nil
		}(w)
	}

	time.Sleep(200 * time.Millisecond)
	if mdb.ActiveDatabaseID() != 0 {
		t.Fatalf("active = %d before the kill, want 0", mdb.ActiveDatabaseID())
	}
	proxy.kill()

	deadline := time.Now().Add(5 * time.Second)
	for mdb.ActiveDatabaseID() != 1 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := mdb.ActiveDatabaseID(); got != 1 {
		t.Fatalf("active = %d after the member died, want 1", got)
	}
	before := ops.Load()
	time.Sleep(300 * time.Millisecond)
	stop.Store(true)

	for w := 0; w < workers; w++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("a worker hung after the member died")
		}
	}
	if ops.Load() == before {
		t.Fatal("no progress after the failover")
	}
}

// A cluster member runs full duplex through a cluster FD child.
func TestMultiDBFullDuplexClusterMember(t *testing.T) {
	cc := NewClusterClient(&ClusterOptions{Addrs: []string{":16600", ":16601", ":16602"}})
	if err := cc.Ping(context.Background()).Err(); err != nil {
		_ = cc.Close()
		t.Skipf("no local cluster: %v", err)
	}
	_ = cc.Close()

	mdb := newFDTestMultiDB(
		t,
		MultiDBClientConfig{ClusterOptions: &ClusterOptions{Addrs: []string{":16600", ":16601", ":16602"}}, Weight: 2},
		MultiDBClientConfig{ClusterOptions: &ClusterOptions{Addrs: []string{":16600", ":16601", ":16602"}}, Weight: 1},
	)
	ctx := context.Background()
	ap, err := mdb.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	if !ap.Config().FullDuplex {
		t.Fatal("full duplex not engaged on a cluster member")
	}
	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("mdbfd:cluster:{%d}", i)
		if err := ap.Set(ctx, key, i, 0).Err(); err != nil {
			t.Fatalf("Set %s: %v", key, err)
		}
		if got, err := ap.Get(ctx, key).Int(); err != nil || got != i {
			t.Fatalf("Get %s = %d, %v", key, got, err)
		}
	}
	child, _ := ap.mdbFD.childFor(mdb.core.dbByID(0))
	if child == nil || child.clusterFD == nil {
		t.Fatal("cluster member has no cluster FD child")
	}
}

// Close drains the children and later submits are rejected.
func TestMultiDBFullDuplexClose(t *testing.T) {
	requireLocalRedis(t)
	mdb := newFDTestMultiDB(t, member(":6379", 1))
	ctx := context.Background()
	ap, err := mdb.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	cmds := make([]*StatusCmd, 100)
	for i := range cmds {
		cmds[i] = ap.Set(ctx, fmt.Sprintf("mdbfd:close:%d", i), i, 0)
	}
	if err := ap.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	for i, c := range cmds {
		if err := c.Err(); err != nil {
			t.Fatalf("accepted command %d failed on Close: %v", i, err)
		}
	}
	if err := ap.Set(ctx, "mdbfd:close:after", 1, 0).Err(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Set after Close = %v, want ErrClosed", err)
	}
}
