package redis

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

// rerouteTo returns an fdReroute that runs the commands on target and counts
// them.
func rerouteTo(target *Client, n *atomic.Int64) func(context.Context, []Cmder) error {
	return func(ctx context.Context, cmds []Cmder) error {
		n.Add(int64(len(cmds)))
		return target.processPipeline(ctx, cmds)
	}
}

// With an owner reroute, a persistent lease failure runs the accepted commands
// through the owner instead of failing them.
func TestFDLeaseFailureReroutesBacklog(t *testing.T) {
	ctx := context.Background()
	target := NewClient(&Options{Addr: ":6379"})
	defer target.Close()
	if err := target.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}

	c := NewClient(&Options{
		Addr:             "127.0.0.1:1", // nothing listening: every lease fails
		Protocol:         3,
		PipelinePoolSize: 2,
		PoolSize:         2,
		MaxRetries:       2,
		DialTimeout:      150 * time.Millisecond,
		MinRetryBackoff:  time.Millisecond,
		MaxRetryBackoff:  5 * time.Millisecond,
	})
	defer c.Close()
	var rerouted atomic.Int64
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex: true,
		fdReroute:  rerouteTo(target, &rerouted),
	})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil || ap.fd.reroute == nil {
		t.Fatal("full-duplex engine with a reroute not active")
	}

	key := "fd:reroute:lease"
	done := make(chan error, 1)
	go func() { done <- ap.Set(ctx, key, "v", 0).Err() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Set = %v, want it served through the reroute", err)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("command hung on a persistent lease failure")
	}
	if rerouted.Load() == 0 {
		t.Fatal("reroute not used")
	}
	if got, err := target.Get(ctx, key).Result(); err != nil || got != "v" {
		t.Fatalf("GET on the reroute target = %q, %v", got, err)
	}
}

// With an owner reroute, the replayable tail after a connection error runs
// through the owner, in order: no worker sees a failed or misaligned reply.
func TestFDConnErrReroutesTail(t *testing.T) {
	ctx := context.Background()
	target := NewClient(&Options{Addr: ":6379"})
	defer target.Close()
	if err := target.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	c := fdTestClient(":6379")
	defer c.Close()
	var rerouted atomic.Int64
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex: true,
		fdReroute:  rerouteTo(target, &rerouted),
	})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()

	killer := NewClient(&Options{Addr: ":6379"})
	defer killer.Close()

	const workers = 16
	var stop atomic.Bool
	done := make(chan error, workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			key := "fd:reroute:kill:" + itoa(w)
			for n := 0; !stop.Load(); n++ {
				val := itoa(w) + ":" + itoa(n)
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
			}
			done <- nil
		}(w)
	}

	time.Sleep(120 * time.Millisecond)
	for i := 0; i < 5; i++ {
		_ = killer.Do(ctx, "CLIENT", "KILL", "TYPE", "normal", "SKIPME", "yes").Err()
		time.Sleep(90 * time.Millisecond)
	}
	time.Sleep(120 * time.Millisecond)
	stop.Store(true)

	deadline := time.After(15 * time.Second)
	for w := 0; w < workers; w++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-deadline:
			t.Fatal("timeout: a worker hung after a connection kill")
		}
	}
	if rerouted.Load() == 0 {
		t.Fatal("no command went through the reroute across 5 connection kills")
	}
}

// The cluster full-duplex router gives its node children a reroute through
// the ClusterClient, and Config never hands it back.
func TestClusterFDRouterChildReroutes(t *testing.T) {
	cc := NewClusterClient(&ClusterOptions{Addrs: []string{"127.0.0.1:1"}})
	defer cc.Close()
	r := newClusterFDRouter(nil, cc, &AutoPipelineOptions{FullDuplex: true}, true)
	if r.childCfg.fdReroute == nil {
		t.Fatal("cluster FD child config has no reroute")
	}

	ap := &AutoPipeliner{config: r.childCfg}
	if ap.Config().fdReroute != nil {
		t.Fatal("Config() exposes the internal reroute")
	}
}

// Cluster full-duplex under connection kills on every master: each node child's
// replayable tail reroutes through the ClusterClient, in order, so no worker
// sees a failed or misaligned reply.
func TestClusterFDConnKillReroutesTail(t *testing.T) {
	cc := dialClusterFDTest(t)
	defer cc.Close()
	ctx := context.Background()

	ap, err := cc.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipelineWithOptions: %v", err)
	}
	defer ap.Close()
	if ap.clusterFD == nil {
		t.Fatal("cluster full-duplex router not active")
	}

	const workers = 24
	var stop atomic.Bool
	done := make(chan error, workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			key := fmt.Sprintf("cfd:reroute:{%d}", w) // spread over all masters
			for n := 0; !stop.Load(); n++ {
				val := itoa(w) + ":" + itoa(n)
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
			}
			done <- nil
		}(w)
	}

	time.Sleep(150 * time.Millisecond)
	for i := 0; i < 4; i++ {
		_ = cc.ForEachMaster(ctx, func(ctx context.Context, m *Client) error {
			return m.Do(ctx, "CLIENT", "KILL", "TYPE", "normal", "SKIPME", "yes").Err()
		})
		time.Sleep(100 * time.Millisecond)
	}
	time.Sleep(150 * time.Millisecond)
	stop.Store(true)

	deadline := time.After(20 * time.Second)
	for w := 0; w < workers; w++ {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-deadline:
			t.Fatal("timeout: a worker hung after a connection kill")
		}
	}
}
