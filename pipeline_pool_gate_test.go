package redis

import (
	"context"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// gateTestAddr mirrors autopipeline_test.go's apTestAddr, which lives in the
// external test package (redis_test) and is unreachable from here.
func gateTestAddr() string {
	if p := os.Getenv("REDIS_PORT"); p != "" {
		return ":" + p
	}
	return ":6379"
}

// TestPipelinePoolAlwaysCreated pins the creation rule: the dedicated pipeline
// pool is built unconditionally at NewClient — like the pubsub pool — because
// it is pure burst capacity (no pre-dialing, small cap, larger buffers) and an
// unused one holds zero connections. Pipelines therefore never compete with
// regular commands for main-pool connections by default. A negative
// PipelinePoolSize is the explicit opt-out and restores the old
// pipelines-on-the-main-pool behavior.
func TestPipelinePoolAlwaysCreated(t *testing.T) {
	cases := []struct {
		name string
		opt  *Options
		want bool
	}{
		{
			name: "plain client gets the pool by default",
			opt:  &Options{Addr: "127.0.0.1:0"},
			want: true,
		},
		{
			name: "explicit size gets the pool",
			opt:  &Options{Addr: "127.0.0.1:0", PipelinePoolSize: 8},
			want: true,
		},
		{
			name: "buffer sizes get the pool",
			opt:  &Options{Addr: "127.0.0.1:0", PipelineReadBufferSize: 64 * 1024},
			want: true,
		},
		{
			name: "negative size opts out",
			opt:  &Options{Addr: "127.0.0.1:0", PipelinePoolSize: -1},
			want: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := NewClient(tc.opt)
			defer c.Close()
			if got := c.baseClient.loadPipelinePool() != nil; got != tc.want {
				t.Fatalf("pipelinePool present = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestPipelinePoolSharedByClones: WithTimeout clones share the parent's pools;
// the pipeline pool must be the SAME pool, not a second one over the same
// server, or the clone pair would hold up to 2x the pipeline connections.
func TestPipelinePoolSharedByClones(t *testing.T) {
	c := NewClient(&Options{Addr: "127.0.0.1:0"})
	defer c.Close()
	clone := c.WithTimeout(0)
	if p, q := c.baseClient.loadPipelinePool(), clone.baseClient.loadPipelinePool(); p == nil || p != q {
		t.Fatalf("clone pipeline pool %p != parent %p", q, p)
	}
}

// TestPipelinePoolOptionsResolution pins the rules the dedicated pool is built
// with. The three that matter:
//
//   - buffers default to DefaultPipelineBufferSize (not the regular 32 KiB) —
//     pipeline connections move whole batches per round trip — but never
//     SHRINK a larger explicitly-configured regular buffer;
//   - MinIdleConns is forced to 0: the pool is burst capacity, and inheriting
//     the main pool's MinIdleConns would pre-dial that many pipeline
//     connections at creation, silently doubling the client's idle footprint;
//   - the regular buffers and pool size of the MAIN pool are never touched.
func TestPipelinePoolOptionsResolution(t *testing.T) {
	t.Run("buffers default to DefaultPipelineBufferSize", func(t *testing.T) {
		opt := &Options{Addr: "127.0.0.1:0", PipelinePoolSize: 4}
		opt.init()
		po := pipelinePoolOptions(opt)
		if po.ReadBufferSize != DefaultPipelineBufferSize || po.WriteBufferSize != DefaultPipelineBufferSize {
			t.Fatalf("pipeline buffers = %d/%d, want %d each",
				po.ReadBufferSize, po.WriteBufferSize, DefaultPipelineBufferSize)
		}
		if po.PoolSize != 4 {
			t.Fatalf("pipeline PoolSize = %d, want 4", po.PoolSize)
		}
	})

	t.Run("larger regular buffers are kept, not shrunk", func(t *testing.T) {
		opt := &Options{Addr: "127.0.0.1:0", PipelinePoolSize: 4,
			ReadBufferSize: 128 * 1024, WriteBufferSize: 128 * 1024}
		opt.init()
		po := pipelinePoolOptions(opt)
		if po.ReadBufferSize != 128*1024 || po.WriteBufferSize != 128*1024 {
			t.Fatalf("pipeline buffers = %d/%d, want 131072 each (never shrink)",
				po.ReadBufferSize, po.WriteBufferSize)
		}
	})

	t.Run("explicit pipeline buffers always win", func(t *testing.T) {
		opt := &Options{Addr: "127.0.0.1:0",
			PipelineReadBufferSize: 96 * 1024, PipelineWriteBufferSize: 96 * 1024}
		opt.init()
		po := pipelinePoolOptions(opt)
		if po.ReadBufferSize != 96*1024 || po.WriteBufferSize != 96*1024 {
			t.Fatalf("pipeline buffers = %d/%d, want 98304 each",
				po.ReadBufferSize, po.WriteBufferSize)
		}
	})

	t.Run("MinIdleConns is never inherited", func(t *testing.T) {
		opt := &Options{Addr: "127.0.0.1:0", PipelinePoolSize: 4, MinIdleConns: 8}
		opt.init()
		if po := pipelinePoolOptions(opt); po.MinIdleConns != 0 {
			t.Fatalf("pipeline MinIdleConns = %d, want 0: the pipeline pool is burst "+
				"capacity and must not pre-dial", po.MinIdleConns)
		}
	})

	t.Run("main pool options are untouched", func(t *testing.T) {
		opt := &Options{Addr: "127.0.0.1:0", PipelinePoolSize: 4, MinIdleConns: 8}
		opt.init()
		_ = pipelinePoolOptions(opt)
		if opt.ReadBufferSize != 32*1024 || opt.MinIdleConns != 8 {
			t.Fatalf("main options mutated: ReadBufferSize=%d MinIdleConns=%d",
				opt.ReadBufferSize, opt.MinIdleConns)
		}
	})
}

// TestClusterPipelinePoolSizePropagates: node clients are built from
// clientOptions, which must hand PipelinePoolSize through verbatim — 0 means
// each node client creates the default pool (the always-on rule applies at
// NewClient), and the negative opt-out must survive the copy.
func TestClusterPipelinePoolSizePropagates(t *testing.T) {
	for _, tc := range []struct{ in, want int }{{0, 0}, {8, 8}, {-1, -1}} {
		co := &ClusterOptions{Addrs: []string{"127.0.0.1:0"}, PipelinePoolSize: tc.in}
		co.init()
		if got := co.clientOptions().PipelinePoolSize; got != tc.want {
			t.Fatalf("PipelinePoolSize %d propagated as %d, want %d", tc.in, got, tc.want)
		}
	}
}

// TestFailoverPipelinePoolCreated: NewFailoverClient builds its pools in its
// own constructor (it duplicated — and drifted from — NewClient's creation
// logic once before), so pin the always-on rule and the opt-out there too.
func TestFailoverPipelinePoolCreated(t *testing.T) {
	c := NewFailoverClient(&FailoverOptions{
		MasterName:    "mymaster",
		SentinelAddrs: []string{"127.0.0.1:0"},
	})
	defer c.Close()
	if c.baseClient.loadPipelinePool() == nil {
		t.Fatal("failover client must create the pipeline pool by default")
	}

	optOut := NewFailoverClient(&FailoverOptions{
		MasterName:       "mymaster",
		SentinelAddrs:    []string{"127.0.0.1:0"},
		PipelinePoolSize: -1,
	})
	defer optOut.Close()
	if optOut.baseClient.loadPipelinePool() != nil {
		t.Fatal("PipelinePoolSize < 0 must opt the failover client out")
	}
}

// TestPipelinePoolSpillsToMainPool: the pipeline pool is a small burst-capacity
// pool; a burst of concurrent pipelines wider than its cap must SPILL to the
// main pool instead of queueing behind PoolTimeout. Without the spill, capping
// the pool at PipelinePoolSize would be a silent concurrency regression for
// heavy Pipelined callers, who shared the (much larger) main pool before the
// dedicated pool existed.
func TestPipelinePoolSpillsToMainPool(t *testing.T) {
	ctx := context.Background()
	c := NewClient(&Options{
		Addr:             gateTestAddr(),
		PipelinePoolSize: 1,
		PoolTimeout:      100 * time.Millisecond, // spill latency bound
	})
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}

	// Occupy the single pipeline-pool connection: BLPOP inside a pipeline
	// blocks server-side for its timeout, holding the connection busy.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = c.Pipelined(ctx, func(p Pipeliner) error {
			p.BLPop(ctx, time.Second, "pps:never")
			return nil
		})
	}()
	// Wait until the blocker has actually taken the pipeline pool's only
	// connection (1 total, 0 idle) instead of a fixed sleep, which can flake on
	// slow/contended CI.
	pp := c.getPipelinePool()
	if pp == nil {
		t.Fatal("no pipeline pool")
	}
	for deadline := time.Now().Add(2 * time.Second); ; {
		st := pp.Stats()
		if st.TotalConns >= 1 && st.IdleConns == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("blocker did not acquire the pipeline conn (total=%d idle=%d)", st.TotalConns, st.IdleConns)
		}
		time.Sleep(2 * time.Millisecond)
	}

	// A second pipeline must not wait out the blocker: it spills to the main
	// pool after PoolTimeout (100ms) and completes well under the 1s the
	// blocker holds the pipeline conn.
	start := time.Now()
	cmds, err := c.Pipelined(ctx, func(p Pipeliner) error {
		p.Set(ctx, "pps:k", "v", 0)
		p.Get(ctx, "pps:k")
		return nil
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("spilled pipeline failed: %v", err)
	}
	if got := cmds[1].(*StringCmd).Val(); got != "v" {
		t.Fatalf("spilled pipeline GET = %q, want v", got)
	}
	if elapsed >= time.Second {
		t.Fatalf("second pipeline took %v: it queued behind the blocker instead of spilling", elapsed)
	}
	wg.Wait()
}

// TestPipelineConnsSkipClientTracking: with the built-in client-side cache
// enabled, pipeline-pool connections must NOT run CLIENT TRACKING ON during
// init. Pipelined commands never consult or populate the cache — only the
// single-command cached path on main-pool connections does — so tracking
// pipeline reads would only grow the server's tracking table and produce
// invalidation pushes for keys the cache does not hold.
func TestPipelineConnsSkipClientTracking(t *testing.T) {
	ctx := context.Background()
	name := "pipetrackskip"
	c := NewClient(&Options{
		Addr:                  gateTestAddr(),
		ClientName:            name,
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{},
	})
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}

	// One main-pool connection (tracked: CSC is on) and one pipeline-pool
	// connection (must not be tracked).
	if err := c.Set(ctx, "pts:k", "v", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Pipelined(ctx, func(p Pipeliner) error {
		p.Get(ctx, "pts:k")
		p.Get(ctx, "pts:k2")
		return nil
	}); err != nil && err != Nil { // pts:k2 does not exist; Nil is the expected outcome
		t.Fatal(err)
	}

	list, err := c.ClientList(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	tracked, untracked := 0, 0
	for _, line := range strings.Split(list, "\n") {
		if !strings.Contains(line, "name="+name) {
			continue
		}
		for _, f := range strings.Fields(line) {
			if strings.HasPrefix(f, "flags=") {
				if strings.Contains(strings.TrimPrefix(f, "flags="), "t") {
					tracked++
				} else {
					untracked++
				}
			}
		}
	}
	if tracked < 1 {
		t.Fatalf("tracked=%d: CSC main-pool connections must run CLIENT TRACKING", tracked)
	}
	if untracked < 1 {
		t.Fatalf("untracked=%d: the pipeline-pool connection must NOT be tracked", untracked)
	}
}
