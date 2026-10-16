package redis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/otel"
)

// rerouteTo returns an fdReroute that runs the commands on target and counts
// them.
func rerouteTo(target *Client, n *atomic.Int64) func(context.Context, []Cmder, int) error {
	return func(ctx context.Context, cmds []Cmder, maxRetries int) error {
		n.Add(int64(len(cmds)))
		return target.processPipelineRetries(ctx, cmds, maxRetries)
	}
}

// fdRerouteTestEngine is an engine with only what the flush paths need, and
// a reroute that records each call's retry bound and serves the commands with
// serve.
func fdRerouteTestEngine(budget int, serve func(cmds []Cmder) error) (*fdEngine, *[]int) {
	var bounds []int
	fd := &fdEngine{
		ap:                 &AutoPipeliner{config: &AutoPipelineOptions{}},
		client:             &Client{baseClient: &baseClient{opt: &Options{}}},
		epoch:              time.Now(),
		maxBatch:           8,
		q:                  newFDQueue(8),
		redirectAware:      true,
		clusterRetryBudget: budget,
	}
	fd.reroute = func(_ context.Context, cmds []Cmder, maxRetries int) error {
		bounds = append(bounds, maxRetries)
		return serve(cmds)
	}
	return fd, &bounds
}

func fdServeOK(cmds []Cmder) error {
	for _, c := range cmds {
		c.SetErr(nil)
	}
	return nil
}

// A rerouted command keeps its remaining retry budget: the owner gets the
// budget left after the attempts already made on the FD connection, not a
// fresh one (codex P1 on #4111). Red before: the bound was dropped, and the
// cluster owner ran its full MaxRedirects loop.
func TestFDRerouteKeepsRemainingRetryBudget(t *testing.T) {
	ctx := context.Background()
	fd, bounds := fdRerouteTestEngine(3, fdServeOK)
	// POST-BUMP carry: attempts=2 is one execution done on the FD connection.
	// attempts=1 is a queued command never sent.
	reqs := []fdReq{
		{cmd: NewStatusCmd(ctx, "set", "a", "1"), batch: newAPBatch(), attempts: 2},
		{cmd: NewStatusCmd(ctx, "set", "b", "1"), batch: newAPBatch(), attempts: 2},
		{cmd: NewStatusCmd(ctx, "set", "c", "1"), batch: newAPBatch(), attempts: 1},
	}
	fd.rerouteReqs(ctx, reqs)
	if got, want := fmt.Sprint(*bounds), "[2 3]"; got != want {
		t.Fatalf("reroute retry bounds = %s, want %s (budget 3 minus executions done)", got, want)
	}
}

// The Close-time flush of an engine with an owner goes through the owner, not
// through the engine's own client (codex P2 on #4111: a child closed by
// topology GC flushed through its dropped node).
func TestFDShutdownFlushReroutes(t *testing.T) {
	ctx := context.Background()
	fd, bounds := fdRerouteTestEngine(3, fdServeOK)
	// Nothing listens here: a flush through the engine's own client fails.
	fd.client = NewClient(&Options{Addr: "127.0.0.1:1", MaxRetries: -1, DialTimeout: 50 * time.Millisecond})
	defer fd.client.Close()

	carry := NewStatusCmd(ctx, "set", "a", "1")
	queued := NewStatusCmd(ctx, "set", "b", "1")
	if fd.q.push(fdReq{cmd: queued, batch: newAPBatch(), attempts: 1}) != fdPushOK {
		t.Fatal("queue push failed")
	}
	fd.shutdownFlush(ctx, []fdReq{{cmd: carry, batch: newAPBatch(), attempts: 2}})
	if got, want := fmt.Sprint(*bounds), "[2 3]"; got != want {
		t.Fatalf("reroute retry bounds = %s, want %s", got, want)
	}
	for _, c := range []*StatusCmd{carry, queued} {
		if err := c.rawErr(); err != nil {
			t.Fatalf("%v: err = %v, want it served through the reroute", c.Args(), err)
		}
	}
}

// An owner pipeline that returns an earlier Redis error (redis.Nil) while a
// later command failed in transport still aborts the rest of the flush
// (codex P2 on #4111).
func TestFDRerouteDetectsMaskedTransportError(t *testing.T) {
	ctx := context.Background()
	calls := 0
	fd, _ := fdRerouteTestEngine(3, func(cmds []Cmder) error {
		calls++
		cmds[0].SetErr(Nil)
		for _, c := range cmds[1:] {
			c.SetErr(io.EOF)
		}
		return Nil // first command error, as ClusterClient.processPipeline returns
	})
	fd.maxBatch = 2 // two chunks
	reqs := make([]fdReq, 4)
	for i := range reqs {
		reqs[i] = fdReq{cmd: NewStringCmd(ctx, "get", "k"+itoa(i)), batch: newAPBatch(), attempts: 1}
	}
	fd.rerouteReqs(ctx, reqs)
	if calls != 1 {
		t.Fatalf("owner ran %d chunks, want 1: the second ran after a transport failure", calls)
	}
	for i := 2; i < len(reqs); i++ {
		if err := reqs[i].cmd.rawErr(); !errors.Is(err, io.EOF) {
			t.Fatalf("reqs[%d] err = %v, want io.EOF (failed by the abort)", i, err)
		}
	}
}

// Rerouted commands report the per-command duration metric the reader would
// have reported (codex P2 on #4111).
func TestFDRerouteRecordsCommandMetrics(t *testing.T) {
	rec := &fdOtelRecorder{}
	otel.SetGlobalRecorder(rec)
	defer otel.SetGlobalRecorder(nil)

	ctx := context.Background()
	fd, _ := fdRerouteTestEngine(3, fdServeOK)
	reqs := []fdReq{
		{cmd: NewStatusCmd(ctx, "set", "a", "1"), batch: newAPBatch(), attempts: 2},
		{cmd: NewStatusCmd(ctx, "set", "b", "1"), batch: newAPBatch(), attempts: 1},
	}
	fd.rerouteReqs(ctx, reqs)
	if n := rec.opDurations.Load(); n != int64(len(reqs)) {
		t.Fatalf("operation durations recorded = %d, want %d", n, len(reqs))
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
