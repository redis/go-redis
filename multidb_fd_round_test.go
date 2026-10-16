package redis

import (
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"sync/atomic"
	"testing"
	"time"

	imultidb "github.com/redis/go-redis/v9/internal/multidb"
)

// fdProcessFuncHook is a Hook with only a process hook.
type fdProcessFuncHook struct {
	process func(next ProcessHook) ProcessHook
}

func (h fdProcessFuncHook) DialHook(next DialHook) DialHook { return next }
func (h fdProcessFuncHook) ProcessHook(next ProcessHook) ProcessHook {
	return h.process(next)
}

func (h fdProcessFuncHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}

// settleFDOutcome skips an error only when the caller's own context ended, as
// core.process does. A context error with the caller's context live (here a
// dial cut short by a member-side deadline) is a health signal.
func TestSettleFDOutcomeCallerContext(t *testing.T) {
	dialErr := &net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded}
	setup := func() (*multidbCore, *multidbDatabase, *countingFD) {
		det := &countingFD{}
		core := newMultidbCore(&MultiDBOptions{FailureDetector: det})
		db := &multidbDatabase{cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{
			FailureThreshold: 1,
			SuccessThreshold: 1,
		})}
		core.dbs[0] = db
		core.active.Store(0)
		return core, db, det
	}
	cmd := NewStatusCmd(context.Background(), "set", "k", "v")

	t.Run("live caller context records the failure", func(t *testing.T) {
		core, db, det := setup()
		core.settleFDOutcome(context.Background(), db, cmd, dialErr)
		if det.failures != 1 {
			t.Errorf("detector failures = %d, want 1", det.failures)
		}
		if got := db.cb.State(); got != imultidb.CircuitOpen {
			t.Errorf("breaker state = %v, want open", got)
		}
	})

	t.Run("ended caller context skips", func(t *testing.T) {
		core, db, det := setup()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		core.settleFDOutcome(ctx, db, cmd, dialErr)
		if det.failures != 0 || det.successes != 0 {
			t.Errorf("detector recorded %d failures, %d successes; want none", det.failures, det.successes)
		}
		if got := db.cb.State(); got != imultidb.CircuitClosed {
			t.Errorf("breaker state = %v, want closed", got)
		}
	})
}

// NumShards>1 with the ordered async face is coerced to 1 for MultiDB full
// duplex instead of being rejected by the generic ordering check.
func TestMultiDBFullDuplexForcesNumShards(t *testing.T) {
	requireLocalRedis(t)
	mdb := newFDTestMultiDB(t, member(":6379", 1))
	ap, err := mdb.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true, NumShards: 4})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	if ap.mdbFD == nil {
		t.Fatal("router not installed")
	}
	if got := ap.Config().NumShards; got != 1 {
		t.Fatalf("Config().NumShards = %d, want 1", got)
	}
	if got := ap.mdbFD.hdCfg.NumShards; got != 1 {
		t.Fatalf("half-duplex lane NumShards = %d, want 1", got)
	}
	if err := ap.Set(context.Background(), "mdbfd:numshards", "v", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
}

// A member hook that turns a reply into a transport error is what MultiDB
// accounts: the breaker sees the hook chain's final result, not the raw reply.
func TestMultiDBFullDuplexAccountsHookResult(t *testing.T) {
	requireLocalRedis(t)
	mdb := newFDTestMultiDB(t, member(":6379", 2), member(":6379", 1))
	var hooked atomic.Int64
	if err := mdb.AddDatabaseHook(0, fdProcessFuncHook{process: func(next ProcessHook) ProcessHook {
		return func(ctx context.Context, cmd Cmder) error {
			if err := next(ctx, cmd); err != nil {
				return err
			}
			if cmd.Name() == "set" {
				hooked.Add(1)
				return io.EOF
			}
			return nil
		}
	}}); err != nil {
		t.Fatalf("AddDatabaseHook: %v", err)
	}
	ap, err := mdb.AutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AutoPipeline: %v", err)
	}
	ctx := context.Background()
	// FailureThreshold is 3 (newFDTestMultiDB).
	for i := 0; i < 3; i++ {
		if err := ap.Set(ctx, "mdbfd:hookresult", i, 0).Err(); err != io.EOF {
			t.Fatalf("Set %d: err = %v, want io.EOF from the hook", i, err)
		}
	}
	if hooked.Load() != 3 {
		t.Fatalf("hook ran %d times, want 3 (FD path did not host the member hook)", hooked.Load())
	}
	if got := mdb.core.dbByID(0).cb.State(); got != imultidb.CircuitOpen {
		t.Fatalf("member 0 breaker = %v, want open: the hook's error was not accounted", got)
	}
}

// Len counts the half-duplex lane's backlog on the async face.
func TestMultiDBFullDuplexLenCountsHalfDuplexLane(t *testing.T) {
	requireLocalRedis(t)
	mdb := newFDTestMultiDB(t, member(":6379", 2), member(":6379", 1))
	ap, err := mdb.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex:    true,
		MaxFlushDelay: 300 * time.Millisecond,
		MaxBatchSize:  1000,
	})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	ctx := context.Background()
	hd, reject := ap.mdbFD.hdLane()
	if reject || hd == nil {
		t.Fatal("no half-duplex lane")
	}
	if ap.mdbFD.switchLane(hd) {
		t.Fatal("router closed")
	}
	const n = 10
	cmds := make([]*StatusCmd, n)
	for i := range cmds {
		cmds[i] = NewStatusCmd(ctx, "set", "mdbfd:len", i)
		hd.processAsync(ctx, cmds[i])
	}
	if got := ap.Len(); got == 0 {
		t.Fatalf("Len() = 0 with %d commands queued on the half-duplex lane", hd.Len())
	}
	for i, c := range cmds {
		if err := c.Err(); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
}

// Removing a member drains and closes the private child built for it, without
// waiting for a later cache miss.
func TestMultiDBFullDuplexRemoveClosesChild(t *testing.T) {
	requireLocalRedis(t)
	mdb := newFDTestMultiDB(t, member(":6379", 2), member(":6379", 1))
	ap, err := mdb.AutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AutoPipeline: %v", err)
	}
	ctx := context.Background()
	if err := ap.Set(ctx, "mdbfd:remove", "v", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	db1 := mdb.core.dbByID(1)
	child, _ := ap.mdbFD.childFor(db1)
	if child == nil {
		t.Fatal("no child for member 1")
	}
	if err := mdb.RemoveDatabase(ctx, 1); err != nil {
		t.Fatalf("RemoveDatabase: %v", err)
	}
	if !child.IsClosed() {
		t.Fatal("removed member's child still open")
	}
	ap.mdbFD.mu.RLock()
	_, kept := ap.mdbFD.children[db1]
	ap.mdbFD.mu.RUnlock()
	if kept {
		t.Fatal("removed member's child still cached")
	}
	if err := ap.Set(ctx, "mdbfd:remove", "v2", 0).Err(); err != nil {
		t.Fatalf("Set after removal: %v", err)
	}
}

// On a cluster member, a command the cluster FD router cannot place on a node
// child (here keyless ECHO) runs through MultiDB's path, so MultiDB process
// hooks, admission and accounting apply, not db.cc.Process.
func TestMultiDBFullDuplexClusterMissTakesMultiDBPath(t *testing.T) {
	addrs := []string{":16600", ":16601", ":16602"}
	cc := NewClusterClient(&ClusterOptions{Addrs: addrs})
	if err := cc.Ping(context.Background()).Err(); err != nil {
		_ = cc.Close()
		t.Skipf("no local cluster: %v", err)
	}
	_ = cc.Close()

	mdb := newFDTestMultiDB(
		t,
		MultiDBClientConfig{ClusterOptions: &ClusterOptions{Addrs: addrs}, Weight: 2},
		MultiDBClientConfig{ClusterOptions: &ClusterOptions{Addrs: addrs}, Weight: 1},
	)
	var echoes atomic.Int64
	mdb.AddHook(fdProcessFuncHook{process: func(next ProcessHook) ProcessHook {
		return func(ctx context.Context, cmd Cmder) error {
			if cmd.Name() == "echo" {
				echoes.Add(1)
			}
			return next(ctx, cmd)
		}
	}})
	ap, err := mdb.AutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AutoPipeline: %v", err)
	}
	ctx := context.Background()
	if err := ap.Set(ctx, "mdbfd:clustermiss", "v", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	child, _ := ap.mdbFD.childFor(mdb.core.dbByID(0))
	if child == nil || child.clusterFD == nil {
		t.Fatal("cluster member has no cluster FD child")
	}
	if got, err := ap.Echo(ctx, "x").Result(); err != nil || got != "x" {
		t.Fatalf("Echo = %q, %v", got, err)
	}
	if echoes.Load() != 1 {
		t.Fatalf("MultiDB hook saw %d ECHO, want 1: the routing miss bypassed MultiDB", echoes.Load())
	}
}

// With an owner retry policy (MultiDB CommandRetries), a rerouted command's
// bound comes from the owner budget minus the executions already done, and a
// budget of math.MaxInt does not wrap into "exhausted".
func TestFDRerouteOwnerBudget(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		budget int
		want   string
	}{
		{1, "[0 1]"},
		{math.MaxInt, fmt.Sprintf("[%d %d]", math.MaxInt-1, math.MaxInt)},
	} {
		fd, bounds := fdRerouteTestEngine(0, fdServeOK)
		fd.ownerRetries = true
		fd.ownerRetryBudget = tc.budget
		reqs := []fdReq{
			{cmd: NewStatusCmd(ctx, "set", "a", "1"), batch: newAPBatch(), attempts: 2},
			{cmd: NewStatusCmd(ctx, "set", "b", "1"), batch: newAPBatch(), attempts: 1},
		}
		fd.rerouteReqs(ctx, reqs, io.EOF)
		if got := fmt.Sprint(*bounds); got != tc.want {
			t.Fatalf("budget %d: reroute bounds = %s, want %s", tc.budget, got, tc.want)
		}
		for i, r := range reqs {
			if err := r.cmd.rawErr(); err != nil {
				t.Fatalf("budget %d: reqs[%d] err = %v, want served", tc.budget, i, err)
			}
		}
	}
}
