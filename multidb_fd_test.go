package redis

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	imultidb "github.com/redis/go-redis/v9/internal/multidb"
	"github.com/redis/go-redis/v9/internal/proto"
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
	c := NewClient(&Options{Addr: internalTestRedisAddr()})
	defer c.Close()
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Skipf("no redis on %s: %v", internalTestRedisAddr(), err)
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
		// One probe per pass: the default 3 probes 500ms apart do not fit
		// the 100ms timeout.
		HealthChecks: []MultiDBHealthCheck{onePingCheck{}},
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
	mdb := newFDTestMultiDB(t, member(internalTestRedisAddr(), 0.75), member(internalTestRedisAddr(), 0.5))
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
	mdb := newFDTestMultiDB(t, member(internalTestRedisAddr(), 0.75), member(internalTestRedisAddr(), 0.5))
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
	proxy := startFDTestProxy(t, internalTestRedisAddr())
	mdb := newFDTestMultiDBRetries(t, retries, member(proxy.addr(), 0.75), member(internalTestRedisAddr(), 0.5))
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
		MultiDBClientConfig{ClusterOptions: &ClusterOptions{Addrs: []string{":16600", ":16601", ":16602"}}, Weight: 0.75},
		MultiDBClientConfig{ClusterOptions: &ClusterOptions{Addrs: []string{":16600", ":16601", ":16602"}}, Weight: 0.5},
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
	mdb := newFDTestMultiDB(t, member(internalTestRedisAddr(), 0.5))
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

// A member where full duplex cannot engage (no pipeline pool) diverts to
// MultiDB's normal path and is remembered: no child is built per command.
func TestMultiDBFullDuplexMemberWithoutFD(t *testing.T) {
	requireLocalRedis(t)
	noPool := MultiDBClientConfig{
		Options: &Options{Addr: internalTestRedisAddr(), PipelinePoolSize: -1, MaxRetries: -1},
		Weight:  0.5,
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
	proxy := startFDTestProxy(t, internalTestRedisAddr())
	mdb := newFDTestMultiDB(t, member(proxy.addr(), 0.75), member(internalTestRedisAddr(), 0.5))
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
	direct := NewClient(&Options{Addr: internalTestRedisAddr()})
	defer direct.Close()
	got, err := direct.Get(ctx, key).Int()
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	if got != n-1 {
		t.Fatalf("final value = %d, want %d (the last write submitted): a write submitted earlier ran after a later one", got, n-1)
	}
}

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
		core.settleFDOutcome(context.Background(), db, cmd, dialErr, nil)
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
		core.settleFDOutcome(ctx, db, cmd, dialErr, nil)
		if det.failures != 0 || det.successes != 0 {
			t.Errorf("detector recorded %d failures, %d successes; want none", det.failures, det.successes)
		}
		if got := db.cb.State(); got != imultidb.CircuitClosed {
			t.Errorf("breaker state = %v, want closed", got)
		}
	})

	t.Run("member read timeout after the caller context ended records", func(t *testing.T) {
		core, db, det := setup()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		readErr := &net.OpError{Op: "read", Net: "tcp", Err: os.ErrDeadlineExceeded}
		core.settleFDOutcome(ctx, db, cmd, readErr, nil)
		if det.failures != 1 {
			t.Errorf("detector failures = %d, want 1", det.failures)
		}
		if got := db.cb.State(); got != imultidb.CircuitOpen {
			t.Errorf("breaker state = %v, want open", got)
		}
	})

	t.Run("one failure group charges the breaker and detector once", func(t *testing.T) {
		det := &countingFD{}
		core := newMultidbCore(&MultiDBOptions{FailureDetector: det})
		db := &multidbDatabase{cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{
			FailureThreshold: 100,
			SuccessThreshold: 1,
		})}
		core.dbs[0] = db
		core.active.Store(0)
		grp := &fdFailGroup{}
		for i := 0; i < 4; i++ {
			core.settleFDOutcome(context.Background(), db, cmd, io.EOF, grp)
		}
		core.settleFDOutcome(context.Background(), db, cmd, io.EOF, &fdFailGroup{})
		if got := db.cb.Stats().Failures; got != 2 {
			t.Errorf("breaker failures = %d, want 2 (one per group)", got)
		}
		if det.failures != 2 {
			t.Errorf("detector failures = %d, want 2 (one per group)", det.failures)
		}
	})
}

// NumShards>1 with the ordered async face is coerced to 1 for MultiDB full
// duplex instead of being rejected by the generic ordering check.
func TestMultiDBFullDuplexForcesNumShards(t *testing.T) {
	requireLocalRedis(t)
	mdb := newFDTestMultiDB(t, member(internalTestRedisAddr(), 0.5))
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
	mdb := newFDTestMultiDB(t, member(internalTestRedisAddr(), 0.75), member(internalTestRedisAddr(), 0.5))
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
	mdb := newFDTestMultiDB(t, member(internalTestRedisAddr(), 0.75), member(internalTestRedisAddr(), 0.5))
	ap, err := mdb.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
		FullDuplex:    true,
		MaxFlushDelay: 300 * time.Millisecond,
		MaxBatchSize:  1000,
	})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	ctx := context.Background()
	ap.mdbFD.lockLane()
	hd, reject := ap.mdbFD.hdLaneLocked()
	ap.mdbFD.unlockLane()
	if reject || hd == nil {
		t.Fatal("no half-duplex lane")
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

// A submit waiting for the lane (held by a lane switch's drain) honors its
// context: it returns the context error once the deadline passes, and the
// command is never sent.
func TestMultiDBFullDuplexLaneWaitHonorsContext(t *testing.T) {
	requireLocalRedis(t)
	mdb := newFDTestMultiDB(t, member(internalTestRedisAddr(), 0.75), member(internalTestRedisAddr(), 0.5))
	ap, err := mdb.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	const key = "mdbfd:lanewait"
	if err := mdb.Del(context.Background(), key).Err(); err != nil {
		t.Fatalf("Del: %v", err)
	}
	r := ap.mdbFD
	r.lockLane()
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	start := time.Now()
	go func() { done <- ap.Set(ctx, key, "v", 0).Err() }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("Set = %v, want context.DeadlineExceeded", err)
		}
		if d := time.Since(start); d > time.Second {
			t.Errorf("Set returned after %v, want about 50ms", d)
		}
		r.unlockLane()
	case <-time.After(2 * time.Second):
		t.Error("Set still waiting for the held lane past its deadline")
		r.unlockLane()
		if err := <-done; err == nil {
			t.Fatal("Set succeeded after the lane was released")
		}
	}
	if n, err := mdb.Exists(context.Background(), key).Result(); err != nil || n != 0 {
		t.Fatalf("Exists = %d, %v; want 0: a command that timed out waiting for the lane was sent", n, err)
	}
}

// A full-duplex connection drop with N commands in flight charges the
// member's breaker and detector once, not N times.
// With retries off the tail fails on the engine (failReqs); with retries on it
// is rerouted through MultiDB (rerouteReqs), and every further drop on the
// reroute is charged once too.
func TestMultiDBFullDuplexDropChargesBreakerOnce(t *testing.T) {
	for _, retries := range []int{-1, 1} {
		t.Run(fmt.Sprintf("retries=%d", retries), func(t *testing.T) {
			var sets, drops atomic.Int64
			d := &scriptedDialer{handle: func(addr string, _ int, args []string) (string, fakeAction) {
				if addr == "a:1" && strings.EqualFold(args[0], "set") && sets.Add(1) > 3 {
					drops.Add(1)
					return "", fakeClose
				}
				return "+OK\r\n", fakeReply
			}}
			det := &countingDetector{}
			mdb := newScriptedClient(t, d, func(o *Options) { o.MaxRetries = -1 }, func(o *MultiDBOptions) {
				o.CommandRetries = retries
				o.FailureDetector = det
				o.CircuitBreakerConfig = &MultiDBCircuitBreakerConfig{
					FailureThreshold: 100,
					SuccessThreshold: 1,
					GracePeriod:      time.Hour,
				}
			})
			ap, err := mdb.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{
				FullDuplex:    true,
				MaxBatchSize:  1000,
				MaxFlushDelay: 20 * time.Millisecond,
			})
			if err != nil {
				t.Fatalf("AsyncAutoPipeline: %v", err)
			}
			if !ap.Config().FullDuplex || ap.mdbFD == nil {
				t.Fatal("full duplex not engaged")
			}
			ctx := context.Background()
			const n = 10
			cmds := make([]*StatusCmd, n)
			for i := range cmds {
				cmds[i] = ap.Set(ctx, "k", i, 0)
			}
			failed := 0
			for _, c := range cmds {
				if c.Err() != nil {
					failed++
				}
			}
			if failed == 0 {
				t.Fatal("no command failed: the drop was not exercised")
			}
			a := mdb.core.dbByID(0)
			if got, want := a.cb.Stats().Failures, int32(drops.Load()); got != want {
				t.Errorf("breaker failures = %d, want %d (one per connection drop; %d commands failed)", got, want, failed)
			}
			if retries < 0 {
				if got := det.failures.Load(); got != drops.Load() {
					t.Errorf("detector failures = %d, want %d (one per connection drop; %d commands failed)", got, drops.Load(), failed)
				}
			}
		})
	}
}

// Removing a member drains and closes the private child built for it, without
// waiting for a later cache miss.
func TestMultiDBFullDuplexRemoveClosesChild(t *testing.T) {
	requireLocalRedis(t)
	mdb := newFDTestMultiDB(t, member(internalTestRedisAddr(), 0.75), member(internalTestRedisAddr(), 0.5))
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
		MultiDBClientConfig{ClusterOptions: &ClusterOptions{Addrs: addrs}, Weight: 0.75},
		MultiDBClientConfig{ClusterOptions: &ClusterOptions{Addrs: addrs}, Weight: 0.5},
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

// Reference for TestMultiDBFullDuplexOrderAcrossMemberDeath on the half-duplex
// async face.
func TestMultiDBHalfDuplexOrderAcrossMemberDeath(t *testing.T) {
	requireLocalRedis(t)
	proxy := startFDTestProxy(t, internalTestRedisAddr())
	mdb := newFDTestMultiDB(t, member(proxy.addr(), 0.75), member(internalTestRedisAddr(), 0.5))
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
	direct := NewClient(&Options{Addr: internalTestRedisAddr()})
	defer direct.Close()
	got, err := direct.Get(ctx, key).Int()
	t.Logf("half-duplex: final=%d want=%d failed=%d err=%v", got, n-1, failed, err)
	if got != n-1 {
		t.Fatalf("final value = %d, want %d", got, n-1)
	}
}

// FD on a MultiDBClient vs FD on a plain *Client, same backend: the difference
// is MultiDB's per-command admission (AllowReserve, detector check) and outcome
// accounting on the fast path.
func benchmarkFDSet(b *testing.B, ap *AutoPipeliner) {
	ctx := context.Background()
	if err := ap.Set(ctx, "bench:fd:warm", "v", 0).Err(); err != nil {
		b.Skipf("no redis: %v", err)
	}
	const window = 64
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		cmds := make([]*StatusCmd, 0, window)
		i := 0
		for pb.Next() {
			cmds = append(cmds, ap.Set(ctx, "bench:fd:"+strconv.Itoa(i&1023), "v", 0))
			i++
			if len(cmds) == window {
				for _, c := range cmds {
					if err := c.Err(); err != nil {
						b.Error(err)
						return
					}
				}
				cmds = cmds[:0]
			}
		}
		for _, c := range cmds {
			_ = c.Err()
		}
	})
}

func BenchmarkFDSetPlainClient(b *testing.B) {
	c := NewClient(&Options{Addr: internalTestRedisAddr()})
	defer c.Close()
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		b.Fatal(err)
	}
	defer ap.Close()
	benchmarkFDSet(b, ap)
}

func BenchmarkFDSetMultiDB(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mdb, err := NewMultiDBClient(ctx, &MultiDBOptions{
		Clients: []MultiDBClientConfig{
			{Options: &Options{Addr: internalTestRedisAddr()}, Weight: 0.75},
			{Options: &Options{Addr: internalTestRedisAddr()}, Weight: 0.5},
		},
		AutoFallbackInterval: -1,
	})
	if err != nil {
		b.Skipf("NewMultiDBClient: %v", err)
	}
	defer mdb.Close()
	ap, err := mdb.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		b.Fatal(err)
	}
	if ap.mdbFD == nil {
		b.Fatal("MultiDB full duplex not engaged")
	}
	benchmarkFDSet(b, ap)
}

// Reference: MultiDB on the half-duplex autopipeline path.
func BenchmarkFDSetMultiDBHalfDuplex(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mdb, err := NewMultiDBClient(ctx, &MultiDBOptions{
		Clients: []MultiDBClientConfig{
			{Options: &Options{Addr: internalTestRedisAddr()}, Weight: 0.75},
			{Options: &Options{Addr: internalTestRedisAddr()}, Weight: 0.5},
		},
		AutoFallbackInterval: -1,
	})
	if err != nil {
		b.Skipf("NewMultiDBClient: %v", err)
	}
	defer mdb.Close()
	ap, err := mdb.AsyncAutoPipeline()
	if err != nil {
		b.Fatal(err)
	}
	benchmarkFDSet(b, ap)
}

// A reroute that aborts after a transport failure does not report the
// commands it never ran a second time: rerouteReqs already reported their
// failed attempt on the FD connection, and the owner settles what it ran.
func TestFDRerouteAbortReportsOncePerAttempt(t *testing.T) {
	ctx := context.Background()
	fd, _ := fdRerouteTestEngine(3, func(cmds []Cmder) error {
		for _, c := range cmds {
			c.SetErr(io.EOF)
		}
		return io.EOF
	})
	reports := 0
	fd.outcome = func(context.Context, Cmder, error, *fdFailGroup) { reports++ }
	fd.maxBatch = 2 // two chunks: the second never runs
	reqs := make([]fdReq, 4)
	for i := range reqs {
		reqs[i] = fdReq{cmd: NewStatusCmd(ctx, "set", "k"+strconv.Itoa(i), "v"), batch: newAPBatch(), attempts: 1, sent: true}
	}
	fd.rerouteReqs(ctx, reqs, io.EOF)
	if reports != len(reqs) {
		t.Fatalf("outcome reports = %d, want %d (one per failed FD attempt)", reports, len(reqs))
	}
	for i, r := range reqs {
		if err := r.cmd.rawErr(); !errors.Is(err, io.EOF) {
			t.Fatalf("reqs[%d] err = %v, want io.EOF", i, err)
		}
	}
}

// A rerouted command whose retry budget is spent is failed, not re-run, and
// is not reported a second time: rerouteReqs already reported its failed
// attempt on the FD connection.
func TestFDRerouteSpentBudgetReportsOncePerAttempt(t *testing.T) {
	ctx := context.Background()
	fd, bounds := fdRerouteTestEngine(0, fdServeOK)
	reports := 0
	fd.outcome = func(context.Context, Cmder, error, *fdFailGroup) { reports++ }
	reqs := make([]fdReq, 3)
	for i := range reqs {
		reqs[i] = fdReq{cmd: NewStatusCmd(ctx, "set", "k"+strconv.Itoa(i), "v"), batch: newAPBatch(), attempts: 2, sent: true}
	}
	fd.rerouteReqs(ctx, reqs, io.EOF)
	if reports != len(reqs) {
		t.Fatalf("outcome reports = %d, want %d (one per failed FD attempt)", reports, len(reqs))
	}
	if len(*bounds) != 0 {
		t.Fatalf("owner ran %d chunks, want none (budget spent)", len(*bounds))
	}
	for i, r := range reqs {
		if err := r.cmd.rawErr(); !errors.Is(err, errFDRetryBudgetExhausted) {
			t.Fatalf("reqs[%d] err = %v, want errFDRetryBudgetExhausted", i, err)
		}
	}
}

// A lane left over from before a failover (here: the non-active member's
// child) is drained and replaced by the active member's child on the next
// submit, under laneSem; the command lands on the new lane.
func TestMultiDBFullDuplexLaneSwitchesToActive(t *testing.T) {
	requireLocalRedis(t)
	mdb := newFDTestMultiDB(t, member(internalTestRedisAddr(), 0.75), member(internalTestRedisAddr(), 0.5))
	ap, err := mdb.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	ctx := context.Background()
	r := ap.mdbFD
	active, _ := mdb.core.activeSnapshot()
	var other *multidbDatabase
	for _, id := range []int{0, 1} {
		if db := mdb.core.dbByID(id); db != active {
			other = db
		}
	}
	old, _ := r.childFor(other)
	if old == nil {
		t.Fatal("no child for the other member")
	}
	if err := old.Set(ctx, "mdbfd:laneswitch", "old", 0).Err(); err != nil {
		t.Fatalf("Set on the old lane: %v", err)
	}
	r.lane.Store(old)

	if err := ap.Set(ctx, "mdbfd:laneswitch", "new", 0).Err(); err != nil {
		t.Fatalf("Set: %v", err)
	}
	want, _ := r.childFor(active)
	if r.lane.Load() != want {
		t.Fatal("lane did not move to the active member's child")
	}
	if !old.IsClosed() {
		t.Fatal("old lane not drained and closed")
	}
	if got, err := ap.Get(ctx, "mdbfd:laneswitch").Result(); err != nil || got != "new" {
		t.Fatalf("Get = %q, %v; want new", got, err)
	}
}

// Removing a member that could not run full duplex drops its noFD mark at
// once, without waiting for a later cache miss.
func TestMultiDBFullDuplexRemoveClearsNoFD(t *testing.T) {
	requireLocalRedis(t)
	noPool := MultiDBClientConfig{
		Options: &Options{Addr: internalTestRedisAddr(), PipelinePoolSize: -1, MaxRetries: -1},
		Weight:  0.5,
	}
	mdb := newFDTestMultiDB(t, member(internalTestRedisAddr(), 0.75), noPool)
	ap, err := mdb.AutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AutoPipeline: %v", err)
	}
	ctx := context.Background()
	r := ap.mdbFD
	db1 := mdb.core.dbByID(1)
	if child, _ := r.childFor(db1); child != nil {
		t.Fatal("member without a pipeline pool got an FD child")
	}
	r.mu.RLock()
	_, marked := r.noFD[db1]
	r.mu.RUnlock()
	if !marked {
		t.Fatal("member without FD not marked")
	}
	if err := mdb.RemoveDatabase(ctx, 1); err != nil {
		t.Fatalf("RemoveDatabase: %v", err)
	}
	r.mu.RLock()
	_, marked = r.noFD[db1]
	r.mu.RUnlock()
	if marked {
		t.Fatal("removed member still in the noFD cache")
	}
}

// When no lane can take a command, submitLocked's divert completes before it
// returns, so a later command on a new lane cannot run ahead of it.
func TestMultiDBFullDuplexDivertKeepsOrder(t *testing.T) {
	requireLocalRedis(t)
	mdb := newFDTestMultiDB(t, member(internalTestRedisAddr(), 0.75), member(internalTestRedisAddr(), 0.5))
	mdb.AddHook(fdProcessFuncHook{process: func(next ProcessHook) ProcessHook {
		return func(ctx context.Context, cmd Cmder) error {
			if cmd.Name() == "set" {
				time.Sleep(100 * time.Millisecond)
			}
			return next(ctx, cmd)
		}
	}})
	ap, err := mdb.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	ctx := context.Background()
	r := ap.mdbFD
	// No FD fast path (active breaker open) and no half-duplex lane (its
	// config fails validation): the command must divert.
	active, _ := mdb.core.activeSnapshot()
	active.cb.ForceOpen()
	r.hdCfg.MaxBatchSize = -1

	cmd := NewStatusCmd(ctx, "set", "mdbfd:divertorder", "v")
	fut := r.submitOrdered(ctx, cmd)
	select {
	case <-fut.batch.done:
	default:
		t.Fatal("submitOrdered returned before the diverted command completed")
	}
	if err := cmd.Err(); err != nil {
		t.Fatalf("diverted SET: %v", err)
	}
}

// A rerouted command that never reached the FD connection (the refunded
// unwritten suffix) reports no failure to the owner: it made no attempt on
// the member.
func TestFDRerouteNeverSentReportsNothing(t *testing.T) {
	ctx := context.Background()
	fd, _ := fdRerouteTestEngine(3, fdServeOK)
	var reported []string
	fd.outcome = func(_ context.Context, cmd Cmder, _ error, _ *fdFailGroup) {
		reported = append(reported, fmt.Sprint(cmd.Args()[1]))
	}
	reqs := []fdReq{
		{cmd: NewStatusCmd(ctx, "set", "sent", "v"), batch: newAPBatch(), attempts: 2, sent: true},
		{cmd: NewStatusCmd(ctx, "set", "unsent", "v"), batch: newAPBatch(), attempts: 1},
	}
	fd.rerouteReqs(ctx, reqs, io.EOF)
	if got := fmt.Sprint(reported); got != "[sent]" {
		t.Fatalf("reported = %s, want [sent]", got)
	}
}

// A member pipeline hook that reads a deferred-face command's result during
// a reroute does not wedge the flush: the reroute goroutine is the batch's
// executor, and the batch completes only after the owner returns.
func TestFDRerouteHookReadsDeferredResult(t *testing.T) {
	ctx := context.Background()
	fd, _ := fdRerouteTestEngine(3, func(cmds []Cmder) error {
		for _, c := range cmds {
			c.SetErr(nil)
			_ = c.Err() // a hook reading the result after next()
		}
		return nil
	})
	reqs := make([]fdReq, 2)
	for i := range reqs {
		b := newAPBatch()
		cmd := NewStatusCmd(ctx, "set", "k"+itoa(i), "v")
		cmd.setReady(b)
		reqs[i] = fdReq{cmd: cmd, batch: b, attempts: 1}
	}
	done := make(chan struct{})
	go func() {
		fd.rerouteReqs(ctx, reqs, io.EOF)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("reroute wedged: a hook reading its command awaited the batch the reroute completes")
	}
}

// The childFor sweep leaves a removed member's child to its onRemove hook:
// RemoveDatabase must find it and drain it before the member's client closes.
func TestMultiDBFullDuplexSweepLeavesRemovedChildToRemoval(t *testing.T) {
	requireLocalRedis(t)
	mdb := newFDTestMultiDB(t, member(internalTestRedisAddr(), 0.75), member(internalTestRedisAddr(), 0.5))
	ap, err := mdb.AutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AutoPipeline: %v", err)
	}
	r := ap.mdbFD
	db0, db1 := mdb.core.dbByID(0), mdb.core.dbByID(1)
	child, _ := r.childFor(db1)
	if child == nil {
		t.Fatal("no child for member 1")
	}
	// RemoveDatabase marks the member removed before it runs onRemove; a
	// cache miss for another member in that window runs the sweep.
	db1.removed.Store(true)
	if c, _ := r.childFor(db0); c == nil {
		t.Fatal("no child for member 0")
	}
	r.mu.RLock()
	kept := r.children[db1]
	r.mu.RUnlock()
	if kept != child || child.IsClosed() {
		t.Fatal("sweep dropped or closed the removed member's child before its removal hook")
	}
	_ = db1.onRemove.run()
	if !child.IsClosed() {
		t.Fatal("removal hook did not close the child")
	}
}

// A child that is closed but still draining (a lane switch) is not lost by
// the childFor sweep: RemoveDatabase's hook still waits for its drain before
// the member's client closes.
func TestMultiDBFullDuplexSweepKeepsDrainingChild(t *testing.T) {
	requireLocalRedis(t)
	mdb := newFDTestMultiDB(t, member(internalTestRedisAddr(), 0.75), member(internalTestRedisAddr(), 0.5))
	ap, err := mdb.AutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AutoPipeline: %v", err)
	}
	r := ap.mdbFD
	db0, db1 := mdb.core.dbByID(0), mdb.core.dbByID(1)
	if c, _ := r.childFor(db1); c == nil {
		t.Fatal("no child for member 1")
	}
	// Stand-in for a child mid-drain: closed, Close not finished.
	draining := &AutoPipeliner{closeDone: make(chan struct{})}
	draining.closed.Store(true)
	r.mu.Lock()
	real := r.children[db1]
	r.children[db1] = draining
	r.mu.Unlock()
	defer func() { _ = real.Close() }()

	// A cache miss for another member runs the sweep.
	if c, _ := r.childFor(db0); c == nil {
		t.Fatal("no child for member 0")
	}
	dropped := make(chan struct{})
	go func() {
		r.dropChild(db1)
		close(dropped)
	}()
	select {
	case <-dropped:
		t.Fatal("removal hook returned before the draining child finished its drain")
	case <-time.After(100 * time.Millisecond):
	}
	close(draining.closeDone)
	select {
	case <-dropped:
	case <-time.After(5 * time.Second):
		t.Fatal("removal hook did not return after the drain finished")
	}
}

// A Close-time carry flush through the owner that aborts fails the queued
// commands behind it without reporting them to the owner: they never ran.
func TestFDShutdownFlushOwnerAbortSkipsQueuedReports(t *testing.T) {
	ctx := context.Background()
	fd, _ := fdRerouteTestEngine(3, func(cmds []Cmder) error {
		for _, c := range cmds {
			c.SetErr(io.EOF)
		}
		return io.EOF
	})
	reports := 0
	fd.outcome = func(context.Context, Cmder, error, *fdFailGroup) { reports++ }
	queued := NewStatusCmd(ctx, "set", "queued", "v")
	if fd.q.push(fdReq{cmd: queued, batch: newAPBatch(), attempts: 1}) != fdPushOK {
		t.Fatal("queue push failed")
	}
	fd.shutdownFlush(ctx, []fdReq{{cmd: NewStatusCmd(ctx, "set", "carry", "v"), batch: newAPBatch(), attempts: 2, sent: true}})
	if reports != 0 {
		t.Fatalf("outcome reports = %d, want 0 (the owner settles what it ran)", reports)
	}
	if err := queued.rawErr(); !errors.Is(err, io.EOF) {
		t.Fatalf("queued err = %v, want io.EOF (failed behind the aborted carry)", err)
	}
}

// Closing the router drops its noFD marks along with its children: its
// removal hooks are unregistered, so nothing would clear them later.
func TestMultiDBFullDuplexCloseClearsNoFD(t *testing.T) {
	requireLocalRedis(t)
	noPool := MultiDBClientConfig{
		Options: &Options{Addr: internalTestRedisAddr(), PipelinePoolSize: -1, MaxRetries: -1},
		Weight:  0.5,
	}
	mdb := newFDTestMultiDB(t, member(internalTestRedisAddr(), 0.75), noPool)
	ap, err := mdb.AutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AutoPipeline: %v", err)
	}
	r := ap.mdbFD
	if child, _ := r.childFor(mdb.core.dbByID(1)); child != nil {
		t.Fatal("member without a pipeline pool got an FD child")
	}
	if err := ap.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	r.mu.RLock()
	n := len(r.noFD)
	r.mu.RUnlock()
	if n != 0 {
		t.Fatalf("noFD holds %d members after Close, want 0", n)
	}
}

// A RemoveDatabase racing the router's Close waits for the member child's
// drain, so the member's client is not closed under accepted commands.
func TestMultiDBFullDuplexRemoveWaitsForCloseDrain(t *testing.T) {
	requireLocalRedis(t)
	mdb := newFDTestMultiDB(t, member(internalTestRedisAddr(), 0.75), member(internalTestRedisAddr(), 0.5))
	release := make(chan struct{})
	var held atomic.Bool
	if err := mdb.AddDatabaseHook(1, fdProcessFuncHook{process: func(next ProcessHook) ProcessHook {
		return func(ctx context.Context, cmd Cmder) error {
			if cmd.Name() == "set" && held.CompareAndSwap(false, true) {
				<-release
			}
			return next(ctx, cmd)
		}
	}}); err != nil {
		t.Fatalf("AddDatabaseHook: %v", err)
	}
	ap, err := mdb.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	ctx := context.Background()
	r := ap.mdbFD
	child, _ := r.childFor(mdb.core.dbByID(1))
	if child == nil {
		t.Fatal("no child for member 1")
	}
	cmd := child.Set(ctx, "mdbfd:removedrain", "v", 0)
	for !held.Load() {
		time.Sleep(time.Millisecond)
	}

	closed := make(chan error, 1)
	go func() { closed <- ap.Close() }()
	for !r.isClosed() {
		time.Sleep(time.Millisecond)
	}
	removed := make(chan error, 1)
	go func() { removed <- mdb.RemoveDatabase(ctx, 1) }()
	select {
	case err := <-removed:
		close(release)
		t.Fatalf("RemoveDatabase returned (%v) while the member's child was still draining", err)
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	if err := <-removed; err != nil {
		t.Fatalf("RemoveDatabase: %v", err)
	}
	if err := <-closed; err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := cmd.Err(); err != nil {
		t.Fatalf("accepted SET: %v", err)
	}
}

// newFDReplyRetryMultiDB builds a full-duplex MultiDB over two fdStateServer
// members (A preferred) with members' retries off, as MultiDB defaults them.
func newFDReplyRetryMultiDB(t *testing.T, retries, failureThreshold int, a, b *fdStateServer) *AutoPipeliner {
	t.Helper()
	return newFDReplyRetryAP(t, false, retries, failureThreshold, a, b)
}

// newFDReplyRetryAP is newFDReplyRetryMultiDB; async picks the ordered async
// face.
func newFDReplyRetryAP(t *testing.T, async bool, retries, failureThreshold int, a, b *fdStateServer) *AutoPipeliner {
	t.Helper()
	mem := func(addr string, weight float64) MultiDBClientConfig {
		return MultiDBClientConfig{
			Options: &Options{Addr: addr, Protocol: 2, DisableIdentity: true, MaxRetries: -1},
			Weight:  weight,
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mdb, err := NewMultiDBClient(ctx, &MultiDBOptions{
		Clients:             []MultiDBClientConfig{mem(a.addr(), 0.75), mem(b.addr(), 0.5)},
		HealthCheckInterval: time.Hour,
		HealthChecks:        []MultiDBHealthCheck{onePingCheck{}},
		CircuitBreakerConfig: &MultiDBCircuitBreakerConfig{
			FailureThreshold: failureThreshold,
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
	newAP := mdb.AutoPipelineWithOptions
	if async {
		newAP = mdb.AsyncAutoPipelineWithOptions
	}
	ap, err := newAP(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AutoPipeline: %v", err)
	}
	if ap.mdbFD == nil {
		t.Fatal("full duplex not engaged")
	}
	return ap
}

// A retryable reply (READONLY) on the FD path follows MultiDB's retry policy,
// not the member's disabled MaxRetries: the failed attempt is recorded (the
// breaker opens at FailureThreshold 1) and the command reruns on the member
// MultiDB fails over to.
func TestMultiDBFullDuplexRetryableReplyFailsOver(t *testing.T) {
	a, b := newFDStateServer(t), newFDStateServer(t)
	a.readOnly.Store(true)
	ap := newFDReplyRetryMultiDB(t, 2, 1, a, b)
	if err := ap.Set(context.Background(), "k", "v", 0).Err(); err != nil {
		t.Fatalf("Set: %v, want served by the second member", err)
	}
	if a.setCalls.Load() != 1 || b.setCalls.Load() != 1 {
		t.Fatalf("SETs: a=%d b=%d, want 1 each", a.setCalls.Load(), b.setCalls.Load())
	}
}

// A command that keeps getting a retryable reply runs at most
// CommandRetries+1 times in total, FD attempt included.
func TestMultiDBFullDuplexRetryableReplyBudget(t *testing.T) {
	a, b := newFDStateServer(t), newFDStateServer(t)
	a.readOnly.Store(true)
	b.readOnly.Store(true)
	const retries = 2
	// A high threshold keeps the breaker closed, so every retry runs.
	ap := newFDReplyRetryMultiDB(t, retries, 100, a, b)
	err := ap.Set(context.Background(), "k", "v", 0).Err()
	if err == nil || !strings.HasPrefix(err.Error(), "READONLY") {
		t.Fatalf("Set err = %v, want READONLY", err)
	}
	if got := a.setCalls.Load() + b.setCalls.Load(); got != retries+1 {
		t.Fatalf("SETs sent = %d, want CommandRetries+1 = %d", got, retries+1)
	}
}

// Retryable replies rerouted through MultiDB keep submit order on the ordered
// async face: B applies the SETs in the order they were submitted. B's slow
// odd SETs reorder them when reroutes run in parallel.
func TestMultiDBFullDuplexRetryableReplyKeepsOrder(t *testing.T) {
	a, b := newFDStateServer(t), newFDStateServer(t)
	a.readOnly.Store(true)
	// A's replies come after every SET is submitted, faster than B applies
	// an odd SET.
	a.delay = 2 * time.Millisecond
	b.onSet = func(val string) {
		if n, _ := strconv.Atoi(val); n%2 == 1 {
			time.Sleep(50 * time.Millisecond)
		}
	}
	ap := newFDReplyRetryAP(t, true, 5, 1, a, b)
	ctx := context.Background()
	const n = 20
	cmds := make([]*StatusCmd, n)
	want := make([]string, n)
	for i := range cmds {
		want[i] = strconv.Itoa(i)
		cmds[i] = ap.Set(ctx, "k", i, 0)
	}
	for i, c := range cmds {
		if err := c.Err(); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	if got := b.appliedSets(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("B applied %v, want submit order %v", got, want)
	}
}

// A dropped session's tail, rerouted through MultiDB, waits for the earlier
// commands' reply reroutes: B still applies the SETs in submit order.
func TestMultiDBFullDuplexTailRerouteAfterReplyReroutes(t *testing.T) {
	a, b := newFDStateServer(t), newFDStateServer(t)
	a.readOnly.Store(true)
	a.dropAt.Store(5) // READONLY for 0..3, then the connection drops
	b.onSet = func(val string) {
		if n, _ := strconv.Atoi(val); n%2 == 1 {
			time.Sleep(50 * time.Millisecond)
		}
	}
	ap := newFDReplyRetryAP(t, true, 5, 1, a, b)
	ctx := context.Background()
	const n = 20
	cmds := make([]*StatusCmd, n)
	want := make([]string, n)
	for i := range cmds {
		want[i] = strconv.Itoa(i)
		cmds[i] = ap.Set(ctx, "k", i, 0)
	}
	for i, c := range cmds {
		if err := c.Err(); err != nil {
			t.Fatalf("Set %d: %v", i, err)
		}
	}
	if got := b.appliedSets(); fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("B applied %v, want submit order %v", got, want)
	}
}

// A retryable reply the engine cannot reroute because it is closing (a lane
// switch closes the child while its reader drains READONLY replies) fails
// with the reply's own error, reported to the owner once, not ErrClosed.
func TestFDOwnerRerouteClosingKeepsReplyError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	fd, _ := fdRerouteTestEngine(3, fdServeOK)
	fd.ap.ctx = ctx
	fd.ownerRetries = true
	fd.ownerRetryBudget = 3
	fd.retrySem = make(chan struct{}, 1)
	fd.retrySem <- struct{}{} // no free slot
	var outcomes []error
	fd.outcome = func(_ context.Context, cmd Cmder, err error, _ *fdFailGroup) {
		if err == nil {
			err = cmd.Err()
		}
		outcomes = append(outcomes, err)
	}
	cmd := NewStatusCmd(context.Background(), "set", "k", "v")
	cause := proto.RedisError("READONLY You can't write against a read only replica.")
	cmd.SetErr(cause)
	fd.rerouteReplyToOwner(fdReq{ctx: context.Background(), cmd: cmd, batch: newAPBatch(), attempts: 1, sent: true}, cause)
	fd.retryWg.Wait()
	if err := cmd.Err(); err == nil || !strings.HasPrefix(err.Error(), "READONLY") {
		t.Fatalf("cmd err = %v, want the READONLY reply", err)
	}
	if len(outcomes) != 1 || outcomes[0] == nil || !strings.HasPrefix(outcomes[0].Error(), "READONLY") {
		t.Fatalf("outcomes = %v, want one READONLY", outcomes)
	}
}

// Close with owner reroutes queued returns, and every queued command
// completes.
func TestMultiDBFullDuplexCloseDuringReroute(t *testing.T) {
	a, b := newFDStateServer(t), newFDStateServer(t)
	a.readOnly.Store(true)
	a.delay = 5 * time.Millisecond
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock() // a failure before the release must not hang cleanup
	first := make(chan struct{})
	var once sync.Once
	b.onSet = func(string) {
		once.Do(func() { close(first) })
		<-release
	}
	ap := newFDReplyRetryAP(t, true, 5, 1, a, b)
	ctx := context.Background()
	const n = 20
	cmds := make([]*StatusCmd, n)
	for i := range cmds {
		cmds[i] = ap.Set(ctx, "k", i, 0)
	}
	select {
	case <-first:
	case <-time.After(5 * time.Second):
		t.Fatal("no reroute reached B")
	}
	closed := make(chan error, 1)
	go func() { closed <- ap.Close() }()
	time.Sleep(50 * time.Millisecond)
	unblock()
	select {
	case <-closed:
	case <-time.After(10 * time.Second):
		t.Fatal("Close hung with reroutes queued")
	}
	for i, c := range cmds {
		done := make(chan struct{})
		go func() {
			_ = c.Err()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("Set %d never completed", i)
		}
	}
}
