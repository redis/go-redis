package redis

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	imultidb "github.com/redis/go-redis/v9/internal/multidb"
)

// countingDetector counts outcomes and can run a hook inside RecordFailure.
type countingDetector struct {
	failures, resets atomic.Int64
	onFailure        func()
}

func (d *countingDetector) RecordSuccess() {}
func (d *countingDetector) RecordFailure(error) {
	d.failures.Add(1)
	if f := d.onFailure; f != nil {
		d.onFailure = nil
		f()
	}
}
func (d *countingDetector) ShouldFailover() bool { return false }
func (d *countingDetector) Reset()               { d.failures.Store(0); d.resets.Add(1) }

// A stale write cleaned up after a reset must not move the window again: a
// recorder that already sampled the new window would be dropped, and under
// steady traffic each cleanup would invalidate the next recorder.
func TestMultiDBStaleDetectorCleanupKeepsWindow(t *testing.T) {
	d := &countingDetector{}
	c := &multidbCore{detector: d}
	stale := c.detectorGen.Load()
	var fresh uint64
	// A reset lands between the stale recorder's check and its write.
	d.onFailure = func() {
		c.resetDetectorSafely()
		fresh = c.detectorGen.Load()
	}
	c.recordDetectorFailure(stale, errors.New("stale"))
	if got := c.detectorGen.Load(); got != fresh {
		t.Fatalf("detector generation after the stale cleanup = %d, want %d (cleanup moved the window again)", got, fresh)
	}
	if n := d.failures.Load(); n != 0 {
		t.Fatalf("stale failure left in the fresh window: failures = %d, want 0", n)
	}
	c.recordDetectorFailure(fresh, errors.New("fresh"))
	if n := d.failures.Load(); n != 1 {
		t.Fatalf("failure recorded with the current generation = %d, want 1 (dropped as stale)", n)
	}
}

// ErrClusterTopologyUnhealthy means the active cluster cannot route the
// command after its topology-reload retries: an availability failure.
func TestClassifyOutcomeClusterTopologyUnhealthy(t *testing.T) {
	err := fmt.Errorf("%w: slot 1 master 127.0.0.1:7000 is fail", ErrClusterTopologyUnhealthy)
	if got := classifyOutcome(err, true); got != outcomeFailure {
		t.Errorf("classifyOutcome(ErrClusterTopologyUnhealthy) = %v, want outcomeFailure", got)
	}
}

// Close must not return while a concurrent RemoveDatabase is still draining
// and closing the member it detached.
func TestMultiDBCloseWaitsForDetachedRemoval(t *testing.T) {
	opts := &MultiDBOptions{
		HealthCheckInterval:  time.Hour,
		AutoFallbackInterval: -1,
		HealthCheckTimeout:   time.Second,
		Clients: []MultiDBClientConfig{
			{Options: &Options{Addr: "127.0.0.1:1"}, Weight: 2, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
			{Options: &Options{Addr: "127.0.0.1:2"}, Weight: 1, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mdb, err := NewMultiDBClient(ctx, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	db := mdb.core.dbs[1]
	entered := make(chan struct{})
	release := make(chan struct{})
	db.onRemove.register("test-block", func() error {
		close(entered)
		<-release
		return nil
	})

	removed := make(chan error, 1)
	go func() { removed <- mdb.RemoveDatabase(ctx, 1) }()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("RemoveDatabase never reached the member's remove hooks")
	}

	closed := make(chan error, 1)
	go func() { closed <- mdb.Close() }()
	select {
	case <-closed:
		close(release)
		t.Fatal("Close returned while RemoveDatabase was still closing the detached member")
	case <-time.After(200 * time.Millisecond):
	}
	close(release)
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Close did not return after the removal finished")
	}
	if err := <-removed; err != nil {
		t.Fatalf("RemoveDatabase: %v", err)
	}
	if err := db.c.Ping(context.Background()).Err(); !errors.Is(err, ErrClosed) {
		t.Fatalf("detached member client after Close: Ping = %v, want ErrClosed", err)
	}
}

// slowCheck is healthy but takes delay once armed.
type slowCheck struct {
	armed *atomic.Bool
	delay time.Duration
}

func (c slowCheck) wait(ctx context.Context) (bool, error) {
	if c.armed.Load() {
		select {
		case <-time.After(c.delay):
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
	return true, nil
}

func (c slowCheck) CheckHealth(ctx context.Context, _ *Client) (bool, error) { return c.wait(ctx) }
func (c slowCheck) CheckClusterHealth(ctx context.Context, _ *ClusterClient) (bool, error) {
	return c.wait(ctx)
}

// Slow passive members are probed concurrently: one health pass takes about the
// slowest probe, not their sum, so the active's next probe is not pushed back.
func TestMultiDBHealthPassProbesPassivesConcurrently(t *testing.T) {
	const delay = 400 * time.Millisecond
	var armed atomic.Bool
	slow := slowCheck{armed: &armed, delay: delay}
	opts := &MultiDBOptions{
		HealthCheckInterval:  time.Hour,
		AutoFallbackInterval: -1,
		HealthCheckTimeout:   2 * time.Second,
		Clients: []MultiDBClientConfig{
			{Options: &Options{Addr: "127.0.0.1:1"}, Weight: 3, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
			{Options: &Options{Addr: "127.0.0.1:2"}, Weight: 2, HealthChecks: []MultiDBHealthCheck{slow}},
			{Options: &Options{Addr: "127.0.0.1:3"}, Weight: 1, HealthChecks: []MultiDBHealthCheck{slow}},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mdb, err := NewMultiDBClient(ctx, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	t.Cleanup(func() { _ = mdb.Close() })

	armed.Store(true)
	start := time.Now()
	mdb.core.runHealthChecksOnce(ctx)
	if got := time.Since(start); got >= 2*delay-100*time.Millisecond {
		t.Fatalf("health pass with two %v passive probes took %v, want about %v (probed one after another)", delay, got, delay)
	}
}

// waitForStack waits until some goroutine's stack contains frame.
func waitForStack(t *testing.T, frame string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	buf := make([]byte, 1<<20)
	for time.Now().Before(deadline) {
		n := runtime.Stack(buf, true)
		if strings.Contains(string(buf[:n]), frame) {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("no goroutine reached %s", frame)
}

// lifecycleHook is a Hook whose process and pipeline stages call the given
// funcs (when set) before next.
type lifecycleHook struct {
	process  func(cmd Cmder)
	pipeline func(cmds []Cmder)
}

func (h lifecycleHook) DialHook(next DialHook) DialHook { return next }

func (h lifecycleHook) ProcessHook(next ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error {
		if h.process != nil {
			h.process(cmd)
		}
		return next(ctx, cmd)
	}
}

func (h lifecycleHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return func(ctx context.Context, cmds []Cmder) error {
		if h.pipeline != nil {
			h.pipeline(cmds)
		}
		return next(ctx, cmds)
	}
}

func newLifecycleTestClient(t *testing.T, tweak func(*MultiDBOptions), clients ...MultiDBClientConfig) *MultiDBClient {
	t.Helper()
	opts := &MultiDBOptions{
		HealthCheckInterval:  time.Hour,
		AutoFallbackInterval: -1,
		HealthCheckTimeout:   time.Second,
		Clients:              clients,
	}
	if tweak != nil {
		tweak(opts)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mdb, err := NewMultiDBClient(ctx, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	t.Cleanup(func() { _ = mdb.Close() })
	return mdb
}

// A hook added while RemoveDatabase removes the member must not be reported
// as installed: the member's client is closed and the hook never sees traffic.
func TestMultiDBAddDatabaseHookRacingRemoveReportsNotFound(t *testing.T) {
	mdb := newLifecycleTestClient(
		t, nil,
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:1"}, Weight: 2, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:2"}, Weight: 1, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
	)
	db := mdb.core.dbs[1]
	// Park AddDatabaseHook inside AddHook, after the member lookup.
	db.c.hooksMu.Lock()
	added := make(chan error, 1)
	go func() { added <- mdb.AddDatabaseHook(1, lifecycleHook{}) }()
	waitForStack(t, "(*hooksMixin).AddHook")

	removed := make(chan error, 1)
	go func() { removed <- mdb.RemoveDatabase(context.Background(), 1) }()
	var removeErr error
	select {
	case removeErr = <-removed:
	case <-time.After(3 * time.Second):
		db.c.hooksMu.Unlock()
		t.Fatal("RemoveDatabase did not return")
	}
	db.c.hooksMu.Unlock()
	if removeErr != nil {
		t.Fatalf("RemoveDatabase: %v", removeErr)
	}
	if err := <-added; !errors.Is(err, ErrDatabaseNotFound) {
		t.Fatalf("AddDatabaseHook on a member removed during the call = %v, want ErrDatabaseNotFound", err)
	}
}

// When the gate rejects the active and nothing replaces it, the cluster
// control commands fail with the gate error. They must not fall back to the
// generic MultiDB command path, which gates again and, if a cluster member
// became active meanwhile, sends the command to one shard only.
func TestMultiDBClusterControlCommandsGateErrorSkipsGenericPath(t *testing.T) {
	mdb := newLifecycleTestClient(
		t, nil,
		MultiDBClientConfig{ClusterOptions: &ClusterOptions{Addrs: []string{"127.0.0.1:1"}}, Weight: 1, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
	)
	var generic atomic.Int32
	mdb.AddHook(lifecycleHook{process: func(Cmder) { generic.Add(1) }})
	mdb.core.dbs[0].cb.ForceOpen()

	ctx := context.Background()
	for _, tc := range []struct {
		name string
		run  func() Cmder
	}{
		{"DBSize", func() Cmder { return mdb.DBSize(ctx) }},
		{"ScriptLoad", func() Cmder { return mdb.ScriptLoad(ctx, "return 1") }},
		{"ScriptFlush", func() Cmder { return mdb.ScriptFlush(ctx) }},
		{"ScriptExists", func() Cmder { return mdb.ScriptExists(ctx, "abc") }},
	} {
		generic.Store(0)
		err := tc.run().Err()
		if !errors.Is(err, ErrTemporarilyNotAvailable) && !errors.Is(err, ErrPermanentlyNotAvailable) {
			t.Errorf("%s error = %v, want the gate's unavailability verdict", tc.name, err)
		}
		if n := generic.Load(); n != 0 {
			t.Errorf("%s ran %d time(s) on the generic MultiDB command path after the gate failed", tc.name, n)
		}
	}
}

// A passive probe cut short by the pass context (Close stops the loop this
// way) is not a health verdict and must not be recorded on the breaker.
func TestMultiDBCanceledPassiveProbeNotRecorded(t *testing.T) {
	var armed atomic.Bool
	slow := slowCheck{armed: &armed, delay: time.Minute}
	// Threshold 1: a single recorded probe failure opens the breaker.
	mdb := newLifecycleTestClient(
		t, func(o *MultiDBOptions) {
			o.CircuitBreakerConfig = &MultiDBCircuitBreakerConfig{FailureThreshold: 1}
		},
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:1"}, Weight: 2, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:2"}, Weight: 1, HealthChecks: []MultiDBHealthCheck{slow}},
	)
	passive := mdb.core.dbs[1]
	armed.Store(true)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		mdb.core.runHealthChecksOnce(ctx)
	}()
	waitForStack(t, "slowCheck.wait")
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("health pass did not return after cancel")
	}
	if st := passive.cb.State(); st != imultidb.CircuitClosed {
		t.Fatalf("passive breaker after a canceled probe = %v, want closed", st)
	}
}

// gatedCheck reports healthy once released; it blocks until then or until
// ctx ends.
type gatedCheck struct {
	entered chan struct{}
	release chan struct{}
}

func (c gatedCheck) wait(ctx context.Context) (bool, error) {
	select {
	case c.entered <- struct{}{}:
	default:
	}
	select {
	case <-c.release:
		return true, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}

func (c gatedCheck) CheckHealth(ctx context.Context, _ *Client) (bool, error) { return c.wait(ctx) }
func (c gatedCheck) CheckClusterHealth(ctx context.Context, _ *ClusterClient) (bool, error) {
	return c.wait(ctx)
}

// Once Close has started, an AddDatabase still in flight must not succeed:
// Close would tear the member down and the caller would hold a dead id. Close
// is held in its autopipeliner drain, before core.close runs.
func TestMultiDBAddDatabaseDuringCloseDrainFails(t *testing.T) {
	// One dial attempt: the drained command fails fast once released.
	mdb := newLifecycleTestClient(
		t, nil,
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:1", DialerRetries: 1}, Weight: 1, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
	)
	inHook := make(chan struct{}, 1)
	releaseHook := make(chan struct{})
	block := func() {
		select {
		case inHook <- struct{}{}:
		default:
		}
		<-releaseHook
	}
	mdb.AddHook(lifecycleHook{process: func(Cmder) { block() }, pipeline: func([]Cmder) { block() }})
	ap, err := mdb.AutoPipeline()
	if err != nil {
		t.Fatalf("AutoPipeline: %v", err)
	}
	go func() { _ = ap.Do(context.Background(), "get", "k") }()
	select {
	case <-inHook:
	case <-time.After(3 * time.Second):
		t.Fatal("autopipeline batch never reached the pipeline hook")
	}

	check := gatedCheck{entered: make(chan struct{}, 1), release: make(chan struct{})}
	added := make(chan error, 1)
	go func() {
		_, err := mdb.AddDatabase(context.Background(), MultiDBClientConfig{
			Options: &Options{Addr: "127.0.0.1:2"}, Weight: 1, HealthChecks: []MultiDBHealthCheck{check},
		})
		added <- err
	}()
	select {
	case <-check.entered:
	case <-time.After(3 * time.Second):
		close(releaseHook)
		t.Fatal("AddDatabase never ran its health check")
	}

	closed := make(chan error, 1)
	go func() { closed <- mdb.Close() }()
	deadline := time.Now().Add(3 * time.Second)
	for {
		// closing, not autopipelinerClosed: Close sets closing one statement
		// after it releases autopipelinerMu.
		if mdb.core.closing.Load() {
			break
		}
		if time.Now().After(deadline) {
			close(releaseHook)
			t.Fatal("Close never started")
		}
		time.Sleep(time.Millisecond)
	}
	if mdb.core.closed.Load() {
		close(releaseHook)
		t.Fatal("core closed before the drain finished: Close is not held in the drain")
	}
	close(check.release)
	var addErr error
	select {
	case addErr = <-added:
	case <-time.After(3 * time.Second):
		close(releaseHook)
		t.Fatal("AddDatabase did not return")
	}
	close(releaseHook)
	if !errors.Is(addErr, ErrClosed) {
		t.Errorf("AddDatabase that finished while Close drained = %v, want ErrClosed", addErr)
	}
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not return")
	}
}

// closingStrategy runs onSelect and selects nothing.
type closingStrategy struct{ onSelect func() }

func (s closingStrategy) Select([]MultiDBDatabaseState) int {
	s.onSelect()
	return -1
}

// A failover that finds no candidate because Close drained the membership
// reports ErrClosed, not an availability error, and charges no attempt.
func TestMultiDBFailoverNoCandidateAfterCloseReportsClosed(t *testing.T) {
	mdb := newLifecycleTestClient(
		t, nil,
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:1"}, Weight: 2, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:2"}, Weight: 1, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
	)
	core := mdb.core
	// Close sets closed without failoverMu; simulate it landing while the
	// failover runs.
	core.strategy = closingStrategy{onSelect: func() { core.closed.Store(true) }}
	// Let the cleanup Close run for real.
	t.Cleanup(func() { core.closed.Store(false) })
	from := int(core.active.Load())
	core.dbs[from].cb.ForceOpen()
	before := core.failoverAttempts
	err := core.tryFailover(context.Background(), from)
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("tryFailover after close = %v, want ErrClosed", err)
	}
	if core.failoverAttempts != before {
		t.Fatalf("failoverAttempts = %d, want %d (no attempt charged after close)", core.failoverAttempts, before)
	}
}

// canPipeline must not hold dbMu while a cluster member resolves routing:
// before the first live metadata fetch completes that waits for the fetch,
// and every control operation would queue behind it.
func TestMultiDBCanPipelineDoesNotHoldDBMuDuringResolve(t *testing.T) {
	mdb := newLifecycleTestClient(
		t, nil,
		MultiDBClientConfig{ClusterOptions: &ClusterOptions{Addrs: []string{"127.0.0.1:1"}}, Weight: 1, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
	)
	cc := mdb.core.dbs[0].cc
	cc.cmdMeta.stopAndJoin()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	cc.cmdMeta = newCommandMetadataStoreForLive(nil, func(ctx context.Context) (commandMetadataFetchResult, error) {
		select {
		case entered <- struct{}{}:
		default:
		}
		select {
		case <-release:
		case <-ctx.Done():
		}
		return commandMetadataFetchResult{}, errors.New("metadata unavailable")
	})
	cc.SetCommandInfoResolver(cc.NewDynamicResolver())

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = mdb.canPipeline(ctx, NewStringCmd(ctx, "get", "k"))
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("canPipeline never started the metadata fetch")
	}
	select {
	case <-done:
		t.Fatal("canPipeline returned before the metadata fetch finished: the test no longer exercises the wait")
	default:
	}
	locked := mdb.core.dbMu.TryLock()
	if locked {
		mdb.core.dbMu.Unlock()
	}
	close(release)
	<-done
	if !locked {
		t.Fatal("dbMu was held while canPipeline waited on the member's metadata fetch")
	}
}
