package redis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
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
			{Options: &Options{Addr: "127.0.0.1:1"}, Weight: 0.75, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
			{Options: &Options{Addr: "127.0.0.1:2"}, Weight: 0.5, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
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
	oneProbe
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
			{Options: &Options{Addr: "127.0.0.1:1"}, Weight: 1.0, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
			{Options: &Options{Addr: "127.0.0.1:2"}, Weight: 0.75, HealthChecks: []MultiDBHealthCheck{slow}},
			{Options: &Options{Addr: "127.0.0.1:3"}, Weight: 0.5, HealthChecks: []MultiDBHealthCheck{slow}},
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
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:1"}, Weight: 0.75, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:2"}, Weight: 0.5, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
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
		MultiDBClientConfig{ClusterOptions: &ClusterOptions{Addrs: []string{"127.0.0.1:1"}}, Weight: 0.5, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
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
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:1"}, Weight: 0.75, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:2"}, Weight: 0.5, HealthChecks: []MultiDBHealthCheck{slow}},
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
	oneProbe
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
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:1", DialerRetries: 1}, Weight: 0.5, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
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
			Options: &Options{Addr: "127.0.0.1:2"}, Weight: 0.5, HealthChecks: []MultiDBHealthCheck{check},
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
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:1"}, Weight: 0.75, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:2"}, Weight: 0.5, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
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
		MultiDBClientConfig{ClusterOptions: &ClusterOptions{Addrs: []string{"127.0.0.1:1"}}, Weight: 0.5, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
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

// CommandRetries follows Options.MaxRetries: 0 is the default (2), -1 is no
// retries, and any other negative value is rejected.
func TestMultiDBCommandRetriesConvention(t *testing.T) {
	member := []MultiDBClientConfig{{Options: &Options{Addr: "127.0.0.1:1"}}}
	for _, tc := range []struct {
		in, want int
		wantErr  bool
	}{
		{in: 0, want: 2},
		{in: -1, want: 0},
		{in: 3, want: 3},
		{in: -2, wantErr: true},
	} {
		opts := &MultiDBOptions{Clients: member, CommandRetries: tc.in}
		err := opts.init()
		if tc.wantErr {
			if err == nil {
				t.Errorf("CommandRetries %d: init() = nil, want a validation error", tc.in)
			}
			continue
		}
		if err != nil {
			t.Errorf("CommandRetries %d: init() = %v", tc.in, err)
			continue
		}
		if opts.CommandRetries != tc.want {
			t.Errorf("CommandRetries %d normalized to %d, want %d", tc.in, opts.CommandRetries, tc.want)
		}
	}
}

// downCheck always reports the member unhealthy.
type downCheck struct{ oneProbe }

func (downCheck) CheckHealth(context.Context, *Client) (bool, error) {
	return false, errors.New("down")
}

func (downCheck) CheckClusterHealth(context.Context, *ClusterClient) (bool, error) {
	return false, errors.New("down")
}

// Weight is -1 (disabled), 0 (default 1.0), or in (0, 1]. Anything else is
// rejected at construction and by SetWeight.
func TestMultiDBWeightValidation(t *testing.T) {
	for _, w := range []float64{-1, 0, 0.25, 1} {
		cfg := MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:1"}, Weight: w}
		if err := cfg.validate(); err != nil {
			t.Errorf("Weight %v: validate() = %v, want nil", w, err)
		}
	}
	for _, w := range []float64{-2, -0.5, 1.5, 2, math.NaN(), math.Inf(1), math.Inf(-1)} {
		cfg := MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:1"}, Weight: w}
		if err := cfg.validate(); err == nil {
			t.Errorf("Weight %v: validate() = nil, want an error", w)
		}
	}

	mdb := newLifecycleTestClient(
		t, nil,
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:1"}, Weight: 0.5, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:2"}, Weight: 0.25, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
	)
	for _, w := range []float64{-2, 1.5, math.NaN(), math.Inf(1)} {
		if err := mdb.SetWeight(1, w); err == nil {
			t.Errorf("SetWeight(1, %v) = nil, want an error", w)
		}
	}
	if err := mdb.SetWeight(1, 0); err != nil {
		t.Fatalf("SetWeight(1, 0) = %v", err)
	}
	if w := mdb.core.dbByID(1).weight; w != defaultMultiDBWeight {
		t.Errorf("SetWeight(1, 0) stored %v, want the default %v", w, defaultMultiDBWeight)
	}
	if err := mdb.SetWeight(0, -1); err == nil {
		t.Error("SetWeight(active, -1) = nil, want an error (disable the active only after switching away)")
	}
	if err := mdb.SetWeight(1, -1); err != nil {
		t.Errorf("SetWeight(1, -1) = %v, want nil", err)
	}
}

// A disabled member (Weight -1) is never picked by initial selection or
// failover, and SetActiveDatabase refuses it. ForceActiveDatabase still
// works.
func TestMultiDBDisabledMemberNeverSelected(t *testing.T) {
	ctx := context.Background()
	// Only the disabled member is healthy: there is nothing to start on.
	_, err := NewMultiDBClient(ctx, &MultiDBOptions{
		HealthCheckInterval:  time.Hour,
		AutoFallbackInterval: -1,
		InitialDBState:       InitialDBStateOneAvailable,
		Clients: []MultiDBClientConfig{
			{Options: &Options{Addr: "127.0.0.1:1"}, Weight: -1, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
			{Options: &Options{Addr: "127.0.0.1:2"}, Weight: 0.25, HealthChecks: []MultiDBHealthCheck{downCheck{}}},
		},
	})
	if !errors.Is(err, ErrInsufficientHealthyDatabases) {
		t.Fatalf("NewMultiDBClient = %v, want ErrInsufficientHealthyDatabases (the disabled member must not count)", err)
	}

	// Every member disabled: rejected up front.
	_, err = NewMultiDBClient(ctx, &MultiDBOptions{
		Clients: []MultiDBClientConfig{
			{Options: &Options{Addr: "127.0.0.1:1"}, Weight: -1, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
		},
	})
	if err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("all members disabled: err = %v, want a 'disabled' validation error", err)
	}

	mdb := newLifecycleTestClient(
		t, nil,
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:1"}, Weight: 0.25, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:2"}, Weight: -1, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
	)
	if got := mdb.ActiveDatabaseID(); got != 0 {
		t.Fatalf("active = %d, want 0", got)
	}
	for _, c := range mdb.core.candidates(0) {
		if c.ID == 1 {
			t.Errorf("disabled member offered as a failover candidate: %+v", c)
		}
	}
	mdb.core.dbByID(0).cb.ForceOpen()
	_ = mdb.core.tryFailover(ctx, 0)
	if got := mdb.ActiveDatabaseID(); got == 1 {
		t.Error("automatic failover selected the disabled member")
	}
	if err := mdb.SetActiveDatabase(ctx, 1); !errors.Is(err, ErrDatabaseDisabled) {
		t.Errorf("SetActiveDatabase(disabled) = %v, want ErrDatabaseDisabled", err)
	}
	if err := mdb.ForceActiveDatabase(ctx, 1); err != nil {
		t.Errorf("ForceActiveDatabase(disabled) = %v, want nil", err)
	}
}

// Auto-fallback skips a disabled member even when its old weight was the
// highest: it moves to the best enabled member instead.
func TestMultiDBFallbackSkipsDisabledMember(t *testing.T) {
	ctx := context.Background()
	mdb := newLifecycleTestClient(
		t, nil,
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:1"}, Weight: 1, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:2"}, Weight: 0.25, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:3"}, Weight: 0.5, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
	)
	if err := mdb.ForceActiveDatabase(ctx, 1); err != nil {
		t.Fatalf("ForceActiveDatabase(1) = %v", err)
	}
	if err := mdb.SetWeight(0, -1); err != nil {
		t.Fatalf("SetWeight(0, -1) = %v", err)
	}
	mdb.SetAutoFallback(true)
	mdb.core.tryFallbackToPrimary(ctx)
	if got := mdb.ActiveDatabaseID(); got != 2 {
		t.Fatalf("active after fallback = %d, want 2 (0 is disabled)", got)
	}
}

// countingHook answers every command locally (nil or err) and counts them.
type countingHook struct {
	n   *atomic.Int64
	err error
}

func (h countingHook) DialHook(next DialHook) DialHook { return next }
func (h countingHook) ProcessHook(ProcessHook) ProcessHook {
	return func(_ context.Context, cmd Cmder) error {
		h.n.Add(1)
		cmd.SetErr(h.err)
		return h.err
	}
}

func (h countingHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}

// FailureDetectorConfig is validated: 0 ignores one condition, both 0 is
// rejected, and it cannot be combined with a custom FailureDetector.
func TestMultiDBFailureDetectorConfigValidation(t *testing.T) {
	member := []MultiDBClientConfig{{Options: &Options{Addr: "127.0.0.1:1"}}}
	for _, tc := range []struct {
		name     string
		cfg      *MultiDBFailureDetectorConfig
		detector MultiDBFailureDetector
		ok       bool
	}{
		{"nil", nil, nil, true},
		{"count only", &MultiDBFailureDetectorConfig{MinNumFailures: 5}, nil, true},
		{"rate only", &MultiDBFailureDetectorConfig{FailureRateThreshold: 0.5}, nil, true},
		{"both", &MultiDBFailureDetectorConfig{MinNumFailures: 5, FailureRateThreshold: 1, Window: time.Second}, nil, true},
		{"both zero", &MultiDBFailureDetectorConfig{}, nil, false},
		{"negative count", &MultiDBFailureDetectorConfig{MinNumFailures: -1, FailureRateThreshold: 0.1}, nil, false},
		{"rate above 1", &MultiDBFailureDetectorConfig{FailureRateThreshold: 1.5}, nil, false},
		{"negative rate", &MultiDBFailureDetectorConfig{MinNumFailures: 1, FailureRateThreshold: -0.1}, nil, false},
		{"NaN rate", &MultiDBFailureDetectorConfig{MinNumFailures: 1, FailureRateThreshold: math.NaN()}, nil, false},
		{"negative window", &MultiDBFailureDetectorConfig{MinNumFailures: 1, Window: -time.Second}, nil, false},
		{"with custom detector", &MultiDBFailureDetectorConfig{MinNumFailures: 1}, &countingDetector{}, false},
	} {
		opts := &MultiDBOptions{Clients: member, FailureDetectorConfig: tc.cfg, FailureDetector: tc.detector}
		err := opts.init()
		if tc.ok && err != nil {
			t.Errorf("%s: init() = %v, want nil", tc.name, err)
		}
		if !tc.ok && err == nil {
			t.Errorf("%s: init() = nil, want a validation error", tc.name)
		}
	}
}

// The default detector honors FailureDetectorConfig, including the 0 =
// "ignore this condition" semantics and the window.
func TestMultiDBFailureDetectorConfigHonored(t *testing.T) {
	newDetector := func(cfg *MultiDBFailureDetectorConfig) MultiDBFailureDetector {
		opts := &MultiDBOptions{
			Clients:               []MultiDBClientConfig{{Options: &Options{Addr: "127.0.0.1:1"}}},
			FailureDetectorConfig: cfg,
		}
		if err := opts.init(); err != nil {
			t.Fatalf("init: %v", err)
		}
		return newMultidbCore(opts).detector
	}
	record := func(d MultiDBFailureDetector, failures, successes int) {
		for i := 0; i < failures; i++ {
			d.RecordFailure(io.EOF)
		}
		for i := 0; i < successes; i++ {
			d.RecordSuccess()
		}
	}

	// Count only: 2 failures trip at any rate.
	d := newDetector(&MultiDBFailureDetectorConfig{MinNumFailures: 2})
	record(d, 2, 100)
	if !d.ShouldFailover() {
		t.Error("count-only detector: 2 failures in 102 did not trip")
	}
	// Rate only: 50% trips with a single failure.
	d = newDetector(&MultiDBFailureDetectorConfig{FailureRateThreshold: 0.5})
	record(d, 1, 1)
	if !d.ShouldFailover() {
		t.Error("rate-only detector: 1 of 2 failed did not trip")
	}
	d = newDetector(&MultiDBFailureDetectorConfig{FailureRateThreshold: 0.5})
	record(d, 1, 3)
	if d.ShouldFailover() {
		t.Error("rate-only detector: 1 of 4 failed tripped a 50% threshold")
	}
	// Defaults (1000 failures) do not trip on 2.
	d = newDetector(nil)
	record(d, 2, 0)
	if d.ShouldFailover() {
		t.Error("default detector tripped on 2 failures")
	}
	// Window: outcomes age out.
	d = newDetector(&MultiDBFailureDetectorConfig{MinNumFailures: 1, Window: 50 * time.Millisecond})
	record(d, 1, 0)
	if !d.ShouldFailover() {
		t.Fatal("1-failure detector did not trip")
	}
	time.Sleep(150 * time.Millisecond)
	if d.ShouldFailover() {
		t.Error("failure outlived a 50ms window")
	}
}

// A low detector threshold fails a command over to the next member inside
// the same command, before the breaker (threshold 5) opens.
func TestMultiDBFailureDetectorConfigTripsFailover(t *testing.T) {
	var aCalls, bCalls atomic.Int64
	mdb := newLifecycleTestClient(
		t, func(o *MultiDBOptions) {
			o.FailureDetectorConfig = &MultiDBFailureDetectorConfig{MinNumFailures: 1}
		},
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:1"}, Weight: 1, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:2"}, Weight: 0.5, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
	)
	if err := mdb.AddDatabaseHook(0, countingHook{n: &aCalls, err: io.EOF}); err != nil {
		t.Fatal(err)
	}
	if err := mdb.AddDatabaseHook(1, countingHook{n: &bCalls}); err != nil {
		t.Fatal(err)
	}
	if err := mdb.Get(context.Background(), "k").Err(); err != nil {
		t.Fatalf("GET = %v, want the retry to succeed on member 1", err)
	}
	if a, b := aCalls.Load(), bCalls.Load(); a != 1 || b != 1 {
		t.Errorf("calls: member 0 = %d, member 1 = %d; want 1 and 1", a, b)
	}
	if got := mdb.ActiveDatabaseID(); got != 1 {
		t.Errorf("active = %d, want 1", got)
	}
}

// getCountingDialer fakes one server per address over a loopback TCP pair. It
// answers HELLO with an empty map and counts every GET it reads. A GET to an
// address in dead closes the connection (the client reads EOF); otherwise it
// answers nil.
type getCountingDialer struct {
	dead map[string]bool
	gets sync.Map // addr -> *atomic.Int64
}

func (d *getCountingDialer) count(addr string) int64 {
	v, _ := d.gets.LoadOrStore(addr, new(atomic.Int64))
	return v.(*atomic.Int64).Load()
}

func (d *getCountingDialer) dial(ctx context.Context, _, addr string) (net.Conn, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	defer ln.Close()
	accepted := make(chan net.Conn, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			c = nil
		}
		accepted <- c
	}()
	var dialer net.Dialer
	c1, err := dialer.DialContext(ctx, "tcp", ln.Addr().String())
	if err != nil {
		return nil, err
	}
	c2 := <-accepted
	if c2 == nil {
		_ = c1.Close()
		return nil, errors.New("accept failed")
	}
	v, _ := d.gets.LoadOrStore(addr, new(atomic.Int64))
	gets := v.(*atomic.Int64)
	go func() {
		defer c2.Close()
		rd := bufio.NewReader(c2)
		for {
			line, err := rd.ReadString('\n')
			if err != nil {
				return
			}
			if len(line) == 0 || line[0] != '*' {
				continue
			}
			n, _ := strconv.Atoi(strings.TrimSpace(line[1:]))
			var args []string
			for i := 0; i < n; i++ {
				if _, err := rd.ReadString('\n'); err != nil {
					return
				}
				arg, err := rd.ReadString('\n')
				if err != nil {
					return
				}
				args = append(args, strings.TrimSpace(arg))
			}
			reply := "+OK\r\n"
			switch {
			case len(args) > 0 && strings.EqualFold(args[0], "hello"):
				reply = "%0\r\n"
			case len(args) > 0 && strings.EqualFold(args[0], "get"):
				gets.Add(1)
				if d.dead[addr] {
					return
				}
				reply = "_\r\n"
			}
			if _, err := c2.Write([]byte(reply)); err != nil {
				return
			}
		}
	}()
	return c1, nil
}

func newGetCountingClient(t *testing.T, d *getCountingDialer, tweak func(*MultiDBOptions)) *MultiDBClient {
	member := func(addr string, weight float64) MultiDBClientConfig {
		return MultiDBClientConfig{
			Options:      &Options{Addr: addr, Dialer: d.dial, DisableIdentity: true},
			Weight:       weight,
			HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}},
		}
	}
	return newLifecycleTestClient(t, tweak, member("a:1", 1), member("b:1", 0.5))
}

// MultiDB owns retries: a standalone member left at MaxRetries 0 gets no
// member-level retries, so a dead active sees CommandRetries+1 sends per
// command, each one counted by the breaker and detector.
func TestMultiDBMemberRetriesDisabledByDefault(t *testing.T) {
	d := &getCountingDialer{dead: map[string]bool{"a:1": true}}
	mdb := newGetCountingClient(t, d, nil)
	err := mdb.Get(context.Background(), "k").Err()
	if !errors.Is(err, io.EOF) {
		t.Fatalf("GET = %v, want EOF from the dead active", err)
	}
	if a, b := d.count("a:1"), d.count("b:1"); a != 3 || b != 0 {
		t.Errorf("GET sends: a = %d, b = %d; want 3 (CommandRetries+1) and 0", a, b)
	}
	if got := mdb.core.dbByID(0).cb.Stats().Failures; got != 3 {
		t.Errorf("breaker failures = %d, want 3 (one per send)", got)
	}
}

// A retry goes to the member selected at that moment: once the breaker opens,
// the next attempt of the same command lands on the new active.
func TestMultiDBRetryLandsOnNewActive(t *testing.T) {
	d := &getCountingDialer{dead: map[string]bool{"a:1": true}}
	mdb := newGetCountingClient(t, d, func(o *MultiDBOptions) {
		o.CircuitBreakerConfig = &MultiDBCircuitBreakerConfig{FailureThreshold: 2, SuccessThreshold: 1, GracePeriod: time.Minute}
	})
	err := mdb.Get(context.Background(), "k").Err()
	if err != Nil {
		t.Fatalf("GET = %v, want redis.Nil from member b", err)
	}
	if a, b := d.count("a:1"), d.count("b:1"); a != 2 || b != 1 {
		t.Errorf("GET sends: a = %d, b = %d; want 2 and 1", a, b)
	}
	if got := mdb.ActiveDatabaseID(); got != 1 {
		t.Errorf("active = %d, want 1", got)
	}
}

// An explicit member MaxRetries is kept.
func TestMultiDBExplicitMemberMaxRetriesKept(t *testing.T) {
	d := &getCountingDialer{dead: map[string]bool{"a:1": true}}
	mdb := newLifecycleTestClient(t, func(o *MultiDBOptions) { o.CommandRetries = -1 },
		MultiDBClientConfig{
			Options:      &Options{Addr: "a:1", Dialer: d.dial, DisableIdentity: true, MaxRetries: 1, MinRetryBackoff: -1},
			HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}},
		})
	_ = mdb.Get(context.Background(), "k").Err()
	if a := d.count("a:1"); a != 2 {
		t.Errorf("GET sends = %d, want 2 (MaxRetries 1 kept)", a)
	}
}

// probeCountingCheck counts probes; fail makes the nth probe (1-based) fail,
// block makes every probe wait for ctx.
type probeCountingCheck struct {
	n     atomic.Int64
	fail  int64
	block bool
}

func (c *probeCountingCheck) probe(ctx context.Context) (bool, error) {
	if c.n.Add(1) == c.fail {
		return false, errors.New("probe failed")
	}
	if c.block {
		<-ctx.Done()
		return false, ctx.Err()
	}
	return true, nil
}

func (c *probeCountingCheck) CheckHealth(ctx context.Context, _ *Client) (bool, error) {
	return c.probe(ctx)
}

func (c *probeCountingCheck) CheckClusterHealth(ctx context.Context, _ *ClusterClient) (bool, error) {
	return c.probe(ctx)
}

// configuredCheck is a probeCountingCheck with its own probe config.
type configuredCheck struct {
	probeCountingCheck
	cfg MultiDBHealthCheckConfig
}

func (c *configuredCheck) Config() MultiDBHealthCheckConfig { return c.cfg }

// The default policy probes a check without Config() 3 times, 500ms apart
// (HEALTHY_ALL).
func TestMultiDBDefaultPolicyProbesThreeTimes(t *testing.T) {
	hc := &probeCountingCheck{}
	start := time.Now()
	if !(defaultMultiDBPolicy{}).Execute(context.Background(), []MultiDBHealthCheck{hc}, nil) {
		t.Fatal("default policy failed a passing check")
	}
	if n := hc.n.Load(); n != 3 {
		t.Errorf("probes = %d, want 3", n)
	}
	if elapsed := time.Since(start); elapsed < 900*time.Millisecond {
		t.Errorf("3 probes took %v, want about 2 x 500ms", elapsed)
	}

	// One failed probe fails the check and stops probing.
	hc = &probeCountingCheck{fail: 1}
	if (defaultMultiDBPolicy{}).ExecuteCluster(context.Background(), []MultiDBHealthCheck{hc}, nil) {
		t.Error("default policy passed a check whose first probe failed")
	}
	if n := hc.n.Load(); n != 1 {
		t.Errorf("probes after a failure = %d, want 1", n)
	}
}

// The default policy honors a check's Config(): Probes, Delay and Timeout.
func TestMultiDBDefaultPolicyHonorsCheckConfig(t *testing.T) {
	hc := &configuredCheck{cfg: MultiDBHealthCheckConfig{Probes: 5, Delay: time.Millisecond, Timeout: time.Second}}
	if !(defaultMultiDBPolicy{}).Execute(context.Background(), []MultiDBHealthCheck{hc}, nil) {
		t.Fatal("default policy failed a passing check")
	}
	if n := hc.n.Load(); n != 5 {
		t.Errorf("probes = %d, want 5", n)
	}

	hc = &configuredCheck{cfg: MultiDBHealthCheckConfig{Probes: 5, Delay: time.Millisecond, Timeout: time.Second}}
	hc.fail = 2
	if (defaultMultiDBPolicy{}).Execute(context.Background(), []MultiDBHealthCheck{hc}, nil) {
		t.Error("default policy passed a check whose 2nd probe failed")
	}

	hc = &configuredCheck{cfg: MultiDBHealthCheckConfig{Probes: 1, Timeout: 50 * time.Millisecond}}
	hc.block = true
	start := time.Now()
	if (defaultMultiDBPolicy{}).Execute(context.Background(), []MultiDBHealthCheck{hc}, nil) {
		t.Error("default policy passed a check that never answered")
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("check Timeout 50ms not honored: took %v", elapsed)
	}
}

// newDefaultChecksClient builds a two-member client on the test Redis with
// the default checks (3 probes 500ms apart) and the default timeout.
func newDefaultChecksClient(t *testing.T, tweak func(*MultiDBOptions)) *MultiDBClient {
	t.Helper()
	probe := NewClient(&Options{Addr: internalTestRedisAddr()})
	err := probe.Ping(context.Background()).Err()
	_ = probe.Close()
	if err != nil {
		t.Skipf("no redis on %s: %v", internalTestRedisAddr(), err)
	}
	opts := &MultiDBOptions{
		HealthCheckInterval:  time.Hour,
		AutoFallbackInterval: -1,
		Clients: []MultiDBClientConfig{
			{Options: &Options{Addr: internalTestRedisAddr()}, Weight: 1},
			{Options: &Options{Addr: internalTestRedisAddr()}, Weight: 0.5},
		},
	}
	if tweak != nil {
		tweak(opts)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	mdb, err := NewMultiDBClient(ctx, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	t.Cleanup(func() { _ = mdb.Close() })
	return mdb
}

// SetActiveDatabase gates on one fresh probe under the default policy, not
// the 3-probe pass the background loop runs.
func TestMultiDBSetActiveDatabaseSingleProbe(t *testing.T) {
	mdb := newDefaultChecksClient(t, nil)
	start := time.Now()
	if err := mdb.SetActiveDatabase(context.Background(), 1); err != nil {
		t.Fatalf("SetActiveDatabase(1) = %v", err)
	}
	if took := time.Since(start); took >= 500*time.Millisecond {
		t.Fatalf("SetActiveDatabase took %v, want < 500ms (one probe)", took)
	}
	if got := mdb.ActiveDatabaseID(); got != 1 {
		t.Fatalf("active = %d, want 1", got)
	}
}

// ProbeTargetBeforeFailover runs one probe under the default policy, so a
// command deadline below the 3-probe pass still fails over.
func TestMultiDBProbeTargetBeforeFailoverSingleProbe(t *testing.T) {
	mdb := newDefaultChecksClient(t, func(o *MultiDBOptions) { o.ProbeTargetBeforeFailover = true })
	mdb.core.dbByID(0).cb.ForceOpen()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := mdb.core.tryFailover(ctx, 0); err != nil {
		t.Fatalf("tryFailover with a 300ms deadline = %v, want nil", err)
	}
	if got := mdb.ActiveDatabaseID(); got != 1 {
		t.Fatalf("active = %d, want 1", got)
	}
}

// gateCheck blocks its first probe after arming until release is closed.
type gateCheck struct {
	oneProbe
	armed   atomic.Bool
	entered chan struct{}
	release chan struct{}
}

func newGateCheck() *gateCheck {
	return &gateCheck{entered: make(chan struct{}), release: make(chan struct{})}
}

func (g *gateCheck) wait() {
	if g.armed.CompareAndSwap(true, false) {
		close(g.entered)
		<-g.release
	}
}

func (g *gateCheck) CheckHealth(context.Context, *Client) (bool, error) {
	g.wait()
	return true, nil
}

func (g *gateCheck) CheckClusterHealth(context.Context, *ClusterClient) (bool, error) {
	g.wait()
	return true, nil
}

// SetWeight(id, -1) racing SetActiveDatabase(id) mid-probe waits for the
// switch, which commits; SetWeight is then rejected because id is active.
// The switch never lands on a disabled member.
func TestMultiDBSetWeightDisableRacesSetActive(t *testing.T) {
	gate := newGateCheck()
	mdb := newLifecycleTestClient(
		t, nil,
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:1"}, Weight: 1, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:2"}, Weight: 0.5, HealthChecks: []MultiDBHealthCheck{gate}},
	)
	gate.armed.Store(true)
	switchErr := make(chan error, 1)
	go func() { switchErr <- mdb.SetActiveDatabase(context.Background(), 1) }()
	<-gate.entered

	weightErr := make(chan error, 1)
	go func() { weightErr <- mdb.SetWeight(1, -1) }()
	select {
	case err := <-weightErr:
		t.Errorf("SetWeight(1, -1) returned %v while the switch to 1 was probing, want it to wait", err)
		weightErr <- err
	case <-time.After(100 * time.Millisecond):
	}
	close(gate.release)

	if err := <-switchErr; err != nil {
		t.Fatalf("SetActiveDatabase(1) = %v, want nil", err)
	}
	if err := <-weightErr; err == nil {
		t.Error("SetWeight(1, -1) = nil, want an error (1 is active)")
	}
	if got := mdb.ActiveDatabaseID(); got != 1 {
		t.Fatalf("active = %d, want 1", got)
	}
	mdb.core.dbMu.RLock()
	w := mdb.core.dbs[1].weight
	mdb.core.dbMu.RUnlock()
	if w < 0 {
		t.Errorf("active member 1 has weight %v (disabled)", w)
	}
}

// SetWeight rejects disabling the last enabled member.
func TestMultiDBSetWeightKeepsOneEnabled(t *testing.T) {
	ctx := context.Background()
	mdb := newLifecycleTestClient(
		t, nil,
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:1"}, Weight: 1, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:2"}, Weight: -1, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
	)
	if err := mdb.ForceActiveDatabase(ctx, 1); err != nil {
		t.Fatalf("ForceActiveDatabase(1) = %v", err)
	}
	if err := mdb.SetWeight(0, -1); err == nil {
		t.Fatal("SetWeight(0, -1) = nil, want an error (no enabled member would remain)")
	}
	if w := mdb.core.dbByID(0).weight; w != 1 {
		t.Errorf("weight of 0 = %v, want 1", w)
	}
}

// RemoveDatabase rejects removing the last enabled member, and SetWeight(-1)
// on an already-disabled active is a no-op.
func TestMultiDBRemoveKeepsOneEnabled(t *testing.T) {
	ctx := context.Background()
	mdb := newLifecycleTestClient(
		t, nil,
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:1"}, Weight: 1, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:2"}, Weight: 0.5, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
		MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:3"}, Weight: -1, HealthChecks: []MultiDBHealthCheck{okLivenessCheck{}}},
	)
	if err := mdb.ForceActiveDatabase(ctx, 2); err != nil {
		t.Fatalf("ForceActiveDatabase(2) = %v", err)
	}
	if err := mdb.SetWeight(2, -1); err != nil {
		t.Fatalf("SetWeight(2, -1) on the already-disabled active = %v, want nil", err)
	}
	if err := mdb.SetWeight(0, -1); err != nil {
		t.Fatalf("SetWeight(0, -1) = %v (member 1 stays enabled)", err)
	}
	if err := mdb.RemoveDatabase(ctx, 1); err == nil {
		t.Fatal("RemoveDatabase(1) = nil, want an error (no enabled member would remain)")
	}
	if mdb.core.dbByID(1) == nil {
		t.Fatal("member 1 was removed")
	}
}

// A start cut short by the deadline before any pass completed did not prove
// the members down: the error must not match ErrPermanentlyNotAvailable.
func TestMultiDBInitDeadlineBeforeVerdictsNotPermanent(t *testing.T) {
	addr := internalTestRedisAddr()
	probe := NewClient(&Options{Addr: addr})
	if err := probe.Ping(context.Background()).Err(); err != nil {
		_ = probe.Close()
		t.Skipf("no redis at %s: %v", addr, err)
	}
	_ = probe.Close()
	// Default checks: 3 probes 500ms apart, so a pass takes >= 1s.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	mdb, err := NewMultiDBClient(ctx, &MultiDBOptions{
		HealthCheckInterval:  time.Hour,
		AutoFallbackInterval: -1,
		Clients:              []MultiDBClientConfig{{Options: &Options{Addr: addr}}},
	})
	if err == nil {
		_ = mdb.Close()
		t.Fatal("NewMultiDBClient succeeded within 200ms; the default probe schedule takes >= 1s")
	}
	if !errors.Is(err, ErrInsufficientHealthyDatabases) {
		t.Fatalf("err = %v, want ErrInsufficientHealthyDatabases", err)
	}
	if errors.Is(err, ErrPermanentlyNotAvailable) {
		t.Fatalf("err = %v matches ErrPermanentlyNotAvailable, but no member answered before the deadline", err)
	}
}
