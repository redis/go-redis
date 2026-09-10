package e2e

import (
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	fi "github.com/redis/go-redis/v9/maintnotifications/e2e"
)

// proxyFarm drives the per-member proxy containers (MockMembers, shared
// with the mock fault-injector) with docker CLI faults directly. It exists
// only for the two scenarios that have no real-FI equivalent
// (TestBackgroundDrivenFailover's hang semantics under the default
// mechanism, TestEscalationWhenAllMembersDown's multi-restart
// choreography) — see scenarios_test.go. Every other scenario drives faults
// through faultInjector instead, which works unmodified against either the
// local mock or a real fault-injector service.
type proxyFarm struct {
	t       *testing.T
	members []MockMember
}

func newProxyFarm(t *testing.T) *proxyFarm {
	t.Helper()
	f := &proxyFarm{t: t, members: append([]MockMember(nil), MockMembers...)}
	// Whatever a test did, the next one starts from "everything running".
	t.Cleanup(f.RestoreAll)
	f.RestoreAll()
	return f
}

func (f *proxyFarm) docker(args ...string) error {
	return RunDocker(context.Background(), args...)
}

func (f *proxyFarm) Stop(i int) {
	f.t.Helper()
	if err := f.docker("stop", "-t", "0", f.members[i].Container); err != nil {
		f.t.Fatalf("stop member %d: %v", i, err)
	}
}

func (f *proxyFarm) Start(i int) {
	f.t.Helper()
	if err := f.docker("start", f.members[i].Container); err != nil {
		f.t.Fatalf("start member %d: %v", i, err)
	}
	f.awaitListening(i, 30*time.Second)
}

func (f *proxyFarm) Pause(i int) {
	f.t.Helper()
	if err := f.docker("pause", f.members[i].Container); err != nil {
		f.t.Fatalf("pause member %d: %v", i, err)
	}
}

// RestoreAll brings every member back to a running, listening state.
func (f *proxyFarm) RestoreAll() {
	for i, m := range f.members {
		// Only "is not paused" is benign for unpause (start below is a no-op
		// when already running). A missing container means the compose
		// profile is not up: fail fast instead of a 30s dial timeout per
		// member. Any other unpause failure could leave the member frozen —
		// a paused container still accepts TCP dials, so awaitListening
		// would not catch it.
		if err := f.docker("unpause", m.Container); err != nil {
			switch {
			case isMissingContainer(err):
				f.t.Fatalf("proxy container %s does not exist — start the stack with `docker compose --profile multidb up -d`: %v", m.Container, err)
			case !isNotPaused(err):
				f.t.Fatalf("unpause %s: %v", m.Container, err)
			}
		}
		if err := f.docker("start", m.Container); err != nil {
			f.t.Fatalf("start %s: %v", m.Container, err)
		}
		f.awaitListening(i, 30*time.Second)
	}
}

func isMissingContainer(err error) bool {
	return strings.Contains(err.Error(), "No such container")
}

func isNotPaused(err error) bool {
	return strings.Contains(err.Error(), "is not paused")
}

func (f *proxyFarm) awaitListening(i int, timeout time.Duration) {
	f.t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", f.members[i].Addr, 250*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	f.t.Fatalf("member %d (%s) never came back up", i, f.members[i].Addr)
}

// fast timings so scenarios complete in seconds while staying CI-jitter safe.
func fastMultiDBOptions(f *proxyFarm) *redis.MultiDBOptions {
	return &redis.MultiDBOptions{
		Clients: []redis.MultiDBClientConfig{
			{Options: memberOptions(f, 0), Weight: 3},
			{Options: memberOptions(f, 1), Weight: 2},
			{Options: memberOptions(f, 2), Weight: 1},
		},
		// Every proxy must be genuinely healthy at startup: with the default
		// majority policy a mis-wired member could slip through and scenarios
		// that stop member 0 would silently test the wrong Topology.
		InitialDBState:      redis.InitialDBStateAllAvailable,
		HealthCheckInterval: 500 * time.Millisecond,
		HealthCheckTimeout:  250 * time.Millisecond,
		CircuitBreakerConfig: &redis.MultiDBCircuitBreakerConfig{
			FailureThreshold: 3,
			SuccessThreshold: 1,
			GracePeriod:      2 * time.Second,
		},
		CommandRetries:       3,
		AutoFallbackInterval: 3 * time.Second,
		MaxFailoverAttempts:  4,
		FailoverAttemptDelay: 500 * time.Millisecond,
	}
}

func memberOptions(f *proxyFarm, i int) *redis.Options {
	return &redis.Options{
		Addr:         f.members[i].Addr,
		DialTimeout:  500 * time.Millisecond,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		// Fail fast inside a single command attempt so MultiDB's own retry
		// and failover logic drives recovery, not the per-client retries.
		MaxRetries: -1,
		// Let the probe context cut socket waits short: without this a
		// paused (hung) proxy stalls health checks for the full read
		// timeout instead of the intended HealthCheckTimeout.
		ContextTimeoutEnabled: true,
	}
}

// --- Fault-injector-driven harness (both mock and real mode) ---
//
// buildOptions/endpointOptions/triggerNetworkFailure below back every
// scenario that has a real-FI equivalent. They read e2eTopology (resolved
// once in TestMain from REDIS_ENDPOINTS_CONFIG_PATH, or the local mock
// default) instead of proxyFarm, so the same test code runs against N
// real Active-Active regions or the 3-member local mock Topology alike.

// fiMultiDBOptions builds MultiDBOptions from e2eTopology: weights descend
// N..1 so index 0 is always the initial highest-weight active, matching the
// fixed-Topology fastMultiDBOptions' convention.
func fiMultiDBOptions() *redis.MultiDBOptions {
	n := len(e2eTopology.Endpoints)
	clients := make([]redis.MultiDBClientConfig, n)
	for i, addr := range e2eTopology.Endpoints {
		clients[i] = redis.MultiDBClientConfig{Options: endpointOptions(addr), Weight: float64(n - i)}
	}
	return &redis.MultiDBOptions{
		Clients:             clients,
		InitialDBState:      redis.InitialDBStateAllAvailable,
		HealthCheckInterval: 500 * time.Millisecond,
		HealthCheckTimeout:  250 * time.Millisecond,
		CircuitBreakerConfig: &redis.MultiDBCircuitBreakerConfig{
			FailureThreshold: 3,
			SuccessThreshold: 1,
			GracePeriod:      2 * time.Second,
		},
		CommandRetries:       3,
		AutoFallbackInterval: 3 * time.Second,
		MaxFailoverAttempts:  4,
		FailoverAttemptDelay: 500 * time.Millisecond,
	}
}

// endpointOptions builds *redis.Options for one Topology endpoint. Real-mode
// entries are redis:// URLs; the local mock Topology uses plain host:port.
func endpointOptions(raw string) *redis.Options {
	var opts *redis.Options
	if strings.Contains(raw, "://") {
		parsed, err := redis.ParseURL(raw)
		if err != nil {
			panic(fmt.Sprintf("multidb/e2e: invalid endpoint URL %q: %v", raw, err))
		}
		opts = parsed
	} else {
		opts = &redis.Options{Addr: raw}
	}
	if e2eTopology.Username != "" {
		opts.Username = e2eTopology.Username
	}
	if e2eTopology.Password != "" {
		opts.Password = e2eTopology.Password
	}
	opts.DialTimeout = 500 * time.Millisecond
	opts.ReadTimeout = time.Second
	opts.WriteTimeout = time.Second
	// Fail fast inside a single command attempt so MultiDB's own retry and
	// failover logic drives recovery, not the per-client retries.
	opts.MaxRetries = -1
	opts.ContextTimeoutEnabled = true
	return opts
}

// triggerNetworkFailure fires fi.ActionNetworkFailure against member (a
// cluster_index into e2eTopology.Endpoints) for delay, and registers a
// t.Cleanup that waits for the action's own timer to restore the member —
// guaranteeing the next test starts from a healthy Topology without any
// docker-specific reset. It does NOT wait before returning: callers that
// need to observe behavior during the outage must assert first (see
// fi.ActionNetworkFailure's doc comment); callers whose assertion is about
// post-restore behavior should call waitForActionDone with the returned id.
func triggerNetworkFailure(t *testing.T, member int, delay time.Duration) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := faultInjector.TriggerNetworkFailure(ctx, e2eTopology.BDBID, member, int(delay.Seconds()))
	if err != nil {
		t.Fatalf("TriggerNetworkFailure(member=%d): %v", member, err)
	}
	id := resp.ActionID
	t.Cleanup(func() {
		wctx, wcancel := context.WithTimeout(context.Background(), delay+30*time.Second)
		defer wcancel()
		st, err := faultInjector.WaitForAction(wctx, id, fi.WithMaxWaitTime(delay+30*time.Second))
		switch {
		case err != nil:
			t.Logf("cleanup: waiting for network_failure action %s to finish: %v", id, err)
		case st.Status == fi.StatusFailed:
			t.Logf("cleanup: network_failure action %s failed: %v", id, st.Error)
		}
	})
	return id
}

// waitForActionDone blocks until id reaches a terminal status, failing the
// test on error, failure, or timeout. Use for assertions that are
// specifically about post-restore behavior (e.g. auto-fallback) — see
// triggerNetworkFailure.
func waitForActionDone(t *testing.T, id string, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	st, err := faultInjector.WaitForAction(ctx, id, fi.WithMaxWaitTime(timeout))
	if err != nil {
		t.Fatalf("waiting for action %s: %v", id, err)
	}
	if st.Status == fi.StatusFailed {
		t.Fatalf("action %s failed: %v", id, st.Error)
	}
}

// awaitUnreachable polls addr until a TCP dial fails or timeout elapses.
// Used after triggerNetworkFailure when a test needs the fault to be
// observably in effect before proceeding, rather than merely triggered
// (TriggerAction returns before the effect is guaranteed — see its doc
// comment).
func awaitUnreachable(t *testing.T, addr string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 250*time.Millisecond)
		if err != nil {
			return
		}
		_ = conn.Close()
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("%s never became unreachable", addr)
}

// lastMemberAddr returns the address of the highest-index (lowest-weight)
// Topology member — the harness's convention for "a member that stays up"
// when a scenario faults member 0, generalized over Topology size (index 2
// for the 3-member mock, index 1 for a 2-region real deployment).
func lastMemberAddr() string {
	return e2eTopology.Endpoints[len(e2eTopology.Endpoints)-1]
}

func newE2EClient(t *testing.T, opts *redis.MultiDBOptions) *redis.MultiDBClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	mdb, err := redis.NewMultiDBClient(ctx, opts)
	if err != nil {
		t.Fatalf("NewMultiDBClient: %v", err)
	}
	t.Cleanup(func() { _ = mdb.Close() })
	return mdb
}

// eventually polls cond until it is true or the timeout elapses.
func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}
