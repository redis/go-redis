package e2e

import (
	"context"
	"fmt"
	"net"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// memberProxy is one MultiDB member endpoint: a cae-resp-proxy container
// fronting the shared target Redis.
type memberProxy struct {
	Container string
	Addr      string
}

// proxyFarm drives the per-member proxy containers with docker CLI faults.
type proxyFarm struct {
	t       *testing.T
	members []memberProxy
}

func newProxyFarm(t *testing.T) *proxyFarm {
	t.Helper()
	f := &proxyFarm{
		t: t,
		members: []memberProxy{
			{Container: "cae-proxy-db0", Addr: "localhost:17100"},
			{Container: "cae-proxy-db1", Addr: "localhost:17101"},
			{Container: "cae-proxy-db2", Addr: "localhost:17102"},
		},
	}
	// Whatever a test did, the next one starts from "everything running".
	t.Cleanup(f.RestoreAll)
	f.RestoreAll()
	return f
}

func (f *proxyFarm) docker(args ...string) error {
	// Bounded: a stuck docker daemon must fail the scenario, not hang the
	// whole suite until the package timeout.
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "docker", args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker %v: %v: %s", args, err, out)
	}
	return nil
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

func (f *proxyFarm) Unpause(i int) {
	f.t.Helper()
	if err := f.docker("unpause", f.members[i].Container); err != nil {
		f.t.Fatalf("unpause member %d: %v", i, err)
	}
}

// RestoreAll brings every member back to a running, listening state.
func (f *proxyFarm) RestoreAll() {
	for i, m := range f.members {
		// unpause fails when not paused and start is a no-op when running —
		// those are benign. A missing container means the compose profile is
		// not up: fail fast instead of a 30s dial timeout per member.
		if err := f.docker("unpause", m.Container); err != nil && isMissingContainer(err) {
			f.t.Fatalf("proxy container %s does not exist — start the stack with `docker compose --profile multidb up -d`: %v", m.Container, err)
		}
		_ = f.docker("start", m.Container)
		f.awaitListening(i, 30*time.Second)
	}
}

func isMissingContainer(err error) bool {
	return strings.Contains(err.Error(), "No such container")
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
	}
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
