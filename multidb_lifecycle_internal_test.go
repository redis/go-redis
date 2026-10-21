package redis

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

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
