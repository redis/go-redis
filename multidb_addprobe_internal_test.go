package redis

import (
	"context"
	"testing"
	"time"
)

// lockProbeCheck records whether failoverMu was UNheld at the moment the health
// check ran — i.e. whether the initial probe runs off the failover lock.
type lockProbeCheck struct {
	core    *multidbCore
	offLock *bool
}

func (c *lockProbeCheck) CheckHealth(context.Context, *Client) (bool, error) {
	if c.core.failoverMu.TryLock() {
		c.core.failoverMu.Unlock()
		*c.offLock = true
	}
	return true, nil
}

func (c *lockProbeCheck) CheckClusterHealth(context.Context, *ClusterClient) (bool, error) {
	return c.CheckHealth(nil, nil)
}

// TestAddDatabaseProbesOffFailoverLock pins that AddDatabase runs the initial
// health probe WITHOUT holding failoverMu. Holding it across the probe (up to
// HealthCheckTimeout, or unbounded for an uncooperative custom check) would
// block an urgent tryFailover, which needs the same lock, and let short command
// contexts expire.
func TestAddDatabaseProbesOffFailoverLock(t *testing.T) {
	core := newMultidbCore(&MultiDBOptions{
		HealthCheckTimeout:   time.Second,
		HealthCheckPolicy:    defaultMultiDBPolicy{},
		CircuitBreakerConfig: &MultiDBCircuitBreakerConfig{FailureThreshold: 1, SuccessThreshold: 1, GracePeriod: time.Second},
	})
	offLock := false
	chk := &lockProbeCheck{core: core, offLock: &offLock}

	_, _ = core.addDatabase(context.Background(), MultiDBClientConfig{
		Options:      &Options{Addr: "127.0.0.1:6379"},
		HealthChecks: []MultiDBHealthCheck{chk},
		Weight:       1,
	})

	if !offLock {
		t.Fatal("initial health probe ran while holding failoverMu — a slow check would block failover")
	}
}
