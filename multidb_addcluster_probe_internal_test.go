package redis

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// apCallingCheck is a health check that calls the MultiDBClient's own
// AutoPipeline() from inside the probe — the re-entrant pattern that would
// deadlock a cluster AddDatabase holding autopipelinerMu across the probe.
type apCallingCheck struct {
	mdb *MultiDBClient
	got chan error
}

func (c *apCallingCheck) CheckHealth(context.Context, *Client) (bool, error) {
	_, err := c.mdb.AutoPipeline()
	c.got <- err
	return true, nil
}

func (c *apCallingCheck) CheckClusterHealth(context.Context, *ClusterClient) (bool, error) {
	_, err := c.mdb.AutoPipeline()
	c.got <- err
	return true, nil
}

// TestAddClusterDatabaseProbeMayCallAutoPipeline pins that a cluster AddDatabase
// does not hold autopipelinerMu across the member's initial health probe: a
// custom check that calls AutoPipeline() must not deadlock. While the add is
// pending the call is refused (a cluster member is landing, so an unsharded
// autopipeliner must not be created) — the invariant the lock used to protect,
// now carried by the pending-add count.
func TestAddClusterDatabaseProbeMayCallAutoPipeline(t *testing.T) {
	core := newMultidbCore(&MultiDBOptions{
		HealthCheckTimeout:   time.Second,
		HealthCheckPolicy:    defaultMultiDBPolicy{},
		CircuitBreakerConfig: &MultiDBCircuitBreakerConfig{FailureThreshold: 1, SuccessThreshold: 1, GracePeriod: time.Second},
	})
	t.Cleanup(func() { _ = core.close() })
	mdb := &MultiDBClient{core: core, autopipelinerMu: new(sync.Mutex)}
	got := make(chan error, 1)
	chk := &apCallingCheck{mdb: mdb, got: got}

	done := make(chan error, 1)
	go func() {
		_, err := mdb.AddDatabase(context.Background(), MultiDBClientConfig{
			ClusterOptions: &ClusterOptions{Addrs: []string{"127.0.0.1:6379"}},
			HealthChecks:   []MultiDBHealthCheck{chk},
			Weight:         1,
		})
		done <- err
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("AddDatabase deadlocked: the probe's AutoPipeline() call blocked on autopipelinerMu held across the probe")
	}
	select {
	case err := <-got:
		if !errors.Is(err, errMultiDBAutoPipelineCluster) {
			t.Fatalf("AutoPipeline() during a pending cluster add: got %v, want errMultiDBAutoPipelineCluster (refused, not created)", err)
		}
	default:
		t.Fatal("health check never ran")
	}
}
