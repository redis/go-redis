package redis

import (
	"context"
	"testing"

	imultidb "github.com/redis/go-redis/v9/internal/multidb"
)

// failbackLagCheck is a fail-back-only check that reports unhealthy and, as a
// side effect, flips the active member mid-probe — simulating a failover that
// lands while a passive member is being probed with the full check set.
type failbackLagCheck struct{ onCheck func() }

func (c *failbackLagCheck) CheckHealth(context.Context, *Client) (bool, error) {
	c.onCheck()
	return false, nil
}

func (c *failbackLagCheck) CheckClusterHealth(context.Context, *ClusterClient) (bool, error) {
	c.onCheck()
	return false, nil
}

func (c *failbackLagCheck) FailbackOnly() bool { return true }

// TestBackgroundProbeDoesNotEvictMemberGoneActive pins the split-verdict: the
// background pass runs a passive member's full checks without recording, then
// revalidates the active. If the member became active mid-probe, only the
// active-safe subset is recorded — so a fail-back-only failure (lag) cannot open
// the breaker of the member now serving traffic.
func TestBackgroundProbeDoesNotEvictMemberGoneActive(t *testing.T) {
	core := newMultidbCore(&MultiDBOptions{})
	mkCB := func() *imultidb.CircuitBreaker {
		return imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{FailureThreshold: 1, SuccessThreshold: 1})
	}
	a := &multidbDatabase{id: 0, cb: mkCB(), policy: defaultMultiDBPolicy{}}
	b := &multidbDatabase{id: 1, cb: mkCB(), policy: defaultMultiDBPolicy{}}
	core.dbs[0] = a
	core.dbs[1] = b
	core.active.Store(0) // A active at the start of the pass

	b.checks = []MultiDBHealthCheck{&failbackLagCheck{onCheck: func() { core.active.Store(1) }}}

	core.runHealthChecksOnce(context.Background())

	if b.cb.State() != imultidb.CircuitClosed {
		t.Fatalf("member that became active mid-probe was evicted by a fail-back-only check: breaker=%v", b.cb.State())
	}
}
