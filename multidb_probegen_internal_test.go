package redis

import (
	"context"
	"testing"
	"time"

	imultidb "github.com/redis/go-redis/v9/internal/multidb"
)

// resettingHealthCheck reports unhealthy and runs onCheck while it executes,
// simulating an operator reselect (breaker Reset) landing mid-probe.
type resettingHealthCheck struct{ onCheck func() }

func (c *resettingHealthCheck) CheckHealth(context.Context, *Client) (bool, error) {
	c.onCheck()
	return false, nil
}

func (c *resettingHealthCheck) CheckClusterHealth(context.Context, *ClusterClient) (bool, error) {
	c.onCheck()
	return false, nil
}

// TestProbeVerdictVoidedByResetDuringProbe pins that the probe records through
// the reset-generation gate: an operator reselect (breaker Reset) that lands
// while the probe's checks run voids the probe's failure verdict, so a stale
// probe cannot re-open the member the operator just selected.
func TestProbeVerdictVoidedByResetDuringProbe(t *testing.T) {
	cb := imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{FailureThreshold: 1, SuccessThreshold: 1})
	db := &multidbDatabase{
		id:     0,
		cb:     cb,
		policy: defaultMultiDBPolicy{},
		c:      NewClient(&Options{Addr: "127.0.0.1:6379"}),
	}
	defer db.c.Close()

	chk := &resettingHealthCheck{onCheck: func() { cb.Reset() }}
	db.probeWith(context.Background(), time.Second, []MultiDBHealthCheck{chk})

	if cb.State() != imultidb.CircuitClosed {
		t.Fatalf("breaker %v, want closed: a probe failure overtaken by an operator Reset must not re-open the member", cb.State())
	}
}
