package multidb

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/redis/go-redis/v9"
)

// PingHealthCheck checks health using the PING command.
// Each call to CheckHealth/CheckClusterHealth performs a SINGLE ping probe.
// The number of probes and how they're interpreted is controlled by the
// HealthCheckPolicy and the config (Probes, Delay, Timeout).
type PingHealthCheck struct {
	config HealthCheckConfig
}

// NewPingHealthCheck creates a new PingHealthCheck with optional configuration.
//
// Example:
//
//	hc := multidb.NewPingHealthCheck()
//
//	hc := multidb.NewPingHealthCheck(
//	    multidb.WithProbes(5),
//	    multidb.WithDelay(100*time.Millisecond),
//	    multidb.WithTimeout(5*time.Second),
//	)
func NewPingHealthCheck(opts ...HealthCheckOption) *PingHealthCheck {
	return &PingHealthCheck{
		config: applyOptions(opts),
	}
}

// Config returns the health check configuration.
func (h *PingHealthCheck) Config() HealthCheckConfig {
	return h.config
}

// CheckHealth performs a single PING probe against the client. It returns
// (false, err) with the PING error when the probe fails so callers can record
// why the check was unhealthy.
func (h *PingHealthCheck) CheckHealth(ctx context.Context, client *redis.Client) (bool, error) {
	if err := client.Ping(ctx).Err(); err != nil {
		return false, err
	}
	return true, nil
}

// CheckClusterHealth performs a single PING probe against ALL nodes in the
// cluster. It returns (false, err) with the first shard's PING error when the
// probe fails. An empty topology (no shards pinged) is reported as unhealthy
// rather than trivially healthy, matching LagAwareHealthCheck which fails when
// the cluster has no addresses.
func (h *PingHealthCheck) CheckClusterHealth(ctx context.Context, client *redis.ClusterClient) (bool, error) {
	var pinged int64
	err := client.ForEachShard(ctx, func(ctx context.Context, shard *redis.Client) error {
		atomic.AddInt64(&pinged, 1)
		return shard.Ping(ctx).Err()
	})
	if err != nil {
		return false, err
	}
	if atomic.LoadInt64(&pinged) == 0 {
		return false, fmt.Errorf("multidb: cluster has no shards to ping")
	}
	return true, nil
}
