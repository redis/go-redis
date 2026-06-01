package multidb

import (
	"context"

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

// CheckHealth performs a single PING probe against the client.
func (h *PingHealthCheck) CheckHealth(ctx context.Context, client *redis.Client) bool {
	return client.Ping(ctx).Err() == nil
}

// CheckClusterHealth performs a single PING probe against ALL nodes in the cluster.
func (h *PingHealthCheck) CheckClusterHealth(ctx context.Context, client *redis.ClusterClient) bool {
	err := client.ForEachShard(ctx, func(ctx context.Context, shard *redis.Client) error {
		return shard.Ping(ctx).Err()
	})
	return err == nil
}
