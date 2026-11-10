// Package multidb provides health checks (ping, lag-aware) and health-check
// policies (all, majority, any) for use with redis.MultiDBClient.
//
// EXPERIMENTAL: this API is subject to change, use with caution.
package multidb

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	imultidb "github.com/redis/go-redis/v9/internal/multidb"
)

// Default values for health check configuration
const (
	DefaultHealthCheckProbes  = imultidb.DefaultProbes       // 3
	DefaultHealthCheckDelay   = imultidb.DefaultProbeDelay   // 500ms
	DefaultHealthCheckTimeout = imultidb.DefaultProbeTimeout // 3s
)

// HealthCheckConfig holds configuration for health checks. It is the same
// type as redis.MultiDBHealthCheckConfig, so the default MultiDBClient policy
// honors it too.
type HealthCheckConfig = redis.MultiDBHealthCheckConfig

// DefaultHealthCheckConfig returns the default health check configuration.
func DefaultHealthCheckConfig() HealthCheckConfig {
	return HealthCheckConfig{
		Probes:  DefaultHealthCheckProbes,
		Delay:   DefaultHealthCheckDelay,
		Timeout: DefaultHealthCheckTimeout,
	}
}

// HealthCheckOption is a functional option for configuring health checks.
type HealthCheckOption func(*HealthCheckConfig)

// WithProbes sets the number of probes. Every policy honors it, including
// the default redis.MultiDBClient policy. A check still running when
// MultiDBOptions.HealthCheckTimeout (default 3s; lowered to
// HealthCheckInterval/2 when not below the interval) expires fails, so keep
// (Probes-1)*Delay plus the probe time below it. Under the default policy
// NewMultiDBClient and AddDatabase reject a check whose (Probes-1)*Delay is
// not below that timeout or the check's own Timeout. Under the default policy
// SetActiveDatabase and ProbeTargetBeforeFailover run one probe.
func WithProbes(probes int) HealthCheckOption {
	return func(c *HealthCheckConfig) {
		if probes > 0 {
			c.Probes = probes
		}
	}
}

// WithDelay sets the delay between probes. See WithProbes for the timeout
// cap.
func WithDelay(delay time.Duration) HealthCheckOption {
	return func(c *HealthCheckConfig) {
		if delay >= 0 {
			c.Delay = delay
		}
	}
}

// WithTimeout sets the timeout for the health check (all its probes). It is
// capped by MultiDBOptions.HealthCheckTimeout.
func WithTimeout(timeout time.Duration) HealthCheckOption {
	return func(c *HealthCheckConfig) {
		if timeout > 0 {
			c.Timeout = timeout
		}
	}
}

func applyOptions(opts []HealthCheckOption) HealthCheckConfig {
	cfg := DefaultHealthCheckConfig()
	for _, opt := range opts {
		opt(&cfg)
	}
	return cfg
}

// HealthCheckPolicy defines how health check probes are evaluated.
type HealthCheckPolicy interface {
	Execute(ctx context.Context, checks []redis.MultiDBHealthCheck, client *redis.Client) bool
	ExecuteCluster(ctx context.Context, checks []redis.MultiDBHealthCheck, client *redis.ClusterClient) bool
}

// Compile-time interface compliance checks
var (
	_ redis.MultiDBHealthCheck = (*PingHealthCheck)(nil)
	_ redis.MultiDBHealthCheck = (*LagAwareHealthCheck)(nil)
	_ HealthCheckPolicy        = (*HealthyAllPolicy)(nil)
	_ HealthCheckPolicy        = (*HealthyMajorityPolicy)(nil)
	_ HealthCheckPolicy        = (*HealthyAnyPolicy)(nil)
)

// ConfigurableHealthCheck is an interface for health checks that have configuration.
type ConfigurableHealthCheck interface {
	redis.MultiDBHealthCheck
	Config() HealthCheckConfig
}

// probeFunc runs one probe of a health check.
type probeFunc func(context.Context, redis.MultiDBHealthCheck) (bool, error)

// runChecks runs every check concurrently and returns true only if all pass
// (AND across checks). runner interprets the probes of one check. The logic is
// shared with the default redis.MultiDBClient policy, which also runs one
// probe per check for SetActiveDatabase and ProbeTargetBeforeFailover; these
// policies always run the configured probes.
func runChecks(ctx context.Context, checks []redis.MultiDBHealthCheck, probe probeFunc, runner imultidb.ProbeRunner) bool {
	return imultidb.RunChecks(ctx, len(checks), func(ctx context.Context, i int) bool {
		hc := checks[i]
		return runner(ctx, imultidb.ProbeConfig(getConfig(hc)), func(ctx context.Context) (bool, error) {
			return probe(ctx, hc)
		})
	})
}

func standaloneProbe(client *redis.Client) probeFunc {
	return func(ctx context.Context, hc redis.MultiDBHealthCheck) (bool, error) {
		return hc.CheckHealth(ctx, client)
	}
}

func clusterProbe(client *redis.ClusterClient) probeFunc {
	return func(ctx context.Context, hc redis.MultiDBHealthCheck) (bool, error) {
		return hc.CheckClusterHealth(ctx, client)
	}
}

// HealthyAllPolicy returns true if ALL probes succeed for each health check.
// For each health check, it runs the configured number of probes with delays.
// ALL probes must succeed for the health check to be considered healthy.
//
// Note: The policy applies to probes within each individual health check.
// When multiple health checks are provided (e.g. Ping + LagAware), ALL
// health checks must pass — the policy does not control the relationship
// between different health checks, only between probes of the same check.
type HealthyAllPolicy struct{}

func NewHealthyAllPolicy() *HealthyAllPolicy { return &HealthyAllPolicy{} }

func (p *HealthyAllPolicy) Execute(ctx context.Context, checks []redis.MultiDBHealthCheck, client *redis.Client) bool {
	return runChecks(ctx, checks, standaloneProbe(client), imultidb.ProbesAllMustPass)
}

func (p *HealthyAllPolicy) ExecuteCluster(ctx context.Context, checks []redis.MultiDBHealthCheck, client *redis.ClusterClient) bool {
	return runChecks(ctx, checks, clusterProbe(client), imultidb.ProbesAllMustPass)
}

// HealthyMajorityPolicy returns true if a MAJORITY of probes succeed.
// For each health check, it runs the configured number of probes with delays.
// More than half of the probes must succeed.
//
// Note: The policy applies to probes within each individual health check.
// When multiple health checks are provided, ALL health checks must pass
// (each needing a majority of its probes to succeed).
type HealthyMajorityPolicy struct{}

func NewHealthyMajorityPolicy() *HealthyMajorityPolicy { return &HealthyMajorityPolicy{} }

func (p *HealthyMajorityPolicy) Execute(ctx context.Context, checks []redis.MultiDBHealthCheck, client *redis.Client) bool {
	return runChecks(ctx, checks, standaloneProbe(client), imultidb.ProbesMajority)
}

func (p *HealthyMajorityPolicy) ExecuteCluster(ctx context.Context, checks []redis.MultiDBHealthCheck, client *redis.ClusterClient) bool {
	return runChecks(ctx, checks, clusterProbe(client), imultidb.ProbesMajority)
}

// HealthyAnyPolicy returns true if AT LEAST ONE probe succeeds.
// For each health check, it runs probes until one succeeds or all fail.
//
// Note: The policy applies to probes within each individual health check.
// When multiple health checks are provided, ALL health checks must pass
// (each needing at least one successful probe).
type HealthyAnyPolicy struct{}

func NewHealthyAnyPolicy() *HealthyAnyPolicy { return &HealthyAnyPolicy{} }

func (p *HealthyAnyPolicy) Execute(ctx context.Context, checks []redis.MultiDBHealthCheck, client *redis.Client) bool {
	return runChecks(ctx, checks, standaloneProbe(client), imultidb.ProbesAny)
}

func (p *HealthyAnyPolicy) ExecuteCluster(ctx context.Context, checks []redis.MultiDBHealthCheck, client *redis.ClusterClient) bool {
	return runChecks(ctx, checks, clusterProbe(client), imultidb.ProbesAny)
}

// getConfig returns the health check config, using defaults if not
// configurable. Invalid values from a configurable check are clamped to their
// defaults so the probe runners stay robust: a non-positive Probes would
// otherwise make a check trivially pass (zero iterations), and a non-positive
// Timeout would expire the context immediately.
func getConfig(hc redis.MultiDBHealthCheck) HealthCheckConfig {
	cfg := DefaultHealthCheckConfig()
	if chc, ok := hc.(ConfigurableHealthCheck); ok {
		cfg = chc.Config()
	}
	if cfg.Probes <= 0 {
		cfg.Probes = DefaultHealthCheckProbes
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = DefaultHealthCheckTimeout
	}
	if cfg.Delay < 0 {
		cfg.Delay = DefaultHealthCheckDelay
	}
	return cfg
}
