// Package multidb provides health check and failover strategy implementations
// for use with redis.MultiDBClient.
package multidb

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// Default values for health check configuration
const (
	DefaultHealthCheckProbes   = 3
	DefaultHealthCheckDelay    = 500 * time.Millisecond
	DefaultHealthCheckTimeout  = 3 * time.Second
	DefaultHealthCheckInterval = 5 * time.Second
)

// HealthCheckConfig holds configuration for health checks.
type HealthCheckConfig struct {
	Probes  int
	Delay   time.Duration
	Timeout time.Duration
}

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

// WithProbes sets the number of probes.
func WithProbes(probes int) HealthCheckOption {
	return func(c *HealthCheckConfig) {
		if probes > 0 {
			c.Probes = probes
		}
	}
}

// WithDelay sets the delay between probes.
func WithDelay(delay time.Duration) HealthCheckOption {
	return func(c *HealthCheckConfig) {
		if delay >= 0 {
			c.Delay = delay
		}
	}
}

// WithTimeout sets the timeout for the health check.
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
	return p.execute(ctx, checks, func(ctx context.Context, hc redis.MultiDBHealthCheck) bool {
		return hc.CheckHealth(ctx, client)
	})
}

func (p *HealthyAllPolicy) ExecuteCluster(ctx context.Context, checks []redis.MultiDBHealthCheck, client *redis.ClusterClient) bool {
	return p.execute(ctx, checks, func(ctx context.Context, hc redis.MultiDBHealthCheck) bool {
		return hc.CheckClusterHealth(ctx, client)
	})
}

func (p *HealthyAllPolicy) execute(ctx context.Context, checks []redis.MultiDBHealthCheck, probeFn func(context.Context, redis.MultiDBHealthCheck) bool) bool {
	if len(checks) == 0 {
		return true
	}
	// Run all health checks concurrently, each with its own timeout
	results := make(chan bool, len(checks))
	var wg sync.WaitGroup
	for _, hc := range checks {
		wg.Add(1)
		go func(check redis.MultiDBHealthCheck) {
			defer wg.Done()
			results <- runProbesAllMustPass(ctx, check, probeFn)
		}(hc)
	}
	go func() { wg.Wait(); close(results) }()

	// All health checks must pass
	for result := range results {
		if !result {
			return false
		}
	}
	return true
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
	return p.execute(ctx, checks, func(ctx context.Context, hc redis.MultiDBHealthCheck) bool {
		return hc.CheckHealth(ctx, client)
	})
}

func (p *HealthyMajorityPolicy) ExecuteCluster(ctx context.Context, checks []redis.MultiDBHealthCheck, client *redis.ClusterClient) bool {
	return p.execute(ctx, checks, func(ctx context.Context, hc redis.MultiDBHealthCheck) bool {
		return hc.CheckClusterHealth(ctx, client)
	})
}

func (p *HealthyMajorityPolicy) execute(ctx context.Context, checks []redis.MultiDBHealthCheck, probeFn func(context.Context, redis.MultiDBHealthCheck) bool) bool {
	if len(checks) == 0 {
		return true
	}
	// Run all health checks concurrently
	results := make(chan bool, len(checks))
	var wg sync.WaitGroup
	for _, hc := range checks {
		wg.Add(1)
		go func(check redis.MultiDBHealthCheck) {
			defer wg.Done()
			results <- runProbesMajority(ctx, check, probeFn)
		}(hc)
	}
	go func() { wg.Wait(); close(results) }()

	// All health checks must pass (each uses majority internally)
	for result := range results {
		if !result {
			return false
		}
	}
	return true
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
	return p.execute(ctx, checks, func(ctx context.Context, hc redis.MultiDBHealthCheck) bool {
		return hc.CheckHealth(ctx, client)
	})
}

func (p *HealthyAnyPolicy) ExecuteCluster(ctx context.Context, checks []redis.MultiDBHealthCheck, client *redis.ClusterClient) bool {
	return p.execute(ctx, checks, func(ctx context.Context, hc redis.MultiDBHealthCheck) bool {
		return hc.CheckClusterHealth(ctx, client)
	})
}

func (p *HealthyAnyPolicy) execute(ctx context.Context, checks []redis.MultiDBHealthCheck, probeFn func(context.Context, redis.MultiDBHealthCheck) bool) bool {
	if len(checks) == 0 {
		return true
	}
	// Run all health checks concurrently
	results := make(chan bool, len(checks))
	var wg sync.WaitGroup
	for _, hc := range checks {
		wg.Add(1)
		go func(check redis.MultiDBHealthCheck) {
			defer wg.Done()
			results <- runProbesAny(ctx, check, probeFn)
		}(hc)
	}
	go func() { wg.Wait(); close(results) }()

	// All health checks must pass (each uses any internally)
	for result := range results {
		if !result {
			return false
		}
	}
	return true
}

// getConfig returns the health check config, using defaults if not configurable.
func getConfig(hc redis.MultiDBHealthCheck) HealthCheckConfig {
	if chc, ok := hc.(ConfigurableHealthCheck); ok {
		return chc.Config()
	}
	return DefaultHealthCheckConfig()
}

// runProbesAllMustPass runs probes where ALL must succeed.
func runProbesAllMustPass(ctx context.Context, hc redis.MultiDBHealthCheck, probeFn func(context.Context, redis.MultiDBHealthCheck) bool) bool {
	cfg := getConfig(hc)
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	for i := 0; i < cfg.Probes; i++ {
		if !probeFn(ctx, hc) {
			return false
		}
		if i < cfg.Probes-1 && cfg.Delay > 0 {
			select {
			case <-ctx.Done():
				return false
			case <-time.After(cfg.Delay):
			}
		}
	}
	return true
}

// runProbesMajority runs probes where MAJORITY must succeed.
func runProbesMajority(ctx context.Context, hc redis.MultiDBHealthCheck, probeFn func(context.Context, redis.MultiDBHealthCheck) bool) bool {
	cfg := getConfig(hc)
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	// Strict majority: more than half must pass
	// (probes - 1) // 2 gives the max allowed failures
	allowedFailures := (cfg.Probes - 1) / 2
	failures := 0

	for i := 0; i < cfg.Probes; i++ {
		if !probeFn(ctx, hc) {
			failures++
			if failures > allowedFailures {
				return false
			}
		}
		if i < cfg.Probes-1 && cfg.Delay > 0 {
			select {
			case <-ctx.Done():
				return false
			case <-time.After(cfg.Delay):
			}
		}
	}
	return true
}

// runProbesAny runs probes where ANY success is enough.
func runProbesAny(ctx context.Context, hc redis.MultiDBHealthCheck, probeFn func(context.Context, redis.MultiDBHealthCheck) bool) bool {
	cfg := getConfig(hc)
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	for i := 0; i < cfg.Probes; i++ {
		if probeFn(ctx, hc) {
			return true
		}
		if i < cfg.Probes-1 && cfg.Delay > 0 {
			select {
			case <-ctx.Done():
				return false
			case <-time.After(cfg.Delay):
			}
		}
	}
	return false
}
