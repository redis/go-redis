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

// probeFunc runs one probe of a health check and reports whether it passed
// along with any error that caused a failure.
type probeFunc func(context.Context, redis.MultiDBHealthCheck) (bool, error)

// checkRunner interprets the probes of a single health check (all / majority /
// any) and reports whether that check is healthy.
type checkRunner func(ctx context.Context, hc redis.MultiDBHealthCheck, probe probeFunc) bool

// runChecks executes every health check concurrently, each with its own
// timeout, and returns true only if all of them pass (AND across checks). The
// per-check probe interpretation is delegated to runner. It is the single
// implementation shared by every policy; the policies differ only in which
// runner they pass in.
func runChecks(ctx context.Context, checks []redis.MultiDBHealthCheck, probe probeFunc, runner checkRunner) bool {
	if len(checks) == 0 {
		return true
	}
	// Buffered to the number of checks so a worker never blocks on send even
	// when we return early, preventing goroutine leaks.
	results := make(chan bool, len(checks))
	var wg sync.WaitGroup
	for _, hc := range checks {
		wg.Add(1)
		go func(check redis.MultiDBHealthCheck) {
			defer wg.Done()
			results <- runner(ctx, check, probe)
		}(hc)
	}
	go func() { wg.Wait(); close(results) }()

	// Every health check must pass (AND across checks).
	for result := range results {
		if !result {
			return false
		}
	}
	return true
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
	return runChecks(ctx, checks, standaloneProbe(client), runProbesAllMustPass)
}

func (p *HealthyAllPolicy) ExecuteCluster(ctx context.Context, checks []redis.MultiDBHealthCheck, client *redis.ClusterClient) bool {
	return runChecks(ctx, checks, clusterProbe(client), runProbesAllMustPass)
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
	return runChecks(ctx, checks, standaloneProbe(client), runProbesMajority)
}

func (p *HealthyMajorityPolicy) ExecuteCluster(ctx context.Context, checks []redis.MultiDBHealthCheck, client *redis.ClusterClient) bool {
	return runChecks(ctx, checks, clusterProbe(client), runProbesMajority)
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
	return runChecks(ctx, checks, standaloneProbe(client), runProbesAny)
}

func (p *HealthyAnyPolicy) ExecuteCluster(ctx context.Context, checks []redis.MultiDBHealthCheck, client *redis.ClusterClient) bool {
	return runChecks(ctx, checks, clusterProbe(client), runProbesAny)
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

// runProbesAllMustPass runs probes where ALL must succeed.
func runProbesAllMustPass(ctx context.Context, hc redis.MultiDBHealthCheck, probe probeFunc) bool {
	cfg := getConfig(hc)
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	for i := 0; i < cfg.Probes; i++ {
		if ok, _ := probe(ctx, hc); !ok {
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
func runProbesMajority(ctx context.Context, hc redis.MultiDBHealthCheck, probe probeFunc) bool {
	cfg := getConfig(hc)
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	// Strict majority: more than half must pass
	// (probes - 1) // 2 gives the max allowed failures
	allowedFailures := (cfg.Probes - 1) / 2
	// requiredSuccesses is the number of successful probes that guarantees a
	// strict majority, allowing an early exit once it is reached.
	requiredSuccesses := cfg.Probes - allowedFailures
	failures := 0
	successes := 0

	for i := 0; i < cfg.Probes; i++ {
		if ok, _ := probe(ctx, hc); ok {
			successes++
			if successes >= requiredSuccesses {
				return true
			}
		} else {
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
func runProbesAny(ctx context.Context, hc redis.MultiDBHealthCheck, probe probeFunc) bool {
	cfg := getConfig(hc)
	ctx, cancel := context.WithTimeout(ctx, cfg.Timeout)
	defer cancel()

	for i := 0; i < cfg.Probes; i++ {
		if ok, _ := probe(ctx, hc); ok {
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
