package redisotel

import (
	"go.opentelemetry.io/otel/metric"
)

type MetricGroup string

const (
	MetricGroupCommand            MetricGroup = "command"
	MetricGroupConnectionBasic    MetricGroup = "connection-basic"
	MetricGroupResiliency         MetricGroup = "resiliency"
	MetricGroupConnectionAdvanced MetricGroup = "connection-advanced"
	MetricGroupPubSub             MetricGroup = "pubsub"
	MetricGroupStream             MetricGroup = "stream"
)

type HistogramAggregation string

const (
	HistogramAggregationExplicitBucket   HistogramAggregation = "explicit_bucket_histogram"
	HistogramAggregationBase2Exponential HistogramAggregation = "base2_exponential_bucket_histogram"
)

type config struct {
	// Core settings
	meterProvider metric.MeterProvider
	enabled       bool

	// Metric group settings
	enabledMetricGroups map[MetricGroup]bool

	// Command filtering
	includeCommands map[string]bool // nil means include all
	excludeCommands map[string]bool // nil means exclude none

	// Cardinality reduction
	hidePubSubChannelNames bool
	hideStreamNames        bool

	// Histogram settings
	histAggregation HistogramAggregation

	// Bucket configurations for different histogram metrics
	bucketsOperationDuration        []float64
	bucketsStreamProcessingDuration []float64
	bucketsConnectionCreateTime     []float64
	bucketsConnectionWaitTime       []float64
	bucketsConnectionUseTime        []float64
}

func (c *config) isMetricGroupEnabled(group MetricGroup) bool {
	return c.enabledMetricGroups[group]
}

func (c *config) isCommandIncluded(command string) bool {
	if c.excludeCommands != nil && c.excludeCommands[command] {
		return false
	}

	if c.includeCommands != nil {
		return c.includeCommands[command]
	}

	return true
}

// defaultHistogramBuckets returns the default histogram buckets for all duration metrics.
// These buckets are designed to capture typical Redis operation and connection latencies:
// - Sub-millisecond: 0.0001s (0.1ms), 0.0005s (0.5ms)
// - Milliseconds: 0.001s (1ms), 0.005s (5ms), 0.01s (10ms), 0.05s (50ms), 0.1s (100ms)
// - Sub-second: 0.5s (500ms)
// - Seconds: 1s, 5s, 10s
//
// This covers the range from 0.1ms to 10s, which is suitable for:
// - db.client.operation.duration (command execution time)
// - db.client.connection.create_time (connection establishment)
// - db.client.connection.wait_time (waiting for connection from pool)
// - db.client.connection.use_time (time connection is checked out)
// - redis.client.stream.processing_duration (stream message processing)
func defaultHistogramBuckets() []float64 {
	return []float64{
		0.0001, // 0.1ms
		0.0005, // 0.5ms
		0.001,  // 1ms
		0.005,  // 5ms
		0.01,   // 10ms
		0.05,   // 50ms
		0.1,    // 100ms
		0.5,    // 500ms
		1.0,    // 1s
		5.0,    // 5s
		10.0,   // 10s
	}
}

// MetricGroupFlags represents metric groups as bitwise flags
type MetricGroupFlags uint32

const (
	MetricGroupFlagCommand            MetricGroupFlags = 1 << 0
	MetricGroupFlagConnectionBasic    MetricGroupFlags = 1 << 1
	MetricGroupFlagResiliency         MetricGroupFlags = 1 << 2
	MetricGroupFlagConnectionAdvanced MetricGroupFlags = 1 << 3
	MetricGroupFlagPubSub             MetricGroupFlags = 1 << 4
	MetricGroupFlagStream             MetricGroupFlags = 1 << 5

	// MetricGroupAll enables all metric groups
	MetricGroupAll MetricGroupFlags = MetricGroupFlagCommand |
		MetricGroupFlagConnectionBasic |
		MetricGroupFlagResiliency |
		MetricGroupFlagConnectionAdvanced |
		MetricGroupFlagPubSub |
		MetricGroupFlagStream
)

// Use NewConfig() to create a new instance with defaults, then chain
// builder methods to customize.
//
// Example:
//
//	config := redisotel.NewConfig().
//	    WithEnabled(true).
//	    WithMetricGroups(redisotel.MetricGroupAll).
//	    WithMeterProvider(myProvider)
//
//	otel := redisotel.GetObservabilityInstance()
//	otel.Init(config)
type Config struct {
	// Core settings
	Enabled       bool
	MeterProvider metric.MeterProvider

	// Metric groups (bitwise flags)
	MetricGroups MetricGroupFlags

	// Command filtering
	IncludeCommands map[string]bool // nil means include all
	ExcludeCommands map[string]bool // nil means exclude none

	// Cardinality reduction
	HidePubSubChannelNames bool
	HideStreamNames        bool

	// Histogram settings
	HistogramAggregation HistogramAggregation

	// Bucket configurations for different histogram metrics
	BucketsOperationDuration    []float64
	BucketsStreamLag            []float64
	BucketsConnectionCreateTime []float64
	BucketsConnectionWaitTime   []float64
	BucketsConnectionUseTime    []float64
}

// NewConfig creates a new Config with default values.
// Default configuration:
// - Enabled: false (must explicitly enable)
// - MetricGroups: connection-basic + resiliency
// - HistogramAggregation: explicit bucket
// - Buckets: 0.1ms to 10s (suitable for Redis operations)
//
// Example:
//
//	config := redisotel.NewConfig().
//	    WithEnabled(true).
//	    WithMetricGroups(redisotel.MetricGroupAll)
func NewConfig() *Config {
	return &Config{
		Enabled:       false,
		MeterProvider: nil, // Will use global otel.GetMeterProvider() if nil

		// Default metric groups: connection-basic + resiliency
		MetricGroups: MetricGroupFlagConnectionBasic | MetricGroupFlagResiliency,

		// No command filtering by default
		IncludeCommands: nil,
		ExcludeCommands: nil,

		// Don't hide labels by default
		HidePubSubChannelNames: false,
		HideStreamNames:        false,

		// Use explicit bucket histogram by default
		HistogramAggregation: HistogramAggregationExplicitBucket,

		// Default buckets for all duration metrics
		BucketsOperationDuration:    defaultHistogramBuckets(),
		BucketsStreamLag:            defaultHistogramBuckets(),
		BucketsConnectionCreateTime: defaultHistogramBuckets(),
		BucketsConnectionWaitTime:   defaultHistogramBuckets(),
		BucketsConnectionUseTime:    defaultHistogramBuckets(),
	}
}

// WithEnabled enables or disables metrics emission.
// Default: false (must explicitly enable)
func (c *Config) WithEnabled(enabled bool) *Config {
	c.Enabled = enabled
	return c
}

// WithMeterProvider sets the meter provider to use for creating metrics.
// If not provided, the global meter provider from otel.GetMeterProvider() will be used.
func (c *Config) WithMeterProvider(provider metric.MeterProvider) *Config {
	c.MeterProvider = provider
	return c
}

// WithMetricGroups sets which metric groups to register using bitwise flags.
// You can combine multiple groups with the | operator.
func (c *Config) WithMetricGroups(groups MetricGroupFlags) *Config {
	c.MetricGroups = groups
	return c
}

// WithIncludeCommands sets a command allow-list for metrics.
func (c *Config) WithIncludeCommands(commands []string) *Config {
	c.IncludeCommands = make(map[string]bool)
	for _, cmd := range commands {
		c.IncludeCommands[cmd] = true
	}
	return c
}

// WithExcludeCommands sets a command deny-list for metrics.
// Commands in this list will not have metrics recorded.
func (c *Config) WithExcludeCommands(commands []string) *Config {
	c.ExcludeCommands = make(map[string]bool)
	for _, cmd := range commands {
		c.ExcludeCommands[cmd] = true
	}
	return c
}

// WithHidePubSubChannelNames omits channel label from Pub/Sub metrics to reduce cardinality.
func (c *Config) WithHidePubSubChannelNames(hide bool) *Config {
	c.HidePubSubChannelNames = hide
	return c
}

// WithHideStreamNames omits stream label from stream metrics to reduce cardinality.
func (c *Config) WithHideStreamNames(hide bool) *Config {
	c.HideStreamNames = hide
	return c
}

// WithHistogramAggregation sets the histogram aggregation mode.
func (c *Config) WithHistogramAggregation(agg HistogramAggregation) *Config {
	c.HistogramAggregation = agg
	return c
}

// WithHistogramBuckets sets custom histogram buckets for ALL duration metrics.
// If not set, uses defaultHistogramBuckets() which covers 0.1ms to 10s.
// Buckets should be in seconds (e.g., 0.001 = 1ms, 0.1 = 100ms, 1.0 = 1s).
func (c *Config) WithHistogramBuckets(buckets []float64) *Config {
	c.BucketsOperationDuration = buckets
	c.BucketsStreamLag = buckets
	c.BucketsConnectionCreateTime = buckets
	c.BucketsConnectionWaitTime = buckets
	c.BucketsConnectionUseTime = buckets
	return c
}
