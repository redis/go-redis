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

// config holds the configuration for the instrumentation
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

func defaultConfig() config {
	return config{
		meterProvider: nil, // Will use global otel.GetMeterProvider() if nil
		enabled:       false,

		// Default metric groups: connection-basic, resiliency
		enabledMetricGroups: map[MetricGroup]bool{
			MetricGroupConnectionBasic: true,
			MetricGroupResiliency:      true,
		},

		// No command filtering by default
		includeCommands: nil,
		excludeCommands: nil,

		// Don't hide labels by default
		hidePubSubChannelNames: false,
		hideStreamNames:        false,

		// Use explicit bucket histogram by default
		histAggregation: HistogramAggregationExplicitBucket,

		// Default buckets for all duration metrics
		bucketsOperationDuration:        defaultHistogramBuckets(),
		bucketsStreamProcessingDuration: defaultHistogramBuckets(),
		bucketsConnectionCreateTime:     defaultHistogramBuckets(),
		bucketsConnectionWaitTime:       defaultHistogramBuckets(),
		bucketsConnectionUseTime:        defaultHistogramBuckets(),
	}
}

// isMetricGroupEnabled checks if a metric group is enabled
func (c *config) isMetricGroupEnabled(group MetricGroup) bool {
	return c.enabledMetricGroups[group]
}

// isCommandIncluded checks if a command should be included in metrics
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

// Option is a functional option for configuring the instrumentation
type Option interface {
	apply(*config)
}

// optionFunc wraps a function to implement the Option interface
type optionFunc func(*config)

func (f optionFunc) apply(c *config) {
	f(c)
}

// WithMeterProvider sets the meter provider to use for creating metrics.
// If not provided, the global meter provider from otel.GetMeterProvider() will be used.
func WithMeterProvider(provider metric.MeterProvider) Option {
	return optionFunc(func(c *config) {
		c.meterProvider = provider
	})
}

// WithEnabled enables or disables metrics emission
func WithEnabled(enabled bool) Option {
	return optionFunc(func(c *config) {
		c.enabled = enabled
	})
}

// WithEnabledMetricGroups sets which metric groups to register
// Default: ["connection-basic", "resiliency"]
func WithEnabledMetricGroups(groups []MetricGroup) Option {
	return optionFunc(func(c *config) {
		c.enabledMetricGroups = make(map[MetricGroup]bool)
		for _, group := range groups {
			c.enabledMetricGroups[group] = true
		}
	})
}

// WithIncludeCommands sets a command allow-list for metrics
// Only commands in this list will have metrics recorded
// If not set, all commands are included (unless excluded)
func WithIncludeCommands(commands []string) Option {
	return optionFunc(func(c *config) {
		c.includeCommands = make(map[string]bool)
		for _, cmd := range commands {
			c.includeCommands[cmd] = true
		}
	})
}

// WithExcludeCommands sets a command deny-list for metrics
// Commands in this list will not have metrics recorded
func WithExcludeCommands(commands []string) Option {
	return optionFunc(func(c *config) {
		c.excludeCommands = make(map[string]bool)
		for _, cmd := range commands {
			c.excludeCommands[cmd] = true
		}
	})
}

// WithHidePubSubChannelNames omits channel label from Pub/Sub metrics to reduce cardinality
func WithHidePubSubChannelNames(hide bool) Option {
	return optionFunc(func(c *config) {
		c.hidePubSubChannelNames = hide
	})
}

// WithHideStreamNames omits stream label from stream metrics to reduce cardinality
func WithHideStreamNames(hide bool) Option {
	return optionFunc(func(c *config) {
		c.hideStreamNames = hide
	})
}

// WithHistogramAggregation sets the histogram aggregation mode
// Controls whether bucket overrides apply
func WithHistogramAggregation(agg HistogramAggregation) Option {
	return optionFunc(func(c *config) {
		c.histAggregation = agg
	})
}

// WithHistogramBuckets sets custom histogram buckets for ALL duration metrics.
// If not set, uses defaultHistogramBuckets() which covers 0.1ms to 10s.
// Buckets should be in seconds (e.g., 0.001 = 1ms, 0.1 = 100ms, 1.0 = 1s).
//
// This applies to all duration histograms:
// - db.client.operation.duration
// - db.client.connection.create_time
// - db.client.connection.wait_time
// - db.client.connection.use_time
// - redis.client.stream.processing_duration
//
// Example:
//
//	redisotel.Init(rdb,
//	    redisotel.WithHistogramBuckets([]float64{0.001, 0.01, 0.1, 1.0}),
//	)
func WithHistogramBuckets(buckets []float64) Option {
	return optionFunc(func(c *config) {
		c.bucketsOperationDuration = buckets
		c.bucketsStreamProcessingDuration = buckets
		c.bucketsConnectionCreateTime = buckets
		c.bucketsConnectionWaitTime = buckets
		c.bucketsConnectionUseTime = buckets
	})
}
