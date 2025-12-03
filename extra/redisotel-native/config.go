package redisotel

import (
	"go.opentelemetry.io/otel/metric"
)

// MetricGroup represents a group of related metrics
type MetricGroup string

const (
	// MetricGroupCommand includes command-level metrics
	MetricGroupCommand MetricGroup = "command"
	// MetricGroupConnectionBasic includes basic connection metrics
	MetricGroupConnectionBasic MetricGroup = "connection-basic"
	// MetricGroupResiliency includes resiliency metrics (errors, retries, etc.)
	MetricGroupResiliency MetricGroup = "resiliency"
	// MetricGroupConnectionAdvanced includes advanced connection metrics
	MetricGroupConnectionAdvanced MetricGroup = "connection-advanced"
	// MetricGroupStream includes stream-specific metrics
	MetricGroupStream MetricGroup = "stream"
)

// HistogramAggregation represents the histogram aggregation mode
type HistogramAggregation string

const (
	// HistogramAggregationExplicitBucket uses explicit bucket boundaries
	HistogramAggregationExplicitBucket HistogramAggregation = "explicit_bucket_histogram"
	// HistogramAggregationBase2Exponential uses base-2 exponential buckets
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

// defaultConfig returns the default configuration
func defaultConfig() config {
	return config{
		meterProvider: nil, // Will use global otel.GetMeterProvider() if nil
		enabled:       false,

		// Default metric groups: command, connection-basic, resiliency
		enabledMetricGroups: map[MetricGroup]bool{
			MetricGroupCommand:         true,
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

		// Default buckets for different metrics
		bucketsOperationDuration:        defaultOperationDurationBuckets(),
		bucketsStreamProcessingDuration: defaultStreamProcessingDurationBuckets(),
		bucketsConnectionCreateTime:     defaultConnectionCreateTimeBuckets(),
		bucketsConnectionWaitTime:       defaultConnectionWaitTimeBuckets(),
		bucketsConnectionUseTime:        defaultConnectionUseTimeBuckets(),
	}
}

// isMetricGroupEnabled checks if a metric group is enabled
func (c *config) isMetricGroupEnabled(group MetricGroup) bool {
	return c.enabledMetricGroups[group]
}

// isCommandIncluded checks if a command should be included in metrics
func (c *config) isCommandIncluded(command string) bool {
	// If there's an exclude list and command is in it, exclude
	if c.excludeCommands != nil && c.excludeCommands[command] {
		return false
	}

	// If there's an include list, only include if command is in it
	if c.includeCommands != nil {
		return c.includeCommands[command]
	}

	// No filtering, include all
	return true
}

// defaultOperationDurationBuckets returns the default histogram buckets for db.client.operation.duration
// These buckets are designed to capture typical Redis operation latencies:
// - Sub-millisecond: 0.0001s (0.1ms), 0.0005s (0.5ms)
// - Milliseconds: 0.001s (1ms), 0.005s (5ms), 0.01s (10ms), 0.05s (50ms), 0.1s (100ms)
// - Sub-second: 0.5s (500ms)
// - Seconds: 1s, 2.5s
func defaultOperationDurationBuckets() []float64 {
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
		2.5,    // 2.5s
	}
}

// defaultStreamProcessingDurationBuckets returns the default histogram buckets for redis.client.stream.processing_duration
// Stream processing can take longer than regular operations
func defaultStreamProcessingDurationBuckets() []float64 {
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

// defaultConnectionCreateTimeBuckets returns the default histogram buckets for db.client.connection.create_time
// Connection creation can take longer than regular operations
func defaultConnectionCreateTimeBuckets() []float64 {
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

// defaultConnectionWaitTimeBuckets returns the default histogram buckets for db.client.connection.wait_time
// Time waiting for a connection from the pool
func defaultConnectionWaitTimeBuckets() []float64 {
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

// defaultConnectionUseTimeBuckets returns the default histogram buckets for db.client.connection.use_time
// Time a connection is in use (checked out from pool)
func defaultConnectionUseTimeBuckets() []float64 {
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
// Default: ["command", "connection-basic", "resiliency"]
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

// WithBucketsOperationDuration sets explicit buckets (seconds) for db.client.operation.duration
func WithBucketsOperationDuration(buckets []float64) Option {
	return optionFunc(func(c *config) {
		c.bucketsOperationDuration = buckets
	})
}

// WithBucketsStreamProcessingDuration sets explicit buckets (seconds) for redis.client.stream.processing_duration
func WithBucketsStreamProcessingDuration(buckets []float64) Option {
	return optionFunc(func(c *config) {
		c.bucketsStreamProcessingDuration = buckets
	})
}

// WithBucketsConnectionCreateTime sets buckets for db.client.connection.create_time
func WithBucketsConnectionCreateTime(buckets []float64) Option {
	return optionFunc(func(c *config) {
		c.bucketsConnectionCreateTime = buckets
	})
}

// WithBucketsConnectionWaitTime sets buckets for db.client.connection.wait_time
func WithBucketsConnectionWaitTime(buckets []float64) Option {
	return optionFunc(func(c *config) {
		c.bucketsConnectionWaitTime = buckets
	})
}

// WithBucketsConnectionUseTime sets buckets for db.client.connection.use_time
func WithBucketsConnectionUseTime(buckets []float64) Option {
	return optionFunc(func(c *config) {
		c.bucketsConnectionUseTime = buckets
	})
}
