package redisotel

import (
	"go.opentelemetry.io/otel/metric"
)

// config holds the configuration for the instrumentation
type config struct {
	meterProvider    metric.MeterProvider
	histogramBuckets []float64
}

// defaultConfig returns the default configuration
func defaultConfig() config {
	return config{
		meterProvider:    nil, // Will use global otel.GetMeterProvider() if nil
		histogramBuckets: defaultHistogramBuckets(),
	}
}

// defaultHistogramBuckets returns the default histogram buckets for operation duration
// These buckets are designed to capture typical Redis operation latencies:
// - Sub-millisecond: 0.0001s (0.1ms), 0.0005s (0.5ms)
// - Milliseconds: 0.001s (1ms), 0.005s (5ms), 0.01s (10ms), 0.05s (50ms), 0.1s (100ms)
// - Sub-second: 0.5s (500ms)
// - Seconds: 1s, 5s, 10s
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

// WithHistogramBuckets sets custom histogram buckets for operation duration
// Buckets should be in seconds and in ascending order
func WithHistogramBuckets(buckets []float64) Option {
	return optionFunc(func(c *config) {
		c.histogramBuckets = buckets
	})
}
