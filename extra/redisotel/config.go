package redisotel

import (
	"strings"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"
)

type config struct {
	// Common options.

	dbSystem string
	attrs    []attribute.KeyValue

	// Tracing options.

	tp     trace.TracerProvider
	tracer trace.Tracer

	dbStmtEnabled         bool
	callerEnabled         bool
	filterDial            bool
	filterProcessPipeline func(cmds []redis.Cmder) bool
	filterProcess         func(cmd redis.Cmder) bool

	// Metrics options.

	mp    metric.MeterProvider
	meter metric.Meter

	poolName string

	closeChan chan struct{}
}

type baseOption interface {
	apply(conf *config)
}

type Option interface {
	baseOption
	tracing()
	metrics()
}

type option func(conf *config)

func (fn option) apply(conf *config) {
	fn(conf)
}

func (fn option) tracing() {}

func (fn option) metrics() {}

func newConfig(opts ...baseOption) *config {
	conf := &config{
		dbSystem: "redis",
		attrs:    []attribute.KeyValue{},

		tp:            otel.GetTracerProvider(),
		mp:            otel.GetMeterProvider(),
		dbStmtEnabled: true,
		callerEnabled: true,
		filterProcess: DefaultCommandFilter,
		filterProcessPipeline: func(cmds []redis.Cmder) bool {
			for _, cmd := range cmds {
				if DefaultCommandFilter(cmd) {
					return true
				}
			}
			return false
		},
	}

	for _, opt := range opts {
		opt.apply(conf)
	}

	conf.attrs = append(conf.attrs, semconv.DBSystemKey.String(conf.dbSystem))

	return conf
}

func WithDBSystem(dbSystem string) Option {
	return option(func(conf *config) {
		conf.dbSystem = dbSystem
	})
}

// WithAttributes specifies additional attributes to be added to the span.
func WithAttributes(attrs ...attribute.KeyValue) Option {
	return option(func(conf *config) {
		conf.attrs = append(conf.attrs, attrs...)
	})
}

//------------------------------------------------------------------------------

type TracingOption interface {
	baseOption
	tracing()
}

type tracingOption func(conf *config)

var _ TracingOption = (*tracingOption)(nil)

func (fn tracingOption) apply(conf *config) {
	fn(conf)
}

func (fn tracingOption) tracing() {}

// WithTracerProvider specifies a tracer provider to use for creating a tracer.
// If none is specified, the global provider is used.
func WithTracerProvider(provider trace.TracerProvider) TracingOption {
	return tracingOption(func(conf *config) {
		conf.tp = provider
	})
}

// WithDBStatement tells the tracing hook to log raw redis commands.
func WithDBStatement(on bool) TracingOption {
	return tracingOption(func(conf *config) {
		conf.dbStmtEnabled = on
	})
}

// WithCallerEnabled tells the tracing hook to log the calling function, file and line.
func WithCallerEnabled(on bool) TracingOption {
	return tracingOption(func(conf *config) {
		conf.callerEnabled = on
	})
}

// WithCommandFilter allows filtering of commands when tracing to omit commands that may have sensitive details like
// passwords.
func WithCommandFilter(filter func(cmd redis.Cmder) bool) TracingOption {
	return tracingOption(func(conf *config) {
		conf.filterProcess = filter
	})
}

// WithCommandsFilter allows filtering of pipeline commands
// when tracing to omit commands that may have sensitive details like
// passwords in a pipeline.
func WithCommandsFilter(filter func(cmds []redis.Cmder) bool) TracingOption {
	return tracingOption(func(conf *config) {
		conf.filterProcessPipeline = filter
	})
}

// WithDialFilter enables or disables filtering of dial commands.
func WithDialFilter(on bool) TracingOption {
	return tracingOption(func(conf *config) {
		conf.filterDial = on
	})
}

// DefaultCommandFilter filters out AUTH commands from tracing.
func DefaultCommandFilter(cmd redis.Cmder) bool {
	if strings.ToLower(cmd.Name()) == "auth" {
		return true
	}

	if strings.ToLower(cmd.Name()) == "hello" {
		if len(cmd.Args()) < 3 {
			return false
		}

		arg, exists := cmd.Args()[2].(string)
		if !exists {
			return false
		}

		if strings.ToLower(arg) == "auth" {
			return true
		}
	}

	return false
}

// BasicCommandFilter filters out AUTH commands from tracing.
// Deprecated: use DefaultCommandFilter instead.
func BasicCommandFilter(cmd redis.Cmder) bool {
	return DefaultCommandFilter(cmd)
}

//------------------------------------------------------------------------------

type MetricsOption interface {
	baseOption
	metrics()
}

type metricsOption func(conf *config)

var _ MetricsOption = (*metricsOption)(nil)

func (fn metricsOption) apply(conf *config) {
	fn(conf)
}

func (fn metricsOption) metrics() {}

// WithMeterProvider configures a metric.Meter used to create instruments.
func WithMeterProvider(mp metric.MeterProvider) MetricsOption {
	return metricsOption(func(conf *config) {
		conf.mp = mp
	})
}

func WithCloseChan(closeChan chan struct{}) MetricsOption {
	return metricsOption(func(conf *config) {
		conf.closeChan = closeChan
	})
}
