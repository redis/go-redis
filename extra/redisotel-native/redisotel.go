package redisotel

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

var (
	// Global observability instance
	observabilityInstance     *ObservabilityInstance
	observabilityInstanceOnce sync.Once
)

// ObservabilityInstance manages the global observability singleton.
type ObservabilityInstance struct {
	mu          sync.RWMutex
	config      *Config
	recorder    *metricsRecorder
	initialized bool
}

// GetObservabilityInstance returns the global observability singleton.
func GetObservabilityInstance() *ObservabilityInstance {
	observabilityInstanceOnce.Do(func() {
		observabilityInstance = &ObservabilityInstance{}
	})
	return observabilityInstance
}

// Init initializes OpenTelemetry observability globally for all Redis clients.
// This should be called once at application startup, BEFORE creating any Redis clients.
// After initialization, all Redis clients will automatically collect and export
// metrics without needing any additional configuration.
func (o *ObservabilityInstance) Init(cfg *Config) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	// If already initialized, shutdown previous instance
	if o.initialized {
		o.shutdownLocked()
	}

	o.config = cfg

	if !cfg.Enabled {
		return nil
	}

	// Get meter provider (use global if not provided)
	meterProvider := cfg.MeterProvider
	if meterProvider == nil {
		meterProvider = otel.GetMeterProvider()
	}

	meter := meterProvider.Meter(
		"github.com/redis/go-redis",
		metric.WithInstrumentationVersion(redis.Version()),
	)

	internalCfg := o.configToInternal(cfg)
	recorder, err := o.createRecorder(meter, internalCfg)
	if err != nil {
		return fmt.Errorf("failed to create metrics recorder: %w", err)
	}

	o.recorder = recorder
	o.initialized = true
	redis.SetOTelRecorder(recorder)

	return nil
}

// IsEnabled returns true if observability is initialized and enabled.
func (o *ObservabilityInstance) IsEnabled() bool {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.initialized && o.config != nil && o.config.Enabled
}

// Shutdown cleans up resources and flushes any pending metrics.
// This should be called at application shutdown.
func (o *ObservabilityInstance) Shutdown() error {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.shutdownLocked()
}

func (o *ObservabilityInstance) shutdownLocked() error {
	if !o.initialized {
		return nil
	}

	redis.SetOTelRecorder(nil)

	// Note: We don't shutdown the MeterProvider since it's owned by the application
	// The application should call provider.Shutdown() when appropriate

	o.recorder = nil
	o.initialized = false

	return nil
}

// configToInternal converts the public Config to internal config format
func (o *ObservabilityInstance) configToInternal(cfg *Config) config {
	enabledGroups := make(map[MetricGroup]bool)
	if cfg.MetricGroups&MetricGroupFlagCommand != 0 {
		enabledGroups[MetricGroupCommand] = true
	}
	if cfg.MetricGroups&MetricGroupFlagConnectionBasic != 0 {
		enabledGroups[MetricGroupConnectionBasic] = true
	}
	if cfg.MetricGroups&MetricGroupFlagResiliency != 0 {
		enabledGroups[MetricGroupResiliency] = true
	}
	if cfg.MetricGroups&MetricGroupFlagConnectionAdvanced != 0 {
		enabledGroups[MetricGroupConnectionAdvanced] = true
	}
	if cfg.MetricGroups&MetricGroupFlagPubSub != 0 {
		enabledGroups[MetricGroupPubSub] = true
	}
	if cfg.MetricGroups&MetricGroupFlagStream != 0 {
		enabledGroups[MetricGroupStream] = true
	}

	return config{
		meterProvider:                   cfg.MeterProvider,
		enabled:                         cfg.Enabled,
		enabledMetricGroups:             enabledGroups,
		includeCommands:                 cfg.IncludeCommands,
		excludeCommands:                 cfg.ExcludeCommands,
		hidePubSubChannelNames:          cfg.HidePubSubChannelNames,
		hideStreamNames:                 cfg.HideStreamNames,
		histAggregation:                 cfg.HistogramAggregation,
		bucketsOperationDuration:        cfg.BucketsOperationDuration,
		bucketsStreamProcessingDuration: cfg.BucketsStreamLag,
		bucketsConnectionCreateTime:     cfg.BucketsConnectionCreateTime,
		bucketsConnectionWaitTime:       cfg.BucketsConnectionWaitTime,
		bucketsConnectionUseTime:        cfg.BucketsConnectionUseTime,
	}
}

// createRecorder creates a metricsRecorder with all instruments based on config.
func (o *ObservabilityInstance) createRecorder(meter metric.Meter, cfg config) (*metricsRecorder, error) {
	var err error

	var operationDuration metric.Float64Histogram
	if cfg.isMetricGroupEnabled(MetricGroupCommand) {
		var operationDurationOpts []metric.Float64HistogramOption
		operationDurationOpts = append(operationDurationOpts,
			metric.WithDescription("Duration of database client operations"),
			metric.WithUnit("s"),
		)
		if cfg.histAggregation == HistogramAggregationExplicitBucket {
			operationDurationOpts = append(operationDurationOpts,
				metric.WithExplicitBucketBoundaries(cfg.bucketsOperationDuration...),
			)
		}
		operationDuration, err = meter.Float64Histogram(
			"db.client.operation.duration",
			operationDurationOpts...,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create operation duration histogram: %w", err)
		}
	}

	var connectionCountGauge metric.Int64ObservableGauge
	var connectionCreateTime metric.Float64Histogram
	var connectionRelaxedTimeout metric.Int64UpDownCounter
	var connectionHandoff metric.Int64Counter

	if cfg.isMetricGroupEnabled(MetricGroupConnectionBasic) {
		connectionCountGauge, err = meter.Int64ObservableGauge(
			"db.client.connection.count",
			metric.WithDescription("The number of connections that are currently in state described by the state attribute"),
			metric.WithUnit("{connection}"),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create connection count metric: %w", err)
		}

		var connectionCreateTimeOpts []metric.Float64HistogramOption
		connectionCreateTimeOpts = append(connectionCreateTimeOpts,
			metric.WithDescription("The time it took to create a new connection"),
			metric.WithUnit("s"),
		)
		if cfg.histAggregation == HistogramAggregationExplicitBucket {
			connectionCreateTimeOpts = append(connectionCreateTimeOpts,
				metric.WithExplicitBucketBoundaries(cfg.bucketsConnectionCreateTime...),
			)
		}
		connectionCreateTime, err = meter.Float64Histogram(
			"db.client.connection.create_time",
			connectionCreateTimeOpts...,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create connection create time histogram: %w", err)
		}

		connectionRelaxedTimeout, err = meter.Int64UpDownCounter(
			"redis.client.connection.relaxed_timeout",
			metric.WithDescription("How many times the connection timeout has been increased/decreased (after a server maintenance notification)"),
			metric.WithUnit("{relaxation}"),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create connection relaxed timeout metric: %w", err)
		}

		connectionHandoff, err = meter.Int64Counter(
			"redis.client.connection.handoff",
			metric.WithDescription("Connections that have been handed off to another node (e.g after a MOVING notification)"),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create connection handoff metric: %w", err)
		}
	}

	var clientErrors metric.Int64Counter
	var maintenanceNotifications metric.Int64Counter

	if cfg.isMetricGroupEnabled(MetricGroupResiliency) {
		clientErrors, err = meter.Int64Counter(
			"redis.client.errors",
			metric.WithDescription("Number of errors handled by the Redis client"),
			metric.WithUnit("{error}"),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create client errors metric: %w", err)
		}

		maintenanceNotifications, err = meter.Int64Counter(
			"redis.client.maintenance.notifications",
			metric.WithDescription("Number of maintenance notifications received"),
			metric.WithUnit("{notification}"),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create maintenance notifications metric: %w", err)
		}
	}

	var connectionWaitTime metric.Float64Histogram
	var connectionUseTime metric.Float64Histogram
	var connectionClosed metric.Int64Counter
	var connectionPendingReqsGauge metric.Int64ObservableGauge

	if cfg.isMetricGroupEnabled(MetricGroupConnectionAdvanced) {
		var connectionWaitTimeOpts []metric.Float64HistogramOption
		connectionWaitTimeOpts = append(connectionWaitTimeOpts,
			metric.WithDescription("The time it took to obtain a connection from the pool"),
			metric.WithUnit("s"),
		)
		if cfg.histAggregation == HistogramAggregationExplicitBucket {
			connectionWaitTimeOpts = append(connectionWaitTimeOpts,
				metric.WithExplicitBucketBoundaries(cfg.bucketsConnectionWaitTime...),
			)
		}
		connectionWaitTime, err = meter.Float64Histogram(
			"db.client.connection.wait_time",
			connectionWaitTimeOpts...,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create connection wait time histogram: %w", err)
		}

		var connectionUseTimeOpts []metric.Float64HistogramOption
		connectionUseTimeOpts = append(connectionUseTimeOpts,
			metric.WithDescription("The time between borrowing a connection and returning it to the pool"),
			metric.WithUnit("s"),
		)
		if cfg.histAggregation == HistogramAggregationExplicitBucket {
			connectionUseTimeOpts = append(connectionUseTimeOpts,
				metric.WithExplicitBucketBoundaries(cfg.bucketsConnectionUseTime...),
			)
		}
		connectionUseTime, err = meter.Float64Histogram(
			"db.client.connection.use_time",
			connectionUseTimeOpts...,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create connection use time histogram: %w", err)
		}

		connectionClosed, err = meter.Int64Counter(
			"redis.client.connection.closed",
			metric.WithDescription("The number of connections that have been closed"),
			metric.WithUnit("{connection}"),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create connection closed metric: %w", err)
		}

		connectionPendingReqsGauge, err = meter.Int64ObservableGauge(
			"db.client.connection.pending_requests",
			metric.WithDescription("The number of pending requests waiting for a connection"),
			metric.WithUnit("{request}"),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create connection pending requests metric: %w", err)
		}
	}

	var pubsubMessages metric.Int64Counter

	if cfg.isMetricGroupEnabled(MetricGroupPubSub) {
		pubsubMessages, err = meter.Int64Counter(
			"redis.client.pubsub.messages",
			metric.WithDescription("The number of Pub/Sub messages sent or received"),
			metric.WithUnit("{message}"),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create Pub/Sub messages metric: %w", err)
		}
	}

	var streamLag metric.Float64Histogram

	if cfg.isMetricGroupEnabled(MetricGroupStream) {
		var streamLagOpts []metric.Float64HistogramOption
		streamLagOpts = append(streamLagOpts,
			metric.WithDescription("The lag between message creation and consumption in a stream consumer group"),
			metric.WithUnit("s"),
		)
		if cfg.histAggregation == HistogramAggregationExplicitBucket {
			streamLagOpts = append(streamLagOpts,
				metric.WithExplicitBucketBoundaries(cfg.bucketsStreamProcessingDuration...),
			)
		}
		streamLag, err = meter.Float64Histogram(
			"redis.client.stream.lag",
			streamLagOpts...,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create stream lag histogram: %w", err)
		}
	}

	// Create recorder
	recorder := &metricsRecorder{
		operationDuration:          operationDuration,
		connectionCountGauge:       connectionCountGauge,
		connectionCreateTime:       connectionCreateTime,
		connectionRelaxedTimeout:   connectionRelaxedTimeout,
		connectionHandoff:          connectionHandoff,
		clientErrors:               clientErrors,
		maintenanceNotifications:   maintenanceNotifications,
		connectionWaitTime:         connectionWaitTime,
		connectionUseTime:          connectionUseTime,
		connectionClosed:           connectionClosed,
		connectionPendingReqsGauge: connectionPendingReqsGauge,
		pubsubMessages:             pubsubMessages,
		streamLag:                  streamLag,
		cfg:                        &cfg,
		pools:                      make([]poolInfo, 0),
	}

	// Register async callbacks for ObservableGauges
	// These callbacks will pull stats from registered pools
	if err := o.registerAsyncCallbacks(meter, recorder); err != nil {
		return nil, fmt.Errorf("failed to register async callbacks: %w", err)
	}

	return recorder, nil
}

// parsePoolName extracts server address, port, and database index from a pool name.
// Pool name format: "host:port/db" or "host/db" or "host:port" or "host"
// Returns: (serverAddr, serverPort, dbIndex)
func parsePoolName(poolName string) (string, string, string) {
	// Handle special pool names
	if poolName == "main" || poolName == "pubsub" {
		return "", "", ""
	}

	parts := strings.Split(poolName, "/")
	addrPart := parts[0]
	dbIndex := ""
	if len(parts) > 1 {
		dbIndex = parts[1]
	}
	host, port := parseAddr(addrPart)
	return host, port, dbIndex
}

func (o *ObservabilityInstance) registerAsyncCallbacks(meter metric.Meter, recorder *metricsRecorder) error {
	// Register connection count gauge callback
	if recorder.connectionCountGauge != nil {
		_, err := meter.RegisterCallback(
			func(ctx context.Context, observer metric.Observer) error {
				recorder.poolsMu.RLock()
				pools := recorder.pools
				pubsubPools := recorder.pubsubPools
				recorder.poolsMu.RUnlock()

				// Iterate over all registered main pools
				for _, poolInfo := range pools {
					stats := poolInfo.pool.PoolStats()
					if stats == nil {
						continue
					}

					// Extract server info from pool name
					serverAddr, serverPort, _ := parsePoolName(poolInfo.name)

					// Build base attributes
					baseAttrs := []attribute.KeyValue{
						attribute.String("db.system.name", "redis"),
						getLibraryVersionAttr(),
					}
					if serverAddr != "" {
						baseAttrs = append(baseAttrs, attribute.String("server.address", serverAddr))
					}
					if serverPort != "" && serverPort != "6379" {
						baseAttrs = append(baseAttrs, attribute.String("server.port", serverPort))
					}

					// Add pool name
					baseAttrs = append(baseAttrs, attribute.String("db.client.connection.pool.name", poolInfo.name))

					// Observe idle connections
					idleAttrs := append([]attribute.KeyValue{}, baseAttrs...)
					idleAttrs = append(idleAttrs,
						attribute.String("db.client.connection.state", "idle"),
						attribute.Bool("redis.client.connection.pubsub", false),
					)
					observer.ObserveInt64(recorder.connectionCountGauge, int64(stats.IdleConns),
						metric.WithAttributes(idleAttrs...))

					// Observe used connections
					usedConns := stats.TotalConns - stats.IdleConns
					usedAttrs := append([]attribute.KeyValue{}, baseAttrs...)
					usedAttrs = append(usedAttrs,
						attribute.String("db.client.connection.state", "used"),
						attribute.Bool("redis.client.connection.pubsub", false),
					)
					observer.ObserveInt64(recorder.connectionCountGauge, int64(usedConns),
						metric.WithAttributes(usedAttrs...))
				}

				for _, pubsubPoolInfo := range pubsubPools {
					stats := pubsubPoolInfo.pool.Stats()
					if stats == nil {
						continue
					}

					// PubSub pools don't have server info in the pool name
					// We'll use empty server address/port for now
					// TODO: Consider storing server info with the pool

					// Build base attributes
					baseAttrs := []attribute.KeyValue{
						attribute.String("db.system.name", "redis"),
						getLibraryVersionAttr(),
						attribute.String("db.client.connection.pool.name", "pubsub"),
					}

					// PubSub pools report Active connections (not idle/used split
					// We'll report Active as "used" and 0 as "idle"
					idleAttrs := append([]attribute.KeyValue{}, baseAttrs...)
					idleAttrs = append(idleAttrs,
						attribute.String("db.client.connection.state", "idle"),
						attribute.Bool("redis.client.connection.pubsub", true),
					)
					observer.ObserveInt64(recorder.connectionCountGauge, 0,
						metric.WithAttributes(idleAttrs...))

					usedAttrs := append([]attribute.KeyValue{}, baseAttrs...)
					usedAttrs = append(usedAttrs,
						attribute.String("db.client.connection.state", "used"),
						attribute.Bool("redis.client.connection.pubsub", true),
					)
					observer.ObserveInt64(recorder.connectionCountGauge, int64(stats.Active),
						metric.WithAttributes(usedAttrs...))
				}

				return nil
			},
			recorder.connectionCountGauge,
		)
		if err != nil {
			return fmt.Errorf("failed to register connection count callback: %w", err)
		}
	}

	// Register pending requests gauge callback
	if recorder.connectionPendingReqsGauge != nil {
		_, err := meter.RegisterCallback(
			func(ctx context.Context, observer metric.Observer) error {
				recorder.poolsMu.RLock()
				pools := recorder.pools
				recorder.poolsMu.RUnlock()

				// Iterate over all registered pools
				for _, poolInfo := range pools {
					stats := poolInfo.pool.PoolStats()
					if stats == nil {
						continue
					}

					// Extract server info from pool name
					serverAddr, serverPort, _ := parsePoolName(poolInfo.name)

					// Build base attributes
					baseAttrs := []attribute.KeyValue{
						attribute.String("db.system.name", "redis"),
						getLibraryVersionAttr(),
					}
					if serverAddr != "" {
						baseAttrs = append(baseAttrs, attribute.String("server.address", serverAddr))
					}
					if serverPort != "" && serverPort != "6379" {
						baseAttrs = append(baseAttrs, attribute.String("server.port", serverPort))
					}

					// Add pool name
					baseAttrs = append(baseAttrs, attribute.String("db.client.connection.pool.name", poolInfo.name))

					// Observe pending requests count
					observer.ObserveInt64(recorder.connectionPendingReqsGauge, int64(stats.PendingRequests),
						metric.WithAttributes(baseAttrs...))
				}

				return nil
			},
			recorder.connectionPendingReqsGauge,
		)
		if err != nil {
			return fmt.Errorf("failed to register pending requests callback: %w", err)
		}
	}

	return nil
}
