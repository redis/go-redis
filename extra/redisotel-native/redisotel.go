package redisotel

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/semconv/v1.38.0/dbconv"

	"github.com/redis/go-redis/v9"
)

// Metric name constants
const (
	// Operation metrics
	MetricOperationDuration = "db.client.operation.duration"

	// Connection metrics
	MetricConnectionCount          = "db.client.connection.count"
	MetricConnectionCreateTime     = "db.client.connection.create_time"
	MetricConnectionWaitTime       = "db.client.connection.wait_time"
	MetricConnectionPendingReqs    = "db.client.connection.pending_requests"
	MetricConnectionRelaxedTimeout = "redis.client.connection.relaxed_timeout"
	MetricConnectionHandoff        = "redis.client.connection.handoff"
	MetricConnectionClosed         = "redis.client.connection.closed"

	// Resiliency metrics
	MetricClientErrors             = "redis.client.errors"
	MetricMaintenanceNotifications = "redis.client.maintenance.notifications"

	// Pub/Sub metrics
	MetricPubSubMessages = "redis.client.pubsub.messages"

	// Stream metrics
	MetricStreamLag = "redis.client.stream.lag"

	// MultiDB (Active-Active failover) metrics
	MetricMultiDBFailovers             = "redis.client.multidb.failovers"
	MetricMultiDBFailoverDuration      = "redis.client.multidb.failover.duration"
	MetricMultiDBActiveDatabaseChanges = "redis.client.multidb.active_database.changes"
	MetricMultiDBCircuitStateChanges   = "redis.client.multidb.circuit_breaker.state_changes"
	MetricMultiDBHealthChecks          = "redis.client.multidb.health_checks"
	MetricMultiDBHealthCheckDuration   = "redis.client.multidb.health_check.duration"

	// Special pool names
	PoolNameMain   = "main"
	PoolNamePubSub = "pubsub"
)

var (
	// Global observability instance
	observabilityInstance     *ObservabilityInstance
	observabilityInstanceOnce sync.Once
)

// ObservabilityInstance manages the global observability singleton.
type ObservabilityInstance struct {
	mu       sync.RWMutex
	config   *Config
	recorder *metricsRecorder
	// provider is the meter provider Init resolved; NewRecorder defaults to it.
	provider metric.MeterProvider
	// stopped is shared by the global recorder and every recorder NewRecorder
	// issued since Init; Shutdown sets it so they all stop recording together.
	stopped *atomic.Bool
	// ownQueues are the operation-duration queues of recorders NewRecorder
	// built on another provider; Shutdown stops them with the global one.
	ownQueues   []*opQueue
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
// It is usually called once at application startup. Clients created before Init
// are covered too: their existing connections are counted when Init runs.
// After initialization, all Redis clients will automatically collect and export
// metrics without needing any additional configuration.
func (o *ObservabilityInstance) Init(cfg *Config) error {
	o.mu.Lock()
	defer o.mu.Unlock()

	// If already initialized, return error
	if o.initialized {
		return errors.New("redisotel: already initialized, call Shutdown() before reinitializing")
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

	o.stopped = &atomic.Bool{}
	recorder, err := o.newRecorder(cfg, nil, meterProvider, nil)
	if err != nil {
		return err
	}

	o.recorder = recorder
	o.provider = meterProvider
	o.initialized = true
	redis.SetOTelRecorder(recorder)

	return nil
}

// NewRecorder returns a recorder for one client, built from cfg the way Init
// builds the global one. Pass it as Options.OTelRecorder (the same field exists
// on ClusterOptions, RingOptions, FailoverOptions, UniversalOptions and
// MultiDBOptions): that client's commands, errors, Pub/Sub messages, stream lag
// and pool events are recorded to it instead of the global recorder.
//
// Its metrics go to cfg.MeterProvider, or to the provider Init used when nil,
// and carry the custom attributes Init was configured with followed by cfg's
// own, so a per-client attribute overrides a global one with the same key,
// which in turn overrides a built-in one. cfg.Enabled is ignored: Init decides
// whether the process records. Histogram settings that differ from Init's on a
// shared provider are subject to the SDK's duplicate-instrument rule (the first
// registration wins). The recorder stops recording when Shutdown is called.
//
// If Init was called with a disabled config, NewRecorder returns a nil recorder
// and no error: a client given a nil Options.OTelRecorder uses the global
// recorder (a no-op unless one is set), so an app with a metrics on/off switch
// can pass the result through unchanged. It returns an error only when Init
// has not been called.
func (o *ObservabilityInstance) NewRecorder(cfg *Config) (redis.OTelRecorder, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if !o.initialized {
		if o.config != nil && !o.config.Enabled {
			return nil, nil
		}
		return nil, errors.New("redisotel: not initialized, call Init() first")
	}
	provider := cfg.MeterProvider
	var shared *opQueue
	if provider == nil || sameProvider(provider, o.provider) {
		// Same provider, same instruments: share the global recorder's queue
		// instead of starting another goroutine and gauge callback per client.
		provider = o.provider
		shared = o.recorder.opq
	}
	// The base layer is the global recorder's own copy of Init's attributes,
	// not the caller-owned Config, which may have been mutated since Init.
	rec, err := o.newRecorder(cfg, o.recorder.cfg.customAttributes, provider, shared)
	if err != nil {
		return nil, err
	}
	if shared == nil && rec.opq != nil {
		o.ownQueues = append(o.ownQueues, rec.opq)
	}
	return rec, nil
}

// sameProvider reports whether a and b are the same provider. Interface
// equality panics on a non-comparable dynamic type, so such providers are
// treated as distinct.
func sameProvider(a, b metric.MeterProvider) bool {
	ta := reflect.TypeOf(a)
	return ta == reflect.TypeOf(b) && ta.Comparable() && a == b
}

// newRecorder builds a recorder for cfg on provider. Its custom attributes are
// base followed by cfg's own (later wins on a duplicate key); shared, when not
// nil, is the operation-duration queue to record through instead of a new one.
func (o *ObservabilityInstance) newRecorder(cfg *Config, base []attribute.KeyValue, provider metric.MeterProvider, shared *opQueue) (*metricsRecorder, error) {
	meter := provider.Meter(
		"github.com/redis/go-redis",
		metric.WithInstrumentationVersion(redis.Version()),
	)
	internalCfg := o.configToInternal(cfg)
	if len(base) > 0 {
		internalCfg.customAttributes = slices.Concat(base, cfg.CustomAttributes)
	}
	recorder, err := o.createRecorder(meter, internalCfg, shared)
	if err != nil {
		return nil, fmt.Errorf("failed to create metrics recorder: %w", err)
	}
	recorder.stopped = o.stopped
	return recorder, nil
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
	// Recorders issued by NewRecorder stay referenced by their clients: the
	// shared flag turns them off (see metricsRecorder.off).
	o.stopped.Store(true)
	if o.recorder != nil && o.recorder.opq != nil {
		o.recorder.opq.stop() // record what is queued
	}
	for _, q := range o.ownQueues {
		q.stop()
	}
	o.ownQueues = nil

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
	if cfg.MetricGroups&MetricGroupFlagMultiDB != 0 {
		enabledGroups[MetricGroupMultiDB] = true
	}

	return config{
		meterProvider:                   cfg.MeterProvider,
		enabled:                         cfg.Enabled,
		enabledMetricGroups:             enabledGroups,
		includeCommands:                 cfg.IncludeCommands,
		excludeCommands:                 cfg.ExcludeCommands,
		hidePubSubChannelNames:          cfg.HidePubSubChannelNames,
		hideStreamNames:                 cfg.HideStreamNames,
		recordNilErrors:                 cfg.RecordNilErrors,
		histAggregation:                 cfg.HistogramAggregation,
		bucketsOperationDuration:        cfg.BucketsOperationDuration,
		bucketsStreamProcessingDuration: cfg.BucketsStreamLag,
		bucketsConnectionCreateTime:     cfg.BucketsConnectionCreateTime,
		bucketsConnectionWaitTime:       cfg.BucketsConnectionWaitTime,
		customAttributes:                slices.Clone(cfg.CustomAttributes),
	}
}

// createRecorder creates a metricsRecorder with all instruments based on config.
// shared, when not nil, is used as the operation-duration queue instead of a new one.
func (o *ObservabilityInstance) createRecorder(meter metric.Meter, cfg config, shared *opQueue) (*metricsRecorder, error) {
	var err error

	var operationDuration metric.Float64Histogram
	if cfg.isMetricGroupEnabled(MetricGroupCommand) {
		var operationDurationOpts []metric.Float64HistogramOption
		if cfg.histAggregation == HistogramAggregationExplicitBucket {
			operationDurationOpts = append(operationDurationOpts,
				metric.WithExplicitBucketBoundaries(cfg.bucketsOperationDuration...),
			)
		}
		var operationDurationConv dbconv.ClientOperationDuration
		operationDurationConv, err = dbconv.NewClientOperationDuration(meter, operationDurationOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create operation duration histogram: %w", err)
		}
		operationDuration = operationDurationConv.Inst()
	}

	var connectionCount metric.Int64UpDownCounter // OTel semconv: UpDownCounter
	var connectionCreateTime metric.Float64Histogram
	var connectionRelaxedTimeout metric.Int64UpDownCounter
	var connectionHandoff metric.Int64Counter

	if cfg.isMetricGroupEnabled(MetricGroupConnectionBasic) {
		connectionCount, err = meter.Int64UpDownCounter(
			dbconv.ClientConnectionCount{}.Name(),
			metric.WithDescription(dbconv.ClientConnectionCount{}.Description()),
			metric.WithUnit(dbconv.ClientConnectionCount{}.Unit()),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create connection count metric: %w", err)
		}

		var connectionCreateTimeOpts []metric.Float64HistogramOption
		if cfg.histAggregation == HistogramAggregationExplicitBucket {
			connectionCreateTimeOpts = append(connectionCreateTimeOpts,
				metric.WithExplicitBucketBoundaries(cfg.bucketsConnectionCreateTime...),
			)
		}
		var connectionCreateTimeConv dbconv.ClientConnectionCreateTime
		connectionCreateTimeConv, err = dbconv.NewClientConnectionCreateTime(meter, connectionCreateTimeOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create connection create time histogram: %w", err)
		}
		connectionCreateTime = connectionCreateTimeConv.Inst()

		connectionRelaxedTimeout, err = meter.Int64UpDownCounter(
			MetricConnectionRelaxedTimeout,
			metric.WithDescription("How many times the connection timeout has been increased/decreased (after a server maintenance notification)"),
			metric.WithUnit("{relaxation}"),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create connection relaxed timeout metric: %w", err)
		}

		connectionHandoff, err = meter.Int64Counter(
			MetricConnectionHandoff,
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
			MetricClientErrors,
			metric.WithDescription("Number of errors handled by the Redis client"),
			metric.WithUnit("{error}"),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create client errors metric: %w", err)
		}

		maintenanceNotifications, err = meter.Int64Counter(
			MetricMaintenanceNotifications,
			metric.WithDescription("Number of maintenance notifications received"),
			metric.WithUnit("{notification}"),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create maintenance notifications metric: %w", err)
		}
	}

	var connectionWaitTime metric.Float64Histogram
	var connectionClosed metric.Int64Counter
	var connectionPendingReqs metric.Int64UpDownCounter // OTel semconv: UpDownCounter

	if cfg.isMetricGroupEnabled(MetricGroupConnectionAdvanced) {
		var connectionWaitTimeOpts []metric.Float64HistogramOption
		if cfg.histAggregation == HistogramAggregationExplicitBucket {
			connectionWaitTimeOpts = append(connectionWaitTimeOpts,
				metric.WithExplicitBucketBoundaries(cfg.bucketsConnectionWaitTime...),
			)
		}
		var connectionWaitTimeConv dbconv.ClientConnectionWaitTime
		connectionWaitTimeConv, err = dbconv.NewClientConnectionWaitTime(meter, connectionWaitTimeOpts...)
		if err != nil {
			return nil, fmt.Errorf("failed to create connection wait time histogram: %w", err)
		}
		connectionWaitTime = connectionWaitTimeConv.Inst()

		connectionClosed, err = meter.Int64Counter(
			MetricConnectionClosed,
			metric.WithDescription("The number of connections that have been closed"),
			metric.WithUnit("{connection}"),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create connection closed metric: %w", err)
		}

		connectionPendingReqs, err = meter.Int64UpDownCounter(
			dbconv.ClientConnectionPendingRequests{}.Name(),
			metric.WithDescription(dbconv.ClientConnectionPendingRequests{}.Description()),
			metric.WithUnit(dbconv.ClientConnectionPendingRequests{}.Unit()),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create connection pending requests metric: %w", err)
		}
	}

	var pubsubMessages metric.Int64Counter

	if cfg.isMetricGroupEnabled(MetricGroupPubSub) {
		pubsubMessages, err = meter.Int64Counter(
			MetricPubSubMessages,
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
			MetricStreamLag,
			streamLagOpts...,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create stream lag histogram: %w", err)
		}
	}

	// MultiDB (Active-Active failover). The durations share the operation
	// duration buckets: a failover or a health check spans the same range
	// (sub-millisecond to seconds) as a command.
	var multiDBFailovers, multiDBActiveDatabaseChanges, multiDBCircuitStateChanges, multiDBHealthChecks metric.Int64Counter
	var multiDBFailoverDuration, multiDBHealthCheckDuration metric.Float64Histogram
	if cfg.isMetricGroupEnabled(MetricGroupMultiDB) {
		var durationOpts []metric.Float64HistogramOption
		if cfg.histAggregation == HistogramAggregationExplicitBucket {
			durationOpts = append(durationOpts,
				metric.WithExplicitBucketBoundaries(cfg.bucketsOperationDuration...),
			)
		}
		multiDBFailovers, err = meter.Int64Counter(
			MetricMultiDBFailovers,
			metric.WithDescription("Number of MultiDB failovers from one member database to another"),
			metric.WithUnit("{failover}"),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create multidb failovers metric: %w", err)
		}
		multiDBFailoverDuration, err = meter.Float64Histogram(
			MetricMultiDBFailoverDuration,
			append(durationOpts,
				metric.WithDescription("Wall time of a MultiDB failover"),
				metric.WithUnit("s"),
			)...,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create multidb failover duration histogram: %w", err)
		}
		multiDBActiveDatabaseChanges, err = meter.Int64Counter(
			MetricMultiDBActiveDatabaseChanges,
			metric.WithDescription("Number of MultiDB active database changes (failover, fallback or manual selection)"),
			metric.WithUnit("{change}"),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create multidb active database changes metric: %w", err)
		}
		multiDBCircuitStateChanges, err = meter.Int64Counter(
			MetricMultiDBCircuitStateChanges,
			metric.WithDescription("Number of MultiDB member circuit breaker state transitions"),
			metric.WithUnit("{transition}"),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create multidb circuit breaker state changes metric: %w", err)
		}
		multiDBHealthChecks, err = meter.Int64Counter(
			MetricMultiDBHealthChecks,
			metric.WithDescription("Number of MultiDB member health checks, by outcome"),
			metric.WithUnit("{check}"),
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create multidb health checks metric: %w", err)
		}
		multiDBHealthCheckDuration, err = meter.Float64Histogram(
			MetricMultiDBHealthCheckDuration,
			append(durationOpts,
				metric.WithDescription("Wall time of a MultiDB member health check"),
				metric.WithUnit("s"),
			)...,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to create multidb health check duration histogram: %w", err)
		}
	}

	// The queue starts a goroutine: create it last, after every instrument
	// above succeeded, so a failed Init leaves nothing running. It is only an
	// optimization: a provider that rejects its gauge or callback gets inline
	// recording (opq == nil), not a failed Init.
	var opq *opQueue
	if operationDuration != nil {
		if shared != nil {
			opq = shared
		} else {
			opq, _ = newOpQueue(meter, operationDuration)
		}
	}

	// Create recorder
	recorder := &metricsRecorder{
		opq:                      opq,
		operationDuration:        operationDuration,
		connectionCount:          connectionCount,
		connectionCreateTime:     connectionCreateTime,
		connectionRelaxedTimeout: connectionRelaxedTimeout,
		connectionHandoff:        connectionHandoff,
		clientErrors:             clientErrors,
		maintenanceNotifications: maintenanceNotifications,
		connectionWaitTime:       connectionWaitTime,
		connectionClosed:         connectionClosed,
		connectionPendingReqs:    connectionPendingReqs,
		pubsubMessages:           pubsubMessages,
		streamLag:                streamLag,

		multiDBFailovers:             multiDBFailovers,
		multiDBFailoverDuration:      multiDBFailoverDuration,
		multiDBActiveDatabaseChanges: multiDBActiveDatabaseChanges,
		multiDBCircuitStateChanges:   multiDBCircuitStateChanges,
		multiDBHealthChecks:          multiDBHealthChecks,
		multiDBHealthCheckDuration:   multiDBHealthCheckDuration,

		cfg: &cfg,
	}

	return recorder, nil
}

// parsePoolName extracts the server endpoint from an addr_uniqueID pool name,
// optionally suffixed with _pubsub or _pipeline.
func parsePoolName(poolName string) (string, string, string) {
	// Handle special pool names
	if poolName == PoolNameMain || poolName == PoolNamePubSub {
		return "", "", ""
	}

	poolName = strings.TrimSuffix(poolName, "_pubsub")
	poolName = strings.TrimSuffix(poolName, "_pipeline")
	if i := strings.LastIndexByte(poolName, '_'); i >= 0 {
		poolName = poolName[:i]
	}
	host, port := parseAddr(poolName)
	return host, port, ""
}
