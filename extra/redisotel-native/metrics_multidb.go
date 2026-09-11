package redisotel

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// MultiDB (Active-Active failover) metrics.
//
// The MultiDB client reports its events through an optional capability
// interface on the recorder (go-redis #3954): a recorder without these
// methods receives none of them. Database FQDNs are the host-only names the
// client reports for its members. There is one per member, so they are
// recorded as attributes without a cardinality switch.

func multiDBBaseAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{
		attribute.String(AttrDBSystemName, DBSystemRedis),
		getLibraryVersionAttr(),
	}
}

// RecordMultiDBFailover records a failover from one member database to
// another. reason is "automatic" or "manual"; duration is the wall time of
// the failover.
func (r *metricsRecorder) RecordMultiDBFailover(
	ctx context.Context,
	fromFQDN, toFQDN, reason string,
	duration time.Duration,
) {
	if r.multiDBFailovers == nil && r.multiDBFailoverDuration == nil {
		return
	}
	attrs := append(
		multiDBBaseAttrs(),
		attribute.String(AttrRedisClientMultiDBFromDatabase, fromFQDN),
		attribute.String(AttrRedisClientMultiDBToDatabase, toFQDN),
		attribute.String(AttrRedisClientMultiDBFailoverReason, reason),
	)
	if r.multiDBFailovers != nil {
		r.multiDBFailovers.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
	if r.multiDBFailoverDuration != nil {
		r.multiDBFailoverDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
	}
}

// RecordMultiDBActiveDatabaseChange records a change of the active member
// database (failover, fallback or manual selection).
func (r *metricsRecorder) RecordMultiDBActiveDatabaseChange(ctx context.Context, fromFQDN, toFQDN string) {
	if r.multiDBActiveDatabaseChanges == nil {
		return
	}
	attrs := append(
		multiDBBaseAttrs(),
		attribute.String(AttrRedisClientMultiDBFromDatabase, fromFQDN),
		attribute.String(AttrRedisClientMultiDBToDatabase, toFQDN),
	)
	r.multiDBActiveDatabaseChanges.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordMultiDBCircuitStateChange records a member circuit breaker state
// transition ("closed", "open", "half-open").
func (r *metricsRecorder) RecordMultiDBCircuitStateChange(ctx context.Context, dbFQDN, fromState, toState string) {
	if r.multiDBCircuitStateChanges == nil {
		return
	}
	attrs := append(
		multiDBBaseAttrs(),
		attribute.String(AttrRedisClientMultiDBDatabase, dbFQDN),
		attribute.String(AttrRedisClientMultiDBCircuitFromState, fromState),
		attribute.String(AttrRedisClientMultiDBCircuitToState, toState),
	)
	r.multiDBCircuitStateChanges.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordMultiDBHealthCheck records the outcome and wall time of one member
// health check.
func (r *metricsRecorder) RecordMultiDBHealthCheck(ctx context.Context, dbFQDN string, success bool, duration time.Duration) {
	if r.multiDBHealthChecks == nil && r.multiDBHealthCheckDuration == nil {
		return
	}
	attrs := append(
		multiDBBaseAttrs(),
		attribute.String(AttrRedisClientMultiDBDatabase, dbFQDN),
		attribute.Bool(AttrRedisClientMultiDBHealthCheckSuccess, success),
	)
	if r.multiDBHealthChecks != nil {
		r.multiDBHealthChecks.Add(ctx, 1, metric.WithAttributes(attrs...))
	}
	if r.multiDBHealthCheckDuration != nil {
		r.multiDBHealthCheckDuration.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
	}
}
