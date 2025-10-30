package redisotel

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/redis/go-redis/v9"
)

type metricsState struct {
	registrations []metric.Registration
	closed        bool
	mutex         sync.Mutex
}

// InstrumentMetrics starts reporting OpenTelemetry Metrics.
//
// Based on https://github.com/open-telemetry/semantic-conventions/blob/main/docs/database/database-metrics.md
func InstrumentMetrics(rdb redis.UniversalClient, opts ...MetricsOption) error {
	baseOpts := make([]baseOption, len(opts))
	for i, opt := range opts {
		baseOpts[i] = opt
	}
	conf := newConfig(baseOpts...)

	if conf.meter == nil {
		conf.meter = conf.mp.Meter(
			instrumName,
			metric.WithInstrumentationVersion("semver:"+redis.Version()),
		)
	}

	var state *metricsState
	if conf.closeChan != nil {
		state = &metricsState{
			registrations: make([]metric.Registration, 0),
			closed:        false,
			mutex:         sync.Mutex{},
		}

		go func() {
			<-conf.closeChan

			state.mutex.Lock()
			state.closed = true

			for _, registration := range state.registrations {
				if err := registration.Unregister(); err != nil {
					otel.Handle(err)
				}
			}
			state.mutex.Unlock()
		}()
	}

	switch rdb := rdb.(type) {
	case *redis.Client:
		return registerClient(rdb, conf, state)
	case *redis.ClusterClient:
		rdb.OnNewNode(func(rdb *redis.Client) {
			if err := registerClient(rdb, conf, state); err != nil {
				otel.Handle(err)
			}
		})
		return nil
	case *redis.Ring:
		rdb.OnNewNode(func(rdb *redis.Client) {
			if err := registerClient(rdb, conf, state); err != nil {
				otel.Handle(err)
			}
		})
		return nil
	default:
		return fmt.Errorf("redisotel: %T not supported", rdb)
	}
}

func registerClient(rdb *redis.Client, conf *config, state *metricsState) error {
	if state != nil {
		state.mutex.Lock()
		defer state.mutex.Unlock()

		if state.closed {
			return nil
		}
	}

	if conf.poolName == "" {
		opt := rdb.Options()
		conf.poolName = opt.Addr
	}
	conf.attrs = append(conf.attrs, attribute.String("pool.name", conf.poolName))

	registration, err := reportPoolStats(rdb, conf)
	if err != nil {
		return err
	}

	if state != nil {
		state.registrations = append(state.registrations, registration)
	}

	if err := addMetricsHook(rdb, conf); err != nil {
		return err
	}
	return nil
}

func poolStatsAttrs(conf *config) (poolAttrs, idleAttrs, usedAttrs attribute.Set) {
	poolAttrs = attribute.NewSet(conf.attrs...)
	idleAttrs = attribute.NewSet(append(poolAttrs.ToSlice(), attribute.String("state", "idle"))...)
	usedAttrs = attribute.NewSet(append(poolAttrs.ToSlice(), attribute.String("state", "used"))...)
	return
}

func reportPoolStats(rdb *redis.Client, conf *config) (metric.Registration, error) {
	poolAttrs, idleAttrs, usedAttrs := poolStatsAttrs(conf)

	idleMax, err := conf.meter.Int64ObservableUpDownCounter(
		"db.client.connections.idle.max",
		metric.WithDescription("The maximum number of idle open connections allowed"),
	)
	if err != nil {
		return nil, err
	}

	idleMin, err := conf.meter.Int64ObservableUpDownCounter(
		"db.client.connections.idle.min",
		metric.WithDescription("The minimum number of idle open connections allowed"),
	)
	if err != nil {
		return nil, err
	}

	connsMax, err := conf.meter.Int64ObservableUpDownCounter(
		"db.client.connections.max",
		metric.WithDescription("The maximum number of open connections allowed"),
	)
	if err != nil {
		return nil, err
	}

	usage, err := conf.meter.Int64ObservableUpDownCounter(
		"db.client.connections.usage",
		metric.WithDescription("The number of connections that are currently in state described by the state attribute"),
	)
	if err != nil {
		return nil, err
	}

	waits, err := conf.meter.Int64ObservableUpDownCounter(
		"db.client.connections.waits",
		metric.WithDescription("The number of times a connection was waited for"),
	)
	if err != nil {
		return nil, err
	}

	waitsDuration, err := conf.meter.Int64ObservableUpDownCounter(
		"db.client.connections.waits_duration",
		metric.WithDescription("The total time spent for waiting a connection in nanoseconds"),
		metric.WithUnit("ns"),
	)
	if err != nil {
		return nil, err
	}

	timeouts, err := conf.meter.Int64ObservableUpDownCounter(
		"db.client.connections.timeouts",
		metric.WithDescription("The number of connection timeouts that have occurred trying to obtain a connection from the pool"),
	)
	if err != nil {
		return nil, err
	}

	hits, err := conf.meter.Int64ObservableUpDownCounter(
		"db.client.connections.hits",
		metric.WithDescription("The number of times free connection was found in the pool"),
	)
	if err != nil {
		return nil, err
	}

	misses, err := conf.meter.Int64ObservableUpDownCounter(
		"db.client.connections.misses",
		metric.WithDescription("The number of times free connection was not found in the pool"),
	)
	if err != nil {
		return nil, err
	}

	redisConf := rdb.Options()
	return conf.meter.RegisterCallback(
		func(ctx context.Context, o metric.Observer) error {
			stats := rdb.PoolStats()

			o.ObserveInt64(idleMax, int64(redisConf.MaxIdleConns), metric.WithAttributeSet(poolAttrs))
			o.ObserveInt64(idleMin, int64(redisConf.MinIdleConns), metric.WithAttributeSet(poolAttrs))
			o.ObserveInt64(connsMax, int64(redisConf.PoolSize), metric.WithAttributeSet(poolAttrs))

			o.ObserveInt64(usage, int64(stats.IdleConns), metric.WithAttributeSet(idleAttrs))
			o.ObserveInt64(usage, int64(stats.TotalConns-stats.IdleConns), metric.WithAttributeSet(usedAttrs))

			o.ObserveInt64(waits, int64(stats.WaitCount), metric.WithAttributeSet(poolAttrs))
			o.ObserveInt64(waitsDuration, stats.WaitDurationNs, metric.WithAttributeSet(poolAttrs))

			o.ObserveInt64(timeouts, int64(stats.Timeouts), metric.WithAttributeSet(poolAttrs))
			o.ObserveInt64(hits, int64(stats.Hits), metric.WithAttributeSet(poolAttrs))
			o.ObserveInt64(misses, int64(stats.Misses), metric.WithAttributeSet(poolAttrs))
			return nil
		},
		idleMax,
		idleMin,
		connsMax,
		usage,
		waits,
		waitsDuration,
		timeouts,
		hits,
		misses,
	)
}

func addMetricsHook(rdb *redis.Client, conf *config) error {
	createTime, err := conf.meter.Float64Histogram(
		"db.client.connections.create_time",
		metric.WithDescription("The time it took to create a new connection."),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return err
	}

	useTime, err := conf.meter.Float64Histogram(
		"db.client.connections.use_time",
		metric.WithDescription("The time between borrowing a connection and returning it to the pool."),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return err
	}

	rdb.AddHook(&metricsHook{
		createTime: createTime,
		useTime:    useTime,
		attrs:      conf.attrs,
	})
	return nil
}

type metricsHook struct {
	createTime metric.Float64Histogram
	useTime    metric.Float64Histogram
	attrs      []attribute.KeyValue
}

var _ redis.Hook = (*metricsHook)(nil)

func (mh *metricsHook) DialHook(hook redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		start := time.Now()

		conn, err := hook(ctx, network, addr)

		dur := time.Since(start)

		attrs := make([]attribute.KeyValue, 0, len(mh.attrs)+2)
		attrs = append(attrs, mh.attrs...)
		attrs = append(attrs, statusAttr(err))
		attrs = append(attrs, errorTypeAttribute(err))

		mh.createTime.Record(ctx, milliseconds(dur), metric.WithAttributeSet(attribute.NewSet(attrs...)))
		return conn, err
	}
}

func (mh *metricsHook) ProcessHook(hook redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		start := time.Now()

		err := hook(ctx, cmd)

		dur := time.Since(start)

		attrs := make([]attribute.KeyValue, 0, len(mh.attrs)+3)
		attrs = append(attrs, mh.attrs...)
		attrs = append(attrs, attribute.String("type", "command"))
		attrs = append(attrs, statusAttr(err))
		attrs = append(attrs, errorTypeAttribute(err))

		mh.useTime.Record(ctx, milliseconds(dur), metric.WithAttributeSet(attribute.NewSet(attrs...)))

		return err
	}
}

func (mh *metricsHook) ProcessPipelineHook(
	hook redis.ProcessPipelineHook,
) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		start := time.Now()

		err := hook(ctx, cmds)

		dur := time.Since(start)

		attrs := make([]attribute.KeyValue, 0, len(mh.attrs)+3)
		attrs = append(attrs, mh.attrs...)
		attrs = append(attrs, attribute.String("type", "pipeline"))
		attrs = append(attrs, statusAttr(err))
		attrs = append(attrs, errorTypeAttribute(err))

		mh.useTime.Record(ctx, milliseconds(dur), metric.WithAttributeSet(attribute.NewSet(attrs...)))

		return err
	}
}

func milliseconds(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

func statusAttr(err error) attribute.KeyValue {
	if err != nil {
		return attribute.String("status", "error")
	}
	return attribute.String("status", "ok")
}

func errorTypeAttribute(err error) attribute.KeyValue {
	switch {
	case err == nil:
		return attribute.String("error_type", "none")
	case errors.Is(err, context.Canceled):
		return attribute.String("error_type", "context_canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return attribute.String("error_type", "context_timeout")
	default:
		return attribute.String("error_type", "other")
	}
}
