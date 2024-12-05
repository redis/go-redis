package redisotel

import (
	"context"
	"fmt"
	"net"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.27.0"

	"github.com/redis/go-redis/v9"
)

// InstrumentMetrics starts reporting OpenTelemetry Metrics.
//
// Based on https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/metrics/semantic_conventions/database-metrics.md
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

	switch rdb := rdb.(type) {
	case *redis.Client:
		if conf.poolName == "" {
			opt := rdb.Options()
			conf.poolName = opt.Addr
		}
		conf.attrs = append(conf.attrs,semconv.DBClientConnectionPoolName(conf.poolName))
		if err := reportPoolStats(rdb, conf); err != nil {
			return err
		}
		if err := addMetricsHook(rdb, conf); err != nil {
			return err
		}
		return nil
	case *redis.ClusterClient:
		rdb.OnNewNode(func(rdb *redis.Client) {
			if conf.poolName == "" {
				opt := rdb.Options()
				conf.poolName = opt.Addr
			}
			conf.attrs = append(conf.attrs, semconv.DBClientConnectionPoolName(conf.poolName))

			if err := reportPoolStats(rdb, conf); err != nil {
				otel.Handle(err)
			}
			if err := addMetricsHook(rdb, conf); err != nil {
				otel.Handle(err)
			}
		})
		return nil
	case *redis.Ring:
		rdb.OnNewNode(func(rdb *redis.Client) {
			if conf.poolName == "" {
				opt := rdb.Options()
				conf.poolName = opt.Addr
			}
			conf.attrs = append(conf.attrs, semconv.DBClientConnectionPoolName(conf.poolName))

			if err := reportPoolStats(rdb, conf); err != nil {
				otel.Handle(err)
			}
			if err := addMetricsHook(rdb, conf); err != nil {
				otel.Handle(err)
			}
		})
		return nil
	default:
		return fmt.Errorf("redisotel: %T not supported", rdb)
	}
}

func reportPoolStats(rdb *redis.Client, conf *config) error {
	labels := conf.attrs
	idleAttrs := append(labels, attribute.String("state", "idle"))
	usedAttrs := append(labels, attribute.String("state", "used"))

	idleMax, err := conf.meter.Int64ObservableUpDownCounter(
		semconv.DBClientConnectionIdleMaxName,
		metric.WithDescription(semconv.DBClientConnectionIdleMaxDescription),
		metric.WithUnit(semconv.DBClientConnectionIdleMaxUnit),
	)
	if err != nil {
		return err
	}

	idleMin, err := conf.meter.Int64ObservableUpDownCounter(
		semconv.DBClientConnectionIdleMinName,
		metric.WithDescription(semconv.DBClientConnectionIdleMinDescription),
		metric.WithUnit(semconv.DBClientConnectionIdleMinUnit),
	)
	if err != nil {
		return err
	}

	connsMax, err := conf.meter.Int64ObservableUpDownCounter(
		semconv.DBClientConnectionMaxName,
		metric.WithDescription(semconv.DBClientConnectionMaxDescription),
		metric.WithUnit(semconv.DBClientConnectionMaxUnit),
	)
	if err != nil {
		return err
	}

	usage, err := conf.meter.Int64ObservableUpDownCounter(
		semconv.DBClientConnectionCountName,
		metric.WithDescription(semconv.DBClientConnectionCountDescription),
		metric.WithUnit(semconv.DBClientConnectionCountUnit),
	)
	if err != nil {
		return err
	}

	timeouts, err := conf.meter.Int64ObservableUpDownCounter(
		semconv.DBClientConnectionTimeoutsName,
		metric.WithDescription(semconv.DBClientConnectionTimeoutsDescription),
		metric.WithUnit(semconv.DBClientConnectionTimeoutsUnit),
	)
	if err != nil {
		return err
	}

	redisConf := rdb.Options()
	_, err = conf.meter.RegisterCallback(
		func(ctx context.Context, o metric.Observer) error {
			stats := rdb.PoolStats()

			o.ObserveInt64(idleMax, int64(redisConf.MaxIdleConns), metric.WithAttributes(labels...))
			o.ObserveInt64(idleMin, int64(redisConf.MinIdleConns), metric.WithAttributes(labels...))
			o.ObserveInt64(connsMax, int64(redisConf.PoolSize), metric.WithAttributes(labels...))

			o.ObserveInt64(usage, int64(stats.IdleConns), metric.WithAttributes(idleAttrs...))
			o.ObserveInt64(usage, int64(stats.TotalConns-stats.IdleConns), metric.WithAttributes(usedAttrs...))

			o.ObserveInt64(timeouts, int64(stats.Timeouts), metric.WithAttributes(labels...))
			return nil
		},
		idleMax,
		idleMin,
		connsMax,
		usage,
		timeouts,
	)

	return err
}

func addMetricsHook(rdb *redis.Client, conf *config) error {
	createTime, err := conf.meter.Float64Histogram(
		semconv.DBClientConnectionCreateTimeName,
		metric.WithDescription(semconv.DBClientConnectionCreateTimeDescription),
		metric.WithUnit(semconv.DBClientConnectionCreateTimeUnit),
	)
	if err != nil {
		return err
	}

	useTime, err := conf.meter.Float64Histogram(
		semconv.DBClientConnectionUseTimeName,
		metric.WithDescription(semconv.DBClientConnectionUseTimeDescription),
		metric.WithUnit(semconv.DBClientConnectionUseTimeUnit),
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

		attrs := make([]attribute.KeyValue, 0, len(mh.attrs)+1)
		attrs = append(attrs, mh.attrs...)
		attrs = append(attrs, statusAttr(err))

		mh.createTime.Record(ctx, dur.Seconds(), metric.WithAttributes(attrs...))
		return conn, err
	}
}

func (mh *metricsHook) ProcessHook(hook redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		start := time.Now()

		err := hook(ctx, cmd)

		dur := time.Since(start)

		attrs := make([]attribute.KeyValue, 0, len(mh.attrs)+2)
		attrs = append(attrs, mh.attrs...)
		attrs = append(attrs, attribute.String("type", "command"))
		attrs = append(attrs, statusAttr(err))
		attrs = append(attrs, semconv.DBOperationName(cmd.FullName()))
		mh.useTime.Record(ctx, dur.Seconds(), metric.WithAttributes(attrs...))

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

		attrs := make([]attribute.KeyValue, 0, len(mh.attrs)+2)
		attrs = append(attrs, mh.attrs...)
		attrs = append(attrs, attribute.String("type", "pipeline"))
		attrs = append(attrs, statusAttr(err))
		if len(cmds) > 0 {
			attrs = append(attrs, semconv.DBOperationName(cmds[0].FullName()))
			attrs = append(attrs, semconv.DBOperationBatchSize(len(cmds)))
		}

		mh.useTime.Record(ctx, dur.Seconds(), metric.WithAttributes(attrs...))

		return err
	}
}

func statusAttr(err error) attribute.KeyValue {
	if err != nil {
		return attribute.String("status", "error")
	}
	return attribute.String("status", "ok")
}
