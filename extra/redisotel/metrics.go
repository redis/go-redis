package redisotel

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/go-redis/redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/instrument/syncint64"
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
		conf.attrs = append(conf.attrs, attribute.String("pool.name", conf.poolName))

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
			conf.attrs = append(conf.attrs, attribute.String("pool.name", conf.poolName))

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

	idleMax, err := conf.meter.AsyncInt64().UpDownCounter(
		"db.client.connections.idle.max",
		instrument.WithDescription("The maximum number of idle open connections allowed"),
	)
	if err != nil {
		return err
	}

	idleMin, err := conf.meter.AsyncInt64().UpDownCounter(
		"db.client.connections.idle.min",
		instrument.WithDescription("The minimum number of idle open connections allowed"),
	)
	if err != nil {
		return err
	}

	connsMax, err := conf.meter.AsyncInt64().UpDownCounter(
		"db.client.connections.max",
		instrument.WithDescription("The maximum number of open connections allowed"),
	)
	if err != nil {
		return err
	}

	usage, err := conf.meter.AsyncInt64().UpDownCounter(
		"db.client.connections.usage",
		instrument.WithDescription("The number of connections that are currently in state described by the state attribute"),
	)
	if err != nil {
		return err
	}

	timeouts, err := conf.meter.AsyncInt64().UpDownCounter(
		"db.client.connections.timeouts",
		instrument.WithDescription("The number of connection timeouts that have occurred trying to obtain a connection from the pool"),
	)
	if err != nil {
		return err
	}

	redisConf := rdb.Options()
	return conf.meter.RegisterCallback(
		[]instrument.Asynchronous{
			idleMax,
			idleMin,
			connsMax,
			usage,
			timeouts,
		},
		func(ctx context.Context) {
			stats := rdb.PoolStats()

			idleMax.Observe(ctx, int64(redisConf.MinIdleConns))
			idleMin.Observe(ctx, int64(redisConf.MaxIdleConns))
			connsMax.Observe(ctx, int64(redisConf.PoolSize))

			usage.Observe(ctx, int64(stats.IdleConns), idleAttrs...)
			usage.Observe(ctx, int64(stats.TotalConns-stats.IdleConns), usedAttrs...)

			timeouts.Observe(ctx, int64(stats.Timeouts), labels...)
		},
	)
}

func addMetricsHook(rdb *redis.Client, conf *config) error {
	createTime, err := conf.meter.SyncInt64().Histogram(
		"db.client.connections.create_time",
		instrument.WithDescription("The time it took to create a new connection."),
		instrument.WithUnit("ms"),
	)
	if err != nil {
		return err
	}

	useTime, err := conf.meter.SyncInt64().Histogram(
		"db.client.connections.use_time",
		instrument.WithDescription("The time between borrowing a connection and returning it to the pool."),
		instrument.WithUnit("ms"),
	)
	if err != nil {
		return err
	}

	rdb.AddHook(&metricsHook{
		createTime: createTime,
		useTime:    useTime,
	})
	return nil
}

type metricsHook struct {
	createTime syncint64.Histogram
	useTime    syncint64.Histogram
}

var _ redis.Hook = (*metricsHook)(nil)

func (mh *metricsHook) DialHook(hook redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		start := time.Now()

		conn, err := hook(ctx, network, addr)

		mh.createTime.Record(ctx, time.Since(start).Milliseconds())
		return conn, err
	}
}

func (mh *metricsHook) ProcessHook(hook redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		start := time.Now()

		err := hook(ctx, cmd)

		dur := time.Since(start).Milliseconds()
		mh.useTime.Record(ctx, dur, attribute.String("type", "command"), statusAttr(err))

		return err
	}
}

func (mh *metricsHook) ProcessPipelineHook(
	hook redis.ProcessPipelineHook,
) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		start := time.Now()

		err := hook(ctx, cmds)

		dur := time.Since(start).Milliseconds()
		mh.useTime.Record(ctx, dur, attribute.String("type", "pipeline"), statusAttr(err))

		return err
	}
}

func statusAttr(err error) attribute.KeyValue {
	if err != nil {
		return attribute.String("status", "error")
	}
	return attribute.String("status", "ok")
}
