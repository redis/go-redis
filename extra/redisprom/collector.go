package redisprom

import (
	"github.com/go-redis/redis/v8"
	"github.com/prometheus/client_golang/prometheus"
)

type poolStatsCollector struct {
	universeClient redis.UniversalClient

	hits     *prometheus.Desc
	misses   *prometheus.Desc
	timeouts *prometheus.Desc

	totalConns *prometheus.Desc
	idleConns  *prometheus.Desc
	staleConns *prometheus.Desc
}

// NewPoolStatsCollector returns a collector that exports pool metrics about the given redis.UniversalClient.
func NewPoolStatsCollector(universeClient redis.UniversalClient, name string) prometheus.Collector {
	fqName := func(name string) string {
		return "go_redis_pool_" + name
	}

	return &poolStatsCollector{
		universeClient: universeClient,

		hits: prometheus.NewDesc(
			fqName("hits"),
			"Total number of times free connection was found in the pool.",
			nil, prometheus.Labels{"name": name},
		),
		misses: prometheus.NewDesc(
			fqName("misses"),
			"Total number of times free connection was NOT found in the pool.",
			nil, prometheus.Labels{"name": name},
		),
		timeouts: prometheus.NewDesc(
			fqName("timeouts"),
			"Total number of times a wait timeout occurred.",
			nil, prometheus.Labels{"name": name},
		),
		totalConns: prometheus.NewDesc(
			fqName("conns"),
			"Number of total connections in the pool.",
			nil, prometheus.Labels{"name": name},
		),
		idleConns: prometheus.NewDesc(
			fqName("idle_conns"),
			"Number of idle connections in the pool.",
			nil, prometheus.Labels{"name": name},
		),
		staleConns: prometheus.NewDesc(
			fqName("stale_conns"),
			"Total number of stale connections removed from the pool.",
			nil, prometheus.Labels{"name": name},
		),
	}
}

// Describe implements Collector.
func (c *poolStatsCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.hits
	ch <- c.misses
	ch <- c.timeouts
	ch <- c.totalConns
	ch <- c.idleConns
	ch <- c.staleConns
}

// Collect implements Collector.
func (c *poolStatsCollector) Collect(ch chan<- prometheus.Metric) {
	stats := c.universeClient.PoolStats()
	ch <- prometheus.MustNewConstMetric(c.hits, prometheus.CounterValue, float64(stats.Hits))
	ch <- prometheus.MustNewConstMetric(c.misses, prometheus.CounterValue, float64(stats.Misses))
	ch <- prometheus.MustNewConstMetric(c.timeouts, prometheus.CounterValue, float64(stats.Timeouts))
	ch <- prometheus.MustNewConstMetric(c.totalConns, prometheus.GaugeValue, float64(stats.TotalConns))
	ch <- prometheus.MustNewConstMetric(c.idleConns, prometheus.GaugeValue, float64(stats.IdleConns))
	ch <- prometheus.MustNewConstMetric(c.staleConns, prometheus.CounterValue, float64(stats.StaleConns))
}
