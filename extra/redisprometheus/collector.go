package redisprometheus

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/redis/go-redis/v9"
)

// StatGetter provides a method to get pool statistics.
type StatGetter interface {
	PoolStats() *redis.PoolStats
}

// Collector collects statistics from a redis client.
// It implements the prometheus.Collector interface.
type Collector struct {
	getter      StatGetter
	hitDesc     *prometheus.Desc
	missDesc    *prometheus.Desc
	timeoutDesc *prometheus.Desc
	totalDesc   *prometheus.Desc
	idleDesc    *prometheus.Desc
	staleDesc   *prometheus.Desc
}

var _ prometheus.Collector = (*Collector)(nil)

// NewCollector returns a new Collector based on the provided StatGetter.
// The given namespace and subsystem are used to build the fully qualified metric name,
// i.e. "{namespace}_{subsystem}_{metric}".
// The provided metrics are:
//   - pool_hit_total
//   - pool_miss_total
//   - pool_timeout_total
//   - pool_conn_total_current
//   - pool_conn_idle_current
//   - pool_conn_stale_total
func NewCollector(namespace, subsystem string, getter StatGetter, constantLabels prometheus.Labels) *Collector {
	return &Collector{
		getter: getter,
		hitDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "pool_hit_total"),
			"Number of times a connection was found in the pool",
			nil, constantLabels,
		),
		missDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "pool_miss_total"),
			"Number of times a connection was not found in the pool",
			nil, constantLabels,
		),
		timeoutDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "pool_timeout_total"),
			"Number of times a timeout occurred when looking for a connection in the pool",
			nil, constantLabels,
		),
		totalDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "pool_conn_total_current"),
			"Current number of connections in the pool",
			nil, constantLabels,
		),
		idleDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "pool_conn_idle_current"),
			"Current number of idle connections in the pool",
			nil, constantLabels,
		),
		staleDesc: prometheus.NewDesc(
			prometheus.BuildFQName(namespace, subsystem, "pool_conn_stale_total"),
			"Number of times a connection was removed from the pool because it was stale",
			nil, constantLabels,
		),
	}
}

// Describe implements the prometheus.Collector interface.
func (s *Collector) Describe(descs chan<- *prometheus.Desc) {
	descs <- s.hitDesc
	descs <- s.missDesc
	descs <- s.timeoutDesc
	descs <- s.totalDesc
	descs <- s.idleDesc
	descs <- s.staleDesc
}

// Collect implements the prometheus.Collector interface.
func (s *Collector) Collect(metrics chan<- prometheus.Metric) {
	stats := s.getter.PoolStats()
	metrics <- prometheus.MustNewConstMetric(
		s.hitDesc,
		prometheus.CounterValue,
		float64(stats.Hits),
	)
	metrics <- prometheus.MustNewConstMetric(
		s.missDesc,
		prometheus.CounterValue,
		float64(stats.Misses),
	)
	metrics <- prometheus.MustNewConstMetric(
		s.timeoutDesc,
		prometheus.CounterValue,
		float64(stats.Timeouts),
	)
	metrics <- prometheus.MustNewConstMetric(
		s.totalDesc,
		prometheus.GaugeValue,
		float64(stats.TotalConns),
	)
	metrics <- prometheus.MustNewConstMetric(
		s.idleDesc,
		prometheus.GaugeValue,
		float64(stats.IdleConns),
	)
	metrics <- prometheus.MustNewConstMetric(
		s.staleDesc,
		prometheus.CounterValue,
		float64(stats.StaleConns),
	)
}
