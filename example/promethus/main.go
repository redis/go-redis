package main

import (
	"log"
	"net/http"

	"github.com/go-redis/redis/extra/redisprom/v8"
	"github.com/go-redis/redis/v8"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	rdb := redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{":6379"},
	})

	// Create Pool metrics collector for given UniversalClient
	stats := redisprom.NewPoolStatsCollector(rdb, ":6379")

	// Register metrics collector
	prometheus.MustRegister(stats)

	// Expose the registered metrics via HTTP.
	http.Handle("/metrics", promhttp.HandlerFor(
		prometheus.DefaultGatherer,
		promhttp.HandlerOpts{
			// Opt into OpenMetrics to support exemplars.
			EnableOpenMetrics: true,
		},
	))
	log.Fatal(http.ListenAndServe(":9530", nil))
}
