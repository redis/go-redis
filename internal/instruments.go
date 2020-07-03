package internal

import (
	"go.opentelemetry.io/otel/api/global"
	"go.opentelemetry.io/otel/api/metric"
	"go.opentelemetry.io/otel/api/unit"
	"log"
)

type openTelemetryInstrumentation struct {
	// Bytes Written Metric
	WriteCount metric.Int64Counter
	// Number of new connections
	// TODO
	NewConnectionsCount metric.Int64Counter
}

var Instruments *openTelemetryInstrumentation = initInstruments()

func initInstruments() *openTelemetryInstrumentation {
	meter := global.Meter("github.com/go-redis/redis")

	writeCount, err := meter.NewInt64Counter("redis.num_writes",
		metric.WithDescription("the number of writes initiated"),
		metric.WithUnit(unit.Bytes),
	)
	if err != nil {
		// TODO: handle errors
		log.Printf("failed to create instrument WriteCount")
	}

	newConnectionsCount, err := meter.NewInt64Counter("redis.num_new_conn",
		metric.WithDescription("the number of new connections created"),
	)
	if err != nil {
		// TODO: handle errors
		log.Printf("failed to create instrument NumNewConnections")
	}

	return &openTelemetryInstrumentation{
		WriteCount:          writeCount,
		NewConnectionsCount: newConnectionsCount,
	}
}

