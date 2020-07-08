package internal

import (
	"go.opentelemetry.io/otel/api/global"
	"go.opentelemetry.io/otel/api/metric"
)

type openTelemetryInstrumentation struct {
	// Count of write commands performed
	WriteCount metric.Int64Counter
	// Count of new connections
	NewConnectionsCount metric.Int64Counter
}

var Instruments = initInstruments()

func initInstruments() *openTelemetryInstrumentation {
	meter := global.Meter("github.com/go-redis/redis")

	writeCount, err := meter.NewInt64Counter("redis.writes",
		metric.WithDescription("the number of writes initiated"),
	)
	if err != nil {
		// TODO: handle errors
		Logger.Printf("failed to create instrument WriteCount")
	}

	newConnectionsCount, err := meter.NewInt64Counter("redis.connections",
		metric.WithDescription("the number of connections created"),
	)
	if err != nil {
		// TODO: handle errors
		Logger.Printf("failed to create instrument NumNewConnections")
	}

	return &openTelemetryInstrumentation{
		WriteCount:          writeCount,
		NewConnectionsCount: newConnectionsCount,
	}
}
