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
		Logger.Printf("failed to create instrument writeCount")
	}

	newConnectionsCount, err := meter.NewInt64Counter("redis.new_connections",
		metric.WithDescription("the number of connections created"),
	)
	if err != nil {
		Logger.Printf("failed to create instrument newConnectionsCount")
	}

	return &openTelemetryInstrumentation{
		WriteCount:          writeCount,
		NewConnectionsCount: newConnectionsCount,
	}
}
