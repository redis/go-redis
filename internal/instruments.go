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
	defer func() {
		if r := recover(); r != nil {
			Logger.Printf("Error creating meter github.com/go-redis/redis for Instruments", r)
		}
	}()

	meter := metric.Must(global.Meter("github.com/go-redis/redis"))

	writeCount := meter.NewInt64Counter("redis.writes",
		metric.WithDescription("the number of writes initiated"),
	)

	newConnectionsCount := meter.NewInt64Counter("redis.new_connections",
		metric.WithDescription("the number of connections created"),
	)

	return &openTelemetryInstrumentation{
		WriteCount:          writeCount,
		NewConnectionsCount: newConnectionsCount,
	}
}
