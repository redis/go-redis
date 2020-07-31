package internal

import (
	"context"

	"go.opentelemetry.io/otel/api/global"
	"go.opentelemetry.io/otel/api/metric"
	"go.opentelemetry.io/otel/api/unit"
)

var (
	// WritesCounter is a count of write commands performed.
	WritesCounter metric.Int64Counter
	// BytesWritten records the number of bytes written to redis.
	BytesWritten metric.Int64ValueRecorder
	// NewConnectionsCounter is a count of new connections.
	NewConnectionsCounter metric.Int64Counter
)

func init() {
	defer func() {
		if r := recover(); r != nil {
			Logger.Printf(context.Background(), "Error creating meter github.com/go-redis/redis for Instruments", r)
		}
	}()

	meter := metric.Must(global.Meter("github.com/go-redis/redis"))

	WritesCounter = meter.NewInt64Counter("redis.writes",
		metric.WithDescription("the number of writes initiated"),
	)

	BytesWritten = meter.NewInt64ValueRecorder("redis.writes.bytes",
		metric.WithDescription("the number of bytes written to redis"),
		metric.WithUnit(unit.Bytes),
	)

	NewConnectionsCounter = meter.NewInt64Counter("redis.new_connections",
		metric.WithDescription("the number of connections created"),
	)
}
