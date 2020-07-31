package internal

import (
	"context"

	"go.opentelemetry.io/otel/api/global"
	"go.opentelemetry.io/otel/api/metric"
)

var (
	// WritesCounter counts the number of write commands performed.
	WritesCounter metric.Int64Counter

	// NewConnectionsCounter counts the number of new connections.
	NewConnectionsCounter metric.Int64Counter

	// DialErrorCounter counts the number of dial errors that have come up
	DialErrorCounter metric.Int64Counter

	// ConnectionsTakenCounter counts the number of times connections were taken
	ConnectionsTakenCounter metric.Int64Counter

	// ConnectionsClosedCounter counts the number of connections closed
	ConnectionsClosedCounter metric.Int64Counter

	// ConnectionsReturnedCounter counts the number of times connections have been returned to the pool
	ConnectionsReturnedCounter metric.Int64Counter

	// ConnectionsReusedCounter counts the number of times connections have been reused
	ConnectionsReusedCounter metric.Int64Counter

	// TODO impl
	// ConnectionUsedTimeRecorder records the duration in milliseconds that connections are being used
	ConnectionUsedTimeRecorder metric.Int64ValueRecorder
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

	DialErrorCounter = meter.NewInt64Counter("redis.dial_errors",
		metric.WithDescription("the number of errors encountered after dialling"),
	)

	NewConnectionsCounter = meter.NewInt64Counter("redis.new_connections",
		metric.WithDescription("the number of connections created"),
	)

	ConnectionsTakenCounter = meter.NewInt64Counter("redis.connections_taken",
		metric.WithDescription("the number of connections taken"),
	)

	ConnectionsReturnedCounter = meter.NewInt64Counter("redis.connections_returned",
		metric.WithDescription("the number of connections returned to the connection pool"),
	)

	ConnectionsReusedCounter = meter.NewInt64Counter("redis.connections_reused",
		metric.WithDescription("the number of connections that have been reused"),
	)

	ConnectionsClosedCounter = meter.NewInt64Counter("redis.connections_closed",
		metric.WithDescription("the number of connections closed"),
	)

	ConnectionUsedTimeRecorder = meter.NewInt64ValueRecorder("redis.connection_used_time",
		metric.WithDescription("the number of milliseconds for which a connection is used"),
	)
}
