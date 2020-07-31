package internal

import (
	"context"

	"go.opentelemetry.io/otel/api/global"
	"go.opentelemetry.io/otel/api/kv"
	"go.opentelemetry.io/otel/api/metric"
	"go.opentelemetry.io/otel/api/unit"
)

const (
	// Label Keys

	// dbErrorMessageKey is the label key for an error message.
	dbErrorMessageKey = kv.Key("db.redis.error")
	// dbErrorSourceKey is the key for the label describing whether
	// the error originated from a read or a write.
	dbErrorSourceKey = kv.Key("db.redis.error.source")
)

var (
	// WritesCounter is a count of write commands performed.
	WritesCounter metric.Int64Counter
	// BytesWritten records the number of bytes written to redis.
	BytesWritten metric.Int64ValueRecorder
	// BytesRead records the number of bytes read from redis.
	BytesRead metric.Int64ValueRecorder
	// redisErrors is a count of the number of errors encountered.
	redisErrors metric.Int64Counter
	// NewConnectionsCounter is a count of new connections.
	NewConnectionsCounter metric.Int64Counter
)

// dbErrorMessage returns an error message as a KeyValue pair.
func dbErrorMessage(msg string) kv.KeyValue {
	return dbErrorMessageKey.String(msg)
}

// dbErrorSourceWrite returns the write ErrorSource.
func dbErrorSourceWrite() kv.KeyValue {
	return dbErrorSourceKey.String("write")
}

// dbErrorSourceRead returns the read ErrorSource.
func dbErrorSourceRead() kv.KeyValue {
	return dbErrorSourceKey.String("read")
}

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

	BytesRead = meter.NewInt64ValueRecorder("redis.reads.bytes",
		metric.WithDescription("the number of bytes read from redis"),
		metric.WithUnit(unit.Bytes),
	)

	redisErrors = meter.NewInt64Counter("redis.errors",
		metric.WithDescription("the number of errors encountered"),
	)

	NewConnectionsCounter = meter.NewInt64Counter("redis.new_connections",
		metric.WithDescription("the number of connections created"),
	)
}
