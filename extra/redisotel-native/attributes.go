package redisotel

// OpenTelemetry semantic convention attribute keys for Redis client metrics.
// These constants follow the OTel semantic conventions for database clients.
// Reference: https://opentelemetry.io/docs/specs/semconv/database/redis/

const (
	// Database semantic convention attributes
	AttrDBSystemName         = "db.system.name"
	AttrDBNamespace          = "db.namespace"
	AttrDBOperationName      = "db.operation.name"
	AttrDBOperationBatchSize = "db.operation.batch.size"
	AttrDBResponseStatusCode = "db.response.status_code"

	// Connection pool attributes
	AttrDBClientConnectionPoolName = "db.client.connection.pool.name"
	AttrDBClientConnectionState    = "db.client.connection.state"

	// Server attributes
	AttrServerAddress = "server.address"
	AttrServerPort    = "server.port"

	// Network attributes
	AttrNetworkPeerAddress = "network.peer.address"
	AttrNetworkPeerPort    = "network.peer.port"

	// Error attributes
	AttrErrorType = "error.type"

	// Redis-specific attributes
	AttrRedisClientLibrary                = "redis.client.library"
	AttrRedisClientConnectionPubSub       = "redis.client.connection.pubsub"
	AttrRedisClientConnectionCloseReason  = "redis.client.connection.close.reason"
	AttrRedisClientErrorsCategory         = "redis.client.errors.category"
	AttrRedisClientErrorsInternal         = "redis.client.errors.internal"
	AttrRedisClientOperationRetryAttempts = "redis.client.operation.retry_attempts"

	// PubSub attributes
	AttrRedisClientPubSubDirection = "redis.client.pubsub.direction"
	AttrRedisClientPubSubSharded   = "redis.client.pubsub.sharded"
	AttrRedisClientPubSubChannel   = "redis.client.pubsub.channel"

	// Stream attributes
	AttrRedisClientStreamName          = "redis.client.stream.name"
	AttrRedisClientStreamConsumerGroup = "redis.client.stream.consumer_group"
	AttrRedisClientStreamConsumerName  = "redis.client.stream.consumer_name"

	// Notification attributes
	AttrRedisClientConnectionNotification = "redis.client.connection.notification"
)

// Connection state values
const (
	ConnectionStateIdle = "idle"
	ConnectionStateUsed = "used"
)

// PubSub direction values
const (
	PubSubDirectionSent     = "sent"
	PubSubDirectionReceived = "received"
)

// DB system value
const (
	DBSystemRedis = "redis"
)
