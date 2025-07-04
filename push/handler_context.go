package push

import (
	"github.com/redis/go-redis/v9/internal/pool"
)

// NotificationHandlerContext provides context information about where a push notification was received.
// This interface allows handlers to make informed decisions based on the source of the notification
// with strongly typed access to different client types using concrete types.
type NotificationHandlerContext interface {
	// GetClient returns the Redis client instance that received the notification.
	// Returns nil if no client context is available.
	// It is interface to both allow for future expansion and to avoid
	// circular dependencies. The developer is responsible for type assertion.
	// It can be one of the following types:
	// - *redis.Client
	// - *redis.ClusterClient
	// - *redis.Conn
	GetClient() interface{}

	// GetConnPool returns the connection pool from which the connection was obtained.
	// Returns nil if no connection pool context is available.
	// It is interface to both allow for future expansion and to avoid
	// circular dependencies. The developer is responsible for type assertion.
	// It can be one of the following types:
	// - *pool.ConnPool
	// - *pool.SingleConnPool
	// - *pool.StickyConnPool
	GetConnPool() interface{}

	// GetPubSub returns the PubSub instance that received the notification.
	// Returns nil if this is not a PubSub connection.
	// It is interface to both allow for future expansion and to avoid
	// circular dependencies. The developer is responsible for type assertion.
	// It can be one of the following types:
	// - *redis.PubSub
	GetPubSub() interface{}

	// GetConn returns the specific connection on which the notification was received.
	// Returns nil if no connection context is available.
	GetConn() *pool.Conn

	// IsBlocking returns true if the notification was received on a blocking connection.
	IsBlocking() bool
}

// pushNotificationHandlerContext is the concrete implementation of PushNotificationHandlerContext interface
type pushNotificationHandlerContext struct {
	client     interface{}
	connPool   interface{}
	pubSub     interface{}
	conn       *pool.Conn
	isBlocking bool
}

// NewNotificationHandlerContext creates a new push.NotificationHandlerContext instance
func NewNotificationHandlerContext(client, connPool, pubSub interface{}, conn *pool.Conn, isBlocking bool) NotificationHandlerContext {
	return &pushNotificationHandlerContext{
		client:     client,
		connPool:   connPool,
		pubSub:     pubSub,
		conn:       conn,
		isBlocking: isBlocking,
	}
}

// GetClient returns the Redis client instance that received the notification
func (h *pushNotificationHandlerContext) GetClient() interface{} {
	return h.client
}

// GetConnPool returns the connection pool from which the connection was obtained
func (h *pushNotificationHandlerContext) GetConnPool() interface{} {
	return h.connPool
}

func (h *pushNotificationHandlerContext) GetPubSub() interface{} {
	return h.pubSub
}

// GetConn returns the specific connection on which the notification was received
func (h *pushNotificationHandlerContext) GetConn() *pool.Conn {
	return h.conn
}

// IsBlocking returns true if the notification was received on a blocking connection
func (h *pushNotificationHandlerContext) IsBlocking() bool {
	return h.isBlocking
}
