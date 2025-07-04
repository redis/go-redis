package redis

import (
	"github.com/redis/go-redis/v9/internal/pool"
)

// PushNotificationHandlerContext provides context information about where a push notification was received.
// This interface allows handlers to make informed decisions based on the source of the notification
// with strongly typed access to different client types using concrete types.
type PushNotificationHandlerContext interface {
	// GetClient returns the Redis client instance that received the notification.
	// Returns nil if no client context is available.
	GetClient() interface{}
	
	// GetClusterClient returns the client as a ClusterClient if it is one.
	// Returns nil if the client is not a ClusterClient or no client context is available.
	GetClusterClient() *ClusterClient
	
	// GetSentinelClient returns the client as a SentinelClient if it is one.
	// Returns nil if the client is not a SentinelClient or no client context is available.
	GetSentinelClient() *SentinelClient
	
	// GetFailoverClient returns the client as a FailoverClient if it is one.
	// Returns nil if the client is not a FailoverClient or no client context is available.
	GetFailoverClient() *Client
	
	// GetRegularClient returns the client as a regular Client if it is one.
	// Returns nil if the client is not a regular Client or no client context is available.
	GetRegularClient() *Client
	
	// GetConnPool returns the connection pool from which the connection was obtained.
	// Returns nil if no connection pool context is available.
	GetConnPool() interface{}
	
	// GetPubSub returns the PubSub instance that received the notification.
	// Returns nil if this is not a PubSub connection.
	GetPubSub() *PubSub
	
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

// NewPushNotificationHandlerContext creates a new PushNotificationHandlerContext implementation
func NewPushNotificationHandlerContext(client, connPool, pubSub interface{}, conn *pool.Conn, isBlocking bool) PushNotificationHandlerContext {
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

// GetClusterClient returns the client as a ClusterClient if it is one
func (h *pushNotificationHandlerContext) GetClusterClient() *ClusterClient {
	if client, ok := h.client.(*ClusterClient); ok {
		return client
	}
	return nil
}

// GetSentinelClient returns the client as a SentinelClient if it is one
func (h *pushNotificationHandlerContext) GetSentinelClient() *SentinelClient {
	if client, ok := h.client.(*SentinelClient); ok {
		return client
	}
	return nil
}

// GetFailoverClient returns the client as a FailoverClient if it is one
func (h *pushNotificationHandlerContext) GetFailoverClient() *Client {
	if client, ok := h.client.(*Client); ok {
		return client
	}
	return nil
}

// GetRegularClient returns the client as a regular Client if it is one
func (h *pushNotificationHandlerContext) GetRegularClient() *Client {
	if client, ok := h.client.(*Client); ok {
		return client
	}
	return nil
}

// GetConnPool returns the connection pool from which the connection was obtained
func (h *pushNotificationHandlerContext) GetConnPool() interface{} {
	return h.connPool
}

// GetPubSub returns the PubSub instance that received the notification
func (h *pushNotificationHandlerContext) GetPubSub() *PubSub {
	if pubSub, ok := h.pubSub.(*PubSub); ok {
		return pubSub
	}
	return nil
}

// GetConn returns the specific connection on which the notification was received
func (h *pushNotificationHandlerContext) GetConn() *pool.Conn {
	return h.conn
}

// IsBlocking returns true if the notification was received on a blocking connection
func (h *pushNotificationHandlerContext) IsBlocking() bool {
	return h.isBlocking
}
