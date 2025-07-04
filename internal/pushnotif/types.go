package pushnotif

import (
	"context"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

// HandlerContext provides context information about where a push notification was received.
// This interface allows handlers to make informed decisions based on the source of the notification
// with strongly typed access to different client types.
type HandlerContext interface {
	// GetClient returns the Redis client instance that received the notification.
	// Returns nil if no client context is available.
	GetClient() interface{}

	// GetClusterClient returns the client as a ClusterClient if it is one.
	// Returns nil if the client is not a ClusterClient or no client context is available.
	GetClusterClient() ClusterClientInterface

	// GetSentinelClient returns the client as a SentinelClient if it is one.
	// Returns nil if the client is not a SentinelClient or no client context is available.
	GetSentinelClient() SentinelClientInterface

	// GetFailoverClient returns the client as a FailoverClient if it is one.
	// Returns nil if the client is not a FailoverClient or no client context is available.
	GetFailoverClient() FailoverClientInterface

	// GetRegularClient returns the client as a regular Client if it is one.
	// Returns nil if the client is not a regular Client or no client context is available.
	GetRegularClient() RegularClientInterface

	// GetConnPool returns the connection pool from which the connection was obtained.
	// Returns nil if no connection pool context is available.
	GetConnPool() interface{}

	// GetPubSub returns the PubSub instance that received the notification.
	// Returns nil if this is not a PubSub connection.
	GetPubSub() PubSubInterface

	// GetConn returns the specific connection on which the notification was received.
	// Returns nil if no connection context is available.
	GetConn() *pool.Conn

	// IsBlocking returns true if the notification was received on a blocking connection.
	IsBlocking() bool
}

// Client interfaces for strongly typed access
type ClusterClientInterface interface {
	// Add methods that handlers might need from ClusterClient
	String() string
}

type SentinelClientInterface interface {
	// Add methods that handlers might need from SentinelClient
	String() string
}

type FailoverClientInterface interface {
	// Add methods that handlers might need from FailoverClient
	String() string
}

type RegularClientInterface interface {
	// Add methods that handlers might need from regular Client
	String() string
}

type PubSubInterface interface {
	// Add methods that handlers might need from PubSub
	String() string
}

// handlerContext is the concrete implementation of HandlerContext interface
type handlerContext struct {
	client     interface{}
	connPool   interface{}
	pubSub     interface{}
	conn       *pool.Conn
	isBlocking bool
}

// NewHandlerContext creates a new HandlerContext implementation
func NewHandlerContext(client, connPool, pubSub interface{}, conn *pool.Conn, isBlocking bool) HandlerContext {
	return &handlerContext{
		client:     client,
		connPool:   connPool,
		pubSub:     pubSub,
		conn:       conn,
		isBlocking: isBlocking,
	}
}

// GetClient returns the Redis client instance that received the notification
func (h *handlerContext) GetClient() interface{} {
	return h.client
}

// GetClusterClient returns the client as a ClusterClient if it is one
func (h *handlerContext) GetClusterClient() ClusterClientInterface {
	if client, ok := h.client.(ClusterClientInterface); ok {
		return client
	}
	return nil
}

// GetSentinelClient returns the client as a SentinelClient if it is one
func (h *handlerContext) GetSentinelClient() SentinelClientInterface {
	if client, ok := h.client.(SentinelClientInterface); ok {
		return client
	}
	return nil
}

// GetFailoverClient returns the client as a FailoverClient if it is one
func (h *handlerContext) GetFailoverClient() FailoverClientInterface {
	if client, ok := h.client.(FailoverClientInterface); ok {
		return client
	}
	return nil
}

// GetRegularClient returns the client as a regular Client if it is one
func (h *handlerContext) GetRegularClient() RegularClientInterface {
	if client, ok := h.client.(RegularClientInterface); ok {
		return client
	}
	return nil
}

// GetConnPool returns the connection pool from which the connection was obtained
func (h *handlerContext) GetConnPool() interface{} {
	return h.connPool
}

// GetPubSub returns the PubSub instance that received the notification
func (h *handlerContext) GetPubSub() PubSubInterface {
	if pubSub, ok := h.pubSub.(PubSubInterface); ok {
		return pubSub
	}
	return nil
}

// GetConn returns the specific connection on which the notification was received
func (h *handlerContext) GetConn() *pool.Conn {
	return h.conn
}

// IsBlocking returns true if the notification was received on a blocking connection
func (h *handlerContext) IsBlocking() bool {
	return h.isBlocking
}

// Handler defines the interface for push notification handlers.
type Handler interface {
	// HandlePushNotification processes a push notification with context information.
	// The handlerCtx provides information about the client, connection pool, and connection
	// on which the notification was received, allowing handlers to make informed decisions.
	// Returns true if the notification was handled, false otherwise.
	HandlePushNotification(ctx context.Context, handlerCtx HandlerContext, notification []interface{}) bool
}

// ProcessorInterface defines the interface for push notification processors.
type ProcessorInterface interface {
	GetHandler(pushNotificationName string) Handler
	ProcessPendingNotifications(ctx context.Context, handlerCtx HandlerContext, rd *proto.Reader) error
	RegisterHandler(pushNotificationName string, handler Handler, protected bool) error
}

// RegistryInterface defines the interface for push notification registries.
type RegistryInterface interface {
	RegisterHandler(pushNotificationName string, handler Handler, protected bool) error
	UnregisterHandler(pushNotificationName string) error
	GetHandler(pushNotificationName string) Handler
	GetRegisteredPushNotificationNames() []string
}
