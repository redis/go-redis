package pushnotif

import (
	"context"

	"github.com/redis/go-redis/v9/internal/proto"
)

// HandlerContext provides context information about where a push notification was received.
// This allows handlers to make informed decisions based on the source of the notification.
type HandlerContext struct {
	// Client is the Redis client instance that received the notification
	Client interface{}

	// ConnPool is the connection pool from which the connection was obtained
	ConnPool interface{}

	// Conn is the specific connection on which the notification was received
	Conn interface{}
}

// Handler defines the interface for push notification handlers.
type Handler interface {
	// HandlePushNotification processes a push notification with context information.
	// The handlerCtx provides information about the client, connection pool, and connection
	// on which the notification was received, allowing handlers to make informed decisions.
	// Returns true if the notification was handled, false otherwise.
	HandlePushNotification(ctx context.Context, handlerCtx *HandlerContext, notification []interface{}) bool
}

// ProcessorInterface defines the interface for push notification processors.
type ProcessorInterface interface {
	GetHandler(pushNotificationName string) Handler
	ProcessPendingNotifications(ctx context.Context, handlerCtx *HandlerContext, rd *proto.Reader) error
	RegisterHandler(pushNotificationName string, handler Handler, protected bool) error
}

// RegistryInterface defines the interface for push notification registries.
type RegistryInterface interface {
	RegisterHandler(pushNotificationName string, handler Handler, protected bool) error
	UnregisterHandler(pushNotificationName string) error
	GetHandler(pushNotificationName string) Handler
	GetRegisteredPushNotificationNames() []string
}
