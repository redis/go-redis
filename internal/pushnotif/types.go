package pushnotif

import (
	"context"

	"github.com/redis/go-redis/v9/internal/proto"
)

// Handler defines the interface for push notification handlers.
type Handler interface {
	// HandlePushNotification processes a push notification.
	// Returns true if the notification was handled, false otherwise.
	HandlePushNotification(ctx context.Context, notification []interface{}) bool
}

// ProcessorInterface defines the interface for push notification processors.
type ProcessorInterface interface {
	GetHandler(pushNotificationName string) Handler
	ProcessPendingNotifications(ctx context.Context, rd *proto.Reader) error
	RegisterHandler(pushNotificationName string, handler Handler, protected bool) error
}

// RegistryInterface defines the interface for push notification registries.
type RegistryInterface interface {
	RegisterHandler(pushNotificationName string, handler Handler, protected bool) error
	UnregisterHandler(pushNotificationName string) error
	GetHandler(pushNotificationName string) Handler
	GetRegisteredPushNotificationNames() []string
	HandleNotification(ctx context.Context, notification []interface{}) bool
}
