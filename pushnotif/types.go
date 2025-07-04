package pushnotif

import (
	"context"
	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/internal/pushprocessor"
)

// PushProcessorInterface defines the interface for push notification processors.
type PushProcessorInterface interface {
	GetHandler(pushNotificationName string) PushNotificationHandler
	ProcessPendingNotifications(ctx context.Context, handlerCtx PushNotificationHandlerContext, rd *proto.Reader) error
	RegisterHandler(pushNotificationName string, handler PushNotificationHandler, protected bool) error
}

// RegistryInterface defines the interface for push notification registries.
type RegistryInterface interface {
	RegisterHandler(pushNotificationName string, handler PushNotificationHandler, protected bool) error
	UnregisterHandler(pushNotificationName string) error
	GetHandler(pushNotificationName string) PushNotificationHandler
	GetRegisteredPushNotificationNames() []string
}

// NewProcessor creates a new push notification processor.
func NewProcessor() PushProcessorInterface {
	return pushprocessor.NewProcessor()
}

// NewVoidProcessor creates a new void push notification processor.
func NewVoidProcessor() PushProcessorInterface {
	return pushprocessor.NewVoidProcessor()
}
