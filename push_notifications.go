package redis

import (
	"context"

	"github.com/redis/go-redis/v9/internal/proto"
)

// Push notification constants for cluster operations
const (
	// MOVING indicates a slot is being moved to a different node
	PushNotificationMoving = "MOVING"

	// MIGRATING indicates a slot is being migrated from this node
	PushNotificationMigrating = "MIGRATING"

	// MIGRATED indicates a slot has been migrated to this node
	PushNotificationMigrated = "MIGRATED"

	// FAILING_OVER indicates a failover is starting
	PushNotificationFailingOver = "FAILING_OVER"

	// FAILED_OVER indicates a failover has completed
	PushNotificationFailedOver = "FAILED_OVER"
)

// PushNotificationHandlerContext is defined in push_notification_handler_context.go

// PushNotificationHandler defines the interface for push notification handlers.
type PushNotificationHandler interface {
	// HandlePushNotification processes a push notification with context information.
	// The handlerCtx provides information about the client, connection pool, and connection
	// on which the notification was received, allowing handlers to make informed decisions.
	// Returns true if the notification was handled, false otherwise.
	HandlePushNotification(ctx context.Context, handlerCtx PushNotificationHandlerContext, notification []interface{}) bool
}

// NewPushNotificationHandlerContext is defined in push_notification_handler_context.go

// Registry, Processor, and VoidProcessor are defined in push_notification_processor.go

// PushNotificationProcessorInterface defines the interface for push notification processors.
type PushNotificationProcessorInterface interface {
	GetHandler(pushNotificationName string) PushNotificationHandler
	ProcessPendingNotifications(ctx context.Context, handlerCtx PushNotificationHandlerContext, rd *proto.Reader) error
	RegisterHandler(pushNotificationName string, handler PushNotificationHandler, protected bool) error
}

// PushNotificationRegistry manages push notification handlers.
type PushNotificationRegistry struct {
	registry *Registry
}

// NewPushNotificationRegistry creates a new push notification registry.
func NewPushNotificationRegistry() *PushNotificationRegistry {
	return &PushNotificationRegistry{
		registry: NewRegistry(),
	}
}

// RegisterHandler registers a handler for a specific push notification name.
func (r *PushNotificationRegistry) RegisterHandler(pushNotificationName string, handler PushNotificationHandler, protected bool) error {
	return r.registry.RegisterHandler(pushNotificationName, handler, protected)
}

// UnregisterHandler removes a handler for a specific push notification name.
func (r *PushNotificationRegistry) UnregisterHandler(pushNotificationName string) error {
	return r.registry.UnregisterHandler(pushNotificationName)
}

// GetHandler returns the handler for a specific push notification name.
func (r *PushNotificationRegistry) GetHandler(pushNotificationName string) PushNotificationHandler {
	return r.registry.GetHandler(pushNotificationName)
}

// GetRegisteredPushNotificationNames returns a list of all registered push notification names.
func (r *PushNotificationRegistry) GetRegisteredPushNotificationNames() []string {
	return r.registry.GetRegisteredPushNotificationNames()
}

// PushNotificationProcessor handles push notifications with a registry of handlers.
type PushNotificationProcessor struct {
	processor *Processor
}

// NewPushNotificationProcessor creates a new push notification processor.
func NewPushNotificationProcessor() *PushNotificationProcessor {
	return &PushNotificationProcessor{
		processor: NewProcessor(),
	}
}

// GetHandler returns the handler for a specific push notification name.
func (p *PushNotificationProcessor) GetHandler(pushNotificationName string) PushNotificationHandler {
	return p.processor.GetHandler(pushNotificationName)
}

// RegisterHandler registers a handler for a specific push notification name.
func (p *PushNotificationProcessor) RegisterHandler(pushNotificationName string, handler PushNotificationHandler, protected bool) error {
	return p.processor.RegisterHandler(pushNotificationName, handler, protected)
}

// UnregisterHandler removes a handler for a specific push notification name.
func (p *PushNotificationProcessor) UnregisterHandler(pushNotificationName string) error {
	return p.processor.UnregisterHandler(pushNotificationName)
}

// ProcessPendingNotifications checks for and processes any pending push notifications.
// The handlerCtx provides context about the client, connection pool, and connection.
func (p *PushNotificationProcessor) ProcessPendingNotifications(ctx context.Context, handlerCtx PushNotificationHandlerContext, rd *proto.Reader) error {
	return p.processor.ProcessPendingNotifications(ctx, handlerCtx, rd)
}

// VoidPushNotificationProcessor discards all push notifications without processing them.
type VoidPushNotificationProcessor struct {
	processor *VoidProcessor
}

// NewVoidPushNotificationProcessor creates a new void push notification processor.
func NewVoidPushNotificationProcessor() *VoidPushNotificationProcessor {
	return &VoidPushNotificationProcessor{
		processor: NewVoidProcessor(),
	}
}

// GetHandler returns nil for void processor since it doesn't maintain handlers.
func (v *VoidPushNotificationProcessor) GetHandler(pushNotificationName string) PushNotificationHandler {
	return v.processor.GetHandler(pushNotificationName)
}

// RegisterHandler returns an error for void processor since it doesn't maintain handlers.
func (v *VoidPushNotificationProcessor) RegisterHandler(pushNotificationName string, handler PushNotificationHandler, protected bool) error {
	return v.processor.RegisterHandler(pushNotificationName, handler, protected)
}

// ProcessPendingNotifications reads and discards any pending push notifications.
func (v *VoidPushNotificationProcessor) ProcessPendingNotifications(ctx context.Context, handlerCtx PushNotificationHandlerContext, rd *proto.Reader) error {
	return v.processor.ProcessPendingNotifications(ctx, handlerCtx, rd)
}

// PushNotificationInfo contains metadata about a push notification.
type PushNotificationInfo struct {
	Name string
	Args []interface{}
}

// ParsePushNotificationInfo extracts information from a push notification.
func ParsePushNotificationInfo(notification []interface{}) *PushNotificationInfo {
	if len(notification) == 0 {
		return nil
	}

	name, ok := notification[0].(string)
	if !ok {
		return nil
	}

	return &PushNotificationInfo{
		Name: name,
		Args: notification[1:],
	}
}

// String returns a string representation of the push notification info.
func (info *PushNotificationInfo) String() string {
	if info == nil {
		return "<nil>"
	}
	return info.Name
}
