package redis

import (
	"context"

	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/internal/pushnotif"
)

// PushNotificationHandler defines the interface for push notification handlers.
// This is an alias to the internal push notification handler interface.
type PushNotificationHandler = pushnotif.Handler

// PushNotificationProcessorInterface defines the interface for push notification processors.
// This is an alias to the internal push notification processor interface.
type PushNotificationProcessorInterface = pushnotif.ProcessorInterface

// PushNotificationRegistry manages push notification handlers.
type PushNotificationRegistry struct {
	registry *pushnotif.Registry
}

// NewPushNotificationRegistry creates a new push notification registry.
func NewPushNotificationRegistry() *PushNotificationRegistry {
	return &PushNotificationRegistry{
		registry: pushnotif.NewRegistry(),
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
	processor *pushnotif.Processor
}

// NewPushNotificationProcessor creates a new push notification processor.
func NewPushNotificationProcessor() *PushNotificationProcessor {
	return &PushNotificationProcessor{
		processor: pushnotif.NewProcessor(),
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
func (p *PushNotificationProcessor) ProcessPendingNotifications(ctx context.Context, handlerCtx *pushnotif.HandlerContext, rd *proto.Reader) error {
	return p.processor.ProcessPendingNotifications(ctx, handlerCtx, rd)
}

// VoidPushNotificationProcessor discards all push notifications without processing them.
type VoidPushNotificationProcessor struct {
	processor *pushnotif.VoidProcessor
}

// NewVoidPushNotificationProcessor creates a new void push notification processor.
func NewVoidPushNotificationProcessor() *VoidPushNotificationProcessor {
	return &VoidPushNotificationProcessor{
		processor: pushnotif.NewVoidProcessor(),
	}
}

// GetHandler returns nil for void processor since it doesn't maintain handlers.
func (v *VoidPushNotificationProcessor) GetHandler(pushNotificationName string) PushNotificationHandler {
	return nil
}

// RegisterHandler returns an error for void processor since it doesn't maintain handlers.
func (v *VoidPushNotificationProcessor) RegisterHandler(pushNotificationName string, handler PushNotificationHandler, protected bool) error {
	return v.processor.RegisterHandler(pushNotificationName, nil, protected)
}

// ProcessPendingNotifications reads and discards any pending push notifications.
func (v *VoidPushNotificationProcessor) ProcessPendingNotifications(ctx context.Context, handlerCtx *pushnotif.HandlerContext, rd *proto.Reader) error {
	return v.processor.ProcessPendingNotifications(ctx, handlerCtx, rd)
}

// Redis Cluster push notification names
const (
	PushNotificationMoving      = "MOVING"
	PushNotificationMigrating   = "MIGRATING"
	PushNotificationMigrated    = "MIGRATED"
	PushNotificationFailingOver = "FAILING_OVER"
	PushNotificationFailedOver  = "FAILED_OVER"
)

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
