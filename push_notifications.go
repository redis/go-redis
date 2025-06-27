package redis

import (
	"context"

	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/internal/pushnotif"
)

// PushNotificationHandler defines the interface for push notification handlers.
type PushNotificationHandler interface {
	// HandlePushNotification processes a push notification.
	// Returns true if the notification was handled, false otherwise.
	HandlePushNotification(ctx context.Context, notification []interface{}) bool
}

// PushNotificationProcessorInterface defines the interface for push notification processors.
type PushNotificationProcessorInterface interface {
	GetHandler(pushNotificationName string) PushNotificationHandler
	ProcessPendingNotifications(ctx context.Context, rd *proto.Reader) error
	RegisterHandler(pushNotificationName string, handler PushNotificationHandler, protected bool) error
}

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
	return r.registry.RegisterHandler(pushNotificationName, &handlerWrapper{handler}, protected)
}

// UnregisterHandler removes a handler for a specific push notification name.
func (r *PushNotificationRegistry) UnregisterHandler(pushNotificationName string) error {
	return r.registry.UnregisterHandler(pushNotificationName)
}

// GetHandler returns the handler for a specific push notification name.
func (r *PushNotificationRegistry) GetHandler(pushNotificationName string) PushNotificationHandler {
	handler := r.registry.GetHandler(pushNotificationName)
	if handler == nil {
		return nil
	}
	if wrapper, ok := handler.(*handlerWrapper); ok {
		return wrapper.handler
	}
	return nil
}

// GetRegisteredPushNotificationNames returns a list of all registered push notification names.
func (r *PushNotificationRegistry) GetRegisteredPushNotificationNames() []string {
	return r.registry.GetRegisteredPushNotificationNames()
}

// HandleNotification attempts to handle a push notification using registered handlers.
func (r *PushNotificationRegistry) HandleNotification(ctx context.Context, notification []interface{}) bool {
	return r.registry.HandleNotification(ctx, notification)
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
	handler := p.processor.GetHandler(pushNotificationName)
	if handler == nil {
		return nil
	}
	if wrapper, ok := handler.(*handlerWrapper); ok {
		return wrapper.handler
	}
	return nil
}

// RegisterHandler registers a handler for a specific push notification name.
func (p *PushNotificationProcessor) RegisterHandler(pushNotificationName string, handler PushNotificationHandler, protected bool) error {
	return p.processor.RegisterHandler(pushNotificationName, &handlerWrapper{handler}, protected)
}

// UnregisterHandler removes a handler for a specific push notification name.
func (p *PushNotificationProcessor) UnregisterHandler(pushNotificationName string) error {
	return p.processor.UnregisterHandler(pushNotificationName)
}

// ProcessPendingNotifications checks for and processes any pending push notifications.
func (p *PushNotificationProcessor) ProcessPendingNotifications(ctx context.Context, rd *proto.Reader) error {
	return p.processor.ProcessPendingNotifications(ctx, rd)
}

// GetRegistryForTesting returns the push notification registry for testing.
func (p *PushNotificationProcessor) GetRegistryForTesting() *PushNotificationRegistry {
	return &PushNotificationRegistry{
		registry: p.processor.GetRegistryForTesting(),
	}
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
func (v *VoidPushNotificationProcessor) ProcessPendingNotifications(ctx context.Context, rd *proto.Reader) error {
	return v.processor.ProcessPendingNotifications(ctx, rd)
}

// GetRegistryForTesting returns nil for void processor since it doesn't maintain handlers.
func (v *VoidPushNotificationProcessor) GetRegistryForTesting() *PushNotificationRegistry {
	return nil
}

// handlerWrapper wraps the public PushNotificationHandler interface to implement the internal Handler interface.
type handlerWrapper struct {
	handler PushNotificationHandler
}

func (w *handlerWrapper) HandlePushNotification(ctx context.Context, notification []interface{}) bool {
	return w.handler.HandlePushNotification(ctx, notification)
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
