package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/proto"
)

// Registry manages push notification handlers
type Registry struct {
	handlers  map[string]PushNotificationHandler
	protected map[string]bool
}

// NewRegistry creates a new push notification registry
func NewRegistry() *Registry {
	return &Registry{
		handlers:  make(map[string]PushNotificationHandler),
		protected: make(map[string]bool),
	}
}

// RegisterHandler registers a handler for a specific push notification name
func (r *Registry) RegisterHandler(pushNotificationName string, handler PushNotificationHandler, protected bool) error {
	if handler == nil {
		return fmt.Errorf("handler cannot be nil")
	}
	
	// Check if handler already exists and is protected
	if existingProtected, exists := r.protected[pushNotificationName]; exists && existingProtected {
		return fmt.Errorf("cannot overwrite protected handler for push notification: %s", pushNotificationName)
	}
	
	r.handlers[pushNotificationName] = handler
	r.protected[pushNotificationName] = protected
	return nil
}

// GetHandler returns the handler for a specific push notification name
func (r *Registry) GetHandler(pushNotificationName string) PushNotificationHandler {
	return r.handlers[pushNotificationName]
}

// UnregisterHandler removes a handler for a specific push notification name
func (r *Registry) UnregisterHandler(pushNotificationName string) error {
	// Check if handler is protected
	if protected, exists := r.protected[pushNotificationName]; exists && protected {
		return fmt.Errorf("cannot unregister protected handler for push notification: %s", pushNotificationName)
	}
	
	delete(r.handlers, pushNotificationName)
	delete(r.protected, pushNotificationName)
	return nil
}

// GetRegisteredPushNotificationNames returns all registered push notification names
func (r *Registry) GetRegisteredPushNotificationNames() []string {
	names := make([]string, 0, len(r.handlers))
	for name := range r.handlers {
		names = append(names, name)
	}
	return names
}

// Processor handles push notifications with a registry of handlers
type Processor struct {
	registry *Registry
}

// NewProcessor creates a new push notification processor
func NewProcessor() *Processor {
	return &Processor{
		registry: NewRegistry(),
	}
}

// GetHandler returns the handler for a specific push notification name
func (p *Processor) GetHandler(pushNotificationName string) PushNotificationHandler {
	return p.registry.GetHandler(pushNotificationName)
}

// RegisterHandler registers a handler for a specific push notification name
func (p *Processor) RegisterHandler(pushNotificationName string, handler PushNotificationHandler, protected bool) error {
	return p.registry.RegisterHandler(pushNotificationName, handler, protected)
}

// UnregisterHandler removes a handler for a specific push notification name
func (p *Processor) UnregisterHandler(pushNotificationName string) error {
	return p.registry.UnregisterHandler(pushNotificationName)
}

// ProcessPendingNotifications checks for and processes any pending push notifications
func (p *Processor) ProcessPendingNotifications(ctx context.Context, handlerCtx PushNotificationHandlerContext, rd *proto.Reader) error {
	if rd == nil {
		return nil
	}

	for {
		// Check if there's data available to read
		replyType, err := rd.PeekReplyType()
		if err != nil {
			// No more data available or error reading
			break
		}

		// Only process push notifications (arrays starting with >)
		if replyType != proto.RespPush {
			break
		}

		// Read the push notification
		reply, err := rd.ReadReply()
		if err != nil {
			internal.Logger.Printf(ctx, "push: error reading push notification: %v", err)
			break
		}

		// Convert to slice of interfaces
		notification, ok := reply.([]interface{})
		if !ok {
			continue
		}

		// Handle the notification directly
		if len(notification) > 0 {
			// Extract the notification type (first element)
			if notificationType, ok := notification[0].(string); ok {
				// Skip notifications that should be handled by other systems
				if shouldSkipNotification(notificationType) {
					continue
				}

				// Get the handler for this notification type
				if handler := p.registry.GetHandler(notificationType); handler != nil {
					// Handle the notification
					handler.HandlePushNotification(ctx, handlerCtx, notification)
				}
			}
		}
	}

	return nil
}

// shouldSkipNotification checks if a notification type should be ignored by the push notification
// processor and handled by other specialized systems instead (pub/sub, streams, keyspace, etc.).
func shouldSkipNotification(notificationType string) bool {
	switch notificationType {
	// Pub/Sub notifications - handled by pub/sub system
	case "message", // Regular pub/sub message
		"pmessage",     // Pattern pub/sub message
		"subscribe",    // Subscription confirmation
		"unsubscribe",  // Unsubscription confirmation
		"psubscribe",   // Pattern subscription confirmation
		"punsubscribe", // Pattern unsubscription confirmation
		"smessage",     // Sharded pub/sub message (Redis 7.0+)
		"ssubscribe",   // Sharded subscription confirmation
		"sunsubscribe": // Sharded unsubscription confirmation
		return true
	default:
		return false
	}
}

// VoidProcessor discards all push notifications without processing them
type VoidProcessor struct{}

// NewVoidProcessor creates a new void push notification processor
func NewVoidProcessor() *VoidProcessor {
	return &VoidProcessor{}
}

// GetHandler returns nil for void processor since it doesn't maintain handlers
func (v *VoidProcessor) GetHandler(pushNotificationName string) PushNotificationHandler {
	return nil
}

// RegisterHandler returns an error for void processor since it doesn't maintain handlers
func (v *VoidProcessor) RegisterHandler(pushNotificationName string, handler PushNotificationHandler, protected bool) error {
	return fmt.Errorf("cannot register push notification handler '%s': push notifications are disabled (using void processor)", pushNotificationName)
}

// UnregisterHandler returns an error for void processor since it doesn't maintain handlers
func (v *VoidProcessor) UnregisterHandler(pushNotificationName string) error {
	return fmt.Errorf("cannot unregister push notification handler '%s': push notifications are disabled (using void processor)", pushNotificationName)
}

// ProcessPendingNotifications for VoidProcessor does nothing since push notifications
// are only available in RESP3 and this processor is used for RESP2 connections.
// This avoids unnecessary buffer scanning overhead.
func (v *VoidProcessor) ProcessPendingNotifications(ctx context.Context, handlerCtx PushNotificationHandlerContext, rd *proto.Reader) error {
	// VoidProcessor is used for RESP2 connections where push notifications are not available.
	// Since push notifications only exist in RESP3, we can safely skip all processing
	// to avoid unnecessary buffer scanning overhead.
	return nil
}
