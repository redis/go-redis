package pushnotif

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9/internal/proto"
)

// Processor handles push notifications with a registry of handlers.
type Processor struct {
	registry *Registry
}

// NewProcessor creates a new push notification processor.
func NewProcessor() *Processor {
	return &Processor{
		registry: NewRegistry(),
	}
}

// GetHandler returns the handler for a specific push notification name.
// Returns nil if no handler is registered for the given name.
func (p *Processor) GetHandler(pushNotificationName string) Handler {
	return p.registry.GetHandler(pushNotificationName)
}

// RegisterHandler registers a handler for a specific push notification name.
// Returns an error if a handler is already registered for this push notification name.
// If protected is true, the handler cannot be unregistered.
func (p *Processor) RegisterHandler(pushNotificationName string, handler Handler, protected bool) error {
	return p.registry.RegisterHandler(pushNotificationName, handler, protected)
}

// UnregisterHandler removes a handler for a specific push notification name.
// Returns an error if the handler is protected or doesn't exist.
func (p *Processor) UnregisterHandler(pushNotificationName string) error {
	return p.registry.UnregisterHandler(pushNotificationName)
}

// GetRegistryForTesting returns the push notification registry for testing.
// This method should only be used by tests.
func (p *Processor) GetRegistryForTesting() *Registry {
	return p.registry
}

// ProcessPendingNotifications checks for and processes any pending push notifications.
func (p *Processor) ProcessPendingNotifications(ctx context.Context, rd *proto.Reader) error {
	// Check for nil reader
	if rd == nil {
		return nil
	}

	// Check if there are any buffered bytes that might contain push notifications
	if rd.Buffered() == 0 {
		return nil
	}

	// Process all available push notifications
	for {
		// Peek at the next reply type to see if it's a push notification
		replyType, err := rd.PeekReplyType()
		if err != nil {
			// No more data available or error reading
			break
		}

		// Push notifications use RespPush type in RESP3
		if replyType != proto.RespPush {
			break
		}

		// Try to read the push notification
		reply, err := rd.ReadReply()
		if err != nil {
			return fmt.Errorf("failed to read push notification: %w", err)
		}

		// Convert to slice of interfaces
		notification, ok := reply.([]interface{})
		if !ok {
			continue
		}

		// Handle the notification
		p.registry.HandleNotification(ctx, notification)
	}

	return nil
}

// VoidProcessor discards all push notifications without processing them.
type VoidProcessor struct{}

// NewVoidProcessor creates a new void push notification processor.
func NewVoidProcessor() *VoidProcessor {
	return &VoidProcessor{}
}

// GetHandler returns nil for void processor since it doesn't maintain handlers.
func (v *VoidProcessor) GetHandler(pushNotificationName string) Handler {
	return nil
}

// RegisterHandler returns an error for void processor since it doesn't maintain handlers.
// This helps developers identify when they're trying to register handlers on disabled push notifications.
func (v *VoidProcessor) RegisterHandler(pushNotificationName string, handler Handler, protected bool) error {
	return fmt.Errorf("cannot register push notification handler '%s': push notifications are disabled (using void processor)", pushNotificationName)
}

// GetRegistryForTesting returns nil for void processor since it doesn't maintain handlers.
// This method should only be used by tests.
func (v *VoidProcessor) GetRegistryForTesting() *Registry {
	return nil
}

// ProcessPendingNotifications reads and discards any pending push notifications.
func (v *VoidProcessor) ProcessPendingNotifications(ctx context.Context, rd *proto.Reader) error {
	// Check for nil reader
	if rd == nil {
		return nil
	}

	// Read and discard any pending push notifications to clean the buffer
	for {
		// Peek at the next reply type to see if it's a push notification
		replyType, err := rd.PeekReplyType()
		if err != nil {
			// No more data available or error reading
			break
		}

		// Push notifications use RespPush type in RESP3
		if replyType != proto.RespPush {
			break
		}

		// Read and discard the push notification
		_, err = rd.ReadReply()
		if err != nil {
			return fmt.Errorf("failed to read push notification for discarding: %w", err)
		}

		// Notification discarded - continue to next one
	}

	return nil
}
