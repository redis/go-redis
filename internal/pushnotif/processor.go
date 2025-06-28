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

		notificationName, err := rd.PeekPushNotificationName()
		if err != nil {
			// Error reading - continue to next iteration
			break
		}

		// Skip notifications that should be handled by other systems
		if shouldSkipNotification(notificationName) {
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
					handler.HandlePushNotification(ctx, notification)
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
		"sunsubscribe", // Sharded unsubscription confirmation

	// Stream notifications - handled by stream consumers
		"xread-from",      // Stream reading notifications
		"xreadgroup-from", // Stream consumer group notifications

	// Client tracking notifications - handled by client-side cache system
		// Note: "invalidate" is now handled by client-side cache, not filtered

	// Keyspace notifications - handled by keyspace notification subscribers
	// Note: Keyspace notifications typically have prefixes like "__keyspace@0__:" or "__keyevent@0__:"
	// but we'll handle the base notification types here
		"expired",   // Key expiration events
		"evicted",   // Key eviction events
		"set",       // Key set events
		"del",       // Key deletion events
		"rename",    // Key rename events
		"move",      // Key move events
		"copy",      // Key copy events
		"restore",   // Key restore events
		"sort",      // Sort operation events
		"flushdb",   // Database flush events
		"flushall":  // All databases flush events
		return true
	default:
		return false
	}
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

// UnregisterHandler returns an error for void processor since it doesn't maintain handlers.
// This helps developers identify when they're trying to unregister handlers on disabled push notifications.
func (v *VoidProcessor) UnregisterHandler(pushNotificationName string) error {
	return fmt.Errorf("cannot unregister push notification handler '%s': push notifications are disabled (using void processor)", pushNotificationName)
}

// ProcessPendingNotifications for VoidProcessor does nothing since push notifications
// are only available in RESP3 and this processor is used when they're disabled.
// This avoids unnecessary buffer scanning overhead.
func (v *VoidProcessor) ProcessPendingNotifications(ctx context.Context, rd *proto.Reader) error {
	// VoidProcessor is used when push notifications are disabled (typically RESP2 or disabled RESP3).
	// Since push notifications only exist in RESP3, we can safely skip all processing
	// to avoid unnecessary buffer scanning overhead.
	return nil
}
