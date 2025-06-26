package redis

import (
	"context"
	"fmt"
	"sync"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/proto"
)

// PushNotificationHandler defines the interface for handling push notifications.
type PushNotificationHandler interface {
	// HandlePushNotification processes a push notification.
	// Returns true if the notification was handled, false otherwise.
	HandlePushNotification(ctx context.Context, notification []interface{}) bool
}

// PushNotificationHandlerFunc is a function adapter for PushNotificationHandler.
type PushNotificationHandlerFunc func(ctx context.Context, notification []interface{}) bool

// HandlePushNotification implements PushNotificationHandler.
func (f PushNotificationHandlerFunc) HandlePushNotification(ctx context.Context, notification []interface{}) bool {
	return f(ctx, notification)
}

// PushNotificationRegistry manages handlers for different types of push notifications.
type PushNotificationRegistry struct {
	mu       sync.RWMutex
	handlers map[string]PushNotificationHandler // command -> single handler
	global   []PushNotificationHandler          // global handlers for all notifications
}

// NewPushNotificationRegistry creates a new push notification registry.
func NewPushNotificationRegistry() *PushNotificationRegistry {
	return &PushNotificationRegistry{
		handlers: make(map[string]PushNotificationHandler),
		global:   make([]PushNotificationHandler, 0),
	}
}

// RegisterHandler registers a handler for a specific push notification command.
// Returns an error if a handler is already registered for this command.
func (r *PushNotificationRegistry) RegisterHandler(command string, handler PushNotificationHandler) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[command]; exists {
		return fmt.Errorf("handler already registered for command: %s", command)
	}
	r.handlers[command] = handler
	return nil
}

// RegisterGlobalHandler registers a handler that will receive all push notifications.
func (r *PushNotificationRegistry) RegisterGlobalHandler(handler PushNotificationHandler) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.global = append(r.global, handler)
}

// UnregisterHandler removes the handler for a specific push notification command.
func (r *PushNotificationRegistry) UnregisterHandler(command string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.handlers, command)
}

// HandleNotification processes a push notification by calling all registered handlers.
func (r *PushNotificationRegistry) HandleNotification(ctx context.Context, notification []interface{}) bool {
	if len(notification) == 0 {
		return false
	}

	// Extract command from notification
	command, ok := notification[0].(string)
	if !ok {
		return false
	}

	r.mu.RLock()
	defer r.mu.RUnlock()

	handled := false

	// Call global handlers first
	for _, handler := range r.global {
		if handler.HandlePushNotification(ctx, notification) {
			handled = true
		}
	}

	// Call specific handler
	if handler, exists := r.handlers[command]; exists {
		if handler.HandlePushNotification(ctx, notification) {
			handled = true
		}
	}

	return handled
}

// GetRegisteredCommands returns a list of commands that have registered handlers.
func (r *PushNotificationRegistry) GetRegisteredCommands() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	commands := make([]string, 0, len(r.handlers))
	for command := range r.handlers {
		commands = append(commands, command)
	}
	return commands
}

// HasHandlers returns true if there are any handlers registered (global or specific).
func (r *PushNotificationRegistry) HasHandlers() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.global) > 0 || len(r.handlers) > 0
}

// PushNotificationProcessor handles the processing of push notifications from Redis.
type PushNotificationProcessor struct {
	registry *PushNotificationRegistry
	enabled  bool
}

// NewPushNotificationProcessor creates a new push notification processor.
func NewPushNotificationProcessor(enabled bool) *PushNotificationProcessor {
	return &PushNotificationProcessor{
		registry: NewPushNotificationRegistry(),
		enabled:  enabled,
	}
}

// IsEnabled returns whether push notification processing is enabled.
func (p *PushNotificationProcessor) IsEnabled() bool {
	return p.enabled
}

// SetEnabled enables or disables push notification processing.
func (p *PushNotificationProcessor) SetEnabled(enabled bool) {
	p.enabled = enabled
}

// GetRegistry returns the push notification registry.
func (p *PushNotificationProcessor) GetRegistry() *PushNotificationRegistry {
	return p.registry
}

// ProcessPendingNotifications checks for and processes any pending push notifications.
func (p *PushNotificationProcessor) ProcessPendingNotifications(ctx context.Context, rd *proto.Reader) error {
	if !p.enabled || !p.registry.HasHandlers() {
		return nil
	}

	// Check if there are any buffered bytes that might contain push notifications
	if rd.Buffered() == 0 {
		return nil
	}

	// Process any pending push notifications
	for {
		// Peek at the next reply type to see if it's a push notification
		replyType, err := rd.PeekReplyType()
		if err != nil {
			// No more data available or error peeking
			break
		}

		// Check if this is a RESP3 push notification
		if replyType == '>' { // RespPush
			// Read the push notification
			reply, err := rd.ReadReply()
			if err != nil {
				internal.Logger.Printf(ctx, "push: error reading push notification: %v", err)
				break
			}

			// Process the push notification
			if pushSlice, ok := reply.([]interface{}); ok && len(pushSlice) > 0 {
				handled := p.registry.HandleNotification(ctx, pushSlice)
				if handled {
					internal.Logger.Printf(ctx, "push: processed push notification: %v", pushSlice[0])
				} else {
					internal.Logger.Printf(ctx, "push: unhandled push notification: %v", pushSlice[0])
				}
			} else {
				internal.Logger.Printf(ctx, "push: invalid push notification format: %v", reply)
			}
		} else {
			// Not a push notification, stop processing
			break
		}
	}

	return nil
}

// RegisterHandler is a convenience method to register a handler for a specific command.
// Returns an error if a handler is already registered for this command.
func (p *PushNotificationProcessor) RegisterHandler(command string, handler PushNotificationHandler) error {
	return p.registry.RegisterHandler(command, handler)
}

// RegisterGlobalHandler is a convenience method to register a global handler.
func (p *PushNotificationProcessor) RegisterGlobalHandler(handler PushNotificationHandler) {
	p.registry.RegisterGlobalHandler(handler)
}

// RegisterHandlerFunc is a convenience method to register a function as a handler.
// Returns an error if a handler is already registered for this command.
func (p *PushNotificationProcessor) RegisterHandlerFunc(command string, handlerFunc func(ctx context.Context, notification []interface{}) bool) error {
	return p.registry.RegisterHandler(command, PushNotificationHandlerFunc(handlerFunc))
}

// RegisterGlobalHandlerFunc is a convenience method to register a function as a global handler.
func (p *PushNotificationProcessor) RegisterGlobalHandlerFunc(handlerFunc func(ctx context.Context, notification []interface{}) bool) {
	p.registry.RegisterGlobalHandler(PushNotificationHandlerFunc(handlerFunc))
}

// Common push notification commands
const (
	// Redis Cluster notifications
	PushNotificationMoving     = "MOVING"
	PushNotificationMigrating  = "MIGRATING"
	PushNotificationMigrated   = "MIGRATED"
	PushNotificationFailingOver = "FAILING_OVER"
	PushNotificationFailedOver  = "FAILED_OVER"

	// Redis Pub/Sub notifications
	PushNotificationPubSubMessage = "message"
	PushNotificationPMessage      = "pmessage"
	PushNotificationSubscribe  = "subscribe"
	PushNotificationUnsubscribe = "unsubscribe"
	PushNotificationPSubscribe = "psubscribe"
	PushNotificationPUnsubscribe = "punsubscribe"

	// Redis Stream notifications
	PushNotificationXRead      = "xread"
	PushNotificationXReadGroup = "xreadgroup"

	// Redis Keyspace notifications
	PushNotificationKeyspace  = "keyspace"
	PushNotificationKeyevent  = "keyevent"

	// Redis Module notifications
	PushNotificationModule    = "module"

	// Custom application notifications
	PushNotificationCustom    = "custom"
)

// PushNotificationInfo contains metadata about a push notification.
type PushNotificationInfo struct {
	Command   string
	Args      []interface{}
	Timestamp int64
	Source    string
}

// ParsePushNotificationInfo extracts information from a push notification.
func ParsePushNotificationInfo(notification []interface{}) *PushNotificationInfo {
	if len(notification) == 0 {
		return nil
	}

	command, ok := notification[0].(string)
	if !ok {
		return nil
	}

	return &PushNotificationInfo{
		Command: command,
		Args:    notification[1:],
	}
}

// String returns a string representation of the push notification info.
func (info *PushNotificationInfo) String() string {
	if info == nil {
		return "<nil>"
	}
	return info.Command
}
