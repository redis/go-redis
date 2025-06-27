package pushnotif

import (
	"context"
	"fmt"
	"sync"
)

// Registry manages push notification handlers.
type Registry struct {
	mu       sync.RWMutex
	handlers map[string]handlerEntry
}

// NewRegistry creates a new push notification registry.
func NewRegistry() *Registry {
	return &Registry{
		handlers: make(map[string]handlerEntry),
	}
}

// RegisterHandler registers a handler for a specific push notification name.
// Returns an error if a handler is already registered for this push notification name.
// If protected is true, the handler cannot be unregistered.
func (r *Registry) RegisterHandler(pushNotificationName string, handler Handler, protected bool) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, exists := r.handlers[pushNotificationName]; exists {
		return fmt.Errorf("handler already registered for push notification: %s", pushNotificationName)
	}

	r.handlers[pushNotificationName] = handlerEntry{
		handler:   handler,
		protected: protected,
	}
	return nil
}

// UnregisterHandler removes a handler for a specific push notification name.
// Returns an error if the handler is protected or doesn't exist.
func (r *Registry) UnregisterHandler(pushNotificationName string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry, exists := r.handlers[pushNotificationName]
	if !exists {
		return fmt.Errorf("no handler registered for push notification: %s", pushNotificationName)
	}

	if entry.protected {
		return fmt.Errorf("cannot unregister protected handler for push notification: %s", pushNotificationName)
	}

	delete(r.handlers, pushNotificationName)
	return nil
}

// GetHandler returns the handler for a specific push notification name.
// Returns nil if no handler is registered for the given name.
func (r *Registry) GetHandler(pushNotificationName string) Handler {
	r.mu.RLock()
	defer r.mu.RUnlock()

	entry, exists := r.handlers[pushNotificationName]
	if !exists {
		return nil
	}
	return entry.handler
}

// GetRegisteredPushNotificationNames returns a list of all registered push notification names.
func (r *Registry) GetRegisteredPushNotificationNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	names := make([]string, 0, len(r.handlers))
	for name := range r.handlers {
		names = append(names, name)
	}
	return names
}

// HandleNotification attempts to handle a push notification using registered handlers.
// Returns true if a handler was found and successfully processed the notification.
func (r *Registry) HandleNotification(ctx context.Context, notification []interface{}) bool {
	if len(notification) == 0 {
		return false
	}

	// Extract the notification type (first element)
	notificationType, ok := notification[0].(string)
	if !ok {
		return false
	}

	// Get the handler for this notification type
	handler := r.GetHandler(notificationType)
	if handler == nil {
		return false
	}

	// Handle the notification
	return handler.HandlePushNotification(ctx, notification)
}
