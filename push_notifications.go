package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/pool"
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

// PushNotificationHandlerContext provides context information about where a push notification was received.
// This interface allows handlers to make informed decisions based on the source of the notification
// with strongly typed access to different client types using concrete types.
type PushNotificationHandlerContext interface {
	// GetClient returns the Redis client instance that received the notification.
	// Returns nil if no client context is available.
	GetClient() interface{}

	// GetClusterClient returns the client as a ClusterClient if it is one.
	// Returns nil if the client is not a ClusterClient or no client context is available.
	GetClusterClient() *ClusterClient

	// GetSentinelClient returns the client as a SentinelClient if it is one.
	// Returns nil if the client is not a SentinelClient or no client context is available.
	GetSentinelClient() *SentinelClient

	// GetFailoverClient returns the client as a FailoverClient if it is one.
	// Returns nil if the client is not a FailoverClient or no client context is available.
	GetFailoverClient() *Client

	// GetRegularClient returns the client as a regular Client if it is one.
	// Returns nil if the client is not a regular Client or no client context is available.
	GetRegularClient() *Client

	// GetConnPool returns the connection pool from which the connection was obtained.
	// Returns nil if no connection pool context is available.
	GetConnPool() interface{}

	// GetPubSub returns the PubSub instance that received the notification.
	// Returns nil if this is not a PubSub connection.
	GetPubSub() *PubSub

	// GetConn returns the specific connection on which the notification was received.
	// Returns nil if no connection context is available.
	GetConn() *pool.Conn

	// IsBlocking returns true if the notification was received on a blocking connection.
	IsBlocking() bool
}

// PushNotificationHandler defines the interface for push notification handlers.
type PushNotificationHandler interface {
	// HandlePushNotification processes a push notification with context information.
	// The handlerCtx provides information about the client, connection pool, and connection
	// on which the notification was received, allowing handlers to make informed decisions.
	// Returns true if the notification was handled, false otherwise.
	HandlePushNotification(ctx context.Context, handlerCtx PushNotificationHandlerContext, notification []interface{}) bool
}

// pushNotificationHandlerContext is the concrete implementation of PushNotificationHandlerContext interface
type pushNotificationHandlerContext struct {
	client     interface{}
	connPool   interface{}
	pubSub     interface{}
	conn       *pool.Conn
	isBlocking bool
}

// NewPushNotificationHandlerContext creates a new PushNotificationHandlerContext implementation
func NewPushNotificationHandlerContext(client, connPool, pubSub interface{}, conn *pool.Conn, isBlocking bool) PushNotificationHandlerContext {
	return &pushNotificationHandlerContext{
		client:     client,
		connPool:   connPool,
		pubSub:     pubSub,
		conn:       conn,
		isBlocking: isBlocking,
	}
}

// GetClient returns the Redis client instance that received the notification
func (h *pushNotificationHandlerContext) GetClient() interface{} {
	return h.client
}

// GetClusterClient returns the client as a ClusterClient if it is one
func (h *pushNotificationHandlerContext) GetClusterClient() *ClusterClient {
	if client, ok := h.client.(*ClusterClient); ok {
		return client
	}
	return nil
}

// GetSentinelClient returns the client as a SentinelClient if it is one
func (h *pushNotificationHandlerContext) GetSentinelClient() *SentinelClient {
	if client, ok := h.client.(*SentinelClient); ok {
		return client
	}
	return nil
}

// GetFailoverClient returns the client as a FailoverClient if it is one
func (h *pushNotificationHandlerContext) GetFailoverClient() *Client {
	if client, ok := h.client.(*Client); ok {
		return client
	}
	return nil
}

// GetRegularClient returns the client as a regular Client if it is one
func (h *pushNotificationHandlerContext) GetRegularClient() *Client {
	if client, ok := h.client.(*Client); ok {
		return client
	}
	return nil
}

// GetConnPool returns the connection pool from which the connection was obtained
func (h *pushNotificationHandlerContext) GetConnPool() interface{} {
	return h.connPool
}

// GetPubSub returns the PubSub instance that received the notification
func (h *pushNotificationHandlerContext) GetPubSub() *PubSub {
	if pubSub, ok := h.pubSub.(*PubSub); ok {
		return pubSub
	}
	return nil
}

// GetConn returns the specific connection on which the notification was received
func (h *pushNotificationHandlerContext) GetConn() *pool.Conn {
	return h.conn
}

// IsBlocking returns true if the notification was received on a blocking connection
func (h *pushNotificationHandlerContext) IsBlocking() bool {
	return h.isBlocking
}

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
