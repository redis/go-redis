package redis

import (
	"context"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/internal/pushnotif"
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

// handlerAdapter adapts a PushNotificationHandler to the internal pushnotif.Handler interface
type handlerAdapter struct {
	handler PushNotificationHandler
}

// HandlePushNotification adapts the public handler to the internal interface
func (a *handlerAdapter) HandlePushNotification(ctx context.Context, handlerCtx pushnotif.HandlerContext, notification []interface{}) bool {
	// Convert internal HandlerContext to public PushNotificationHandlerContext
	// We need to extract the fields from the internal context and create a public one
	var client, connPool, pubSub interface{}
	var conn *pool.Conn
	var isBlocking bool

	// Extract information from internal context
	client = handlerCtx.GetClient()
	connPool = handlerCtx.GetConnPool()
	conn = handlerCtx.GetConn()
	isBlocking = handlerCtx.IsBlocking()

	// Try to get PubSub if available
	if handlerCtx.GetPubSub() != nil {
		pubSub = handlerCtx.GetPubSub()
	}

	// Create public context
	publicCtx := NewPushNotificationHandlerContext(client, connPool, pubSub, conn, isBlocking)

	// Call the public handler
	return a.handler.HandlePushNotification(ctx, publicCtx, notification)
}

// contextAdapter converts internal HandlerContext to public PushNotificationHandlerContext

// voidProcessorAdapter adapts a VoidProcessor to the public interface
type voidProcessorAdapter struct {
	processor *pushnotif.VoidProcessor
}

// NewVoidProcessorAdapter creates a new void processor adapter
func NewVoidProcessorAdapter() PushNotificationProcessorInterface {
	return &voidProcessorAdapter{
		processor: pushnotif.NewVoidProcessor(),
	}
}

// GetHandler returns nil for void processor since it doesn't maintain handlers
func (v *voidProcessorAdapter) GetHandler(pushNotificationName string) PushNotificationHandler {
	return nil
}

// RegisterHandler returns an error for void processor since it doesn't maintain handlers
func (v *voidProcessorAdapter) RegisterHandler(pushNotificationName string, handler PushNotificationHandler, protected bool) error {
	// Void processor doesn't support handlers
	return v.processor.RegisterHandler(pushNotificationName, nil, protected)
}

// ProcessPendingNotifications reads and discards any pending push notifications
func (v *voidProcessorAdapter) ProcessPendingNotifications(ctx context.Context, handlerCtx PushNotificationHandlerContext, rd *proto.Reader) error {
	// Convert public context to internal context
	internalCtx := pushnotif.NewHandlerContext(
		handlerCtx.GetClient(),
		handlerCtx.GetConnPool(),
		handlerCtx.GetPubSub(),
		handlerCtx.GetConn(),
		handlerCtx.IsBlocking(),
	)
	return v.processor.ProcessPendingNotifications(ctx, internalCtx, rd)
}

// PushNotificationProcessorInterface defines the interface for push notification processors.
type PushNotificationProcessorInterface interface {
	GetHandler(pushNotificationName string) PushNotificationHandler
	ProcessPendingNotifications(ctx context.Context, handlerCtx PushNotificationHandlerContext, rd *proto.Reader) error
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
	// Wrap the public handler in an adapter for the internal interface
	adapter := &handlerAdapter{handler: handler}
	return r.registry.RegisterHandler(pushNotificationName, adapter, protected)
}

// UnregisterHandler removes a handler for a specific push notification name.
func (r *PushNotificationRegistry) UnregisterHandler(pushNotificationName string) error {
	return r.registry.UnregisterHandler(pushNotificationName)
}

// GetHandler returns the handler for a specific push notification name.
func (r *PushNotificationRegistry) GetHandler(pushNotificationName string) PushNotificationHandler {
	internalHandler := r.registry.GetHandler(pushNotificationName)
	if internalHandler == nil {
		return nil
	}

	// If it's our adapter, return the original handler
	if adapter, ok := internalHandler.(*handlerAdapter); ok {
		return adapter.handler
	}

	// This shouldn't happen in normal usage, but handle it gracefully
	return nil
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
	internalHandler := p.processor.GetHandler(pushNotificationName)
	if internalHandler == nil {
		return nil
	}

	// If it's our adapter, return the original handler
	if adapter, ok := internalHandler.(*handlerAdapter); ok {
		return adapter.handler
	}

	// This shouldn't happen in normal usage, but handle it gracefully
	return nil
}

// RegisterHandler registers a handler for a specific push notification name.
func (p *PushNotificationProcessor) RegisterHandler(pushNotificationName string, handler PushNotificationHandler, protected bool) error {
	// Wrap the public handler in an adapter for the internal interface
	adapter := &handlerAdapter{handler: handler}
	return p.processor.RegisterHandler(pushNotificationName, adapter, protected)
}

// UnregisterHandler removes a handler for a specific push notification name.
func (p *PushNotificationProcessor) UnregisterHandler(pushNotificationName string) error {
	return p.processor.UnregisterHandler(pushNotificationName)
}

// ProcessPendingNotifications checks for and processes any pending push notifications.
// The handlerCtx provides context about the client, connection pool, and connection.
func (p *PushNotificationProcessor) ProcessPendingNotifications(ctx context.Context, handlerCtx PushNotificationHandlerContext, rd *proto.Reader) error {
	// Convert public context to internal context
	internalCtx := pushnotif.NewHandlerContext(
		handlerCtx.GetClient(),
		handlerCtx.GetConnPool(),
		handlerCtx.GetPubSub(),
		handlerCtx.GetConn(),
		handlerCtx.IsBlocking(),
	)
	return p.processor.ProcessPendingNotifications(ctx, internalCtx, rd)
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
func (v *VoidPushNotificationProcessor) ProcessPendingNotifications(ctx context.Context, handlerCtx PushNotificationHandlerContext, rd *proto.Reader) error {
	// Convert public context to internal context
	internalCtx := pushnotif.NewHandlerContext(
		handlerCtx.GetClient(),
		handlerCtx.GetConnPool(),
		handlerCtx.GetPubSub(),
		handlerCtx.GetConn(),
		handlerCtx.IsBlocking(),
	)
	return v.processor.ProcessPendingNotifications(ctx, internalCtx, rd)
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
