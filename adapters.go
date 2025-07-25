package redis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	"github.com/redis/go-redis/v9/internal/interfaces"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/push"
)

// ErrInvalidCommand is returned when an invalid command is passed to ExecuteCommand.
var ErrInvalidCommand = errors.New("invalid command type")

// ErrInvalidPool is returned when the pool type is not supported.
var ErrInvalidPool = errors.New("invalid pool type")

// NewClientAdapter creates a new client adapter for regular Redis clients.
func NewClientAdapter(client *Client) interfaces.ClientInterface {
	return &clientAdapter{client: client}
}

// NewClusterClientAdapter creates a new client adapter for cluster Redis clients.
func NewClusterClientAdapter(client interface{}) interfaces.ClientInterface {
	return &clusterClientAdapter{client: client}
}

// clientAdapter adapts a Redis client to implement interfaces.ClientInterface.
type clientAdapter struct {
	client *Client
}

// GetOptions returns the client options.
func (ca *clientAdapter) GetOptions() interfaces.OptionsInterface {
	return &optionsAdapter{options: ca.client.opt}
}

// GetPushProcessor returns the client's push notification processor.
func (ca *clientAdapter) GetPushProcessor() interfaces.NotificationProcessor {
	return &pushProcessorAdapter{processor: ca.client.pushProcessor}
}

// optionsAdapter adapts Redis options to implement interfaces.OptionsInterface.
type optionsAdapter struct {
	options *Options
}

// GetReadTimeout returns the read timeout.
func (oa *optionsAdapter) GetReadTimeout() time.Duration {
	return oa.options.ReadTimeout
}

// GetWriteTimeout returns the write timeout.
func (oa *optionsAdapter) GetWriteTimeout() time.Duration {
	return oa.options.WriteTimeout
}

// GetAddr returns the connection address.
func (oa *optionsAdapter) GetAddr() string {
	return oa.options.Addr
}

// IsTLSEnabled returns true if TLS is enabled.
func (oa *optionsAdapter) IsTLSEnabled() bool {
	return oa.options.TLSConfig != nil
}

// GetProtocol returns the protocol version.
func (oa *optionsAdapter) GetProtocol() int {
	return oa.options.Protocol
}

// GetPoolSize returns the connection pool size.
func (oa *optionsAdapter) GetPoolSize() int {
	return oa.options.PoolSize
}

// NewDialer returns a new dialer function for the connection.
func (oa *optionsAdapter) NewDialer() func(context.Context) (net.Conn, error) {
	baseDialer := oa.options.NewDialer()
	return func(ctx context.Context) (net.Conn, error) {
		// Extract network and address from the options
		network := "tcp"
		addr := oa.options.Addr
		return baseDialer(ctx, network, addr)
	}
}

// connectionAdapter adapts a Redis connection to interfaces.ConnectionWithRelaxedTimeout
type connectionAdapter struct {
	conn *pool.Conn
}

// Close closes the connection.
func (ca *connectionAdapter) Close() error {
	return ca.conn.Close()
}

// IsUsable returns true if the connection is safe to use for new commands.
func (ca *connectionAdapter) IsUsable() bool {
	return ca.conn.IsUsable()
}

// GetPoolConnection returns the underlying pool connection.
func (ca *connectionAdapter) GetPoolConnection() *pool.Conn {
	return ca.conn
}

// SetRelaxedTimeout sets relaxed timeouts for this connection during hitless upgrades.
// These timeouts remain active until explicitly cleared.
func (ca *connectionAdapter) SetRelaxedTimeout(readTimeout, writeTimeout time.Duration) {
	ca.conn.SetRelaxedTimeout(readTimeout, writeTimeout)
}

// SetRelaxedTimeoutWithDeadline sets relaxed timeouts with an expiration deadline.
// After the deadline, timeouts automatically revert to normal values.
func (ca *connectionAdapter) SetRelaxedTimeoutWithDeadline(readTimeout, writeTimeout time.Duration, deadline time.Time) {
	ca.conn.SetRelaxedTimeoutWithDeadline(readTimeout, writeTimeout, deadline)
}

// ClearRelaxedTimeout clears relaxed timeouts for this connection.
func (ca *connectionAdapter) ClearRelaxedTimeout() {
	ca.conn.ClearRelaxedTimeout()
}

// clusterClientAdapter adapts a cluster client to implement interfaces.ClientInterface.
type clusterClientAdapter struct {
	client interface{}
}

// GetOptions returns the client options.
func (cca *clusterClientAdapter) GetOptions() interfaces.OptionsInterface {
	// Return a mock options adapter for cluster clients
	return &mockClusterOptionsAdapter{}
}

// ExecuteCommand executes a command on the cluster client.
func (cca *clusterClientAdapter) ExecuteCommand(ctx context.Context, cmd interface{}) error {
	// Use reflection to call Process method on the cluster client
	// This is a simplified implementation for the refactoring
	return nil // Mock implementation
}

// GetPushProcessor returns the cluster client's push notification processor.
func (cca *clusterClientAdapter) GetPushProcessor() interfaces.NotificationProcessor {
	// For cluster clients, return a mock processor since the actual implementation
	// would be more complex and distributed across nodes
	return &mockClusterPushProcessor{}
}

// DialToEndpoint creates a connection to the specified endpoint for cluster clients.
func (cca *clusterClientAdapter) DialToEndpoint(ctx context.Context, endpoint string) (interface{}, error) {
	// For cluster clients, this would need to handle cluster-specific connection logic
	// For now, return an error indicating this is not implemented for cluster clients
	return nil, fmt.Errorf("DialToEndpoint not implemented for cluster clients")
}

// mockClusterOptionsAdapter is a mock implementation for cluster options.
type mockClusterOptionsAdapter struct{}

// GetReadTimeout returns the read timeout.
func (mcoa *mockClusterOptionsAdapter) GetReadTimeout() time.Duration {
	return 5 * time.Second
}

// GetWriteTimeout returns the write timeout.
func (mcoa *mockClusterOptionsAdapter) GetWriteTimeout() time.Duration {
	return 3 * time.Second
}

// GetAddr returns the connection address.
func (mcoa *mockClusterOptionsAdapter) GetAddr() string {
	return "localhost:6379"
}

// IsTLSEnabled returns true if TLS is enabled.
func (mcoa *mockClusterOptionsAdapter) IsTLSEnabled() bool {
	return false
}

// GetProtocol returns the protocol version.
func (mcoa *mockClusterOptionsAdapter) GetProtocol() int {
	return 3
}

// GetPoolSize returns the connection pool size.
func (mcoa *mockClusterOptionsAdapter) GetPoolSize() int {
	return 50 // Default cluster pool size (5 * runtime.GOMAXPROCS(0))
}

// NewDialer returns a new dialer function for the connection.
func (mcoa *mockClusterOptionsAdapter) NewDialer() func(context.Context) (net.Conn, error) {
	return func(ctx context.Context) (net.Conn, error) {
		return nil, errors.New("mock cluster dialer")
	}
}

// pushProcessorAdapter adapts a push.NotificationProcessor to implement interfaces.NotificationProcessor.
type pushProcessorAdapter struct {
	processor push.NotificationProcessor
}

// RegisterHandler registers a handler for a specific push notification name.
func (ppa *pushProcessorAdapter) RegisterHandler(pushNotificationName string, handler interface{}, protected bool) error {
	if pushHandler, ok := handler.(push.NotificationHandler); ok {
		return ppa.processor.RegisterHandler(pushNotificationName, pushHandler, protected)
	}
	return errors.New("handler must implement push.NotificationHandler")
}

// UnregisterHandler removes a handler for a specific push notification name.
func (ppa *pushProcessorAdapter) UnregisterHandler(pushNotificationName string) error {
	return ppa.processor.UnregisterHandler(pushNotificationName)
}

// GetHandler returns the handler for a specific push notification name.
func (ppa *pushProcessorAdapter) GetHandler(pushNotificationName string) interface{} {
	return ppa.processor.GetHandler(pushNotificationName)
}

// mockClusterPushProcessor is a mock implementation for cluster push processors.
type mockClusterPushProcessor struct{}

// RegisterHandler registers a handler (mock implementation).
func (mcpp *mockClusterPushProcessor) RegisterHandler(pushNotificationName string, handler interface{}, protected bool) error {
	return nil
}

// UnregisterHandler removes a handler (mock implementation).
func (mcpp *mockClusterPushProcessor) UnregisterHandler(pushNotificationName string) error {
	return nil
}

// GetHandler returns the handler (mock implementation).
func (mcpp *mockClusterPushProcessor) GetHandler(pushNotificationName string) interface{} {
	return nil
}
