// Package interfaces provides shared interfaces used by both the main redis package
// and the hitless upgrade package to avoid circular dependencies.
package interfaces

import (
	"context"
	"net"
	"time"
)

// Forward declaration to avoid circular imports
type NotificationProcessor interface {
	RegisterHandler(pushNotificationName string, handler interface{}, protected bool) error
	UnregisterHandler(pushNotificationName string) error
	GetHandler(pushNotificationName string) interface{}
}

// ClientInterface defines the interface that clients must implement for hitless upgrades.
type ClientInterface interface {
	// GetOptions returns the client options.
	GetOptions() OptionsInterface

	// GetPushProcessor returns the client's push notification processor.
	GetPushProcessor() NotificationProcessor
}

// OptionsInterface defines the interface for client options.
type OptionsInterface interface {
	// GetReadTimeout returns the read timeout.
	GetReadTimeout() time.Duration

	// GetWriteTimeout returns the write timeout.
	GetWriteTimeout() time.Duration

	// GetNetwork returns the network type.
	GetNetwork() string

	// GetAddr returns the connection address.
	GetAddr() string

	// IsTLSEnabled returns true if TLS is enabled.
	IsTLSEnabled() bool

	// GetProtocol returns the protocol version.
	GetProtocol() int

	// GetPoolSize returns the connection pool size.
	GetPoolSize() int

	// NewDialer returns a new dialer function for the connection.
	NewDialer() func(context.Context) (net.Conn, error)
}

// ConnectionWithRelaxedTimeout defines the interface for connections that support relaxed timeout adjustment.
// This is used by the hitless upgrade system for per-connection timeout management.
type ConnectionWithRelaxedTimeout interface {
	// SetRelaxedTimeout sets relaxed timeouts for this connection during hitless upgrades.
	// These timeouts remain active until explicitly cleared.
	SetRelaxedTimeout(readTimeout, writeTimeout time.Duration)

	// SetRelaxedTimeoutWithDeadline sets relaxed timeouts with an expiration deadline.
	// After the deadline, timeouts automatically revert to normal values.
	SetRelaxedTimeoutWithDeadline(readTimeout, writeTimeout time.Duration, deadline time.Time)

	// ClearRelaxedTimeout clears relaxed timeouts for this connection.
	ClearRelaxedTimeout()
}
