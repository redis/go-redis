package pubsub

import "time"

// Config carries the settings the Manager reads from the client's
// Options. The root redis package applies option defaulting
// (Options.init) before building a Config, so every field arrives
// resolved: the manager applies no defaults of its own.
type Config struct {
	// Addr is the target every (re)connect dials; a handoff can redirect
	// the manager by rewriting it.
	Addr string

	// WriteTimeout bounds every command write on the shared connection.
	WriteTimeout time.Duration

	// MinRetryBackoff and ReconnectMaxBackoff bound the read loop's
	// exponential reconnect backoff.
	MinRetryBackoff     time.Duration
	ReconnectMaxBackoff time.Duration

	// HealthCheckInterval is how long the shared connection may stay
	// silent before a health-check PING; <= 0 disables the health
	// check.
	HealthCheckInterval time.Duration
	// PendingResyncFallback is the cadence of the dedicated Pending
	// reconciliation loop, which re-sends subscribes that were rejected
	// or lost their confirmation on a healthy connection. It runs even
	// with the health check disabled; <= 0 disables the loop (the root
	// package defaults it via Options.PubSubPendingResyncFallback).
	PendingResyncFallback time.Duration
	// PingTimeout bounds the health-check PING write.
	PingTimeout time.Duration
	// ReconnectTimeout bounds the health checker's re-dial and
	// subscription replay after a failed ping.
	ReconnectTimeout time.Duration

	// SendTimeout bounds each consumer-view send (see pump); <= 0 (the
	// default) blocks instead. Mutable manager-wide via
	// WithChannelSendTimeout; pumps snapshot it at start.
	SendTimeout time.Duration

	// LogInterval rate-limits recurring warning logs.
	LogInterval time.Duration

	// ChanSize is the buffer size of a handle's events stream and of the
	// consumer channels. Mutable manager-wide via WithChannelSize; it
	// applies to handles and views created after the update.
	ChanSize int
}
