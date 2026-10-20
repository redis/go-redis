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
	// Writes run under the manager lock, so they are always bounded:
	// <= 0 means fallbackWriteTimeout, never "no deadline".
	WriteTimeout time.Duration

	// MinRetryBackoff and ReconnectMaxBackoff bound the read loop's
	// exponential reconnect backoff between dials.
	MinRetryBackoff     time.Duration
	ReconnectMaxBackoff time.Duration

	// DialRetries is the number of extra attempts a dial makes after a
	// failed one before the failure is reported; <= 0 means none.
	DialRetries int
	// DialRetryBackoff is the pause between a dial's attempts; attempt is
	// 0-based (0: the pause after the first failed attempt), and a
	// negative result means none. The root package passes the client's
	// dial retry policy (Options.DialerRetryBackoff / DialerRetryTimeout),
	// so the shared connection is dialed like the pool's. nil falls back
	// to the reconnect backoff above.
	DialRetryBackoff func(attempt int) time.Duration

	// HealthCheckInterval is how long the shared connection may stay
	// silent before a health-check PING; <= 0 disables the health
	// check.
	HealthCheckInterval time.Duration
	// SubscribeRetryInterval is the cadence at which subscribes the
	// server rejected with an error reply are re-sent; <= 0 disables
	// the retries (the root package defaults it via
	// Options.PubSubSubscribeRetryInterval).
	SubscribeRetryInterval time.Duration
	// PingTimeout bounds the health-check PING: the write, and the wait
	// for the server's reply (any inbound frame) that follows it; no
	// reply in time drops the connection. <= 0 removes both the probe's
	// deadline (the write still honours WriteTimeout) and the reply
	// requirement.
	PingTimeout time.Duration
	// ReconnectTimeout bounds each dial attempt of the shared
	// connection; <= 0 leaves the dial function's own bound.
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
