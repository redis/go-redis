package redis

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/internal/pubsub"
)

// The pub/sub engine lives in internal/pubsub; the types below are
// re-exported as aliases so they keep their redis.* identity.
type (
	// Message received as result of a PUBLISH command issued by another
	// client.
	Message = pubsub.Message
	// Subscription received after a successful subscription to a
	// channel.
	Subscription = pubsub.Subscription
	// Pong received as result of a PING command issued by another
	// client.
	Pong = pubsub.Pong

	ChannelOption = pubsub.ChannelOption
)

// pubSubClienter is the hook family a standalone pub/sub manager is
// built from (see newPubSubManager). baseClient implements every hook;
// a client that needs different behavior embeds it and overrides only
// what differs (see failoverPubSubHooks). Declared here (not in
// internal/pubsub) because interfaces with unexported methods are only
// satisfiable within the declaring package. ClusterClient runs the
// cluster stub (pubsub.ClusterManager) until the cluster manager is
// ported.
type pubSubClienter interface {
	pubsubConnect(ctx context.Context, addr string) (*pool.Conn, error)
	pubsubDisconnect(cn *pool.Conn) error
	pubsubProcessPush(ctx context.Context, cn *pool.Conn, rd *proto.Reader) error
	pubsubShouldReplaceConn(cn *pool.Conn) bool
	pubsubResolveAddress(prev *pool.Conn, current string) string
}

var (
	_ pubSubClienter = (*Client)(nil)
	_ pubSubClienter = (*SentinelClient)(nil)
	// The failover client is a *Client whose manager is built from these
	// hooks instead.
	_ pubSubClienter = (*failoverPubSubHooks)(nil)
)

// newPubSubManager returns a pub/sub manager whose single connection is
// dialed directly, outside any pool, through h's hooks. The hooks are
// read through the interface, so overrides on a type embedding
// baseClient take effect. opt must be the client's defaulted Options
// (a private copy: the manager keeps it). With a custom Dialer (e.g. the
// failover client's master-resolving dialer) every reconnect
// re-resolves the target through it.
func newPubSubManager(h pubSubClienter, opt *Options) *pubsub.Manager {
	return pubsub.NewManager(
		pubSubConfigFromOptions(opt),
		h.pubsubConnect,
		h.pubsubDisconnect,
		h.pubsubProcessPush,
		func(err error, allowTimeout bool) bool {
			return isBadConn(err, allowTimeout, opt.Addr)
		},
		nil,
		h.pubsubShouldReplaceConn,
		h.pubsubResolveAddress,
	)
}

// pubSubConfigFromOptions maps client Options (already defaulted by
// Options.init) onto the internal pub/sub manager Config.
func pubSubConfigFromOptions(opt *Options) pubsub.Config {
	return pubsub.Config{
		Addr:                   opt.Addr,
		WriteTimeout:           opt.WriteTimeout,
		MinRetryBackoff:        opt.MinRetryBackoff,
		ReconnectMaxBackoff:    opt.PubSubReconnectMaxBackoff,
		DialRetries:            pubSubDialRetries(opt.DialerRetries),
		DialRetryBackoff:       pubSubDialRetryBackoff(opt),
		HealthCheckInterval:    opt.PubSubHealthCheckInterval,
		SubscribeRetryInterval: opt.PubSubSubscribeRetryInterval,
		PingTimeout:            opt.PubSubPingTimeout,
		ReconnectTimeout:       opt.PubSubReconnectTimeout,
		LogInterval:            opt.PubSubLogInterval,
		ChanSize:               opt.PubSubChanSize,
	}
}

// pubSubDialRetries converts Options.DialerRetries — the total number
// of dial attempts the connection pool makes — into the engine's
// Config.DialRetries, the number of attempts after the first.
func pubSubDialRetries(dialerRetries int) int {
	if dialerRetries <= 1 {
		return 0
	}
	return dialerRetries - 1
}

// pubSubDialRetryBackoff is the pause between the shared connection's
// dial attempts: the client's dial retry policy, as the connection pool
// applies it — DialerRetryBackoff when set, else a constant
// DialerRetryTimeout (100ms when unset).
func pubSubDialRetryBackoff(opt *Options) func(attempt int) time.Duration {
	if opt.DialerRetryBackoff != nil {
		return opt.DialerRetryBackoff
	}
	base := opt.DialerRetryTimeout
	if base <= 0 {
		base = 100 * time.Millisecond
	}
	return DialRetryBackoffConstant(base)
}

//------------------------------------------------------------------------------

// PubSub implements Pub/Sub commands as described in
// https://redis.io/docs/latest/develop/pubsub. Message receiving is NOT safe
// for concurrent use by multiple goroutines.
//
// PubSub automatically reconnects to Redis Server and resubscribes
// to the channels in case of network errors.
//
// PubSub is a thin wrapper over an engine subscriber handle
// (internal/pubsub): all PubSub instances of one client multiplex over
// the client's shared pub/sub connection. Consequences of the shared
// connection: ClientSetName names it for every subscriber, and the
// subscription counts the server reports are connection-wide, not
// per-PubSub.
//
// ClusterClient pub/sub is not implemented on this engine yet: its
// PubSub operations return pubsub.ErrClusterNotImplemented.
type PubSub struct {
	handle pubsub.PubSuber
}

// newPubSub returns a PubSub wrapping a fresh subscriber handle of the
// given manager. The handle's delivery stream exists from creation, so
// nothing — including the first subscribe confirmation — can be missed
// or dropped before the consumer attaches.
func newPubSub(manager pubsub.PubSubManagerer) *PubSub {
	return &PubSub{handle: manager.NewHandle()}
}

// reportInitialErr delivers the error of the subscribe a client's
// Subscribe/PSubscribe/SSubscribe ran on the PubSub it hands out without
// that error, so the first Receive reports it instead of blocking. A
// failed dial keeps the registration, so the reconnect still retries it.
func (c *PubSub) reportInitialErr(err error) {
	if err != nil {
		c.handle.DeliverError(err)
	}
}

func (c *PubSub) String() string {
	channels, patterns, schannels := c.handle.Subscriptions()
	names := append(channels, patterns...)
	names = append(names, schannels...)
	return fmt.Sprintf("PubSub(%s)", strings.Join(names, ", "))
}

// Subscribe the client to the specified channels.
func (c *PubSub) Subscribe(ctx context.Context, channels ...string) error {
	if len(channels) == 0 {
		return nil
	}
	_, err := c.handle.Subscribe(ctx, channels...)
	return err
}

// PSubscribe the client to the given patterns. It is a no-op if there
// are no patterns.
func (c *PubSub) PSubscribe(ctx context.Context, patterns ...string) error {
	if len(patterns) == 0 {
		return nil
	}
	_, err := c.handle.PSubscribe(ctx, patterns...)
	return err
}

// SSubscribe subscribes the client to the specified shard channels. It
// is a no-op if there are no channels.
func (c *PubSub) SSubscribe(ctx context.Context, channels ...string) error {
	if len(channels) == 0 {
		return nil
	}
	_, err := c.handle.SSubscribe(ctx, channels...)
	return err
}

// Unsubscribe the client from the given channels, or from all of them
// if none is given. UNSUBSCRIBE is sent server-side only for channels
// no other subscriber of the client uses.
func (c *PubSub) Unsubscribe(ctx context.Context, channels ...string) error {
	return c.handle.Unsubscribe(ctx, channels...)
}

// PUnsubscribe the client from the given patterns, or from all of them
// if none is given.
func (c *PubSub) PUnsubscribe(ctx context.Context, patterns ...string) error {
	return c.handle.PUnsubscribe(ctx, patterns...)
}

// SUnsubscribe unsubscribes the client from the given shard channels,
// or from all of them if none is given.
func (c *PubSub) SUnsubscribe(ctx context.Context, channels ...string) error {
	return c.handle.SUnsubscribe(ctx, channels...)
}

// Ping sends a PING on the shared connection (dialing it if needed);
// the pong is delivered through Receive.
func (c *PubSub) Ping(ctx context.Context, payload ...string) error {
	return c.handle.Ping(ctx, payload...)
}

// ClientSetName assigns a name to the pub/sub connection using CLIENT
// SETNAME. The name is visible in CLIENT LIST output and is useful for
// debugging. The connection is shared by every subscriber of the
// client, so the name applies to all of them; the +OK reply is drained
// through Receive as a Pong.
func (c *PubSub) ClientSetName(ctx context.Context, name string) error {
	return c.handle.ClientSetName(ctx, name)
}

// Close releases the PubSub's subscriptions (server-side only for names
// no other subscriber of the client uses) and ends its delivery
// channels. Subsequent calls return pool.ErrClosed.
func (c *PubSub) Close() error {
	return c.handle.Close()
}

//------------------------------------------------------------------------------

// pubSubTimeoutError is the net.Error ReceiveTimeout returns when the
// timeout expires.
type pubSubTimeoutError struct{}

func (pubSubTimeoutError) Error() string   { return "redis: pubsub: receive timeout" }
func (pubSubTimeoutError) Timeout() bool   { return true }
func (pubSubTimeoutError) Temporary() bool { return true }

// ReceiveTimeout acts like Receive but returns an error if a message
// is not received in time. This is a low-level API and in most cases
// Channel should be used instead. A zero timeout blocks indefinitely.
func (c *PubSub) ReceiveTimeout(ctx context.Context, timeout time.Duration) (interface{}, error) {
	var timeoutCh <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		timeoutCh = timer.C
	}

	select {
	case ev, ok := <-c.handle.Events():
		if !ok {
			return nil, pool.ErrClosed
		}
		// An error reply on the shared connection (e.g. NOPERM) is
		// delivered to every subscriber and surfaces through Receive.
		if err, ok := ev.(error); ok {
			return nil, err
		}
		return ev, nil
	case <-timeoutCh:
		return nil, pubSubTimeoutError{}
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Receive returns a message as a Subscription, Message, Pong, or an error.
// See PubSub example for details. This is a low-level API and in most cases
// Channel should be used instead.
// This method blocks until a message is received or an error occurs.
// It may return early with an error if the context is canceled or the
// PubSub is closed. Connection failures are handled internally: the
// shared connection reconnects and resubscribes on its own. The one
// exception is the subscribe a client's Subscribe/PSubscribe/SSubscribe
// runs: its failure (e.g. an unreachable address or rejected
// credentials) is returned by the first Receive, while the reconnect
// keeps retrying it.
func (c *PubSub) Receive(ctx context.Context) (interface{}, error) {
	return c.ReceiveTimeout(ctx, 0)
}

// ReceiveMessage returns a Message or error ignoring Subscription and Pong
// messages. This is low-level API and in most cases Channel should be used
// instead.
func (c *PubSub) ReceiveMessage(ctx context.Context) (*Message, error) {
	for {
		msgi, err := c.Receive(ctx)
		if err != nil {
			return nil, err
		}

		switch msg := msgi.(type) {
		case *Subscription:
			// Ignore.
		case *Pong:
			// Ignore.
		case *Message:
			return msg, nil
		default:
			return nil, fmt.Errorf("redis: unknown message: %T", msg)
		}
	}
}

// Channel returns a Go channel for concurrently receiving messages.
// The channel is closed together with the PubSub. Messages that cannot
// be buffered are dropped with throttled logging.
// Receive* APIs can not be used after channel is created.
func (c *PubSub) Channel(opts ...ChannelOption) <-chan *Message {
	return c.handle.Channel(opts...)
}

// ChannelSize is like Channel, but creates a Go channel
// with specified buffer size.
//
// Deprecated: use Channel(WithChannelSize(size)), remove in v9.
func (c *PubSub) ChannelSize(size int) <-chan *Message {
	return c.Channel(WithChannelSize(size))
}

// ChannelWithSubscriptions is like Channel, but message type can be either
// *Subscription or *Message. Subscription messages can be used to detect
// reconnections.
//
// ChannelWithSubscriptions can not be used together with Channel or ChannelSize.
func (c *PubSub) ChannelWithSubscriptions(opts ...ChannelOption) <-chan interface{} {
	return c.handle.ChannelWithSubscriptions(opts...)
}

//------------------------------------------------------------------------------

// The WithChannel* options tune the client's shared pub/sub manager:
// the connection — and hence the configuration — is shared by every
// subscriber of the client, so an option applied through one PubSub's
// Channel call is client-wide. Updates affect subscribers and channels
// created after them; a channel that already exists keeps the settings
// it was created with.

// WithChannelSize sets the buffer size used for pub/sub delivery
// streams and the Go channels returned by Channel /
// ChannelWithSubscriptions (client-wide, see above). Defaults to
// Options.PubSubChanSize.
func WithChannelSize(size int) ChannelOption {
	return func(c pubsub.PubSubConfiger) {
		c.UpdateChannelSize(size)
	}
}

// WithChannelHealthCheckInterval sets how long the client's shared
// pub/sub connection may stay silent before a health-check PING
// (client-wide, see above). It applies immediately: a positive value
// re-arms the health checker — reviving a disabled one — and zero or a
// negative value disables it. Defaults to
// Options.PubSubHealthCheckInterval.
func WithChannelHealthCheckInterval(d time.Duration) ChannelOption {
	return func(c pubsub.PubSubConfiger) {
		c.UpdateHealthCheckInterval(d)
	}
}

// WithChannelSendTimeout bounds each send into the channel returned by
// Channel/ChannelWithSubscriptions: a message the consumer doesn't take
// within d is dropped with throttled logging. Zero or negative (the
// default) blocks instead, letting backpressure land in the engine's
// bounded delivery buffer (see Options.PubSubChanSize), where overflow
// drops are counted and logged. Client-wide (see above) and captured
// when a channel's pump starts.
func WithChannelSendTimeout(d time.Duration) ChannelOption {
	return func(c pubsub.PubSubConfiger) {
		c.UpdateSendTimeout(d)
	}
}

// WithChannelPingTimeout sets the timeout of the health-check PING on
// the client's shared pub/sub connection (client-wide, see above).
// Defaults to Options.PubSubPingTimeout.
func WithChannelPingTimeout(d time.Duration) ChannelOption {
	return func(c pubsub.PubSubConfiger) {
		c.UpdatePingTimeout(d)
	}
}

// WithChannelReconnectTimeout bounds each re-dial attempt of the
// client's shared pub/sub connection (client-wide, see above). Defaults
// to Options.PubSubReconnectTimeout.
func WithChannelReconnectTimeout(d time.Duration) ChannelOption {
	return func(c pubsub.PubSubConfiger) {
		c.UpdateReconnectTimeout(d)
	}
}
