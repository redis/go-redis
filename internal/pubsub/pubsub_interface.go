package pubsub

import (
	"context"
	"time"
)

// ChannelOption tunes the client's shared pub/sub manager when passed
// to Channel/ChannelWithSubscriptions — see PubSubConfiger.
type ChannelOption func(c PubSubConfiger)

// PubSuber is one subscriber's view of a pub/sub subscription,
// implemented by handle (standalone) and clusterShardHandle (cluster).
type PubSuber interface {
	// Events is the raw delivery stream — *Message, *Subscription,
	// *Pong and error (attribution is connection-wide) — existing from
	// handle creation and closed with it. Reading it directly after
	// starting Channel/ChannelWithSubscriptions competes with the pump.
	Events() <-chan any
	// Channel returns a channel carrying only messages, fed by a pump
	// over Events started on the first call.
	Channel(opt ...ChannelOption) <-chan *Message
	// ChannelWithSubscriptions is like Channel but also carries
	// *Subscription confirmations. It cannot be combined with Channel.
	ChannelWithSubscriptions(opt ...ChannelOption) <-chan any

	Subscribe(ctx context.Context, channels ...string) (PubSuber, error)
	PSubscribe(ctx context.Context, patterns ...string) (PubSuber, error)
	SSubscribe(ctx context.Context, channels ...string) (PubSuber, error)
	Unsubscribe(ctx context.Context, channels ...string) error
	PUnsubscribe(ctx context.Context, patterns ...string) error
	SUnsubscribe(ctx context.Context, channels ...string) error

	// Subscriptions is a snapshot of the names the subscriber currently
	// owns, one slice per namespace.
	Subscriptions() (channels, patterns, schannels []string)

	// Ping pings the subscriber's connection(s); at most one pong per
	// call surfaces on Events, however many connections were pinged.
	// Like messages, a pong is dropped if the subscriber's buffer is
	// full (see PubSubChanSize), so callers awaiting one should apply
	// a timeout.
	Ping(ctx context.Context, payload ...string) error
	// PingSilent is Ping without the Events pong.
	PingSilent(ctx context.Context, payload ...string) error
	ClientSetName(ctx context.Context, name string) error
	Close() error
}

// PubSubConfiger is the configuration surface a ChannelOption acts on.
// Updates are manager-wide — they apply to every subscriber of the
// client — and take effect for handles and consumer views created
// after them, never for already-created channels.
type PubSubConfiger interface {
	UpdateChannelSize(newSize int)
	UpdatePingTimeout(newTimeout time.Duration)
	UpdateSendTimeout(newTimeout time.Duration)
	UpdateHealthCheckInterval(newInterval time.Duration)
	UpdateReconnectTimeout(newTimeout time.Duration)
}

var (
	_ PubSuber       = (*handle)(nil)
	_ PubSubConfiger = (*handle)(nil)
)

// PubSubManagerer is what the clients expect from a pub/sub manager:
// NewHandle hands out the subscriber view the root PubSub wraps, and
// the unsubscribe methods act across every subscriber of the client.
type PubSubManagerer interface {
	NewHandle() PubSuber

	Unsubscribe(ctx context.Context, channels ...string) error
	PUnsubscribe(ctx context.Context, patterns ...string) error
	SUnsubscribe(ctx context.Context, channels ...string) error

	Close() error
}

var _ PubSubManagerer = (*Manager)(nil)
