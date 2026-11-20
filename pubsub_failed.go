package redis

import (
	"context"
	"sync/atomic"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/pubsub"
)

// failedPubSuber is the handle of a PubSub created in a failed state
// (e.g. Ring subscribe on an empty ring, issue #3761): every operation
// returns err, Receive reports it until Close, and Channel returns a
// closed channel, instead of a panic at construction.
type failedPubSuber struct {
	err    error
	closed atomic.Bool
}

var _ pubsub.PubSuber = (*failedPubSuber)(nil)

func newFailedPubSub(err error) *PubSub {
	return &PubSub{handle: &failedPubSuber{err: err}}
}

func (f *failedPubSuber) opErr() error {
	if f.closed.Load() {
		return pool.ErrClosed
	}
	return f.err
}

// Events yields err once per call (ReceiveTimeout reads a fresh stream
// each time), or a closed stream after Close.
func (f *failedPubSuber) Events() <-chan any {
	ch := make(chan any, 1)
	if f.closed.Load() {
		close(ch)
		return ch
	}
	ch <- f.err
	return ch
}

func (f *failedPubSuber) Channel(...pubsub.ChannelOption) <-chan *Message {
	internal.Logger.Printf(context.Background(),
		"redis: Channel returning closed channel due to sticky error: %s", f.err)
	ch := make(chan *Message)
	close(ch)
	return ch
}

func (f *failedPubSuber) ChannelWithSubscriptions(...pubsub.ChannelOption) <-chan any {
	internal.Logger.Printf(context.Background(),
		"redis: ChannelWithSubscriptions returning closed channel due to sticky error: %s", f.err)
	ch := make(chan any)
	close(ch)
	return ch
}

func (f *failedPubSuber) Subscribe(context.Context, ...string) (pubsub.PubSuber, error) {
	return f, f.opErr()
}

func (f *failedPubSuber) PSubscribe(context.Context, ...string) (pubsub.PubSuber, error) {
	return f, f.opErr()
}

func (f *failedPubSuber) SSubscribe(context.Context, ...string) (pubsub.PubSuber, error) {
	return f, f.opErr()
}

func (f *failedPubSuber) Unsubscribe(context.Context, ...string) error  { return f.opErr() }
func (f *failedPubSuber) PUnsubscribe(context.Context, ...string) error { return f.opErr() }
func (f *failedPubSuber) SUnsubscribe(context.Context, ...string) error { return f.opErr() }

func (f *failedPubSuber) Subscriptions() (channels, patterns, schannels []string) {
	return nil, nil, nil
}

func (f *failedPubSuber) Ping(context.Context, ...string) error       { return f.opErr() }
func (f *failedPubSuber) PingSilent(context.Context, ...string) error { return f.opErr() }
func (f *failedPubSuber) ClientSetName(context.Context, string) error { return f.opErr() }
func (f *failedPubSuber) DeliverError(error)                          {}

func (f *failedPubSuber) Close() error {
	if !f.closed.CompareAndSwap(false, true) {
		return pool.ErrClosed
	}
	return nil
}
