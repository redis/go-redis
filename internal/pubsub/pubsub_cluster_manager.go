package pubsub

import (
	"context"
	"sync"

	"github.com/redis/go-redis/v9/internal/pool"
)

// ErrClusterNotImplemented is what every operation of the cluster stub
// returns: the cluster client is wired to the engine's manager
// interface, but the cluster manager itself (a broadcast connection plus
// per-node shard-channel connections) is not implemented yet.
const ErrClusterNotImplemented pubSubError = "redis: pubsub: not implemented for the cluster client yet"

// ClusterManager is the placeholder cluster pub/sub manager. It holds
// the place the real cluster manager will take in ClusterClient, so the
// client-side wiring (construction, Close, NewHandle) is already in its
// final shape.
//
// The stub fails loudly rather than idly: the client's Subscribe
// discards the subscribe error, so each handle's Events stream carries
// ErrClusterNotImplemented once and is then closed — Receive returns
// the error, then pool.ErrClosed — and its Channel /
// ChannelWithSubscriptions channels are born closed, so consumer loops
// end at once instead of blocking forever. Every call returns
// ErrClusterNotImplemented.
type ClusterManager struct {
	mu     sync.Mutex
	closed bool
}

var _ PubSubManagerer = (*ClusterManager)(nil)

// NewClusterManager returns the cluster pub/sub stub.
func NewClusterManager() *ClusterManager {
	return &ClusterManager{}
}

// NewHandle returns a stub handle whose streams are already drained of
// everything but the not-implemented error (see ClusterManager).
func (m *ClusterManager) NewHandle() PubSuber {
	events := make(chan any, 1)
	events <- error(ErrClusterNotImplemented)
	close(events)

	msgCh := make(chan *Message)
	close(msgCh)
	allCh := make(chan any)
	close(allCh)

	return &clusterStubHandle{m: m, events: events, msgCh: msgCh, allCh: allCh}
}

func (m *ClusterManager) Unsubscribe(context.Context, ...string) error {
	return ErrClusterNotImplemented
}

func (m *ClusterManager) PUnsubscribe(context.Context, ...string) error {
	return ErrClusterNotImplemented
}

func (m *ClusterManager) SUnsubscribe(context.Context, ...string) error {
	return ErrClusterNotImplemented
}

// Close marks the manager closed. Subsequent calls return
// pool.ErrClosed.
func (m *ClusterManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return pool.ErrClosed
	}
	m.closed = true
	return nil
}

// clusterStubHandle is the subscriber view ClusterManager hands out.
// Its streams are closed from birth; closed is guarded by the manager's
// mu.
type clusterStubHandle struct {
	m      *ClusterManager
	closed bool

	events <-chan any
	msgCh  <-chan *Message
	allCh  <-chan any
}

var _ PubSuber = (*clusterStubHandle)(nil)

func (h *clusterStubHandle) Events() <-chan any { return h.events }

func (h *clusterStubHandle) Channel(...ChannelOption) <-chan *Message { return h.msgCh }

func (h *clusterStubHandle) ChannelWithSubscriptions(...ChannelOption) <-chan any {
	return h.allCh
}

func (h *clusterStubHandle) Subscribe(context.Context, ...string) (PubSuber, error) {
	return h, ErrClusterNotImplemented
}

func (h *clusterStubHandle) PSubscribe(context.Context, ...string) (PubSuber, error) {
	return h, ErrClusterNotImplemented
}

func (h *clusterStubHandle) SSubscribe(context.Context, ...string) (PubSuber, error) {
	return h, ErrClusterNotImplemented
}

func (h *clusterStubHandle) Unsubscribe(context.Context, ...string) error {
	return ErrClusterNotImplemented
}

func (h *clusterStubHandle) PUnsubscribe(context.Context, ...string) error {
	return ErrClusterNotImplemented
}

func (h *clusterStubHandle) SUnsubscribe(context.Context, ...string) error {
	return ErrClusterNotImplemented
}

func (h *clusterStubHandle) Subscriptions() (channels, patterns, schannels []string) {
	return nil, nil, nil
}

func (h *clusterStubHandle) Ping(context.Context, ...string) error {
	return ErrClusterNotImplemented
}

func (h *clusterStubHandle) PingSilent(context.Context, ...string) error {
	return ErrClusterNotImplemented
}

func (h *clusterStubHandle) ClientSetName(context.Context, string) error {
	return ErrClusterNotImplemented
}

// Close marks the handle closed. Subsequent calls return
// pool.ErrClosed, except after the manager itself closed (closing a
// subscriber of a closed client is a nil no-op), like handle.Close.
func (h *clusterStubHandle) Close() error {
	h.m.mu.Lock()
	defer h.m.mu.Unlock()
	if h.closed {
		if h.m.closed {
			return nil
		}
		return pool.ErrClosed
	}
	h.closed = true
	return nil
}
