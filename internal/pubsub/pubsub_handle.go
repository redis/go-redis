package pubsub

import (
	"context"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/pool"
)

// handle is one subscriber's view of the manager's shared connection:
// it owns a set of subscription names and the events stream the manager
// fans matching deliveries into. All fields are guarded by the
// manager's mu unless noted.
type handle struct {
	m *Manager

	// mu guards only the consumer-view fields (pump, msgCh, allCh); it
	// is never held together with the manager's mu.
	mu sync.Mutex

	// Names this handle is subscribed to, one set per namespace.
	channels  map[string]struct{}
	patterns  map[string]struct{}
	schannels map[string]struct{}

	// confirmed holds the names whose subscribe confirmation the handle
	// was delivered, with no unsubscribe confirmation since: the running
	// Count its confirmations carry, as a dedicated connection's server
	// would report it (see deliverSubscriptionLocked). It also gates
	// message delivery: an owned name not in it is still joining, and
	// gets no messages until its confirmation (see deliverMessageLocked).
	confirmed map[confirmKey]struct{}
	// nConfirmed counts the confirmed channels and patterns,
	// nConfirmedShard the confirmed shard channels: Redis counts the two
	// apart.
	nConfirmed, nConfirmedShard int

	// closing is set while Close detaches the handle: the confirmations
	// its own unsubscribes would produce are not delivered, so nothing
	// stale is left on events for a Receive after Close to return.
	closing bool

	// events is closed by closeLocked, or by the forwarder when one is
	// running (see backlog); done unblocks a pump or forwarder parked on
	// an abandoned consumer channel.
	events chan any
	done   chan struct{}

	// backlog holds the events that did not fit in events, in order,
	// so the shared read loop never blocks on a slow handle and never
	// drops a confirmation, pong or error. While it is non-empty a
	// forwarder goroutine moves it into events and every new event goes
	// behind it. Only messages count against backlogLimit (bytes); a
	// message over the limit is dropped. qmu guards the backlog fields
	// and qclosed; it is a leaf lock, taken under the manager's mu.
	qmu          sync.Mutex
	backlog      []any
	backlogBytes int
	backlogLimit int
	forwarding   bool
	qclosed      bool

	// The lazily started consumer view over events (guarded by mu).
	pump  pumpMode
	msgCh chan *Message
	allCh chan any

	// dropLog paces the slow-subscriber drop line: one per LogInterval,
	// the drops in between counted on it.
	dropLog internal.Logging

	closed bool
}

// Events returns the raw delivery stream (see PubSuber.Events).
func (h *handle) Events() <-chan any {
	return h.events
}

// Channel returns a Go channel for concurrently receiving messages,
// created on the first call and closed together with the handle.
// Options run before h.mu is taken (h.mu and the manager's mu never
// nest). It cannot be combined with ChannelWithSubscriptions.
func (h *handle) Channel(opt ...ChannelOption) <-chan *Message {
	for _, o := range opt {
		o(h)
	}
	size, sendTimeout := h.m.consumerSettings()

	h.mu.Lock()
	defer h.mu.Unlock()

	switch h.pump {
	case pumpAll:
		panic("redis: Channel can't be called after ChannelWithSubscriptions")
	case pumpNone:
		h.pump = pumpMessages
		h.msgCh = make(chan *Message, size)
		go h.pumpMessages(sendTimeout)
	}
	return h.msgCh
}

// ChannelWithSubscriptions is like Channel, but the returned channel
// also carries *Subscription confirmations (useful to observe
// resubscriptions after a reconnect; pongs and error replies are
// filtered out), so its element type is any. It cannot be used together
// with Channel.
func (h *handle) ChannelWithSubscriptions(opt ...ChannelOption) <-chan any {
	for _, o := range opt {
		o(h)
	}
	size, sendTimeout := h.m.consumerSettings()

	h.mu.Lock()
	defer h.mu.Unlock()

	switch h.pump {
	case pumpMessages:
		panic("redis: ChannelWithSubscriptions can't be called after Channel")
	case pumpNone:
		h.pump = pumpAll
		h.allCh = make(chan any, size)
		go h.pumpAll(sendTimeout)
	}
	return h.allCh
}

func (h *handle) pumpMessages(sendTimeout time.Duration) {
	pump(h.events, h.done, h.msgCh, sendTimeout, h.m.cfg.LogInterval, messageFilter)
}

func (h *handle) pumpAll(sendTimeout time.Duration) {
	pump(h.events, h.done, h.allCh, sendTimeout, h.m.cfg.LogInterval, allEventsFilter)
}

// The PubSubConfiger methods update the manager-wide config, which
// every subscriber of the client shares; updates affect handles and
// consumer views created after them.

// UpdateChannelSize sets the manager-wide delivery buffer size, skipped
// once this handle's own view started.
func (h *handle) UpdateChannelSize(newSize int) {
	if h.pumpStarted() {
		return
	}
	h.m.updateChannelSize(newSize)
}

// UpdateSendTimeout sets the manager-wide consumer send timeout,
// skipped once this handle's own view started.
func (h *handle) UpdateSendTimeout(newTimeout time.Duration) {
	if h.pumpStarted() {
		return
	}
	h.m.updateSendTimeout(newTimeout)
}

// UpdatePingTimeout sets the manager-wide health-check ping timeout.
func (h *handle) UpdatePingTimeout(newTimeout time.Duration) {
	h.m.updatePingTimeout(newTimeout)
}

// UpdateHealthCheckInterval sets the manager-wide health-check
// interval, applied immediately; a positive value revives a disabled
// check and <= 0 disables a running one.
func (h *handle) UpdateHealthCheckInterval(newInterval time.Duration) {
	h.m.updateHealthCheckInterval(newInterval)
}

// UpdateReconnectTimeout sets the manager-wide timeout for the health
// checker's re-dial after a failed ping.
func (h *handle) UpdateReconnectTimeout(newTimeout time.Duration) {
	h.m.updateReconnectTimeout(newTimeout)
}

// pumpStarted reports whether a consumer view is already running.
func (h *handle) pumpStarted() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.pump != pumpNone
}

// deliverLocked sends an event into the handle's stream without
// blocking: a blocking send would stall the shared fan-out for every
// handle. An event that does not fit goes to the backlog (see
// handle.backlog). Callers must hold the manager lock.
func (h *handle) deliverLocked(ev any) {
	if h.closed {
		return
	}
	h.qmu.Lock()
	defer h.qmu.Unlock()
	if !h.forwarding {
		select {
		case h.events <- ev:
			return
		default:
		}
	}
	if msg, ok := ev.(*Message); ok {
		size := messageSize(msg)
		if h.backlogBytes+size > h.backlogLimit {
			h.dropLog.Printf(context.TODO(),
				"redis: pubsub: dropped a message to a slow subscriber (%d bytes queued)",
				h.backlogBytes)
			return
		}
		h.backlogBytes += size
	}
	h.backlog = append(h.backlog, ev)
	if !h.forwarding {
		h.forwarding = true
		go h.forward()
	}
}

// forward moves the backlog into events, in order, and exits once the
// backlog is empty. After the handle closed it drops the backlog and
// closes events itself, so no send can race with the close.
func (h *handle) forward() {
	for {
		h.qmu.Lock()
		if h.qclosed || len(h.backlog) == 0 {
			h.forwarding = false
			h.backlog, h.backlogBytes = nil, 0
			if h.qclosed {
				close(h.events)
			}
			h.qmu.Unlock()
			return
		}
		ev := h.backlog[0]
		h.qmu.Unlock()

		select {
		case h.events <- ev:
		case <-h.done:
			continue // closed: the next pass closes events
		}

		h.qmu.Lock()
		h.backlog[0] = nil // don't pin the delivered event
		h.backlog = h.backlog[1:]
		if msg, ok := ev.(*Message); ok {
			h.backlogBytes -= messageSize(msg)
		}
		h.qmu.Unlock()
	}
}

// messageSize approximates the memory a queued message holds.
func messageSize(msg *Message) int {
	n := 64 + len(msg.Channel) + len(msg.Pattern) + len(msg.Payload)
	for _, p := range msg.PayloadSlice {
		n += 16 + len(p)
	}
	return n
}

// confirmKey is a confirmed name in its namespace.
type confirmKey struct {
	shard, pattern bool
	name           string
}

func confirmKeyFor(kind, name string) confirmKey {
	switch kind {
	case "psubscribe", "punsubscribe":
		return confirmKey{pattern: true, name: name}
	case "ssubscribe", "sunsubscribe":
		return confirmKey{shard: true, name: name}
	}
	return confirmKey{name: name}
}

// deliverSubscriptionLocked delivers the handle its own copy of a
// subscription confirmation: handles are independent consumers, and a
// broadcast fan-out must not share one mutable value across them (see
// deliverLocked). Count is the handle's own running count, never the
// server's: that one is connection-wide and counts other handles' names
// too. Like the server's, it moves with each confirmation — a SUBSCRIBE
// of two channels confirms with 1, then 2 — and counts shard channels
// apart from channels and patterns. Every confirmation passes through
// here, server-sent or synthesized, so all of them agree. A closing
// handle gets none (see handle.closing).
func (h *handle) deliverSubscriptionLocked(sub *Subscription) {
	if h.closing {
		return
	}
	// The empty name is a valid channel or pattern, so it is tracked like
	// any other; a bare unsubscribe reply (no name, sent only when the
	// handle owns nothing) finds nothing to remove.
	key := confirmKeyFor(sub.Kind, sub.Channel)
	_, had := h.confirmed[key]
	switch sub.Kind {
	case "subscribe", "psubscribe", "ssubscribe":
		if !had {
			h.confirmed[key] = struct{}{}
			h.countConfirmed(key, 1)
		}
	default:
		if had {
			delete(h.confirmed, key)
			h.countConfirmed(key, -1)
		}
	}
	c := *sub
	c.Count = h.nConfirmed
	if key.shard {
		c.Count = h.nConfirmedShard
	}
	h.deliverLocked(&c)
}

func (h *handle) countConfirmed(key confirmKey, delta int) {
	if key.shard {
		h.nConfirmedShard += delta
	} else {
		h.nConfirmed += delta
	}
}

// deliverPongLocked delivers a pong like any other event.
func (h *handle) deliverPongLocked(pong *Pong) {
	h.deliverLocked(pong)
}

// Subscribe adds the given channels to the handle's subscriptions and
// returns the handle itself.
func (h *handle) Subscribe(ctx context.Context, channels ...string) (PubSuber, error) {
	if _, err := h.m.handleSubscribe(ctx, h, "subscribe", channels...); err != nil {
		return nil, err
	}
	return h, nil
}

// PSubscribe adds the given patterns to the handle's subscriptions and
// returns the handle itself (see Subscribe).
func (h *handle) PSubscribe(ctx context.Context, patterns ...string) (PubSuber, error) {
	if _, err := h.m.handleSubscribe(ctx, h, "psubscribe", patterns...); err != nil {
		return nil, err
	}
	return h, nil
}

// SSubscribe adds the given shard channels to the handle's
// subscriptions and returns the handle itself (see Subscribe).
func (h *handle) SSubscribe(ctx context.Context, schannels ...string) (PubSuber, error) {
	if _, err := h.m.handleSubscribe(ctx, h, "ssubscribe", schannels...); err != nil {
		return nil, err
	}
	return h, nil
}

// Unsubscribe removes the handle's subscription to the given channels,
// or to all of them if none are given; UNSUBSCRIBE is sent only for
// channels no other handle owns.
func (h *handle) Unsubscribe(ctx context.Context, channels ...string) error {
	return h.m.handleUnsubscribe(ctx, h, "unsubscribe", channels...)
}

// PUnsubscribe removes the handle's subscription to the given patterns,
// or to all of them if none are given (see Unsubscribe).
func (h *handle) PUnsubscribe(ctx context.Context, patterns ...string) error {
	return h.m.handleUnsubscribe(ctx, h, "punsubscribe", patterns...)
}

// SUnsubscribe removes the handle's subscription to the given shard
// channels, or to all of them if none are given (see Unsubscribe).
func (h *handle) SUnsubscribe(ctx context.Context, channels ...string) error {
	return h.m.handleUnsubscribe(ctx, h, "sunsubscribe", channels...)
}

// Subscriptions returns the names the handle is currently subscribed
// to, one slice per namespace.
func (h *handle) Subscriptions() (channels, patterns, schannels []string) {
	h.m.mu.RLock()
	defer h.m.mu.RUnlock()
	return slices.Collect(maps.Keys(h.channels)),
		slices.Collect(maps.Keys(h.patterns)),
		slices.Collect(maps.Keys(h.schannels))
}

// Ping writes a PING on the shared connection; the pong surfaces on
// this handle's Events (see Manager.replyQueue).
func (h *handle) Ping(ctx context.Context, payload ...string) error {
	return h.m.ping(ctx, h, false, payload...)
}

// PingSilent writes a PING without marking the handle as awaiting the
// reply, so the pong never surfaces on this handle's Events. The handle
// still gates the write: a closed one must not dial.
func (h *handle) PingSilent(ctx context.Context, payload ...string) error {
	return h.m.ping(ctx, h, true, payload...)
}

// ClientSetName names the shared connection; the +OK reply surfaces on
// this handle's Events as a pong.
func (h *handle) ClientSetName(ctx context.Context, name string) error {
	return h.m.clientSetName(ctx, h, name)
}

// DeliverError puts err on the handle's Events (see PubSuber).
func (h *handle) DeliverError(err error) {
	h.m.mu.Lock()
	defer h.m.mu.Unlock()
	h.deliverLocked(err)
}

// Close unsubscribes the handle from everything and ends its delivery
// stream. Subsequent calls return pool.ErrClosed, except after the
// manager itself closed (closing a subscriber of a closed client is a
// nil no-op).
func (h *handle) Close() error {
	// One hold of the manager lock throughout, the unsubscribe writes
	// included, keeps a racing Subscribe from landing on the dying
	// handle. A closed manager's teardown closed every handle already,
	// which the check below reports.
	h.m.mu.Lock()
	defer h.m.mu.Unlock()

	if h.closed {
		select {
		case <-h.m.done:
			return nil
		default:
			return pool.ErrClosed
		}
	}

	// Nothing the detach produces is delivered: the confirmations would
	// sit on events for a Receive after Close to return instead of
	// reporting the handle closed.
	h.closing = true
	err := h.m.handleUnsubscribeLocked(h, "unsubscribe")
	if perr := h.m.handleUnsubscribeLocked(h, "punsubscribe"); err == nil {
		err = perr
	}
	if serr := h.m.handleUnsubscribeLocked(h, "sunsubscribe"); err == nil {
		err = serr
	}

	h.closeLocked()
	return err
}

// closeLocked ends the handle's delivery stream (closing events also
// terminates the consumer pump) and its ownership: a closed handle owns
// nothing, whichever path closed it — Close detached every name before
// getting here, the manager's teardown has not. Callers must hold the
// manager lock, so no delivery can race with the close; no-op if
// already closed.
func (h *handle) closeLocked() {
	if h.closed {
		return
	}
	h.closed = true
	delete(h.m.handles, h)
	clear(h.channels)
	clear(h.patterns)
	clear(h.schannels)
	clear(h.confirmed)
	h.m.forgetLeavingLocked(h)
	// Nil (don't remove) the handle's pong waits: each entry must still
	// consume its reply or attribution desyncs for every later waiter.
	for i := range h.m.replyQueue {
		if h.m.replyQueue[i].h == h {
			h.m.replyQueue[i].h = nil
		}
	}
	// A running forwarder closes events itself (see forward).
	h.qmu.Lock()
	h.qclosed = true
	if !h.forwarding {
		close(h.events)
	}
	h.qmu.Unlock()
	close(h.done)
}
