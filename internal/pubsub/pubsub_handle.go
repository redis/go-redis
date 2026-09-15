package pubsub

import (
	"context"
	"maps"
	"slices"
	"sync"
	"time"

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

	// events is closed by closeLocked; done unblocks a pump parked on
	// an abandoned consumer channel.
	events chan any
	done   chan struct{}

	// The lazily started consumer view over events (guarded by mu).
	pump  pumpMode
	msgCh chan *Message
	allCh chan any

	// Slow-consumer drop accounting (written only by the listen
	// goroutine).
	dropped     int
	dropLogTime time.Time

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

// deliverLocked sends an event into the handle's stream, dropping it
// with accounting when the buffer is full (a blocking send would stall
// the shared fan-out for every handle). Callers must hold the manager
// lock.
func (h *handle) deliverLocked(ev any) {
	if h.closed {
		return
	}
	select {
	case h.events <- ev:
	default:
		h.noteDropLocked(cap(h.events))
	}
}

// deliverSubscriptionLocked delivers a subscription confirmation (see
// deliverLocked).
func (h *handle) deliverSubscriptionLocked(sub *Subscription) {
	h.deliverLocked(sub)
}

// deliverPongLocked delivers a pong; pongs are advisory, so a full
// buffer drops them without accounting.
func (h *handle) deliverPongLocked(pong *Pong) {
	if h.closed {
		return
	}
	select {
	case h.events <- pong:
	default:
	}
}

// noteDropLocked records a dropped delivery, logging at most once per
// LogInterval with the drops accumulated since the previous log.
func (h *handle) noteDropLocked(bufSize int) {
	h.dropped++
	if logThrottled(context.TODO(), &h.dropLogTime, h.m.cfg.LogInterval,
		"redis: pubsub: dropped %d message(s) to a slow subscriber (buffer of %d is full, see PubSubChanSize)",
		h.dropped, bufSize) {
		h.dropped = 0
	}
}

// ownedTotal is the number of names the handle owns across all
// namespaces — the Count in synthesized unsubscribe confirmations.
func (h *handle) ownedTotal() int {
	return len(h.channels) + len(h.patterns) + len(h.schannels)
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
	if h.isClosed() {
		return pool.ErrClosed
	}
	return h.m.ping(ctx, h, true, payload...)
}

// PingSilent writes a PING without marking the handle as awaiting the
// reply, so the pong never surfaces on this handle's Events.
func (h *handle) PingSilent(ctx context.Context, payload ...string) error {
	if h.isClosed() {
		return pool.ErrClosed
	}
	return h.m.Ping(ctx, payload...)
}

// ClientSetName names the shared connection; the +OK reply surfaces on
// this handle's Events as a pong.
func (h *handle) ClientSetName(ctx context.Context, name string) error {
	if h.isClosed() {
		return pool.ErrClosed
	}
	return h.m.clientSetName(ctx, h, name)
}

func (h *handle) isClosed() bool {
	h.m.mu.RLock()
	defer h.m.mu.RUnlock()
	return h.closed
}

// Close unsubscribes the handle from everything and ends its delivery
// stream. Subsequent calls return pool.ErrClosed, except after the
// manager itself closed (closing a subscriber of a closed client is a
// nil no-op).
func (h *handle) Close() error {
	ctx := context.TODO()

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

	err := h.m.handleUnsubscribeLocked(ctx, h, "unsubscribe")
	if perr := h.m.handleUnsubscribeLocked(ctx, h, "punsubscribe"); err == nil {
		err = perr
	}
	if serr := h.m.handleUnsubscribeLocked(ctx, h, "sunsubscribe"); err == nil {
		err = serr
	}

	h.closeLocked()
	return err
}

// closeLocked ends the handle's delivery stream (closing events also
// terminates the consumer pump). Callers must hold the manager lock, so
// no send can race with the close; no-op if already closed.
func (h *handle) closeLocked() {
	if h.closed {
		return
	}
	h.closed = true
	delete(h.m.handles, h)
	// Nil (don't remove) the handle's pong waits: each entry must still
	// consume its reply or attribution desyncs for every later waiter.
	for i := range h.m.replyQueue {
		if h.m.replyQueue[i].h == h {
			h.m.replyQueue[i].h = nil
		}
	}
	close(h.done)
	close(h.events)
}
