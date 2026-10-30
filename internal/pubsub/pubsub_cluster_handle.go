package pubsub

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

// shardPart is one node's slice of a composite subscription: a handle
// of that node's owned manager. The broadcast part (addr "") carries
// the regular and pattern subscriptions and has a nil schannels; slot
// parts carry shard channels.
type shardPart struct {
	addr      string
	handle    PubSuber
	schannels map[string]struct{}
}

// shardHandleState is the composite handle's lifecycle state. It only
// moves forward: open → closing (Close called) → closed (last forwarder
// and re-route done; events stream ended).
type shardHandleState uint8

const (
	shardHandleOpen shardHandleState = iota
	shardHandleClosing
	shardHandleClosed
)

// clusterShardHandle is one subscriber's composite view over a cluster:
// regular and pattern subscriptions live on a broadcast part, shard
// channels on one part per owning node, each part's Events forwarded
// into the composite stream. Parts are re-pointed when slots move; the
// composite events stream ends only through Close (or the manager
// closing).
//
// Forwarding blocks: a part's forwarder waits for room in events rather
// than dropping, so a slow consumer backpressures into the part handle,
// whose backlog bounds the memory and never drops a confirmation, pong
// or error (see handle.backlog). Close unblocks the forwarders.
type clusterShardHandle struct {
	m *ClusterManager

	// mu guards the fields below it up to the consumer view. Lock
	// order: rerouteMu → deliverMu → mu; the manager's mu is a leaf
	// and never held while taking mu.
	mu    sync.Mutex
	state shardHandleState
	parts map[*shardPart]struct{}
	// schannels is the desired set of shard channels. Placement lives in
	// the parts; a desired channel on no part could not be placed and is
	// retried by the re-route sweep until it lands.
	schannels map[string]struct{}
	// moving maps a shard channel mid-move to the part it moves to. The
	// part it leaves keeps the channel — and keeps delivering — until
	// the new part confirms the subscription (see landedLocked): the
	// two nodes act on different connections, so an unsubscribe sent
	// before that confirmation could outrun it and open a delivery gap.
	// Both parts may deliver a message published in the overlap.
	moving map[string]*shardPart
	// broadcast is the broadcast part's handle, created lazily.
	broadcast PubSuber

	// confirmed and the counts mirror handle.confirmed for the composite:
	// the Count a delivered confirmation carries is the composite's own
	// (its parts each count only their share), and a shard channel's
	// release from the part it moved off stays confirmed while another
	// part holds it (see stampLocked).
	confirmed                   map[confirmKey]struct{}
	nConfirmed, nConfirmedShard int

	active    int // running forwarders
	rerouting int // in-flight re-routes; defers the close accounting

	// deliverMu serializes deliveries into events, so the Count stamped
	// under mu reaches the consumer in stamping order, and is held by
	// the transition that closes events (no send can race it).
	deliverMu sync.Mutex
	// events is closed by closeConsumerChannel; closing is closed by
	// Close, unblocking the forwarders parked on a full events; done
	// unblocks a pump parked on an abandoned consumer channel.
	events  chan any
	closing chan struct{}
	done    chan struct{}

	// The lazily started consumer view over events (guarded by mu).
	pump  pumpMode
	msgCh chan *Message
	allCh chan any
}

var (
	_ PubSuber       = (*clusterShardHandle)(nil)
	_ PubSubConfiger = (*clusterShardHandle)(nil)
)

func newClusterShardHandle(m *ClusterManager) *clusterShardHandle {
	size, _ := m.consumerSettings()
	return &clusterShardHandle{
		m:         m,
		parts:     make(map[*shardPart]struct{}),
		schannels: make(map[string]struct{}),
		moving:    make(map[string]*shardPart),
		confirmed: make(map[confirmKey]struct{}),
		events:    make(chan any, size),
		closing:   make(chan struct{}),
		done:      make(chan struct{}),
	}
}

// open reports whether the handle still accepts work.
func (h *clusterShardHandle) open() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.state == shardHandleOpen
}

// Events returns the raw composite delivery stream (see PubSuber).
func (h *clusterShardHandle) Events() <-chan any {
	return h.events
}

// Channel returns a Go channel carrying the messages of every part,
// created on the first call. Options run before h.mu is taken (they
// reach for the cluster manager's lock, never acquired after a
// handle's). It cannot be combined with ChannelWithSubscriptions.
func (h *clusterShardHandle) Channel(opt ...ChannelOption) <-chan *Message {
	for _, o := range opt {
		o(h)
	}
	size, sendTimeout := h.m.consumerSettings()
	logInterval := h.m.logInterval()

	h.mu.Lock()
	defer h.mu.Unlock()

	switch h.pump {
	case pumpAll:
		panic("redis: Channel can't be called after ChannelWithSubscriptions")
	case pumpNone:
		h.pump = pumpMessages
		h.msgCh = make(chan *Message, size)
		go pump(h.events, h.done, h.msgCh, sendTimeout, logInterval, messageFilter)
	}
	return h.msgCh
}

// ChannelWithSubscriptions is like Channel, but the returned channel
// also carries *Subscription confirmations (useful to observe
// resubscriptions after a re-route; pongs and error replies are
// filtered out), so its element type is any. It cannot be used together
// with Channel.
func (h *clusterShardHandle) ChannelWithSubscriptions(opt ...ChannelOption) <-chan any {
	for _, o := range opt {
		o(h)
	}
	size, sendTimeout := h.m.consumerSettings()
	logInterval := h.m.logInterval()

	h.mu.Lock()
	defer h.mu.Unlock()

	switch h.pump {
	case pumpMessages:
		panic("redis: ChannelWithSubscriptions can't be called after Channel")
	case pumpNone:
		h.pump = pumpAll
		h.allCh = make(chan any, size)
		go pump(h.events, h.done, h.allCh, sendTimeout, logInterval, allEventsFilter)
	}
	return h.allCh
}

// The PubSubConfiger methods update the cluster-wide config: the seed
// plus every owned node manager (see ClusterManager.updateAll).

// UpdateChannelSize sets the cluster-wide delivery buffer size, skipped
// once this handle's own view started.
func (h *clusterShardHandle) UpdateChannelSize(newSize int) {
	if h.pumpStarted() {
		return
	}
	h.m.updateChannelSize(newSize)
}

// UpdateSendTimeout sets the cluster-wide consumer send timeout,
// skipped once this handle's own view started.
func (h *clusterShardHandle) UpdateSendTimeout(newTimeout time.Duration) {
	if h.pumpStarted() {
		return
	}
	h.m.updateSendTimeout(newTimeout)
}

// UpdatePingTimeout sets the health-check ping timeout of every owned
// manager.
func (h *clusterShardHandle) UpdatePingTimeout(newTimeout time.Duration) {
	h.m.updatePingTimeout(newTimeout)
}

// UpdateHealthCheckInterval sets the health-check interval of every
// owned manager.
func (h *clusterShardHandle) UpdateHealthCheckInterval(newInterval time.Duration) {
	h.m.updateHealthCheckInterval(newInterval)
}

// UpdateReconnectTimeout sets the dial timeout of every owned manager.
func (h *clusterShardHandle) UpdateReconnectTimeout(newTimeout time.Duration) {
	h.m.updateReconnectTimeout(newTimeout)
}

// pumpStarted reports whether a consumer view is already running.
func (h *clusterShardHandle) pumpStarted() bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.pump != pumpNone
}

// deliver puts an event of part p (nil for a DeliverError) on the
// composite stream, waiting for room; a closing handle drops it (its
// stream is about to end, and nothing the teardown produces must sit on
// events for a Receive after Close). A confirmation that lands a moving
// channel on p releases the channel from the parts it left first, so
// the old node stops as soon as the new one is known to deliver.
func (h *clusterShardHandle) deliver(p *shardPart, ev any) {
	h.deliverMu.Lock()
	defer h.deliverMu.Unlock()

	h.mu.Lock()
	if h.state != shardHandleOpen {
		h.mu.Unlock()
		return
	}
	release, emptied := h.landedLocked(p, ev)
	ev, ok := h.stampLocked(ev)
	h.mu.Unlock()

	h.releaseParts(release, emptied)
	if !ok {
		return
	}

	select {
	case h.events <- ev:
	case <-h.closing:
	}
}

// landedLocked completes the move of a shard channel when ev is its
// subscription confirmation from the part the channel moves to: the
// parts it left stop carrying it and are to be sent its unsubscribe,
// and those left empty are to be closed. Callers must hold h.mu.
func (h *clusterShardHandle) landedLocked(p *shardPart, ev any) (release map[*shardPart][]string, emptied []*shardPart) {
	sub, ok := ev.(*Subscription)
	if !ok || p == nil || sub.Kind != "ssubscribe" || h.moving[sub.Channel] != p {
		return nil, nil
	}
	delete(h.moving, sub.Channel)
	for q := range h.parts {
		if q == p {
			continue
		}
		if _, held := q.schannels[sub.Channel]; !held {
			continue
		}
		delete(q.schannels, sub.Channel)
		if release == nil {
			release = make(map[*shardPart][]string)
		}
		release[q] = append(release[q], sub.Channel)
		if len(q.schannels) == 0 {
			emptied = append(emptied, q)
		}
	}
	return release, emptied
}

// releaseParts unsubscribes the released channels on the parts that
// held them and closes the parts left empty: closing the handle ends
// its stream, and the exiting forwarder detaches the part. The old
// part's unsubscribe confirmation is then suppressed by stampLocked —
// the composite stays subscribed. Callers must not hold h.mu.
func (h *clusterShardHandle) releaseParts(release map[*shardPart][]string, emptied []*shardPart) {
	if len(release) == 0 {
		return
	}
	ctx := context.Background()
	for q, chs := range release {
		slices.Sort(chs)
		_ = q.handle.SUnsubscribe(ctx, chs...)
	}
	for _, q := range emptied {
		_ = q.handle.Close()
	}
	if len(emptied) > 0 {
		h.m.prune()
	}
}

// stampLocked gives a subscription confirmation the composite's own
// Count, as handle.deliverSubscriptionLocked does for a standalone
// handle, and reports whether the event is delivered at all: the
// unsubscribe confirmation a shard channel's old part produces when the
// channel moved to another part (see landedLocked) is not — the
// composite is still subscribed, and a consumer watching
// ChannelWithSubscriptions sees the resubscription on the new node (a
// confirmation with an unchanged Count, like a reconnect replay's),
// never a loss. Other events pass through. Callers must hold h.mu.
func (h *clusterShardHandle) stampLocked(ev any) (any, bool) {
	sub, ok := ev.(*Subscription)
	if !ok {
		return ev, true
	}
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
			if key.shard && h.placedLocked(sub.Channel) {
				return nil, false
			}
			delete(h.confirmed, key)
			h.countConfirmed(key, -1)
		}
	}
	c := *sub
	c.Count = h.nConfirmed
	if key.shard {
		c.Count = h.nConfirmedShard
	}
	return &c, true
}

func (h *clusterShardHandle) countConfirmed(key confirmKey, delta int) {
	if key.shard {
		h.nConfirmedShard += delta
	} else {
		h.nConfirmed += delta
	}
}

// placedLocked reports whether some part carries the shard channel.
// Callers must hold h.mu.
func (h *clusterShardHandle) placedLocked(ch string) bool {
	for p := range h.parts {
		if _, ok := p.schannels[ch]; ok {
			return true
		}
	}
	return false
}

// forward moves a part's events into the composite stream until the
// part's stream ends (emptied and closed, pruned, or closing).
func (h *clusterShardHandle) forward(p *shardPart) {
	for ev := range p.handle.Events() {
		h.deliver(p, ev)
	}
	h.forwarderExited(p)
}

// forwarderExited runs the close accounting when a part's delivery
// stream ends. A part that ends while the handle is open (its node
// manager was torn down) leaves its channels desired-but-unplaced for
// the next re-route sweep; a channel that was moving to it stays on
// the part it was leaving, which the sweep moves again.
func (h *clusterShardHandle) forwarderExited(p *shardPart) {
	h.mu.Lock()
	h.active--
	delete(h.parts, p)
	for ch, dest := range h.moving {
		if dest == p {
			delete(h.moving, ch)
		}
	}
	shouldClose := h.shouldCloseLocked()
	h.mu.Unlock()

	if shouldClose {
		h.closeConsumerChannel()
	}
}

// shouldCloseLocked reports whether the handle should transition to
// closed: closing, with no forwarder or re-route left. Callers must
// hold h.mu.
func (h *clusterShardHandle) shouldCloseLocked() bool {
	return h.state == shardHandleClosing && h.active == 0 && h.rerouting == 0
}

// placement snapshots where every shard channel lives: the part and
// addr per placed channel, plus the full list of channels a sweep must
// resolve (placed ∪ desired). A channel mid-move is placed on the part
// it moves to; the part it leaves is released once that one confirms.
// Callers must hold h.mu.
func (h *clusterShardHandle) placementLocked() (current map[string]*shardPart, currentAddrs map[string]string, all []string) {
	current = make(map[string]*shardPart)
	currentAddrs = make(map[string]string)
	for p := range h.parts {
		for ch := range p.schannels {
			if prev, seen := current[ch]; seen && (prev == h.moving[ch] || p != h.moving[ch]) {
				continue
			}
			current[ch] = p
			currentAddrs[ch] = p.addr
		}
	}
	all = slices.Collect(maps.Keys(current))
	// Desired-but-unplaced channels ride the sweep with no current
	// placement and land wherever the resolve picks now.
	for ch := range h.schannels {
		if _, ok := current[ch]; !ok {
			all = append(all, ch)
		}
	}
	return current, currentAddrs, all
}

// reroute moves every shard channel whose slot owner no longer matches
// the part it lives on to the current owner: subscribe on the new node,
// and release the channel on the old part only once the new node
// confirmed it — overlap beats a delivery gap. The events stream is
// untouched. Serialized by the manager's re-route sweep (callers hold
// rerouteMu).
func (h *clusterShardHandle) reroute(ctx context.Context) error {
	h.mu.Lock()
	if h.state != shardHandleOpen {
		h.mu.Unlock()
		return nil
	}
	h.rerouting++
	current, currentAddrs, all := h.placementLocked()
	h.mu.Unlock()
	defer h.endReroute()

	if len(all) == 0 {
		return nil
	}

	// The current placement makes the resolve sticky: a channel moves
	// only when its node stopped serving the slot, not when a
	// non-deterministic replica picker re-rolls.
	groups, err := h.m.hooks.ResolveOwners(ctx, all, currentAddrs)
	if err != nil {
		return err
	}

	var firstErr error
	for addr, chs := range groups {
		if err := h.handleSSubscribe(ctx, addr, chs, current); err != nil {
			if errors.Is(err, pool.ErrClosed) {
				return nil // the handle started closing mid-sweep
			}
			// A dial that failed: the part is attached and recovers on
			// its own; the sweep goes on with the other groups.
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// handleSSubscribe places one resolved owner-group of shard channels on
// addr, preferring to join an existing part there (a fresh part per
// call would leak one emptied part per migration cycle). A relocated
// channel is marked moving before its subscribe is written — the
// confirmation may arrive before the write returns — and stays on the
// part it leaves until the new part confirms it (see landedLocked). A
// failed dial still places the channels — the engine keeps their
// registration for its reconnect replay, and the old part keeps
// delivering meanwhile — and is reported. Callers hold rerouteMu and
// must not hold h.mu.
func (h *clusterShardHandle) handleSSubscribe(ctx context.Context, addr string, channels map[string]struct{}, current map[string]*shardPart) error {
	// Skip channels already placed on the target node; a channel with no
	// placement needs subscribing but has no old part to release.
	moved := make(map[string]struct{}, len(channels))
	relocated := make(map[string]struct{}, len(channels))
	for ch := range channels {
		p := current[ch]
		if p != nil && p.addr == addr {
			continue
		}
		moved[ch] = struct{}{}
		if p != nil {
			relocated[ch] = struct{}{}
		}
	}
	if len(moved) == 0 {
		return nil
	}

	// markMoving points the relocated channels at target, if the part
	// they leave still carries them (one that ended meanwhile left them
	// unplaced: nothing to release). Callers must hold h.mu.
	markMoving := func(target *shardPart) {
		for ch := range relocated {
			old := current[ch]
			if _, live := h.parts[old]; !live {
				continue
			}
			if _, held := old.schannels[ch]; held {
				h.moving[ch] = target
			}
		}
	}
	// unmarkMoving reverts markMoving(target). Callers must hold h.mu.
	unmarkMoving := func(target *shardPart) {
		for ch := range relocated {
			if h.moving[ch] == target {
				delete(h.moving, ch)
			}
		}
	}

	h.mu.Lock()
	var target *shardPart
	for p := range h.parts {
		if p.addr == addr && p.schannels != nil {
			target = p
			break
		}
	}
	if target != nil {
		markMoving(target)
	}
	h.mu.Unlock()

	var err error
	if target != nil {
		_, err = target.handle.SSubscribe(ctx, slices.Sorted(maps.Keys(moved))...)
		if errors.Is(err, pool.ErrClosed) {
			// The part's node manager was pruned between the lookup and
			// the subscribe; fall through to a fresh part.
			h.mu.Lock()
			unmarkMoving(target)
			h.mu.Unlock()
			target = nil
		}
	}
	if target == nil {
		part, perr := h.m.subscribeOnNode(ctx, addr, moved)
		if part == nil {
			return perr
		}
		// The part's forwarder — and so any confirmation — starts after
		// the mark, under the same lock.
		target = &shardPart{addr: addr, handle: part, schannels: make(map[string]struct{}, len(moved))}
		h.mu.Lock()
		if h.state != shardHandleOpen {
			h.mu.Unlock()
			_ = part.Close()
			return pool.ErrClosed
		}
		maps.Copy(target.schannels, moved)
		markMoving(target)
		h.parts[target] = struct{}{}
		h.active++
		go h.forward(target)
		h.mu.Unlock()
		return perr
	}

	h.mu.Lock()
	maps.Copy(target.schannels, moved)
	h.mu.Unlock()
	return err
}

// endReroute reverses the close-accounting hold taken by reroute and
// closes the events stream if Close ran while the re-route was in
// flight.
func (h *clusterShardHandle) endReroute() {
	h.mu.Lock()
	h.rerouting--
	shouldClose := h.shouldCloseLocked()
	h.mu.Unlock()
	if shouldClose {
		h.closeConsumerChannel()
	}
}

// closeConsumerChannel is the transition to closed: it ends the events
// stream and deregisters the handle. The state guard makes it run
// exactly once; deliverMu keeps any deliver out (none can be mid-send:
// closing is closed, and active == 0).
func (h *clusterShardHandle) closeConsumerChannel() {
	h.deliverMu.Lock()
	h.mu.Lock()
	if h.state == shardHandleClosed {
		h.mu.Unlock()
		h.deliverMu.Unlock()
		return
	}
	h.state = shardHandleClosed
	close(h.done)
	close(h.events)
	h.mu.Unlock()
	h.deliverMu.Unlock()

	h.m.dropShardHandle(h)
}

// broadcastPart returns the broadcast part's handle, creating and
// attaching it on first use. Callers must not hold h.mu.
func (h *clusterShardHandle) broadcastPart() (PubSuber, error) {
	h.mu.Lock()
	if h.state != shardHandleOpen {
		h.mu.Unlock()
		return nil, pool.ErrClosed
	}
	if h.broadcast != nil {
		bh := h.broadcast
		h.mu.Unlock()
		return bh, nil
	}
	h.mu.Unlock()

	// Created with h.mu released (the manager's NewHandle takes its own
	// lock), then published under it; a racing creator's handle is closed.
	bh := h.m.broadcast.NewHandle()
	h.mu.Lock()
	if h.state != shardHandleOpen {
		h.mu.Unlock()
		_ = bh.Close()
		return nil, pool.ErrClosed
	}
	if h.broadcast != nil {
		existing := h.broadcast
		h.mu.Unlock()
		_ = bh.Close()
		return existing, nil
	}
	h.broadcast = bh
	p := &shardPart{addr: "", handle: bh}
	h.parts[p] = struct{}{}
	h.active++
	go h.forward(p)
	h.mu.Unlock()
	return bh, nil
}

// Subscribe adds regular channel subscriptions, carried by the
// broadcast part.
func (h *clusterShardHandle) Subscribe(ctx context.Context, channels ...string) (PubSuber, error) {
	bh, err := h.broadcastPart()
	if err != nil {
		return nil, err
	}
	if _, err := bh.Subscribe(ctx, channels...); err != nil {
		return nil, err
	}
	return h, nil
}

// PSubscribe adds pattern subscriptions, carried by the broadcast part.
func (h *clusterShardHandle) PSubscribe(ctx context.Context, patterns ...string) (PubSuber, error) {
	bh, err := h.broadcastPart()
	if err != nil {
		return nil, err
	}
	if _, err := bh.PSubscribe(ctx, patterns...); err != nil {
		return nil, err
	}
	return h, nil
}

// Unsubscribe removes regular channel subscriptions from the broadcast
// part; a no-op if none were ever added.
func (h *clusterShardHandle) Unsubscribe(ctx context.Context, channels ...string) error {
	h.mu.Lock()
	bh, closed := h.broadcast, h.state != shardHandleOpen
	h.mu.Unlock()
	if closed {
		return pool.ErrClosed
	}
	if bh == nil {
		return nil
	}
	return bh.Unsubscribe(ctx, channels...)
}

// PUnsubscribe removes pattern subscriptions from the broadcast part; a
// no-op if none were ever added.
func (h *clusterShardHandle) PUnsubscribe(ctx context.Context, patterns ...string) error {
	h.mu.Lock()
	bh, closed := h.broadcast, h.state != shardHandleOpen
	h.mu.Unlock()
	if closed {
		return pool.ErrClosed
	}
	if bh == nil {
		return nil
	}
	return bh.PUnsubscribe(ctx, patterns...)
}

// SSubscribe adds the given shard channels to the handle. The desired
// set records the intent before any network work: channels that could
// not be placed are retried by the reload-driven re-route sweep, so the
// subscription self-heals even though the root client discards this
// error.
func (h *clusterShardHandle) SSubscribe(ctx context.Context, channels ...string) (PubSuber, error) {
	if len(channels) == 0 {
		return nil, ErrNoShardChannels
	}

	// Serialize with the re-route sweep: an unserialized overlap can
	// register the same channel on two parts of one node.
	h.m.rerouteMu.Lock()
	defer h.m.rerouteMu.Unlock()

	// Record the intent and snapshot the placement in one critical
	// section: resolving against the placement makes a re-added channel
	// join the part already carrying it.
	h.mu.Lock()
	if h.state != shardHandleOpen {
		h.mu.Unlock()
		return nil, pool.ErrClosed
	}
	for _, ch := range channels {
		h.schannels[ch] = struct{}{}
	}
	current, currentAddrs, _ := h.placementLocked()
	h.mu.Unlock()

	groups, err := h.m.hooks.ResolveOwners(ctx, channels, currentAddrs)
	if err != nil {
		// The channels stay desired-but-unplaced; the reload's sweep
		// retries them.
		h.m.hooks.RequestReload()
		return nil, err
	}

	var firstErr error
	for addr, chs := range groups {
		if !h.open() {
			return nil, pool.ErrClosed
		}
		if err := h.handleSSubscribe(ctx, addr, chs, current); err != nil {
			if errors.Is(err, pool.ErrClosed) {
				return nil, err
			}
			// A failed dial: the part is attached and recovers on its
			// own; keep going with the remaining groups.
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	if firstErr != nil {
		h.m.hooks.RequestReload()
		return nil, firstErr
	}
	return h, nil
}

// SUnsubscribe removes the handle's subscription to the given shard
// channels, or to all of them if none are given, routed to the parts
// owning them.
func (h *clusterShardHandle) SUnsubscribe(ctx context.Context, channels ...string) error {
	// Serialize with the sweep and SSubscribe: those paths subscribe on
	// a node and register in the part map as separate steps, and an
	// unsubscribe landing between them leaves the part map and the node
	// registry disagreeing — a divergence nothing reconciles.
	h.m.rerouteMu.Lock()
	defer h.m.rerouteMu.Unlock()

	h.mu.Lock()
	if h.state != shardHandleOpen {
		h.mu.Unlock()
		return pool.ErrClosed
	}
	targets := make(map[*shardPart][]string)
	if len(channels) == 0 {
		clear(h.schannels)
		clear(h.moving)
		for p := range h.parts {
			if len(p.schannels) == 0 {
				continue
			}
			targets[p] = nil // nil means: everything the part owns
			clear(p.schannels)
		}
	} else {
		for _, ch := range channels {
			delete(h.schannels, ch)
			delete(h.moving, ch)
		}
		// A channel mid-move is released on both parts.
		for p := range h.parts {
			var subset []string
			for _, ch := range channels {
				if _, ok := p.schannels[ch]; ok {
					subset = append(subset, ch)
					delete(p.schannels, ch)
				}
			}
			if len(subset) > 0 {
				targets[p] = subset
			}
		}
	}
	h.mu.Unlock()

	var firstErr error
	for p, subset := range targets {
		if err := p.handle.SUnsubscribe(ctx, subset...); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

// Subscriptions returns the names the composite is subscribed to;
// shard channels are desired ∪ placed (a channel awaiting placement is
// registered intent).
func (h *clusterShardHandle) Subscriptions() (channels, patterns, schannels []string) {
	h.mu.Lock()
	bh := h.broadcast
	seen := maps.Clone(h.schannels)
	for p := range h.parts {
		maps.Copy(seen, p.schannels)
	}
	h.mu.Unlock()

	schannels = slices.Collect(maps.Keys(seen))
	if bh != nil {
		channels, patterns, _ = bh.Subscriptions()
	}
	return channels, patterns, schannels
}

// openParts snapshots the parts of an open handle; ErrClosed otherwise.
func (h *clusterShardHandle) openParts() ([]*shardPart, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.state != shardHandleOpen {
		return nil, pool.ErrClosed
	}
	return slices.Collect(maps.Keys(h.parts)), nil
}

// Ping pings every node the handle has a part on, returning the first
// error. Only one (arbitrary) part's pong surfaces on Events, so a Ping
// yields at most one pong like the standalone handle's would. A
// part-less handle pings the broadcast connection through its own
// broadcast part (see pingParts).
func (h *clusterShardHandle) Ping(ctx context.Context, payload ...string) error {
	parts, err := h.pingParts()
	if err != nil {
		return err
	}
	// All pings but the last are silent: one Ping call, one pong.
	for _, p := range parts[:len(parts)-1] {
		if err := p.handle.PingSilent(ctx, payload...); err != nil {
			return err
		}
	}
	return parts[len(parts)-1].handle.Ping(ctx, payload...)
}

// PingSilent pings every part's node with no pong surfacing on Events.
func (h *clusterShardHandle) PingSilent(ctx context.Context, payload ...string) error {
	parts, err := h.pingParts()
	if err != nil {
		return err
	}
	for _, p := range parts {
		if err := p.handle.PingSilent(ctx, payload...); err != nil {
			return err
		}
	}
	return nil
}

// pingParts returns the parts a ping goes to: the handle's parts, or —
// for a handle with none — its broadcast part, created for the purpose.
// Pinging through a part of this handle, never the manager, keeps the
// pong attributed to it (a manager-level ping consumes the pong
// silently, and Receive would wait for the next event instead) and the
// dialed connection owned by it, so Close releases the connection
// instead of leaving it to the cluster client's own Close.
func (h *clusterShardHandle) pingParts() ([]*shardPart, error) {
	parts, err := h.openParts()
	if err != nil {
		return nil, err
	}
	if len(parts) > 0 {
		return parts, nil
	}
	bh, err := h.broadcastPart()
	if err != nil {
		return nil, err
	}
	return []*shardPart{{handle: bh}}, nil
}

// ClientSetName names the broadcast connection through this handle's
// broadcast part, so the +OK reply surfaces on the composite Events as
// a Pong like the standalone handle's does (the manager-level call
// would consume it silently).
func (h *clusterShardHandle) ClientSetName(ctx context.Context, name string) error {
	bh, err := h.broadcastPart()
	if err != nil {
		return err
	}
	return bh.ClientSetName(ctx, name)
}

// DeliverError puts err on the composite stream (see PubSuber).
func (h *clusterShardHandle) DeliverError(err error) {
	h.deliver(nil, err)
}

// Close unsubscribes every part and ends the events stream. Subsequent
// calls return pool.ErrClosed, except after the manager itself closed
// (closing a subscriber of a closed client is a nil no-op).
func (h *clusterShardHandle) Close() error {
	h.mu.Lock()
	if h.state != shardHandleOpen {
		h.mu.Unlock()
		h.m.mu.Lock()
		mClosed := h.m.closed
		h.m.mu.Unlock()
		if mClosed {
			return nil
		}
		return pool.ErrClosed
	}
	h.state = shardHandleClosing
	close(h.closing) // un-parks the forwarders; their parts end below
	parts := slices.Collect(maps.Keys(h.parts))
	h.mu.Unlock()

	var firstErr error
	for _, p := range parts {
		if err := p.handle.Close(); err != nil && !errors.Is(err, pool.ErrClosed) && firstErr == nil {
			firstErr = err
		}
	}

	// No forwarder left to run the close accounting: transition here. A
	// re-route in flight defers to endReroute.
	h.mu.Lock()
	direct := h.shouldCloseLocked()
	h.mu.Unlock()
	if direct {
		h.closeConsumerChannel()
	}
	return firstErr
}
