package pubsub

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/redis/go-redis/v9/internal"
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
	// leaving holds the shard channels unsubscribed on this part whose
	// confirmation is still owed (see leaveLocked). A slot part with
	// nothing placed and nothing leaving is detached and closed (see
	// drainedLocked): closing it earlier would end its stream before
	// the confirmations the consumer is owed came through, keeping it
	// would leave the part, its forwarder and its node manager alive
	// for as long as the composite lives.
	leaving map[string]struct{}
	// pending counts, per shard channel, the subscribe writes of it in
	// flight on this part. A channel is placed under the composite's
	// locks but written with them released — the write dials the node
	// when it has no connection, and a dial must not stall the
	// composite's other operations — so a release can overtake the
	// write. The releaser then leaves the unsubscribe to the writer (see
	// releaseLocked, settleLocked): sent by the releaser, it could reach
	// the node's manager before the subscribe and leave the channel
	// subscribed there with nothing placed here.
	pending map[string]int
}

// placeLocked places chs on the part, each with a subscribe write in
// flight that settleLocked ends. Callers must hold the composite's mu.
func (p *shardPart) placeLocked(chs map[string]struct{}) {
	if p.pending == nil {
		p.pending = make(map[string]int)
	}
	for ch := range chs {
		p.schannels[ch] = struct{}{}
		p.pending[ch]++
	}
}

// leaveLocked moves ch from placed to leaving. Callers must hold the
// composite's mu.
func (p *shardPart) leaveLocked(ch string) {
	delete(p.schannels, ch)
	if p.leaving == nil {
		p.leaving = make(map[string]struct{})
	}
	p.leaving[ch] = struct{}{}
}

// releaseLocked moves ch from placed to leaving and reports whether its
// unsubscribe is the caller's to write: it is not while a subscribe
// write of ch is in flight, whose writer then writes it (see pending).
// Callers must hold the composite's mu.
func (p *shardPart) releaseLocked(ch string) bool {
	p.leaveLocked(ch)
	return p.pending[ch] == 0
}

// settleLocked ends a subscribe write of chs and returns, sorted, the
// channels released while it was in flight, whose unsubscribe the
// writer now owes (see pending). Callers must hold the composite's mu.
func (p *shardPart) settleLocked(chs map[string]struct{}) []string {
	var release []string
	for ch := range chs {
		if p.pending[ch]--; p.pending[ch] > 0 {
			continue
		}
		delete(p.pending, ch)
		if _, placed := p.schannels[ch]; placed {
			continue
		}
		if _, leaving := p.leaving[ch]; leaving {
			release = append(release, ch)
		}
	}
	slices.Sort(release)
	return release
}

// drained reports whether a slot part carries nothing and owes nothing.
func (p *shardPart) drained() bool {
	return p.schannels != nil && len(p.schannels) == 0 && len(p.leaving) == 0
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

	// ops is the handle's operation lock (a slot: full means held),
	// serializing SSubscribe, SUnsubscribe and the re-route sweep's work
	// on this handle with each other — see lockOps. It covers the
	// bookkeeping of an operation (placement snapshot, owner resolve,
	// placement), never a subscribe write: the write dials the node when
	// it has no connection yet, and a dial to a node that stopped
	// answering must stall neither this handle's other operations nor
	// another handle's (see shardPart.pending for what guards the write
	// instead). Lock order: the manager's sweepMu → ops → deliverMu → mu.
	ops chan struct{}

	// mu guards the fields below it up to the consumer view (lock order
	// above); the manager's mu is a leaf and never held while taking mu.
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
	// dialValues carries the context values of the latest SSubscribe
	// call, its cancellation detached (nil before the first). A re-route
	// sweep runs on no caller's behalf, yet the node it dials
	// authenticates through those values (CredentialsProviderContext):
	// a moved or late-placed channel must land with the credentials the
	// subscriber supplied, as the engine's own reconnect redials with
	// (see dialer.attempt).
	dialValues context.Context
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
		ops:       make(chan struct{}, 1),
		parts:     make(map[*shardPart]struct{}),
		schannels: make(map[string]struct{}),
		moving:    make(map[string]*shardPart),
		confirmed: make(map[confirmKey]struct{}),
		events:    make(chan any, size),
		closing:   make(chan struct{}),
		done:      make(chan struct{}),
	}
}

// lockOps takes the operation lock, or gives up with ctx's error once
// ctx ends first: an operation waits for the lock no longer than its
// caller allows. Callers release it with unlockOps.
func (h *clusterShardHandle) lockOps(ctx context.Context) error {
	select {
	case h.ops <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// unlockOps releases the operation lock taken by lockOps.
func (h *clusterShardHandle) unlockOps() { <-h.ops }

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
		// A closed channel instead of a panic, so callers can recover
		// (issue #3761).
		internal.Logger.Printf(context.Background(),
			"redis: Channel can't be called after ChannelWithSubscriptions")
		ch := make(chan *Message)
		close(ch)
		return ch
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
		// A closed channel instead of a panic (issue #3761).
		internal.Logger.Printf(context.Background(),
			"redis: ChannelWithSubscriptions can't be called after Channel")
		ch := make(chan any)
		close(ch)
		return ch
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
	detach := h.drainedLocked(p, ev)
	ev, ok := h.stampLocked(ev)
	h.mu.Unlock()

	h.releaseParts(release, emptied)
	if ok {
		select {
		case h.events <- ev:
		case <-h.closing:
		}
	}
	if detach != nil {
		_ = detach.handle.Close()
		h.m.prune()
	}
}

// drainedLocked settles an unsubscribe confirmation of part p against
// its leaving set and reports p when that was the last thing the part
// owed, with nothing placed on it any more: the caller closes it once
// the confirmation is delivered. The part is taken out of parts here,
// so no ping or subscribe lands on a closing part. Callers must hold
// h.mu.
func (h *clusterShardHandle) drainedLocked(p *shardPart, ev any) *shardPart {
	sub, ok := ev.(*Subscription)
	if !ok || p == nil || p.schannels == nil || sub.Kind != "sunsubscribe" {
		return nil
	}
	delete(p.leaving, sub.Channel)
	if _, live := h.parts[p]; !live || !p.drained() {
		return nil
	}
	delete(h.parts, p)
	return p
}

// landedLocked completes the move of a shard channel when ev is its
// subscription confirmation from the part the channel moves to: the
// parts it left stop carrying it and are to be sent its unsubscribe.
// A part that is left with nothing placed and nothing else owed is to
// be closed right away (the released channel's own confirmation is
// suppressed, nobody waits for it); one still owing confirmations of
// earlier unsubscribes is closed once they came through (see
// drainedLocked). Callers must hold h.mu.
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
		// A part with a subscribe write of the channel in flight is
		// neither closed nor sent the unsubscribe here: the writer does
		// that once its write returned (see shardPart.pending).
		var write bool
		if delete(q.schannels, sub.Channel); q.drained() && q.pending[sub.Channel] == 0 {
			delete(h.parts, q)
			emptied = append(emptied, q)
			write = true
		} else {
			write = q.releaseLocked(sub.Channel)
		}
		if write {
			if release == nil {
				release = make(map[*shardPart][]string)
			}
			release[q] = append(release[q], sub.Channel)
		}
	}
	return release, emptied
}

// releaseParts unsubscribes the released channels on the parts that
// held them and closes the parts landedLocked emptied: closing the
// handle ends its stream, and the exiting forwarder runs the close
// accounting. The old part's unsubscribe confirmation is then
// suppressed by stampLocked — the composite stays subscribed. Callers
// must not hold h.mu.
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
// untouched. Serialized with the handle's own operations by its
// operation lock, held through the placement and released for the
// writes (see ops); callers hold the manager's sweepMu.
func (h *clusterShardHandle) reroute(ctx context.Context) error {
	if err := h.lockOps(ctx); err != nil {
		return err
	}
	unlock := sync.OnceFunc(h.unlockOps)
	defer unlock()

	h.mu.Lock()
	if h.state != shardHandleOpen {
		h.mu.Unlock()
		return nil
	}
	h.rerouting++
	current, currentAddrs, all := h.placementLocked()
	values := h.dialValues
	h.mu.Unlock()
	defer h.endReroute()

	if len(all) == 0 {
		return nil
	}

	// The sweep dials with the subscriber's context values (see
	// dialValues), bounded by the sweep's own cancellation.
	if values != nil {
		var cancel context.CancelFunc
		ctx, cancel = withCancelOf(values, ctx)
		defer cancel()
	}

	// The current placement makes the resolve sticky: a channel moves
	// only when its node stopped serving the slot, not when a
	// non-deterministic replica picker re-rolls.
	groups, err := h.m.hooks.ResolveOwners(ctx, all, currentAddrs)
	if err != nil {
		return err
	}
	placements, err := h.placeGroups(groups, current)
	unlock()
	if err == nil {
		err = h.writePlacements(ctx, placements)
	}
	if errors.Is(err, pool.ErrClosed) {
		return nil // the handle started closing mid-sweep
	}
	// A dial that failed: the part is attached and recovers on its own.
	return err
}

// withCancelOf returns a context with values' values that is cancelled
// when ctrl is (values is expected cancellation-free already). The
// returned cancel releases the link and must be called.
func withCancelOf(values, ctrl context.Context) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(values)
	stop := context.AfterFunc(ctrl, cancel)
	return ctx, func() {
		stop()
		cancel()
	}
}

// placement is one resolved owner-group of shard channels placed on a
// part of the owning node, awaiting its subscribe write.
type placement struct {
	addr   string
	target *shardPart
	// fresh marks a target created for this placement; nm is then its
	// node manager, forgotten if the write finds it closed.
	fresh bool
	nm    *Manager
	// moved are the channels to subscribe on target: those of the group
	// not placed on the node already. old maps each of them that moves
	// off another part to that part.
	moved map[string]struct{}
	old   map[string]*shardPart
}

// placeGroups places every resolved owner-group's channels that are
// not on the owning node yet, against the placement snapshot current
// they were resolved with, and returns the placements for
// writePlacements. Nothing is written here: placing under the locks and
// writing with them released is what keeps a dial from stalling the
// handle's other operations (see ops), and placing BEFORE the write —
// the part registered, its forwarder running — is what makes a release
// or a confirmation that lands mid-write see the channels (see
// shardPart.pending, drainedLocked). Callers must hold ops and must not
// hold h.mu.
func (h *clusterShardHandle) placeGroups(groups map[string]map[string]struct{}, current map[string]*shardPart) ([]*placement, error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.state != shardHandleOpen {
		return nil, pool.ErrClosed
	}
	var placements []*placement
	for _, addr := range slices.Sorted(maps.Keys(groups)) {
		pl := &placement{
			addr:  addr,
			moved: make(map[string]struct{}),
			old:   make(map[string]*shardPart),
		}
		// A channel with no placement needs subscribing but has no old
		// part to release.
		for ch := range groups[addr] {
			p := current[ch]
			if p != nil && p.addr == addr {
				continue
			}
			pl.moved[ch] = struct{}{}
			if p != nil {
				pl.old[ch] = p
			}
		}
		if len(pl.moved) == 0 {
			continue
		}
		if err := h.placeLocked(pl); err != nil {
			return nil, err
		}
		placements = append(placements, pl)
	}
	return placements, nil
}

// placeLocked places pl's channels on the handle's part of pl.addr,
// joining the existing one (a fresh part per call would leak one
// emptied part per migration cycle) or creating it: registered and
// forwarding before anything is written, so a confirmation — which may
// arrive before the write returns — finds it. A relocated channel is
// marked moving to the target if the part it leaves still carries it
// (one that ended meanwhile left it unplaced: nothing to release), and
// stays on that part until the target confirms it (see landedLocked).
// Callers must hold h.mu with the handle open.
func (h *clusterShardHandle) placeLocked(pl *placement) error {
	pl.target, pl.fresh, pl.nm = nil, false, nil
	for p := range h.parts {
		if p.addr == pl.addr && p.schannels != nil {
			pl.target = p
			break
		}
	}
	if pl.target == nil {
		nm := h.m.nodeManager(pl.addr)
		if nm == nil {
			return pool.ErrClosed
		}
		pl.target = &shardPart{
			addr:      pl.addr,
			handle:    nm.NewHandle(),
			schannels: make(map[string]struct{}, len(pl.moved)),
		}
		pl.fresh, pl.nm = true, nm
		h.parts[pl.target] = struct{}{}
		h.active++
		go h.forward(pl.target)
	}
	pl.target.placeLocked(pl.moved)
	for ch, old := range pl.old {
		if _, live := h.parts[old]; !live {
			continue
		}
		if _, held := old.schannels[ch]; held {
			h.moving[ch] = pl.target
		}
	}
	return nil
}

// writePlacements writes the placements' subscribes, in order, and
// returns the first error; ErrClosed (the handle closed) ends it at
// once. Callers must hold no lock.
func (h *clusterShardHandle) writePlacements(ctx context.Context, placements []*placement) error {
	var firstErr error
	for _, pl := range placements {
		if err := h.writePlacement(ctx, pl); err != nil {
			if errors.Is(err, pool.ErrClosed) {
				return err
			}
			// A failed dial: the part is attached and recovers on its
			// own; keep going with the remaining placements.
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// writePlacement subscribes pl's channels on its part and settles the
// write: a channel released while it was in flight is unsubscribed
// here, by the writer (see shardPart.pending). A fresh part whose node
// manager closed under it — pruned between the lookup and the write,
// handing out a handle born closed — is replaced and the write retried,
// with the channels still desired: one released while the write was in
// flight must not come back subscribed on the replacement. A failed
// dial leaves the channels placed — the engine keeps their
// registration for its reconnect replay, and a part they move off keeps
// delivering meanwhile — and is reported. Callers must hold no lock.
func (h *clusterShardHandle) writePlacement(ctx context.Context, pl *placement) error {
	for {
		target := pl.target
		_, err := target.handle.SSubscribe(ctx, slices.Sorted(maps.Keys(pl.moved))...)

		h.mu.Lock()
		release := target.settleLocked(pl.moved)
		retry := errors.Is(err, pool.ErrClosed) && pl.fresh && h.state == shardHandleOpen
		if retry {
			// The dead part's forwarder exits on its own; its channels
			// and moving marks go to the replacement now, so nothing
			// joins it meanwhile.
			delete(h.parts, target)
			for ch := range pl.moved {
				delete(target.schannels, ch)
			}
			for ch, dest := range h.moving {
				if dest == target {
					delete(h.moving, ch)
				}
			}
			h.m.forgetNodeManager(pl.addr, pl.nm)
			for ch := range pl.moved {
				if _, desired := h.schannels[ch]; !desired {
					delete(pl.moved, ch)
					delete(pl.old, ch)
				}
			}
			if len(pl.moved) == 0 {
				retry, err = false, nil // everything was released meanwhile
			} else {
				err = h.placeLocked(pl)
			}
		}
		h.mu.Unlock()

		if len(release) > 0 {
			_ = target.handle.SUnsubscribe(context.Background(), release...)
		}
		if !retry || err != nil {
			return err
		}
	}
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
// part, created for the purpose (no dial) if the handle has none: a
// handle that owns none of the channels still gets the confirmations
// the server would have answered with — synthesized by the part's
// manager, as for a standalone handle — so a Receive awaiting them
// returns instead of blocking.
func (h *clusterShardHandle) Unsubscribe(ctx context.Context, channels ...string) error {
	bh, err := h.broadcastPart()
	if err != nil {
		return err
	}
	return bh.Unsubscribe(ctx, channels...)
}

// PUnsubscribe removes pattern subscriptions from the broadcast part,
// created for the purpose if the handle has none (see Unsubscribe).
func (h *clusterShardHandle) PUnsubscribe(ctx context.Context, patterns ...string) error {
	bh, err := h.broadcastPart()
	if err != nil {
		return err
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

	// Serialized with the handle's other operations and the re-route
	// sweep through the placement (an unserialized overlap could place a
	// channel on two parts of one node), released for the writes (see
	// ops). The wait honors ctx.
	if err := h.lockOps(ctx); err != nil {
		return nil, err
	}
	unlock := sync.OnceFunc(h.unlockOps)
	defer unlock()

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
	h.dialValues = context.WithoutCancel(ctx)
	current, currentAddrs, _ := h.placementLocked()
	h.mu.Unlock()

	groups, err := h.m.hooks.ResolveOwners(ctx, channels, currentAddrs)
	if err != nil {
		// The channels stay desired-but-unplaced; the reload's sweep
		// retries them.
		h.m.hooks.RequestReload()
		return nil, err
	}

	placements, err := h.placeGroups(groups, current)
	unlock()
	if err != nil {
		return nil, err
	}
	if err := h.writePlacements(ctx, placements); err != nil {
		if errors.Is(err, pool.ErrClosed) {
			return nil, err
		}
		// A failed dial: the part is attached and recovers on its own,
		// and the reload's sweep retries the placement.
		h.m.hooks.RequestReload()
		return nil, err
	}
	return h, nil
}

// SUnsubscribe removes the handle's subscription to the given shard
// channels, or to all of them if none are given, routed to the parts
// owning them.
func (h *clusterShardHandle) SUnsubscribe(ctx context.Context, channels ...string) error {
	// Serialized with SSubscribe and the sweep (see ops), through the
	// writes: an unsubscribe written with the lock released could be
	// overtaken by a later subscribe's write of the same channel, and
	// nothing reconciles the node's registry with the parts then. The
	// writes dial nothing.
	if err := h.lockOps(ctx); err != nil {
		return err
	}
	defer h.unlockOps()
	// A cancelled caller gets nothing done on its behalf — before any
	// state changes: the parts are written with the cancellation
	// detached (below), so this is the one place ctx is honored.
	if err := ctx.Err(); err != nil {
		return err
	}

	h.mu.Lock()
	if h.state != shardHandleOpen {
		h.mu.Unlock()
		return pool.ErrClosed
	}
	// Every released channel becomes leaving on its part: the part is
	// detached once its last confirmation came through (see
	// drainedLocked). A channel whose subscribe write is in flight is
	// left to that writer to unsubscribe (see shardPart.pending). A
	// channel on no part — never placed, or released already — gets its
	// confirmation synthesized, as the server would have answered and
	// as a standalone handle does for a channel it does not own, so a
	// Receive awaiting it returns; so does a bare release that finds
	// nothing placed at all.
	targets := make(map[*shardPart][]string)
	var synth []string
	if len(channels) == 0 {
		placed := make(map[string]struct{})
		for p := range h.parts {
			if len(p.schannels) == 0 {
				continue
			}
			var now []string
			every := true
			for ch := range p.schannels {
				placed[ch] = struct{}{}
				if p.releaseLocked(ch) {
					now = append(now, ch)
				} else {
					every = false
				}
			}
			switch {
			case every:
				targets[p] = nil // nil means: everything the part owns
			case len(now) > 0:
				slices.Sort(now)
				targets[p] = now
			}
		}
		for ch := range h.schannels {
			if _, ok := placed[ch]; !ok {
				synth = append(synth, ch)
			}
		}
		slices.Sort(synth)
		if len(targets) == 0 && len(synth) == 0 {
			synth = []string{""} // the bare reply: nothing was subscribed
		}
		clear(h.schannels)
		clear(h.moving)
	} else {
		for _, ch := range channels {
			delete(h.schannels, ch)
			delete(h.moving, ch)
		}
		// A channel mid-move is released on both parts.
		placed := make(map[string]struct{})
		for p := range h.parts {
			var subset []string
			for _, ch := range channels {
				if _, ok := p.schannels[ch]; !ok {
					continue
				}
				placed[ch] = struct{}{}
				if p.releaseLocked(ch) {
					subset = append(subset, ch)
				}
			}
			if len(subset) > 0 {
				targets[p] = subset
			}
		}
		for _, ch := range channels {
			if _, ok := placed[ch]; !ok {
				synth = append(synth, ch)
			}
		}
	}
	h.mu.Unlock()

	if len(synth) > 0 {
		// Delivered off the caller's goroutine: deliver waits for room in
		// events, and a release must not block on a slow consumer (the
		// standalone handle's synthesized replies queue in its backlog).
		go func() {
			for _, ch := range synth {
				h.deliver(nil, &Subscription{Kind: "sunsubscribe", Channel: ch})
			}
		}()
	}

	// The channels are leaving now, so every part must act: a part that
	// bowed out on a cancellation that landed meanwhile would keep the
	// node subscribed to channels nothing here places any more, with no
	// retry to release them (a retry finds nothing placed). Writes are
	// bounded by the write timeout, not by ctx. The errors left — a
	// closed part, a failed write — leave nothing to roll back: the
	// engine has dropped the registration either way and settles the
	// confirmations owed itself.
	writeCtx := context.WithoutCancel(ctx)
	var firstErr error
	for p, subset := range targets {
		if err := p.handle.SUnsubscribe(writeCtx, subset...); err != nil && firstErr == nil {
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
