package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/hashtag"
	"github.com/redis/go-redis/v9/internal/pool"
)

// shardedPubSubMigrationTimeout bounds a single onTopologyChange pass. The
// pass runs on the cluster's notifier goroutine and performs network I/O
// against nodes that may be unhealthy (that is often why the topology changed
// in the first place), so it must not be allowed to hang indefinitely.
const shardedPubSubMigrationTimeout = time.Minute

// ShardedPubSub wraps multiple PubSub connections to support sharded pub/sub
// across different cluster nodes. In a Redis cluster, channels hash to
// different slots which may be served by different nodes. A single connection
// can only receive messages for channels on the shard it is connected to.
// ShardedPubSub transparently manages one PubSub per shard and multiplexes
// all messages into a single Go channel.
//
// Supported API surface mirrors the relevant parts of PubSub: SSubscribe,
// SUnsubscribe, Channel, ReceiveMessage, ReceiveTimeout, Ping and Close.
// ChannelWithSubscriptions is intentionally not supported: subscription
// bookkeeping spans multiple shard connections, so a single merged stream of
// *Subscription messages would not map cleanly onto the per-shard model.
// Use Channel together with the topology-change handling to consume messages.
//
// Connection health: ShardedPubSub does not run a separate health check.
// Transient connection drops are handled by the underlying per-shard PubSub,
// which automatically reconnects on the next Receive. Recovery from a node
// that is permanently gone (e.g. its slots moved during resharding) is
// reload-driven: a cluster topology reload triggers onTopologyChange, which
// migrates the affected subscriptions to the new owning node. Subscriptions
// that could not be established (or re-established) are kept tracked and
// marked pending; every topology pass retries them until they succeed or are
// explicitly unsubscribed. Use Ping as a readiness probe: it returns an error
// while any subscription is pending or has no live shard connection.
//
// During a topology migration the new shard is subscribed before the old one
// is unsubscribed so there is no window with no active subscription. The
// trade-off is a brief window in which the consumer may receive duplicate
// messages for the migrating channels (once from each shard); consumers that
// cannot tolerate duplicates should deduplicate.
//
// Sharded pub/sub always resolves and connects to master nodes, even when the
// client is configured with ReadOnly: the master receives messages first, and
// replica resolution is not deterministic (consecutive resolutions may pick
// different replicas), which would cause spurious shard churn.
type ShardedPubSub struct {
	cluster *ClusterClient

	// opMu serializes the logical operations (SSubscribe, SUnsubscribe and
	// onTopologyChange) so one operation's resolve→subscribe→bookkeep
	// sequence never interleaves with another's. Network I/O happens while
	// holding opMu, but never while holding mu. Close deliberately does not
	// take opMu so it stays responsive even when an operation is stuck in
	// network I/O.
	opMu sync.Mutex

	// mu protects the fields below. It is never held across network I/O.
	mu     sync.Mutex
	shards map[string]*PubSub // addr -> PubSub
	// chanShard tracks which shard address each channel is (or should be) on.
	chanShard map[string]string // channel -> addr
	// pending tracks channels whose mapping exists but whose server-side
	// SSUBSCRIBE has not (yet) succeeded — e.g. after a failed subscribe or
	// shard rebuild. Every topology pass retries them.
	pending map[string]struct{}
	closed  bool

	msgCh  chan *Message
	chOpts []ChannelOption
	// forwarding tracks which shard addresses already have a forwarder
	// goroutine running, so we never start more than one per shard.
	forwarding map[string]struct{}

	chOnce     sync.Once
	exit       chan struct{}
	forwarders sync.WaitGroup
}

func newShardedPubSub(cluster *ClusterClient) *ShardedPubSub {
	sps := &ShardedPubSub{
		cluster:    cluster,
		shards:     make(map[string]*PubSub),
		chanShard:  make(map[string]string),
		pending:    make(map[string]struct{}),
		forwarding: make(map[string]struct{}),
		exit:       make(chan struct{}),
	}
	cluster.registerShardedPubSub(sps)
	return sps
}

// nodeAddrForChannel returns the address of the master node serving the
// channel's slot. See the type documentation for why ReadOnly is deliberately
// ignored here.
func (s *ShardedPubSub) nodeAddrForChannel(ctx context.Context, channel string) (string, error) {
	slot := hashtag.Slot(channel)
	node, err := s.cluster.slotMasterNode(ctx, slot)
	if err != nil {
		return "", err
	}
	return node.Client.opt.Addr, nil
}

// resolveChannels groups channels by the address of their owning node.
// Resolution of every channel is attempted rather than aborting on the first
// failure: a single unresolvable channel must not prevent the channels that
// did resolve from being handled. The first resolution error is returned
// alongside the groups.
//
// It must be called without holding mu: resolution may trigger a cluster
// state reload, which re-enters through notifyShardedPubSubs (asynchronously,
// so no deadlock, but the notifier must not find mu held for the duration of
// network I/O).
func (s *ShardedPubSub) resolveChannels(ctx context.Context, channels []string) (map[string][]string, error) {
	groups := make(map[string][]string)
	var firstErr error
	for _, ch := range channels {
		addr, err := s.nodeAddrForChannel(ctx, ch)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		groups[addr] = append(groups[addr], ch)
	}
	return groups, firstErr
}

// subscribeOnShard subscribes chs on the shard at addr, creating the shard if
// needed. On success it maps the channels to addr, clears their pending mark
// and starts the shard's forwarder. On failure the bookkeeping is left
// untouched — the caller decides whether to remap, mark pending or rebuild.
// The caller must hold opMu and must not hold mu.
func (s *ShardedPubSub) subscribeOnShard(ctx context.Context, addr string, chs []string) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return pool.ErrClosed
	}
	ps, ok := s.shards[addr]
	if !ok {
		// The shard's connections are pinned to addr (pubSubForAddr), so the
		// connection always lands on the node the bookkeeping is keyed by.
		ps = s.cluster.pubSubForAddr(addr)
		s.shards[addr] = ps
	}
	s.mu.Unlock()

	if err := ps.SSubscribe(ctx, chs...); err != nil {
		return err
	}

	s.mu.Lock()
	if s.closed {
		// Close ran while the subscribe was in flight; it already popped and
		// closed the shard, which drops the server-side subscription.
		s.mu.Unlock()
		return pool.ErrClosed
	}
	for _, ch := range chs {
		s.chanShard[ch] = addr
		delete(s.pending, ch)
	}
	// Connection is established to the correct node, so it is safe to start
	// forwarding messages from this shard. Starting earlier would let the
	// forwarder's Receive call PubSub.conn with no channels and connect to a
	// random node, racing the subscribe above for the connection.
	s.startForwarderLocked(addr)
	s.mu.Unlock()
	return nil
}

// startForwarderLocked starts a forwarder goroutine for the shard at addr if
// the user-facing channel is active and no forwarder is already running for
// that shard. The caller must hold s.mu and must only call this after the
// shard's connection has been established (i.e. after a successful subscribe).
func (s *ShardedPubSub) startForwarderLocked(addr string) {
	// Never start a forwarder on a closed ShardedPubSub: Close may have
	// already waited for the forwarders and closed msgCh, so a late forwarder
	// could panic sending on the closed channel.
	if s.closed || s.msgCh == nil {
		return
	}
	if _, ok := s.forwarding[addr]; ok {
		return
	}
	ps, ok := s.shards[addr]
	if !ok {
		return
	}
	s.forwarding[addr] = struct{}{}
	s.forwarders.Add(1)
	go s.forwardMessages(ps, s.chOpts)
}

// markPending marks the given channels as pending so the next topology pass
// retries their subscription. Channels that are no longer tracked are skipped.
func (s *ShardedPubSub) markPending(channels []string) {
	s.mu.Lock()
	if !s.closed {
		for _, ch := range channels {
			if _, ok := s.chanShard[ch]; ok {
				s.pending[ch] = struct{}{}
			}
		}
	}
	s.mu.Unlock()
}

// cleanupUnreferencedShards closes and removes every shard no tracked channel
// points at. Closing the connection also drops any server-side subscriptions
// that may linger on it (e.g. after a failed unsubscribe or a migration), so
// unreferenced shards never keep delivering messages.
// The caller must hold opMu and must not hold mu.
func (s *ShardedPubSub) cleanupUnreferencedShards() {
	var toClose []*PubSub
	s.mu.Lock()
	if !s.closed {
		referenced := make(map[string]struct{}, len(s.chanShard))
		for _, addr := range s.chanShard {
			referenced[addr] = struct{}{}
		}
		for addr, ps := range s.shards {
			if _, ok := referenced[addr]; !ok {
				toClose = append(toClose, ps)
				delete(s.shards, addr)
				delete(s.forwarding, addr)
			}
		}
	}
	s.mu.Unlock()
	for _, ps := range toClose {
		_ = ps.Close()
	}
}

// rebuildShard tears down the shard at addr and re-subscribes every channel
// mapped to it on freshly resolved nodes. Channels it cannot recover stay
// mapped and pending, so the next topology pass retries them.
// The caller must hold opMu and must not hold mu.
func (s *ShardedPubSub) rebuildShard(ctx context.Context, addr string) error {
	// Detach the shard and collect its channels under mu; close the old
	// connection outside the lock. Closing drops every server-side
	// subscription that was on it.
	var old *PubSub
	var channels []string
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return pool.ErrClosed
	}
	if ps, ok := s.shards[addr]; ok {
		old = ps
		delete(s.shards, addr)
	}
	delete(s.forwarding, addr)
	for ch, a := range s.chanShard {
		if a == addr {
			channels = append(channels, ch)
			s.pending[ch] = struct{}{}
		}
	}
	s.mu.Unlock()

	if old != nil {
		_ = old.Close()
	}
	if len(channels) == 0 {
		return nil
	}

	// Re-resolve against current topology and re-subscribe. Channels whose
	// resolution or subscribe fails stay mapped and pending for the next
	// topology pass.
	groups, firstErr := s.resolveChannels(ctx, channels)
	for newAddr, chs := range groups {
		if err := s.subscribeOnShard(ctx, newAddr, chs); err != nil {
			if errors.Is(err, pool.ErrClosed) {
				return err
			}
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
}

// SSubscribe subscribes to the given shard channels. Channels are automatically
// routed to the correct cluster node based on their hash slot.
//
// On failure the channels remain tracked and marked pending: one immediate
// reactive rebuild of the failing shard is attempted, and every later cluster
// topology reload retries the pending subscriptions. The first error is
// returned so the caller knows the operation was not (fully) applied yet.
func (s *ShardedPubSub) SSubscribe(ctx context.Context, channels ...string) error {
	s.opMu.Lock()
	defer s.opMu.Unlock()

	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return pool.ErrClosed
	}

	groups, firstErr := s.resolveChannels(ctx, channels)

	// Attempt every group rather than bailing out on the first failure, so a
	// single failing shard does not prevent the channels in other groups from
	// being subscribed. The first error is returned to the caller.
	for addr, chs := range groups {
		err := s.subscribeOnShard(ctx, addr, chs)
		if err == nil {
			continue
		}
		if errors.Is(err, pool.ErrClosed) {
			return pool.ErrClosed
		}
		// Track the channels as pending on this shard so topology passes
		// retry them even if the reactive rebuild below fails too.
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return pool.ErrClosed
		}
		for _, ch := range chs {
			s.chanShard[ch] = addr
			s.pending[ch] = struct{}{}
		}
		s.mu.Unlock()
		// Reactive reconnect: tear the shard down and retry once against a
		// freshly resolved topology.
		if reErr := s.rebuildShard(ctx, addr); reErr != nil {
			if firstErr == nil {
				firstErr = err // keep the original subscribe error
			}
			if errors.Is(reErr, pool.ErrClosed) {
				return firstErr
			}
		}
	}

	s.cleanupUnreferencedShards()
	return firstErr
}

// SUnsubscribe unsubscribes from the given shard channels. With no channels it
// unsubscribes from all currently-tracked channels, mirroring
// PubSub.SUnsubscribe.
//
// Mappings are only dropped after a successful unsubscribe, so a failure does
// not desync the local bookkeeping from the server (where the subscription and
// its forwarder may still be active). All shard groups are attempted; the
// first error is returned.
func (s *ShardedPubSub) SUnsubscribe(ctx context.Context, channels ...string) error {
	s.opMu.Lock()
	defer s.opMu.Unlock()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return pool.ErrClosed
	}
	if len(channels) == 0 {
		channels = make([]string, 0, len(s.chanShard))
		for ch := range s.chanShard {
			channels = append(channels, ch)
		}
	}
	groups := make(map[string][]string)
	for _, ch := range channels {
		if addr, ok := s.chanShard[ch]; ok {
			groups[addr] = append(groups[addr], ch)
		}
	}
	s.mu.Unlock()

	var firstErr error
	for addr, chs := range groups {
		s.mu.Lock()
		ps, ok := s.shards[addr]
		s.mu.Unlock()

		if ok {
			if err := ps.SUnsubscribe(ctx, chs...); err != nil {
				// Keep this group's mappings (and its shard) in place so our
				// bookkeeping stays consistent with the server, where the
				// subscription may still be active. Record the error and move
				// on to the remaining groups.
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
		}
		// Drop the mapping only after the unsubscribe succeeded (or when
		// there is no live shard connection to unsubscribe from).
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			return pool.ErrClosed
		}
		for _, ch := range chs {
			delete(s.chanShard, ch)
			delete(s.pending, ch)
		}
		s.mu.Unlock()
	}

	// Close shards that no longer carry any channels so we don't leak idle
	// connections and forwarders.
	s.cleanupUnreferencedShards()
	return firstErr
}

// Channel returns a Go channel that receives messages from all shards.
// The channel is closed when Close is called.
// ChannelOption values (e.g. WithChannelSize) are forwarded to each
// underlying PubSub shard when forwardMessages calls ps.Channel(opts...).
func (s *ShardedPubSub) Channel(opts ...ChannelOption) <-chan *Message {
	s.chOnce.Do(func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		// If the ShardedPubSub was already closed, hand back a closed channel
		// so consumers ranging over it return immediately instead of blocking
		// forever on a channel that Close will never close.
		if s.closed {
			s.msgCh = make(chan *Message)
			close(s.msgCh)
			return
		}

		// Determine the channel buffer size. We default to 100 but respect
		// WithChannelSize if provided. We apply all options to a single probe
		// (matching newChannel in pubsub.go) so later options don't reset the
		// values set by earlier ones.
		size := 100
		if len(opts) > 0 {
			probe := &channel{chanSize: 100}
			for _, opt := range opts {
				opt(probe)
			}
			size = probe.chanSize
		}

		s.msgCh = make(chan *Message, size)
		s.chOpts = opts
		// Start forwarding from all existing shards. These shards have already
		// been subscribed, so their connections point at the correct nodes.
		for addr := range s.shards {
			s.startForwarderLocked(addr)
		}
	})
	return s.msgCh
}

// forwardMessages reads from a single PubSub's channel and forwards to the
// multiplexed channel.
func (s *ShardedPubSub) forwardMessages(ps *PubSub, opts []ChannelOption) {
	defer s.forwarders.Done()
	ch := ps.Channel(opts...)
	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				return
			}
			select {
			case s.msgCh <- msg:
			case <-s.exit:
				return
			}
		case <-s.exit:
			return
		}
	}
}

// onTopologyChange is called (via the cluster's coalesced notifier goroutine)
// when the cluster topology changes. It re-resolves all tracked channels and
// migrates subscriptions to the correct nodes, and retries any pending or
// connection-less subscriptions.
func (s *ShardedPubSub) onTopologyChange() {
	ctx, cancel := context.WithTimeout(context.Background(), shardedPubSubMigrationTimeout)
	defer cancel()

	s.opMu.Lock()
	defer s.opMu.Unlock()

	// Snapshot under mu. opMu guarantees no other operation mutates the
	// bookkeeping while this pass runs, so the snapshot stays authoritative
	// for the whole pass.
	s.mu.Lock()
	if s.closed || len(s.chanShard) == 0 {
		s.mu.Unlock()
		return
	}
	snapshot := make(map[string]string, len(s.chanShard))
	for ch, addr := range s.chanShard {
		snapshot[ch] = addr
	}
	pending := make(map[string]struct{}, len(s.pending))
	for ch := range s.pending {
		pending[ch] = struct{}{}
	}
	liveShards := make(map[string]struct{}, len(s.shards))
	for addr := range s.shards {
		liveShards[addr] = struct{}{}
	}
	s.mu.Unlock()

	// Re-resolve all channels without holding any lock. Only channels that
	// actually need work are collected: moved to another node, pending, or
	// mapped to an address with no live shard connection.
	migrations := make(map[string]map[string][]string) // oldAddr -> newAddr -> channels
	for ch, oldAddr := range snapshot {
		newAddr, err := s.nodeAddrForChannel(ctx, ch)
		if err != nil {
			// Could not resolve the channel's node (e.g. transient state
			// reload failure). Keep the existing mapping and log so the
			// stale routing is diagnosable; a later reload will retry.
			internal.Logger.Printf(ctx,
				"redis: sharded pubsub: failed to resolve node for channel %q during topology change: %v",
				ch, err)
			continue
		}
		_, isPending := pending[ch]
		_, hasShard := liveShards[oldAddr]
		if newAddr == oldAddr && !isPending && hasShard {
			continue
		}
		if migrations[oldAddr] == nil {
			migrations[oldAddr] = make(map[string][]string)
		}
		migrations[oldAddr][newAddr] = append(migrations[oldAddr][newAddr], ch)
	}

	if len(migrations) == 0 {
		return
	}

	for oldAddr, targets := range migrations {
		for newAddr, channels := range targets {
			// Subscribe on the new shard first so there is never a window
			// with no active subscription. Until the old subscription is
			// dropped below the consumer may briefly receive duplicates.
			if err := s.subscribeOnShard(ctx, newAddr, channels); err != nil {
				if errors.Is(err, pool.ErrClosed) {
					return
				}
				// Keep the old mapping and subscription intact (no message
				// gap) and mark the channels pending so a later reload
				// retries the migration.
				internal.Logger.Printf(ctx,
					"redis: sharded pubsub: failed to subscribe channels %v on new shard %s during topology change: %v",
					channels, newAddr, err)
				s.markPending(channels)
				continue
			}

			if newAddr == oldAddr {
				// Pending/connection-less retry on the same node; nothing to
				// unsubscribe.
				continue
			}

			// The new subscription is live (the mapping now points at
			// newAddr); drop the old one.
			s.mu.Lock()
			oldPS, ok := s.shards[oldAddr]
			s.mu.Unlock()
			if ok {
				if err := oldPS.SUnsubscribe(ctx, channels...); err != nil {
					// The old shard still has the migrated channels subscribed,
					// so the user would receive duplicate messages until the
					// next reload. The new subscription is already active, so
					// we can safely rebuild the old shard: closing its
					// connection terminates every server-side subscription on
					// it, and any channels that legitimately remain on oldAddr
					// are re-subscribed on a fresh connection.
					internal.Logger.Printf(ctx,
						"redis: sharded pubsub: failed to unsubscribe channels %v from old shard %s during topology change, rebuilding it: %v",
						channels, oldAddr, err)
					if reErr := s.rebuildShard(ctx, oldAddr); reErr != nil {
						if errors.Is(reErr, pool.ErrClosed) {
							return
						}
						internal.Logger.Printf(ctx,
							"redis: sharded pubsub: failed to rebuild old shard %s after unsubscribe error during topology change: %v",
							oldAddr, reErr)
					}
				}
			}
		}
	}

	// Close shards that no longer carry any channels (e.g. fully migrated
	// away) so we don't leak idle connections and forwarders.
	s.cleanupUnreferencedShards()
}

// Close closes all underlying PubSub connections and the message channel.
//
// Close deliberately does not take opMu, so it stays responsive even when a
// subscribe/unsubscribe/migration operation is stuck in network I/O; such an
// in-flight operation observes the closed state at its next bookkeeping step
// and aborts.
func (s *ShardedPubSub) Close() error {
	// Deregister before acquiring the lock — deregister doesn't need it
	// and doing it first avoids holding s.mu longer than necessary.
	s.cluster.deregisterShardedPubSub(s)

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return pool.ErrClosed
	}
	s.closed = true
	close(s.exit)
	shards := make([]*PubSub, 0, len(s.shards))
	for addr, ps := range s.shards {
		shards = append(shards, ps)
		delete(s.shards, addr)
	}
	// Capture the current msgCh under the lock. If a first-ever Channel()
	// call races us below, it will observe s.closed and create+close its own
	// channel; by closing only this captured reference we avoid double-closing
	// the same channel.
	msgCh := s.msgCh
	s.mu.Unlock()

	var firstErr error
	for _, ps := range shards {
		if err := ps.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	// Wait for all forwarder goroutines to exit before closing msgCh to avoid
	// a send-on-closed-channel panic. No forwarder can start after this
	// point: startForwarderLocked checks closed, which was set under mu above.
	s.forwarders.Wait()
	if msgCh != nil {
		close(msgCh)
	}

	return firstErr
}

// Ping sends a PING to all underlying shard PubSub connections and doubles as
// a readiness probe: it returns an error while any tracked subscription is
// still pending or there is no live shard connection for tracked channels.
// Returns the first error encountered, if any.
func (s *ShardedPubSub) Ping(ctx context.Context, payload ...string) error {
	// Snapshot the shards under the lock, then perform the Ping I/O without
	// holding s.mu so a slow/blocked shard does not head-of-line-block
	// SSubscribe/SUnsubscribe/migration work.
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return pool.ErrClosed
	}
	type shardPing struct {
		addr string
		ps   *PubSub
	}
	shards := make([]shardPing, 0, len(s.shards))
	for addr, ps := range s.shards {
		shards = append(shards, shardPing{addr: addr, ps: ps})
	}
	tracked := len(s.chanShard)
	pendingCount := len(s.pending)
	s.mu.Unlock()

	if pendingCount > 0 {
		return fmt.Errorf("redis: sharded pubsub: %d subscription(s) pending", pendingCount)
	}
	// Channels are tracked but every shard connection is gone (e.g. all
	// initial subscribes failed): report it instead of a false-positive nil.
	if len(shards) == 0 && tracked > 0 {
		return fmt.Errorf("redis: sharded pubsub: %d channel(s) tracked but no live shard connections", tracked)
	}

	for _, sh := range shards {
		if err := sh.ps.Ping(ctx, payload...); err != nil {
			return fmt.Errorf("shard %s ping failed: %w", sh.addr, err)
		}
	}
	return nil
}

// ReceiveMessage waits for a message from any shard.
// This is a blocking call that reads from the multiplexed message channel,
// initializing it with default options if Channel has not been called yet.
func (s *ShardedPubSub) ReceiveMessage(ctx context.Context) (*Message, error) {
	msgCh := s.Channel()

	select {
	case msg, ok := <-msgCh:
		if !ok {
			return nil, pool.ErrClosed
		}
		return msg, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ReceiveTimeout is like ReceiveMessage but with a timeout.
func (s *ShardedPubSub) ReceiveTimeout(ctx context.Context, timeout time.Duration) (*Message, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return s.ReceiveMessage(ctx)
}
