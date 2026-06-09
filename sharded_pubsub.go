package redis

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/hashtag"
	"github.com/redis/go-redis/v9/internal/pool"
)

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
// migrates the affected subscriptions to the new owning node.
type ShardedPubSub struct {
	cluster *ClusterClient

	mu     sync.Mutex
	shards map[string]*PubSub // addr -> PubSub
	// Track which PubSub each channel is on
	chanShard map[string]string // channel -> addr

	closed bool
	exit   chan struct{}

	chOnce sync.Once
	msgCh  chan *Message
	chOpts []ChannelOption
	// forwarding tracks which shard addresses already have a forwarder
	// goroutine running, so we never start more than one per shard.
	forwarding map[string]struct{}
	forwarders sync.WaitGroup
}

func newShardedPubSub(cluster *ClusterClient) *ShardedPubSub {
	sps := &ShardedPubSub{
		cluster:    cluster,
		shards:     make(map[string]*PubSub),
		chanShard:  make(map[string]string),
		forwarding: make(map[string]struct{}),
		exit:       make(chan struct{}),
	}
	cluster.registerShardedPubSub(sps)
	return sps
}

// nodeAddrForChannel returns the cluster node address responsible for a channel's slot.
func (s *ShardedPubSub) nodeAddrForChannel(ctx context.Context, channel string) (string, error) {
	slot := hashtag.Slot(channel)
	if s.cluster.opt.ReadOnly {
		state, err := s.cluster.state.Get(ctx)
		if err != nil {
			return "", err
		}
		node, err := s.cluster.slotReadOnlyNode(state, slot)
		if err != nil {
			return "", err
		}
		return node.Client.opt.Addr, nil
	}
	node, err := s.cluster.slotMasterNode(ctx, slot)
	if err != nil {
		return "", err
	}
	return node.Client.opt.Addr, nil
}

// getOrCreateShard returns an existing PubSub for the given node address,
// or creates a new one. The caller must hold s.mu.
//
// It deliberately does not start a forwarder goroutine. Forwarding must only
// begin after the shard's connection has been established to the correct node
// via a successful SSubscribe; otherwise the forwarder's Receive would call
// PubSub.conn with no channels and connect to a random node, racing with the
// caller's SSubscribe for the connection. Use startForwarderLocked after a
// successful subscribe instead.
func (s *ShardedPubSub) getOrCreateShard(addr string) *PubSub {
	if ps, ok := s.shards[addr]; ok {
		return ps
	}

	// Create a new PubSub via the cluster's pubSub factory.
	ps := s.cluster.pubSub()
	s.shards[addr] = ps
	return ps
}

// startForwarderLocked starts a forwarder goroutine for the shard at addr if
// the user-facing channel is active and no forwarder is already running for
// that shard. The caller must hold s.mu and must only call this after the
// shard's connection has been established (i.e. after a successful subscribe).
func (s *ShardedPubSub) startForwarderLocked(addr string) {
	if s.msgCh == nil {
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

// removeShardLocked closes the shard at addr (if any), removes it from the
// shard map and clears its forwarding marker so a future shard created at the
// same address can start a fresh forwarder. The caller must hold s.mu.
func (s *ShardedPubSub) removeShardLocked(addr string) {
	if ps, ok := s.shards[addr]; ok {
		_ = ps.Close()
		delete(s.shards, addr)
	}
	delete(s.forwarding, addr)
}

// SSubscribe subscribes to the given shard channels. Channels are automatically
// routed to the correct cluster node based on their hash slot.
func (s *ShardedPubSub) SSubscribe(ctx context.Context, channels ...string) error {
	// Check closed state first (quick path).
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return pool.ErrClosed
	}
	s.mu.Unlock()

	// Resolve node addresses without holding the mutex to avoid deadlock:
	// nodeAddrForChannel -> state.Get -> Reload -> onReload -> onTopologyChange
	// which also needs to acquire s.mu.
	groups := make(map[string][]string)
	addrMap := make(map[string]string) // channel -> addr
	// Resolve every channel rather than aborting on the first failure: a single
	// unresolvable channel must not prevent the channels that did resolve from
	// being subscribed. The first resolution error is carried into firstErr and
	// returned to the caller.
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
		addrMap[ch] = addr
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return pool.ErrClosed
	}

	for ch, addr := range addrMap {
		s.chanShard[ch] = addr
	}

	// Subscribe on the correct shard for each group. We attempt every group
	// rather than bailing out on the first failure, so a single failing shard
	// does not leave channels in other groups unsubscribed while their
	// chanShard entries remain. The first error is returned to the caller.
	for addr, chs := range groups {
		ps := s.getOrCreateShard(addr)
		if err := ps.SSubscribe(ctx, chs...); err != nil {
			// Reactive reconnect: close failed shard, re-resolve, and retry once.
			// resubscribeShard starts forwarders for the shards it recreates.
			if reconnErr := s.resubscribeShard(ctx, addr); reconnErr != nil {
				if firstErr == nil {
					firstErr = err // keep the original error
				}
				// These channels could not be subscribed; drop their mappings
				// so they don't point at a shard with no live connection.
				for _, ch := range chs {
					if s.chanShard[ch] == addr {
						delete(s.chanShard, ch)
					}
				}
			}
			continue
		}
		// Connection is now established to the correct node, so it is safe to
		// start forwarding messages from this shard.
		s.startForwarderLocked(addr)
	}

	return firstErr
}

// SUnsubscribe unsubscribes from the given shard channels.
func (s *ShardedPubSub) SUnsubscribe(ctx context.Context, channels ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return pool.ErrClosed
	}

	// With no channels, SUnsubscribe targets every currently-tracked channel,
	// mirroring PubSub.SUnsubscribe ("unsubscribe from all").
	if len(channels) == 0 {
		channels = make([]string, 0, len(s.chanShard))
		for ch := range s.chanShard {
			channels = append(channels, ch)
		}
	}

	// Group channels by their known shard. The mapping is intentionally left
	// in place here and only removed after a successful unsubscribe below, so
	// that a failed SUnsubscribe does not desync our bookkeeping from the
	// server (where the subscription and its forwarder may still be active).
	groups := make(map[string][]string)
	for _, ch := range channels {
		if addr, ok := s.chanShard[ch]; ok {
			groups[addr] = append(groups[addr], ch)
		}
	}

	// Attempt every shard group rather than bailing out on the first failure.
	// Returning early would leave earlier groups unsubscribed-and-untracked
	// while later groups stay both subscribed and tracked, with no signal to
	// the caller that the unsubscribe only partially completed. We unsubscribe
	// as many groups as possible and report the first error.
	var firstErr error
	for addr, chs := range groups {
		if ps, ok := s.shards[addr]; ok {
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
		// Drop the mapping only after the unsubscribe succeeded (or when there
		// is no live shard connection to unsubscribe from).
		for _, ch := range chs {
			delete(s.chanShard, ch)
		}
		// If the shard has no remaining channels, close it so we don't leak an
		// idle connection and forwarder, matching onTopologyChange's invariant
		// of one PubSub per active shard.
		hasChannels := false
		for _, a := range s.chanShard {
			if a == addr {
				hasChannels = true
				break
			}
		}
		if !hasChannels {
			s.removeShardLocked(addr)
		}
	}

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

// onTopologyChange is called by the ClusterClient when the cluster topology
// changes. It re-resolves all subscribed channels and migrates subscriptions
// to the correct nodes.
func (s *ShardedPubSub) onTopologyChange() {
	ctx := context.Background()

	// Take a snapshot of current channel->shard mappings without holding
	// the lock during node address resolution, which may trigger further
	// state reloads and re-enter through notifyShardedPubSubs.
	s.mu.Lock()
	if s.closed || len(s.chanShard) == 0 {
		s.mu.Unlock()
		return
	}
	snapshot := make(map[string]string, len(s.chanShard))
	for ch, addr := range s.chanShard {
		snapshot[ch] = addr
	}
	s.mu.Unlock()

	// Re-resolve all channels outside the lock.
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
		if newAddr != oldAddr {
			if migrations[oldAddr] == nil {
				migrations[oldAddr] = make(map[string][]string)
			}
			migrations[oldAddr][newAddr] = append(migrations[oldAddr][newAddr], ch)
		}
	}

	if len(migrations) == 0 {
		return
	}

	// Apply migrations under the lock.
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return
	}

	for oldAddr, targets := range migrations {
		for newAddr, channels := range targets {
			// Subscribe on the new shard first. If this fails we keep the old
			// subscription and mapping intact so messages are never dropped.
			ps := s.getOrCreateShard(newAddr)
			if err := ps.SSubscribe(ctx, channels...); err != nil {
				// Subscribing on the new node failed. Keep the old mapping so
				// a later reload retries the migration, but log it and drop a
				// freshly-created empty shard so we don't pin an idle
				// connection to a node that may not serve these slots.
				internal.Logger.Printf(ctx,
					"redis: sharded pubsub: failed to subscribe channels %v on new shard %s during topology change: %v",
					channels, newAddr, err)
				hasNew := false
				for _, addr := range s.chanShard {
					if addr == newAddr {
						hasNew = true
						break
					}
				}
				if !hasNew {
					s.removeShardLocked(newAddr)
				}
				continue
			}
			// Connection to the new node is established; start forwarding.
			s.startForwarderLocked(newAddr)

			// Now that the new shard is subscribed, unsubscribe from the old
			// one so we never have a window with no active subscription.
			if oldPS, ok := s.shards[oldAddr]; ok {
				if err := oldPS.SUnsubscribe(ctx, channels...); err != nil {
					// The old shard still has the migrated channels subscribed,
					// so without intervention the user would receive duplicate
					// messages (once from oldAddr, once from newAddr) until the
					// next reload. The new subscription is already active, so we
					// can safely drop the old connection entirely: closing it
					// terminates every server-side subscription on it. Any
					// channels that legitimately remain on oldAddr are restored
					// via resubscribeShard, which rebuilds a fresh connection.
					internal.Logger.Printf(ctx,
						"redis: sharded pubsub: failed to unsubscribe channels %v from old shard %s during topology change, rebuilding it: %v",
						channels, oldAddr, err)
					// Move the migrated channels off oldAddr first so
					// resubscribeShard does not try to restore them there.
					for _, ch := range channels {
						if _, exists := s.chanShard[ch]; exists {
							s.chanShard[ch] = newAddr
						}
					}
					// resubscribeShard releases and re-acquires s.mu; that is
					// safe here because we still hold it and have no per-iteration
					// state that must survive the unlock.
					if reErr := s.resubscribeShard(ctx, oldAddr); reErr != nil {
						internal.Logger.Printf(ctx,
							"redis: sharded pubsub: failed to rebuild old shard %s after unsubscribe error during topology change: %v",
							oldAddr, reErr)
					}
					// resubscribeShard already removed oldAddr and remapped its
					// remaining channels, so skip the rest of this iteration.
					continue
				}
			}

			// Update channel->shard mapping only for channels that still exist
			// (a concurrent SUnsubscribe may have removed some while the lock
			// was released during address resolution).
			for _, ch := range channels {
				if _, exists := s.chanShard[ch]; exists {
					s.chanShard[ch] = newAddr
				}
			}
		}

		// If old shard has no more channels, close it.
		hasChannels := false
		for _, addr := range s.chanShard {
			if addr == oldAddr {
				hasChannels = true
				break
			}
		}
		if !hasChannels {
			s.removeShardLocked(oldAddr)
		}
	}
}

// resubscribeShard closes a failed shard connection, re-resolves the channels
// that were on it, and re-subscribes them on fresh connections.
// The caller must hold s.mu. The lock is temporarily released during address
// resolution to avoid deadlock (nodeAddrForChannel -> state.Get -> Reload ->
// onReload -> onTopologyChange -> s.mu).
func (s *ShardedPubSub) resubscribeShard(ctx context.Context, failedAddr string) error {
	// Close the failed shard and clear its forwarding marker.
	s.removeShardLocked(failedAddr)

	// Collect channels that were on the failed shard.
	var channels []string
	for ch, addr := range s.chanShard {
		if addr == failedAddr {
			channels = append(channels, ch)
		}
	}

	// Release lock before resolving addresses to avoid deadlock.
	s.mu.Unlock()

	groups := make(map[string][]string)
	addrMap := make(map[string]string)
	// Resolve every channel rather than aborting on the first failure: a single
	// unresolvable channel must not abandon the other channels that were on the
	// failed shard. The first resolution error is carried into firstErr and
	// returned to the caller after every channel has been attempted, mirroring
	// the tolerance pattern in SSubscribe.
	var firstErr error
	for _, ch := range channels {
		newAddr, err := s.nodeAddrForChannel(ctx, ch)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		groups[newAddr] = append(groups[newAddr], ch)
		addrMap[ch] = newAddr
	}

	// Re-acquire lock for state mutations.
	s.mu.Lock()

	if s.closed {
		return pool.ErrClosed
	}

	// Try every target group rather than bailing out on the first failure.
	// Returning early would leave channels in not-yet-processed groups pointing
	// at the removed shard address with no connection, orphaning them until a
	// full resubscribe. We re-subscribe as many channels as possible and report
	// the first error so the caller knows the operation was not fully clean.
	for addr, chs := range groups {
		ps := s.getOrCreateShard(addr)
		if err := ps.SSubscribe(ctx, chs...); err != nil {
			if firstErr == nil {
				firstErr = err
			}
			// Drop the freshly created (but unusable) shard so a later attempt
			// recreates it against current topology. These channels still point
			// at the removed failedAddr; the sweep after the loop drops those
			// stale mappings so they are not treated as live subscriptions.
			s.removeShardLocked(addr)
			continue
		}
		// Connection is established; start forwarding from the fresh shard.
		s.startForwarderLocked(addr)
		// Only update the mapping on successful subscribe, and only for
		// channels that still exist (a concurrent SUnsubscribe may have
		// removed some while the lock was released during resolution).
		for _, ch := range chs {
			if _, exists := s.chanShard[ch]; exists {
				s.chanShard[ch] = addrMap[ch]
			}
		}
	}

	// Channels that failed to resolve above (firstErr set, not in any group)
	// still point at the now-removed failedAddr. Drop those stale mappings too
	// so they are not treated as live subscriptions with no connection.
	if firstErr != nil {
		for ch, addr := range s.chanShard {
			if addr == failedAddr {
				delete(s.chanShard, ch)
			}
		}
	}

	return firstErr
}

// Close closes all underlying PubSub connections and the message channel.
func (s *ShardedPubSub) Close() error {
	// Deregister before acquiring the lock — deregister doesn't need it
	// and doing it first avoids holding s.mu longer than necessary.
	s.cluster.deregisterShardedPubSub(s)

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return pool.ErrClosed
	}
	s.closed = true
	close(s.exit)

	var firstErr error
	for addr, ps := range s.shards {
		if err := ps.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(s.shards, addr)
	}

	// Capture the current msgCh under the lock. If a first-ever Channel() call
	// races us during the forwarders.Wait() window below, it will observe
	// s.closed and create+close its own channel; by closing only this captured
	// reference we avoid double-closing the same channel.
	msgCh := s.msgCh

	// Wait for all forwarder goroutines to exit before closing msgCh
	// to avoid a send-on-closed-channel panic.
	s.mu.Unlock()
	s.forwarders.Wait()
	s.mu.Lock()

	if msgCh != nil {
		close(msgCh)
	}

	return firstErr
}

// Ping sends a PING to all underlying shard PubSub connections.
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
	s.mu.Unlock()

	for _, sh := range shards {
		if err := sh.ps.Ping(ctx, payload...); err != nil {
			return fmt.Errorf("shard %s ping failed: %w", sh.addr, err)
		}
	}
	return nil
}

// ReceiveMessage waits for a message from any shard.
// This is a blocking call that reads from the multiplexed message channel.
// Channel() must be called before ReceiveMessage.
func (s *ShardedPubSub) ReceiveMessage(ctx context.Context) (*Message, error) {
	// Read msgCh under the lock: Channel() writes it under s.mu, so an
	// unsynchronized read here would be a data race.
	s.mu.Lock()
	msgCh := s.msgCh
	s.mu.Unlock()
	if msgCh == nil {
		return nil, fmt.Errorf("redis: Channel() must be called before ReceiveMessage")
	}

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
