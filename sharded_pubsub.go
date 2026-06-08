package redis

import (
	"context"
	"fmt"
	"sync"
	"time"

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
	for _, ch := range channels {
		addr, err := s.nodeAddrForChannel(ctx, ch)
		if err != nil {
			return err
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

	// Subscribe on the correct shard for each group.
	for addr, chs := range groups {
		ps := s.getOrCreateShard(addr)
		if err := ps.SSubscribe(ctx, chs...); err != nil {
			// Reactive reconnect: close failed shard, re-resolve, and retry once.
			// resubscribeShard starts forwarders for the shards it recreates.
			if reconnErr := s.resubscribeShard(ctx, addr); reconnErr != nil {
				return err // return original error if reconnect also fails
			}
			continue
		}
		// Connection is now established to the correct node, so it is safe to
		// start forwarding messages from this shard.
		s.startForwarderLocked(addr)
	}

	return nil
}

// SUnsubscribe unsubscribes from the given shard channels.
func (s *ShardedPubSub) SUnsubscribe(ctx context.Context, channels ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return pool.ErrClosed
	}

	// Group channels by their known shard.
	groups := make(map[string][]string)
	for _, ch := range channels {
		if addr, ok := s.chanShard[ch]; ok {
			groups[addr] = append(groups[addr], ch)
			delete(s.chanShard, ch)
		}
	}

	for addr, chs := range groups {
		if ps, ok := s.shards[addr]; ok {
			if err := ps.SUnsubscribe(ctx, chs...); err != nil {
				return err
			}
		}
	}

	return nil
}

// Channel returns a Go channel that receives messages from all shards.
// The channel is closed when Close is called.
// ChannelOption values (e.g. WithChannelSize) are forwarded to each
// underlying PubSub shard when forwardMessages calls ps.Channel(opts...).
func (s *ShardedPubSub) Channel(opts ...ChannelOption) <-chan *Message {
	s.chOnce.Do(func() {
		s.mu.Lock()
		defer s.mu.Unlock()

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
				continue
			}
			// Connection to the new node is established; start forwarding.
			s.startForwarderLocked(newAddr)

			// Now that the new shard is subscribed, unsubscribe from the old
			// one so we never have a window with no active subscription.
			if oldPS, ok := s.shards[oldAddr]; ok {
				_ = oldPS.SUnsubscribe(ctx, channels...)
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
	for _, ch := range channels {
		newAddr, err := s.nodeAddrForChannel(ctx, ch)
		if err != nil {
			s.mu.Lock()
			return err
		}
		groups[newAddr] = append(groups[newAddr], ch)
		addrMap[ch] = newAddr
	}

	// Re-acquire lock for state mutations.
	s.mu.Lock()

	if s.closed {
		return pool.ErrClosed
	}

	for addr, chs := range groups {
		ps := s.getOrCreateShard(addr)
		if err := ps.SSubscribe(ctx, chs...); err != nil {
			return err
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

	return nil
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

	// Wait for all forwarder goroutines to exit before closing msgCh
	// to avoid a send-on-closed-channel panic.
	s.mu.Unlock()
	s.forwarders.Wait()
	s.mu.Lock()

	if s.msgCh != nil {
		close(s.msgCh)
	}

	return firstErr
}

// Ping sends a PING to all underlying shard PubSub connections.
// Returns the first error encountered, if any.
func (s *ShardedPubSub) Ping(ctx context.Context, payload ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return pool.ErrClosed
	}

	for addr, ps := range s.shards {
		if err := ps.Ping(ctx, payload...); err != nil {
			return fmt.Errorf("shard %s ping failed: %w", addr, err)
		}
	}
	return nil
}

// ReceiveMessage waits for a message from any shard.
// This is a blocking call that reads from the multiplexed message channel.
// Channel() must be called before ReceiveMessage.
func (s *ShardedPubSub) ReceiveMessage(ctx context.Context) (*Message, error) {
	if s.msgCh == nil {
		return nil, fmt.Errorf("redis: Channel() must be called before ReceiveMessage")
	}

	select {
	case msg, ok := <-s.msgCh:
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
