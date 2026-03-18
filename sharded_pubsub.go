package redis

import (
	"context"
	"sync"

	"github.com/redis/go-redis/v9/internal/hashtag"
	"github.com/redis/go-redis/v9/internal/pool"
)

// ShardedPubSub wraps multiple PubSub connections to support sharded pub/sub
// across different cluster nodes. In a Redis cluster, channels hash to
// different slots which may be served by different nodes. A single connection
// can only receive messages for channels on the shard it is connected to.
// ShardedPubSub transparently manages one PubSub per shard and multiplexes
// all messages into a single Go channel.
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
}

func newShardedPubSub(cluster *ClusterClient) *ShardedPubSub {
	sps := &ShardedPubSub{
		cluster:   cluster,
		shards:    make(map[string]*PubSub),
		chanShard: make(map[string]string),
		exit:      make(chan struct{}),
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
// or creates a new one.
func (s *ShardedPubSub) getOrCreateShard(addr string) *PubSub {
	if ps, ok := s.shards[addr]; ok {
		return ps
	}

	// Create a new PubSub via the cluster's pubSub factory.
	ps := s.cluster.pubSub()
	s.shards[addr] = ps

	// If the message channel is already initialized, start forwarding
	// messages from this new shard.
	if s.msgCh != nil {
		go s.forwardMessages(ps)
	}

	return ps
}

// SSubscribe subscribes to the given shard channels. Channels are automatically
// routed to the correct cluster node based on their hash slot.
func (s *ShardedPubSub) SSubscribe(ctx context.Context, channels ...string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return pool.ErrClosed
	}

	// Group channels by target node address.
	groups := make(map[string][]string)
	for _, ch := range channels {
		addr, err := s.nodeAddrForChannel(ctx, ch)
		if err != nil {
			return err
		}
		groups[addr] = append(groups[addr], ch)
		s.chanShard[ch] = addr
	}

	// Subscribe on the correct shard for each group.
	for addr, chs := range groups {
		ps := s.getOrCreateShard(addr)
		if err := ps.SSubscribe(ctx, chs...); err != nil {
			// Reactive reconnect: close failed shard, re-resolve, and retry once.
			if reconnErr := s.resubscribeShard(ctx, addr); reconnErr != nil {
				return err // return original error if reconnect also fails
			}
		}
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
func (s *ShardedPubSub) Channel(opts ...ChannelOption) <-chan *Message {
	s.chOnce.Do(func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		s.msgCh = make(chan *Message, 100)
		// Start forwarding from all existing shards.
		for _, ps := range s.shards {
			go s.forwardMessages(ps)
		}
	})
	return s.msgCh
}

// forwardMessages reads from a single PubSub's channel and forwards to the
// multiplexed channel.
func (s *ShardedPubSub) forwardMessages(ps *PubSub) {
	ch := ps.Channel()
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
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed || len(s.chanShard) == 0 {
		return
	}

	ctx := context.Background()

	// Re-resolve all channels and check for migrations.
	migrations := make(map[string]map[string][]string) // oldAddr -> newAddr -> channels
	for ch, oldAddr := range s.chanShard {
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

	// Apply migrations.
	for oldAddr, targets := range migrations {
		for newAddr, channels := range targets {
			// Unsubscribe from old shard.
			if ps, ok := s.shards[oldAddr]; ok {
				_ = ps.SUnsubscribe(ctx, channels...)
			}

			// Subscribe on new shard.
			ps := s.getOrCreateShard(newAddr)
			_ = ps.SSubscribe(ctx, channels...)

			// Update channel->shard mapping.
			for _, ch := range channels {
				s.chanShard[ch] = newAddr
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
			if ps, ok := s.shards[oldAddr]; ok {
				_ = ps.Close()
				delete(s.shards, oldAddr)
			}
		}
	}
}

// resubscribeShard closes a failed shard connection, re-resolves the channels
// that were on it, and re-subscribes them on fresh connections.
func (s *ShardedPubSub) resubscribeShard(ctx context.Context, failedAddr string) error {
	// Close the failed shard.
	if ps, ok := s.shards[failedAddr]; ok {
		_ = ps.Close()
		delete(s.shards, failedAddr)
	}

	// Collect channels that were on the failed shard.
	var channels []string
	for ch, addr := range s.chanShard {
		if addr == failedAddr {
			channels = append(channels, ch)
		}
	}

	// Re-resolve and re-subscribe.
	groups := make(map[string][]string)
	for _, ch := range channels {
		newAddr, err := s.nodeAddrForChannel(ctx, ch)
		if err != nil {
			return err
		}
		groups[newAddr] = append(groups[newAddr], ch)
		s.chanShard[ch] = newAddr
	}

	for addr, chs := range groups {
		ps := s.getOrCreateShard(addr)
		if err := ps.SSubscribe(ctx, chs...); err != nil {
			return err
		}
	}

	return nil
}

// Close closes all underlying PubSub connections and the message channel.
func (s *ShardedPubSub) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.closed {
		return pool.ErrClosed
	}
	s.closed = true
	close(s.exit)

	// Deregister from the cluster client.
	s.cluster.deregisterShardedPubSub(s)

	var firstErr error
	for addr, ps := range s.shards {
		if err := ps.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
		delete(s.shards, addr)
	}

	// Give a moment for forwarders to drain, then close the channel.
	if s.msgCh != nil {
		close(s.msgCh)
	}

	return firstErr
}
