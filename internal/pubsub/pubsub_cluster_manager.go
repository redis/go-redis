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
	"github.com/redis/go-redis/v9/internal/proto"
)

// nodeHooks adapts the ClusterHooks to the Hooks of one owned manager:
// the one pinned to addr, or the broadcast manager when addr is empty.
type nodeHooks struct {
	m    *ClusterManager
	addr string
}

var _ Hooks = nodeHooks{}

func (h nodeHooks) NewConn(ctx context.Context, addr string) (*pool.Conn, error) {
	return h.m.hooks.NewConn(ctx, addr)
}

func (h nodeHooks) CloseConn(cn *pool.Conn) error {
	return h.m.hooks.CloseConn(cn)
}

func (h nodeHooks) ProcessPush(ctx context.Context, cn *pool.Conn, rd *proto.Reader) error {
	return h.m.hooks.ProcessPush(ctx, h.addr, cn, rd)
}

func (h nodeHooks) IsBadConn(err error, allowTimeout bool) bool {
	return h.m.hooks.IsBadConn(err, allowTimeout, h.addr)
}

func (h nodeHooks) RequestTopologyRefresh() {
	h.m.hooks.RequestReload()
}

func (h nodeHooks) ShouldReplaceConn(cn *pool.Conn) bool {
	return h.m.hooks.ShouldReplaceConn(cn)
}

// ResolveAddress keeps the broadcast manager on the empty address: a
// handoff still retires its connection (ShouldReplaceConn), but the
// redial goes to any healthy node — that satisfies "leave this
// endpoint", where following the handoff endpoint would pin the
// broadcast to one node for good.
func (h nodeHooks) ResolveAddress(prev *pool.Conn, current string) string {
	if h.addr == "" {
		return ""
	}
	return h.m.hooks.ResolveAddress(prev, current)
}

// ClusterManager routes pub/sub subscriptions over a Redis Cluster.
// Regular and pattern subscriptions multiplex over one shared broadcast
// connection (an owned Manager dialing any healthy node); sharded
// pub/sub is slot-routed over one owned Manager per involved node
// (created lazily, pruned when emptied), fanned back into one delivery
// stream by a composite handle (see clusterShardHandle). After every
// cluster state reload (ScheduleReroute), live shard subscriptions are
// re-routed to their current slot owners.
type ClusterManager struct {
	// cfg seeds every owned manager's Config, with Addr rewritten per
	// node. Guarded by mu (the PubSubConfiger updates mutate it).
	cfg   Config
	hooks ClusterHooks

	broadcast *Manager

	// mu guards the node managers, handles, cfg and closed flag. It is a
	// leaf lock: never held while taking a node manager's or a handle's
	// lock, so the hooks (RequestReload in particular) may call back
	// into the manager from under those.
	mu sync.Mutex
	// nodeManagers holds one owned manager per node addr, created lazily
	// by subscribeOnNode and pruned once emptied (via CloseIfIdle).
	nodeManagers map[string]*Manager
	// handles are the live composite handles, re-routed after state
	// reloads.
	handles map[*clusterShardHandle]struct{}
	closed  bool

	// rerouteMu serializes re-route sweeps, and the composite handles'
	// SSubscribe/SUnsubscribe with them (see clusterShardHandle.SSubscribe).
	rerouteMu sync.Mutex
}

var _ PubSubManagerer = (*ClusterManager)(nil)

// NewClusterManager creates a cluster pub/sub manager wired to the
// cluster through hooks. cfg must arrive with defaults already applied;
// its Addr is ignored. Nothing is dialed until the first subscribe.
func NewClusterManager(cfg Config, hooks ClusterHooks) *ClusterManager {
	m := &ClusterManager{
		cfg:   cfg,
		hooks: hooks,

		nodeManagers: make(map[string]*Manager),
		handles:      make(map[*clusterShardHandle]struct{}),
	}
	m.broadcast = m.newNodeManager("")
	return m
}

// newNodeManager builds an owned Manager pinned to addr (or to "any
// healthy node" when addr is empty — the broadcast manager; see
// nodeHooks). Callers must hold m.mu, or be the constructor.
func (m *ClusterManager) newNodeManager(addr string) *Manager {
	cfg := m.cfg
	cfg.Addr = addr
	return NewManager(cfg, nodeHooks{m: m, addr: addr})
}

// consumerSettings snapshots the delivery-channel settings composite
// handles and their consumer views are built with.
func (m *ClusterManager) consumerSettings() (chanSize int, sendTimeout time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cfg.ChanSize, m.cfg.SendTimeout
}

// logInterval snapshots the log throttling interval.
func (m *ClusterManager) logInterval() time.Duration {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.cfg.LogInterval
}

// managers snapshots the broadcast and every owned node manager.
// Callers must hold m.mu.
func (m *ClusterManager) managersLocked() []*Manager {
	all := make([]*Manager, 0, 1+len(m.nodeManagers))
	all = append(all, m.broadcast)
	return append(all, slices.Collect(maps.Values(m.nodeManagers))...)
}

// updateAll mutates the seed config and forwards the same update to
// every owned manager, broadcast included. The managers are updated
// with m.mu released (see ClusterManager.mu).
func (m *ClusterManager) updateAll(mutate func(*Config), apply func(*Manager)) {
	m.mu.Lock()
	mutate(&m.cfg)
	all := m.managersLocked()
	m.mu.Unlock()
	for _, nm := range all {
		apply(nm)
	}
}

func (m *ClusterManager) updateChannelSize(newSize int) {
	if newSize <= 0 {
		return // see Manager.updateChannelSize
	}
	m.updateAll(
		func(c *Config) { c.ChanSize = newSize },
		func(nm *Manager) { nm.updateChannelSize(newSize) },
	)
}

func (m *ClusterManager) updateSendTimeout(newTimeout time.Duration) {
	m.updateAll(
		func(c *Config) { c.SendTimeout = newTimeout },
		func(nm *Manager) { nm.updateSendTimeout(newTimeout) },
	)
}

func (m *ClusterManager) updatePingTimeout(newTimeout time.Duration) {
	m.updateAll(
		func(c *Config) { c.PingTimeout = newTimeout },
		func(nm *Manager) { nm.updatePingTimeout(newTimeout) },
	)
}

func (m *ClusterManager) updateHealthCheckInterval(newInterval time.Duration) {
	m.updateAll(
		func(c *Config) { c.HealthCheckInterval = newInterval },
		func(nm *Manager) { nm.updateHealthCheckInterval(newInterval) },
	)
}

func (m *ClusterManager) updateReconnectTimeout(newTimeout time.Duration) {
	m.updateAll(
		func(c *Config) { c.ReconnectTimeout = newTimeout },
		func(nm *Manager) { nm.updateReconnectTimeout(newTimeout) },
	)
}

// NewHandle returns an empty composite handle; on a closed manager it
// is born closed.
func (m *ClusterManager) NewHandle() PubSuber {
	h := newClusterShardHandle(m)
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		h.state = shardHandleClosed
		close(h.closing)
		close(h.done)
		close(h.events)
	} else {
		m.handles[h] = struct{}{}
	}
	return h
}

// newSubscribedHandle is the subscribe-family entry point on the
// manager: a fresh composite handle subscribed through subscribe. On
// failure the handle is closed and nothing is returned, like
// Manager.Subscribe (a caller gets no handle to retry with).
func (m *ClusterManager) newSubscribedHandle(subscribe func(h *clusterShardHandle) error) (PubSuber, error) {
	ph := m.NewHandle()
	h := ph.(*clusterShardHandle)
	if err := subscribe(h); err != nil {
		_ = h.Close()
		return nil, err
	}
	return h, nil
}

// Subscribe subscribes to the given channels over the shared broadcast
// connection and returns a composite handle receiving them.
func (m *ClusterManager) Subscribe(ctx context.Context, channels ...string) (PubSuber, error) {
	return m.newSubscribedHandle(func(h *clusterShardHandle) error {
		_, err := h.Subscribe(ctx, channels...)
		return err
	})
}

// PSubscribe subscribes to the given patterns over the shared broadcast
// connection and returns a composite handle receiving them.
func (m *ClusterManager) PSubscribe(ctx context.Context, patterns ...string) (PubSuber, error) {
	return m.newSubscribedHandle(func(h *clusterShardHandle) error {
		_, err := h.PSubscribe(ctx, patterns...)
		return err
	})
}

// SSubscribe subscribes to the given shard channels, slot-routed to
// their owning nodes, and returns a composite handle receiving them.
func (m *ClusterManager) SSubscribe(ctx context.Context, channels ...string) (PubSuber, error) {
	return m.newSubscribedHandle(func(h *clusterShardHandle) error {
		_, err := h.SSubscribe(ctx, channels...)
		return err
	})
}

// Unsubscribe removes channel subscriptions across all handles;
// UNSUBSCRIBE is sent for channels left with no subscriber.
func (m *ClusterManager) Unsubscribe(ctx context.Context, channels ...string) error {
	return m.broadcast.Unsubscribe(ctx, channels...)
}

// PUnsubscribe removes pattern subscriptions across all handles;
// PUNSUBSCRIBE is sent for patterns left with no subscriber.
func (m *ClusterManager) PUnsubscribe(ctx context.Context, patterns ...string) error {
	return m.broadcast.PUnsubscribe(ctx, patterns...)
}

// SUnsubscribe removes shard-channel subscriptions across all handles
// (all of them when none are given), routed through the live handles —
// after a slot migration the current slot owner is not necessarily the
// node a channel lives on. Node managers left empty are pruned.
func (m *ClusterManager) SUnsubscribe(ctx context.Context, channels ...string) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return pool.ErrClosed
	}
	handles := slices.Collect(maps.Keys(m.handles))
	m.mu.Unlock()

	var firstErr error
	for _, h := range handles {
		if err := h.SUnsubscribe(ctx, channels...); err != nil && !errors.Is(err, pool.ErrClosed) && firstErr == nil {
			firstErr = err
		}
	}
	m.prune()
	return firstErr
}

// Ping pings the broadcast connection, dialing it if needed.
func (m *ClusterManager) Ping(ctx context.Context, payload ...string) error {
	return m.broadcast.Ping(ctx, payload...)
}

// ClientSetName names the broadcast connection (see
// Manager.ClientSetName).
func (m *ClusterManager) ClientSetName(ctx context.Context, name string) error {
	return m.broadcast.ClientSetName(ctx, name)
}

// RecheckConn asks every owned manager to re-evaluate ShouldReplaceConn
// on its live connection (see Manager.RecheckConn). It does not block.
func (m *ClusterManager) RecheckConn() {
	m.mu.Lock()
	all := m.managersLocked()
	m.mu.Unlock()
	for _, nm := range all {
		nm.RecheckConn()
	}
}

// Close closes every live composite handle, every owned node manager
// and the broadcast manager. Subsequent calls return pool.ErrClosed.
func (m *ClusterManager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return pool.ErrClosed
	}
	m.closed = true
	handles := slices.Collect(maps.Keys(m.handles))
	m.handles = make(map[*clusterShardHandle]struct{})
	nodeManagers := slices.Collect(maps.Values(m.nodeManagers))
	m.nodeManagers = make(map[string]*Manager)
	m.mu.Unlock()

	var firstErr error
	for _, h := range handles {
		if err := h.Close(); err != nil && !errors.Is(err, pool.ErrClosed) && firstErr == nil {
			firstErr = err
		}
	}
	for _, nm := range nodeManagers {
		if err := nm.Close(); err != nil && !errors.Is(err, pool.ErrClosed) && firstErr == nil {
			firstErr = err
		}
	}
	if err := m.broadcast.Close(); err != nil && !errors.Is(err, pool.ErrClosed) && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

// nodeManager returns addr's owned manager, creating it lazily; nil
// once the cluster manager closed.
func (m *ClusterManager) nodeManager(addr string) *Manager {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	nm, ok := m.nodeManagers[addr]
	if !ok {
		nm = m.newNodeManager(addr)
		m.nodeManagers[addr] = nm
	}
	return nm
}

// forgetNodeManager drops addr's entry if it still is nm (a closed
// manager a prune raced us to).
func (m *ClusterManager) forgetNodeManager(addr string, nm *Manager) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.nodeManagers[addr] == nm {
		delete(m.nodeManagers, addr)
	}
}

// subscribeOnNode subscribes the given shard channels on a fresh handle
// of addr's owned manager, creating the manager lazily. A prune may
// close the manager between lookup and subscribe; the resulting
// ErrClosed retries with a fresh manager. Any other error — a failed
// dial or write — comes back WITH the handle: the engine keeps the
// registration for its reconnect replay, so the caller attaches the
// part and the subscription establishes itself once the node is back
// (or the re-route sweep moves it). A nil handle means the cluster
// manager closed.
func (m *ClusterManager) subscribeOnNode(ctx context.Context, addr string, channels map[string]struct{}) (PubSuber, error) {
	names := slices.Sorted(maps.Keys(channels))
	for {
		nm := m.nodeManager(addr)
		if nm == nil {
			return nil, pool.ErrClosed
		}
		part := nm.NewHandle()
		_, err := part.SSubscribe(ctx, names...)
		if errors.Is(err, pool.ErrClosed) {
			m.forgetNodeManager(addr, nm)
			continue
		}
		return part, err
	}
}

// prune closes and removes owned node managers with no subscriptions
// left (CloseIfIdle never kills a manager with live handles). A
// subscribeOnNode racing the close finds ErrClosed and retries with a
// fresh manager.
func (m *ClusterManager) prune() {
	m.mu.Lock()
	snapshot := maps.Clone(m.nodeManagers)
	m.mu.Unlock()
	for addr, nm := range snapshot {
		if nm.CloseIfIdle() {
			m.forgetNodeManager(addr, nm)
		}
	}
}

// dropShardHandle deregisters a handle whose delivery stream ended and
// prunes node managers its parts may have emptied.
func (m *ClusterManager) dropShardHandle(h *clusterShardHandle) {
	m.mu.Lock()
	delete(m.handles, h)
	m.mu.Unlock()
	m.prune()
}

// ScheduleReroute triggers an asynchronous re-route sweep; the cluster
// client wires it to its state-reload notification. It does not block.
func (m *ClusterManager) ScheduleReroute() {
	m.mu.Lock()
	n := len(m.handles)
	closed := m.closed
	m.mu.Unlock()
	if n == 0 || closed {
		return
	}
	go m.rerouteShards(context.Background())
}

// rerouteShards recomputes every live shard subscription's owner and
// moves the channels whose owner changed; sweeps are serialized,
// idempotent full recomputes. Emptied node managers are pruned
// afterwards.
func (m *ClusterManager) rerouteShards(ctx context.Context) {
	m.rerouteMu.Lock()
	defer m.rerouteMu.Unlock()

	m.mu.Lock()
	handles := slices.Collect(maps.Keys(m.handles))
	m.mu.Unlock()

	var failed bool
	for _, h := range handles {
		if err := h.reroute(ctx); err != nil {
			failed = true
			internal.Logger.Printf(ctx, "redis: pubsub: shard subscription re-route failed: %v", err)
		}
	}
	if failed {
		// Request the reload that triggers the retrying sweep — nothing
		// else would in a pubsub-only workload; the cluster throttles it.
		m.hooks.RequestReload()
	}
	m.prune()
}
