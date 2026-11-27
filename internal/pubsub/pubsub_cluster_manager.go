package pubsub

import (
	"context"
	"errors"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
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
	// by nodeManager and pruned once emptied (via CloseIfIdle).
	nodeManagers map[string]*Manager
	// handles are the live composite handles, re-routed after state
	// reloads.
	handles map[*clusterShardHandle]struct{}
	closed  bool

	// sweeping and sweepPending coalesce re-route requests the way
	// clusterStateHolder.LazyReload coalesces reloads: at most one
	// sweeper goroutine runs (sweeping), and requests arriving while it
	// runs set sweepPending for it to serve with one more sweep after a
	// cooldown, instead of each starting a goroutine of its own to queue
	// behind it (see ScheduleReroute, sweep).
	sweeping     atomic.Bool
	sweepPending atomic.Bool
	// sweepCtx bounds the sweeper's work and is cancelled by Close: a
	// sweep parked in a dial, a resolve or a handle's lock then ends
	// instead of outliving the manager.
	sweepCtx    context.Context
	sweepCancel context.CancelFunc
	// sweepMu serializes sweeps with each other — the sweeper's with a
	// test's direct rerouteShards call. A sweep's work on one handle is
	// serialized with that handle's own operations by the handle's
	// operation lock (see clusterShardHandle.ops), never with another
	// handle's.
	sweepMu sync.Mutex
}

// sweepCooldown is the pause between a sweep and the one serving the
// requests that arrived during it (clusterStateHolder.LazyReload's).
const sweepCooldown = 200 * time.Millisecond

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
	m.sweepCtx, m.sweepCancel = context.WithCancel(context.Background())
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
	m.sweepCancel()

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

// prune closes and removes owned node managers with no subscriptions
// left (CloseIfIdle never kills a manager with live handles). A
// placement racing the close gets a handle born closed, finds ErrClosed
// on its write and retries with a fresh manager (see
// clusterShardHandle.writePlacement).
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

// ScheduleReroute requests a re-route sweep; the cluster client wires
// it to its state-reload notification. It does not block: the sweep
// runs on the sweeper goroutine, started here if none runs, and a
// request made while one runs is served by one more sweep after it
// (see sweep) — a sweep is an idempotent full recompute, so any number
// of requests during one collapse into that single follow-up.
func (m *ClusterManager) ScheduleReroute() {
	m.mu.Lock()
	n := len(m.handles)
	closed := m.closed
	m.mu.Unlock()
	if n == 0 || closed {
		return
	}
	// Pending is set before the start attempt: a sweeper that is on its
	// way out re-checks it after giving sweeping up, so the request is
	// served either by it or by the goroutine started here.
	m.sweepPending.Store(true)
	if m.sweeping.CompareAndSwap(false, true) {
		go m.sweep()
	}
}

// sweep is the sweeper goroutine: it sweeps, pauses for the cooldown,
// and sweeps again while requests arrived meanwhile — a failed move
// requests the reload whose notification lands here, so this is also
// the retry loop, throttled by the cooldown and the cluster's own
// reload throttle. It exits once no request is pending or the manager
// closed. Callers must hold sweeping (see ScheduleReroute).
func (m *ClusterManager) sweep() {
	ctx := m.sweepCtx
	for {
		m.sweepPending.Store(false)
		m.rerouteShards(ctx)
		if !waitCooldown(ctx, sweepCooldown) {
			m.sweeping.Store(false)
			return
		}
		if m.sweepPending.Load() {
			continue
		}
		// Give the flag up, then look once more: a request that landed
		// between the check above and this store found sweeping held and
		// started nothing, so it is ours to serve — unless another
		// starter got the flag first, in which case it is theirs.
		m.sweeping.Store(false)
		if !m.sweepPending.Load() || !m.sweeping.CompareAndSwap(false, true) {
			return
		}
	}
}

// waitCooldown pauses for d, reporting false if ctx ends first.
func waitCooldown(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// rerouteShards recomputes every live shard subscription's owner and
// moves the channels whose owner changed; sweeps are serialized,
// idempotent full recomputes. Emptied node managers are pruned
// afterwards. A ctx that ends mid-sweep (the manager closing) ends it
// quietly.
func (m *ClusterManager) rerouteShards(ctx context.Context) {
	m.sweepMu.Lock()
	defer m.sweepMu.Unlock()

	m.mu.Lock()
	handles := slices.Collect(maps.Keys(m.handles))
	m.mu.Unlock()

	var failed bool
	for _, h := range handles {
		if err := h.reroute(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
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
