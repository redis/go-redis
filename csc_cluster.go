package redis

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"weak"
)

// Storage, tracking and reservation safety stay with Client. Cluster only gates
// routed access and clears affected caches after accepted topology changes.
// This is deliberately not an ownership epoch. Absence-dependent replies are
// excluded by the cluster reply policy, including for background fetches.
type clusterCSC struct {
	client        weak.Pointer[ClusterClient]
	trustInterval time.Duration
	mu            sync.Mutex
	closed        atomic.Bool
	enabled       atomic.Bool
	freshness     atomic.Pointer[time.Time]
	lifetimes     map[*clusterCSCNode]struct{}
	// Topology cleanup keeps at most one pending flush per node.
	pending        map[*clusterCSCNode]struct{}
	wake           chan struct{}
	stop           chan struct{}
	done           chan struct{}
	started        bool
	statsRevision  uint64
	retired        CSCStats
	retiredRefresh CSCRefreshStats
}

type clusterCSCNode struct {
	owner   *clusterCSC
	base    *baseClient
	closed  atomic.Bool
	primary atomic.Bool
	dirty   atomic.Bool
	// Immutable handles let a collected Cluster wrapper stop node workers
	// without retaining Client or racing teardown's mutable worker pointers.
	drain  *cscDrainHandle
	misses *cscMissCoalescer
}

type (
	// The value is the permitted *clusterCSCNode, or a typed nil to force
	// bypass across retries. The routing snapshot stays under its own key.
	clusterCSCContextKey struct{}
	clusterCSCRouteKey   struct{}
)

func newClusterCSC(c *ClusterClient) *clusterCSC {
	g := &clusterCSC{
		client: weak.Make(c), trustInterval: c.opt.ClusterStateReloadInterval,
		lifetimes: make(map[*clusterCSCNode]struct{}),
		pending:   make(map[*clusterCSCNode]struct{}), wake: make(chan struct{}, 1),
		stop: make(chan struct{}), done: make(chan struct{}),
	}
	g.enabled.Store(c.opt.ClientSideCacheConfig != nil)
	runtime.AddCleanup(c, func(g *clusterCSC) { g.signalClose() }, g)
	return g
}

// bind runs before the standalone constructor starts background CSC work.
func (n *clusterCSCNode) bind(c *baseClient) { n.base = c }

func (g *clusterCSC) register(n *clusterCSCNode, c *Client) {
	g.mu.Lock()
	// A factory may return a pool-sharing clone. Workers belong to the
	// canonical base bound during initialization, not the returned wrapper.
	if n.base != nil {
		n.drain = n.base.cscDrainHandle
		n.misses = n.base.cscMissCoalescer.Load()
	}
	g.lifetimes[n] = struct{}{}
	g.statsRevision++
	if c.csc != nil {
		g.enabled.Store(true)
	}
	g.mu.Unlock()
	// Transfer counters only after workers stop, so concurrent snapshots
	// neither omit closing nodes nor count their final counters twice.
	c.onClose.register("cluster-csc-statistics", func() error { g.finishNode(n); return nil })
}

func (g *clusterCSC) fresh(t time.Time) bool {
	return !g.closed.Load() && time.Since(t) <= g.trustInterval
}

func (n *clusterCSCNode) ready() bool {
	return n != nil && !n.closed.Load() && !n.dirty.Load() && n.primary.Load() &&
		n.base != nil && n.base.cscActive != nil && n.base.cscActive.Load()
}

func (n *clusterCSCNode) usable() bool {
	if !n.ready() {
		return false
	}
	t := n.owner.freshness.Load()
	return t != nil && n.owner.fresh(*t)
}

func (c *baseClient) clusterCSCAllowed(ctx context.Context) bool {
	n := c.opt.clusterCSC
	if n == nil {
		return true
	}
	allowed, _ := ctx.Value(clusterCSCContextKey{}).(*clusterCSCNode)
	if allowed != n || !n.ready() {
		return false
	}
	// Admission checks the captured route's age. Hit/publication checks use the
	// latest accepted topology, so do not check both timestamps here on every hit.
	state, _ := ctx.Value(clusterCSCRouteKey{}).(*clusterState)
	return state != nil && n.owner.fresh(state.createdAt)
}

func clusterCSCBypass(ctx context.Context) context.Context {
	return context.WithValue(ctx, clusterCSCContextKey{}, (*clusterCSCNode)(nil))
}

func (c *ClusterClient) cscNodeContext(ctx context.Context, n *clusterNode, cmd Cmder) context.Context {
	if !c.csc.enabled.Load() {
		return ctx
	}
	if allowed, ok := ctx.Value(clusterCSCContextKey{}).(*clusterCSCNode); ok && allowed == nil {
		return ctx
	}
	state, _ := ctx.Value(clusterCSCRouteKey{}).(*clusterState)
	control := n.Client.opt.clusterCSC
	if state == nil || control == nil {
		return clusterCSCBypass(ctx)
	}
	// Endpoint roles can differ by slot in proxy/custom maps.
	entry := state.slotEntry(c.cmdSlot(cmd, -1))
	if entry == nil || len(entry.nodes) == 0 || entry.nodes[0] != n {
		return clusterCSCBypass(ctx)
	}
	return context.WithValue(ctx, clusterCSCContextKey{}, control)
}

func (c *ClusterClient) cscRoutingState(ctx context.Context) (*clusterState, error) {
	if state, _ := ctx.Value(clusterCSCRouteKey{}).(*clusterState); state != nil {
		return state, nil
	}
	return c.state.Get(ctx)
}

// A single worker keeps custom cache callbacks outside topology publication
// locks. Redirects do not schedule cache cleanup.
func (g *clusterCSC) scheduleFlushLocked(n *clusterCSCNode) {
	if n == nil || n.base == nil || n.base.csc == nil || g.closed.Load() || n.closed.Load() {
		return
	}
	n.dirty.Store(true)
	g.pending[n] = struct{}{}
	if !g.started {
		g.started = true
		go g.cleanup()
	}
	select {
	case g.wake <- struct{}{}:
	default:
	}
}

func clusterCSCOwners(state *clusterState) [16384]*clusterNode {
	var owners [16384]*clusterNode
	if state != nil {
		for _, slot := range state.slots {
			if len(slot.nodes) == 0 {
				continue
			}
			for i := slot.start; i <= slot.end && i < len(owners); i++ {
				if i >= 0 {
					owners[i] = slot.nodes[0]
				}
			}
		}
	}
	return owners
}

// publish runs under the holder's publication lock. Cache callbacks run later,
// outside both locks, while affected nodes bypass lookup and population.
func (g *clusterCSC) publish(state *clusterState) bool {
	c := g.client.Value()
	if c == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed.Load() {
		return false
	}
	if !g.enabled.Load() {
		c.state.state.Store(state)
		return true
	}
	var old *clusterState
	if v := c.state.state.Load(); v != nil {
		old = v.(*clusterState)
	}
	before, after := clusterCSCOwners(old), clusterCSCOwners(state)
	affected := make(map[*clusterNode]bool)
	if old != nil {
		for i, a := range before {
			if a != after[i] {
				affected[a] = true
				affected[after[i]] = true
			}
		}
	}
	primary := make(map[*clusterNode]bool, len(state.Masters))
	present := make(map[*clusterNode]bool, len(state.Masters)+len(state.Slaves))
	for _, node := range state.Masters {
		primary[node] = true
		present[node] = true
	}
	for _, node := range state.Slaves {
		present[node] = true
	}
	if old != nil {
		oldPrimary := make(map[*clusterNode]bool, len(old.Masters))
		for node, id := range old.identities {
			if state.identities[node] != id || !present[node] {
				affected[node] = true
			}
		}
		for _, node := range old.Masters {
			oldPrimary[node] = true
			if !primary[node] {
				affected[node] = true
			}
			if n := node.Client.opt.clusterCSC; n != nil && !present[node] {
				n.primary.Store(false)
			}
		}
		for _, node := range old.Slaves {
			if primary[node] && !oldPrimary[node] {
				affected[node] = true
			}
		}
	}
	for node := range affected {
		if node != nil {
			g.scheduleFlushLocked(node.Client.opt.clusterCSC)
		}
	}
	for node := range present {
		if n := node.Client.opt.clusterCSC; n != nil {
			n.primary.Store(primary[node])
		}
	}
	t := state.createdAt
	g.freshness.Store(&t)
	c.state.state.Store(state)
	return true
}

// Background CSC work stays node-local. A permanent redirect can still prompt
// discovery when its foreground caller has already gone away.
func (c *baseClient) reloadCSCOnMoved(err error) {
	n := c.opt.clusterCSC
	if err == nil || n == nil || n.closed.Load() || n.owner.closed.Load() {
		return
	}
	if moved, _, _ := isMovedError(err); moved {
		if cluster := n.owner.client.Value(); cluster != nil {
			cluster.state.LazyReload()
		}
	}
}

func (g *clusterCSC) cleanup() {
	defer close(g.done)
	for {
		select {
		case <-g.stop:
			return
		case <-g.wake:
		}
		for {
			g.mu.Lock()
			var n *clusterCSCNode
			for node := range g.pending {
				n = node
				delete(g.pending, node)
				break
			}
			g.mu.Unlock()
			if n == nil {
				break
			}
			if !g.closed.Load() && !n.closed.Load() {
				n.base.csc.Flush()
			}
			g.mu.Lock()
			// A topology change during the callback needs another pass.
			// Keep bypass enabled until that pass has completed too.
			if _, queued := g.pending[n]; !queued {
				n.dirty.Store(false)
			}
			g.mu.Unlock()
		}
	}
}

func (n *clusterCSCNode) close() { n.closed.Store(true) }

func (g *clusterCSC) signalClose() {
	g.mu.Lock()
	if g.closed.Swap(true) {
		g.mu.Unlock()
		return
	}
	for n := range g.lifetimes {
		n.close()
		if n.drain != nil {
			n.drain.signalStop()
		}
		if n.misses != nil {
			n.misses.stopWorkers()
		}
	}
	close(g.stop)
	g.mu.Unlock()
}

func (g *clusterCSC) close() {
	g.signalClose()
	g.mu.Lock()
	started := g.started
	g.mu.Unlock()
	if started {
		<-g.done
	}
}

func (g *clusterCSC) finishNode(n *clusterCSCNode) {
	n.close()
	stats, refresh := n.statistics()
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.lifetimes[n]; !ok {
		return
	}
	g.retired.Hits += stats.Hits
	g.retired.Misses += stats.Misses
	addCSCRefreshStats(&g.retiredRefresh, refresh)
	delete(g.lifetimes, n)
	delete(g.pending, n)
	g.statsRevision++
}

func addCSCRefreshStats(a *CSCRefreshStats, b CSCRefreshStats) {
	a.Enqueued += b.Enqueued
	a.Dropped += b.Dropped
	a.Refreshed += b.Refreshed
	a.RefreshFailed += b.RefreshFailed
	a.DemandFlushes += b.DemandFlushes
	a.Invalidations += b.Invalidations
	a.Deletions += b.Deletions
	a.DeletionsNoop += b.DeletionsNoop
}

func (g *clusterCSC) statistics() (CSCStats, CSCRefreshStats) {
	for {
		g.mu.Lock()
		version := g.statsRevision
		stats, refresh := g.retired, g.retiredRefresh
		nodes := make([]*clusterCSCNode, 0, len(g.lifetimes))
		for n := range g.lifetimes {
			nodes = append(nodes, n)
		}
		g.mu.Unlock()
		for _, n := range nodes {
			s, r := n.statistics()
			stats.Hits += s.Hits
			stats.Misses += s.Misses
			if !n.closed.Load() {
				stats.Entries += s.Entries
				stats.MemoryUsageBytes += s.MemoryUsageBytes
			}
			addCSCRefreshStats(&refresh, r)
		}
		g.mu.Lock()
		stable := version == g.statsRevision
		g.mu.Unlock()
		if stable {
			return stats, refresh
		}
	}
}

func (n *clusterCSCNode) statistics() (CSCStats, CSCRefreshStats) {
	// Background work must not retain the collectible Client wrapper.
	if n.base == nil {
		return CSCStats{}, CSCRefreshStats{}
	}
	c := Client{baseClient: n.base}
	return c.CSCStats(), c.CSCRefreshStats()
}

// CSCStats sums node-local cache statistics. Limits and residency are per node;
// hit/miss counters include nodes removed during this client's lifetime.
func (c *ClusterClient) CSCStats() CSCStats { s, _ := c.csc.statistics(); return s }

// CSCRefreshStats sums node-local refresh and invalidation counters, including
// final counters of removed nodes.
func (c *ClusterClient) CSCRefreshStats() CSCRefreshStats { _, s := c.csc.statistics(); return s }

func (c *ClusterClient) autopipelineCSCActive() bool {
	return c.csc.enabled.Load() && !c.csc.closed.Load()
}
