package redis

import (
	"context"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"weak"
)

// Storage and tracking stay with Client. This coordinator only controls which
// ownership period a node cache may serve, and serializes physical cleanup.
type clusterCSC struct {
	client         weak.Pointer[ClusterClient]
	trustInterval  time.Duration
	mu             sync.Mutex
	closed         atomic.Bool
	enabled        atomic.Bool
	freshness      atomic.Pointer[time.Time]
	revision       uint64
	nextScope      uint64
	nodes          map[string]*clusterCSCNode
	lifetimes      map[*clusterCSCNode]struct{}
	pending        map[*clusterCSCNode]*clusterCSCScope
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
	addr    string
	base    *baseClient
	current atomic.Pointer[clusterCSCScope]
	closed  atomic.Bool
	flushMu sync.Mutex
	// Immutable handles allow dropped Cluster wrappers to signal node workers
	// without retaining Client or racing teardown's mutable worker pointers.
	drain  *cscDrainHandle
	misses *cscMissCoalescer
	// The fields below are protected by owner.mu.
	primary   bool
	namespace string
}

type clusterCSCScope struct {
	node   *clusterCSCNode
	prefix string
	active atomic.Bool
	// Cleanup/reload prerequisites are protected by owner.mu.
	clean       bool
	reloadAfter uint64
	reloadSeen  uint64
}

type clusterCSCToken struct {
	node   *clusterCSCNode
	scope  *clusterCSCScope
	state  *clusterState
	bypass bool
}

type (
	clusterCSCContextKey struct{}
	clusterCSCRouteKey   struct{}
)

func newClusterCSC(c *ClusterClient) *clusterCSC {
	g := &clusterCSC{
		client: weak.Make(c), trustInterval: c.opt.ClusterStateReloadInterval,
		nodes: make(map[string]*clusterCSCNode), lifetimes: make(map[*clusterCSCNode]struct{}),
		pending: make(map[*clusterCSCNode]*clusterCSCScope), wake: make(chan struct{}, 1),
		stop: make(chan struct{}), done: make(chan struct{}),
	}
	g.enabled.Store(c.opt.ClientSideCacheConfig != nil)
	runtime.AddCleanup(c, func(g *clusterCSC) { g.signalClose() }, g)
	return g
}

func (g *clusterCSC) scopeLocked(n *clusterCSCNode, clean bool) *clusterCSCScope {
	g.nextScope++
	return &clusterCSCScope{node: n, prefix: n.namespace + strconv.FormatUint(g.nextScope, 10) + "\x00", clean: clean}
}

// bind runs before the standalone constructor starts background CSC work.
func (n *clusterCSCNode) bind(c *baseClient) {
	g := n.owner
	g.mu.Lock()
	n.base = c
	n.namespace = c.cscKeyPrefix
	n.current.Store(g.scopeLocked(n, true))
	g.mu.Unlock()
}

func (g *clusterCSC) register(n *clusterCSCNode, c *Client) {
	g.mu.Lock()
	// A factory may return a pool-sharing clone. Worker lifecycle belongs to
	// the canonical base bound during initialization, not the returned wrapper.
	if n.base != nil {
		n.drain = n.base.cscDrainHandle
		n.misses = n.base.cscMissCoalescer.Load()
	}
	// The drainer owns refresher teardown. Its mutable handle may already be
	// cleared by self-disable, so it must not be captured during registration.
	g.nodes[n.addr] = n
	g.lifetimes[n] = struct{}{}
	g.statsRevision++
	if c.csc != nil {
		g.enabled.Store(true)
	}
	g.mu.Unlock()
	// Drainer/refresher teardown precedes onClose callbacks. Transfer counters
	// only then, so a closing node cannot disappear from a concurrent snapshot.
	c.onClose.register("cluster-csc-statistics", func() error { g.finishNode(n); return nil })
}

func (g *clusterCSC) observation() uint64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.revision
}

func (g *clusterCSC) fresh(t time.Time) bool {
	return !g.closed.Load() && time.Since(t) <= g.trustInterval
}

func (s *clusterCSCScope) usable() bool {
	if s == nil || !s.active.Load() || s.node.closed.Load() {
		return false
	}
	g := s.node.owner
	t := g.freshness.Load()
	return t != nil && g.fresh(*t) && s.node.base.cscActive != nil && s.node.base.cscActive.Load()
}

func (c *baseClient) clusterCSCScope(ctx context.Context) *clusterCSCScope {
	n := c.opt.clusterCSC
	if n == nil {
		return nil
	}
	t, _ := ctx.Value(clusterCSCContextKey{}).(*clusterCSCToken)
	if t == nil || t.bypass || t.node != n || t.state == nil || !n.owner.fresh(t.state.createdAt) || !t.scope.usable() {
		return nil
	}
	return t.scope
}

func clusterCSCBypass(ctx context.Context) context.Context {
	return context.WithValue(ctx, clusterCSCContextKey{}, &clusterCSCToken{bypass: true})
}

// A route snapshot and its scope are captured together. Retained node handles
// and clones never get a token implicitly.
func (c *ClusterClient) cscNodeContext(ctx context.Context, n *clusterNode, cmd Cmder) context.Context {
	if !c.csc.enabled.Load() {
		return ctx
	}
	if old, _ := ctx.Value(clusterCSCContextKey{}).(*clusterCSCToken); old != nil && old.bypass {
		return ctx
	}
	state, _ := ctx.Value(clusterCSCRouteKey{}).(*clusterState)
	control := n.Client.opt.clusterCSC
	if state == nil || control == nil {
		return clusterCSCBypass(ctx)
	}
	// Endpoint roles can differ by slot in custom/proxy maps. Validate the
	// actual selection rather than merely membership in state.Masters.
	slot := c.cmdSlot(cmd, -1)
	entry := state.slotEntry(slot)
	if entry == nil || len(entry.nodes) == 0 || entry.nodes[0] != n {
		return clusterCSCBypass(ctx)
	}
	return context.WithValue(ctx, clusterCSCContextKey{}, &clusterCSCToken{node: control, scope: state.cscScopes[n], state: state})
}

func (c *ClusterClient) cscRoutingState(ctx context.Context) (*clusterState, error) {
	if state, _ := ctx.Value(clusterCSCRouteKey{}).(*clusterState); state != nil {
		return state, nil
	}
	return c.state.Get(ctx)
}

func (g *clusterCSC) scheduleLocked(n *clusterCSCNode, s *clusterCSCScope) {
	if g.closed.Load() {
		return
	}
	g.pending[n] = s
	if !g.started {
		g.started = true
		go g.cleanup()
	}
	select {
	case g.wake <- struct{}{}:
	default:
	}
}

func (g *clusterCSC) rotateLocked(n *clusterCSCNode, reloadAfter uint64) *clusterCSCScope {
	old := n.current.Load()
	if old == nil || n.closed.Load() {
		return old
	}
	// Multiple readers of the same transition share one inactive successor.
	if !old.active.Load() && !old.clean {
		if reloadAfter > old.reloadAfter {
			old.reloadAfter = reloadAfter
		}
		return old
	}
	old.active.Store(false)
	next := g.scopeLocked(n, false)
	if old.reloadSeen < old.reloadAfter && old.reloadAfter > reloadAfter {
		reloadAfter = old.reloadAfter
	}
	next.reloadAfter = reloadAfter
	n.current.Store(next)
	g.scheduleLocked(n, next)
	return next
}

func (g *clusterCSC) activateLocked(s *clusterCSCScope) {
	if s != nil && s.node.current.Load() == s && s.clean && s.reloadSeen >= s.reloadAfter && s.node.primary && !s.node.closed.Load() && !g.closed.Load() {
		s.active.Store(true)
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

// publish is called under the holder's publication lock, never while loading
// topology from the network. Redirect retirement shares this short lock via mu.
func (g *clusterCSC) publish(state *clusterState, revision uint64) bool {
	c := g.client.Value()
	if c == nil {
		return false
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closed.Load() {
		return false
	}
	if revision < g.revision {
		return false
	}
	var old *clusterState
	if !g.enabled.Load() {
		c.state.state.Store(state)
		return true
	}
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
		}
		for _, node := range old.Slaves {
			if primary[node] && !oldPrimary[node] {
				affected[node] = true
			}
		}
	}
	state.cscScopes = make(map[*clusterNode]*clusterCSCScope, len(present))
	for node := range affected {
		if node != nil && node.Client.opt.clusterCSC != nil {
			g.rotateLocked(node.Client.opt.clusterCSC, 0)
		}
	}
	if old != nil {
		for node := range old.cscScopes {
			if !present[node] && node.Client.opt.clusterCSC != nil {
				node.Client.opt.clusterCSC.primary = false
			}
		}
	}
	for node := range present {
		n := node.Client.opt.clusterCSC
		if n == nil {
			continue
		}
		n.primary = primary[node]
		s := n.current.Load()
		if s != nil {
			s.reloadSeen = revision
			g.activateLocked(s)
		}
		state.cscScopes[node] = s
	}
	t := state.createdAt
	g.freshness.Store(&t)
	c.state.state.Store(state)
	return true
}

// Replace only the scope table: ASK and invalidation do not renew topology age.
func (g *clusterCSC) publishScopesLocked() {
	c := g.client.Value()
	if c == nil {
		return
	}
	v := c.state.state.Load()
	if v == nil {
		return
	}
	old := v.(*clusterState)
	state := *old
	state.cscScopes = make(map[*clusterNode]*clusterCSCScope, len(old.cscScopes))
	for node := range old.cscScopes {
		state.cscScopes[node] = node.Client.opt.clusterCSC.current.Load()
	}
	c.state.state.Store(&state)
}

func (n *clusterCSCNode) observe(err error) {
	if err == nil || err == Nil {
		return
	}
	moved, ask, addr := isMovedError(err)
	if !moved && !ask {
		return
	}
	g := n.owner
	g.mu.Lock()
	if g.closed.Load() {
		g.mu.Unlock()
		return
	}
	dst := g.nodes[addr]
	needsRetirement := func(node *clusterCSCNode) bool {
		if node == nil || node.closed.Load() {
			return false
		}
		s := node.current.Load()
		return s != nil && (s.active.Load() || s.clean || (moved && s.reloadAfter == 0))
	}
	if !needsRetirement(n) && (!moved || !needsRetirement(dst)) {
		g.mu.Unlock()
		return
	}
	g.revision++
	required := uint64(0)
	if moved {
		required = g.revision
	}
	g.rotateLocked(n, required)
	if moved {
		if dst != nil && dst != n {
			g.rotateLocked(dst, required)
		}
	}
	g.publishScopesLocked()
	g.mu.Unlock()
	if moved {
		if c := g.client.Value(); c != nil {
			c.state.LazyReload()
		}
	}
}

func (c *baseClient) observeCSCRedirect(err error) {
	if n := c.opt.clusterCSC; n != nil && n.base != nil {
		n.observe(err)
	}
}

func (n *clusterCSCNode) invalidateAll() {
	g := n.owner
	g.mu.Lock()
	g.rotateLocked(n, 0)
	g.publishScopesLocked()
	g.mu.Unlock()
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
			var s *clusterCSCScope
			for node, scope := range g.pending {
				n, s = node, scope
				delete(g.pending, node)
				break
			}
			g.mu.Unlock()
			if n == nil {
				break
			}
			if !g.closed.Load() && !n.closed.Load() && n.base.csc != nil {
				n.flushCache()
			}
			g.mu.Lock()
			if n.current.Load() == s {
				s.clean = true
				g.activateLocked(s)
			}
			g.mu.Unlock()
		}
	}
}

func (n *clusterCSCNode) flushCache() {
	n.flushMu.Lock()
	defer n.flushMu.Unlock()
	if n.base != nil && n.base.csc != nil {
		n.base.csc.Flush()
	}
}

func (n *clusterCSCNode) close() {
	n.closed.Store(true)
	if s := n.current.Load(); s != nil {
		s.active.Store(false)
	}
}

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
	if g.nodes[n.addr] == n {
		delete(g.nodes, n.addr)
	}
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

func (n *clusterCSCNode) refreshScope(key string) *clusterCSCScope {
	s := n.current.Load()
	if s.usable() && strings.HasPrefix(key, s.prefix) {
		return s
	}
	return nil
}
