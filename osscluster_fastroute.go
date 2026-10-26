package redis

import "sync/atomic"

// The metadata-driven router (#4093) resolves a full routing decision for every
// command and stores it on the command with an atomic exchange. For a keyed command
// with the default resolver, that decision depends only on the command name and
// the metadata view: the same shared template comes back for every GET, SET, ...
// fastRouteMemo keeps those templates per name for the current view. A hit computes
// only the slot, so the hot path skips the decision and the per-command store.
//
// Only templates are kept (one key at a fixed position, no subcommands), and only
// with the default resolver and routing policies on. A new metadata view (live
// metadata, overrides) starts a new memo. The map is copy-on-write: readers take
// one atomic load.
type fastRouteMemo struct {
	view *commandMetadataView
	m    atomic.Pointer[map[string]*clusterRoutingDecision] // immutable, copy-on-write
}

func (c *ClusterClient) fastRouteEligible() bool {
	r := c.cmdInfoResolver
	return r != nil && r.legacyDefault && r.fallBackResolver == nil && !c.opt.DisableRoutingPolicies
}

func (c *ClusterClient) fastRouteLookup(cmd Cmder) *clusterRoutingDecision {
	if !c.fastRouteEligible() {
		return nil
	}
	memo := c.fastRoute.Load()
	if memo == nil || memo.view != c.metadataView() {
		return nil
	}
	mp := memo.m.Load()
	if mp == nil {
		return nil
	}
	t, ok := (*mp)[cmd.Name()]
	if !ok {
		return nil
	}
	if t.firstKey <= 0 || t.firstKey >= len(cmd.Args()) {
		return nil
	}
	slot := c.cmdSlotWithPos(cmd, t.firstKey, -1)
	if slot < 0 || slot >= 16384 {
		return nil
	}
	cmd.setCachedSlot(slot)
	return t
}

func (c *ClusterClient) fastRouteRemember(cmd Cmder, t *clusterRoutingDecision) {
	if !c.fastRouteEligible() || t.meta == nil || len(t.meta.keySpecs) != 1 || t.meta.keySpecs[0].lastKey != 0 {
		return
	}
	view := c.metadataView()
	if t.view != view {
		return
	}
	if _, parent := view.subcommandParents[cmd.Name()]; parent {
		return
	}
	memo := c.fastRoute.Load()
	if memo == nil || memo.view != view {
		c.fastRoute.CompareAndSwap(memo, &fastRouteMemo{view: view})
		if memo = c.fastRoute.Load(); memo == nil || memo.view != view {
			return
		}
	}
	for {
		old := memo.m.Load()
		if old != nil {
			if _, ok := (*old)[cmd.Name()]; ok {
				return
			}
		}
		nm := make(map[string]*clusterRoutingDecision, 8)
		if old != nil {
			for k, v := range *old {
				nm[k] = v
			}
		}
		nm[cmd.Name()] = t
		if memo.m.CompareAndSwap(old, &nm) {
			return
		}
	}
}
