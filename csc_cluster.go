package redis

// CSCStats sums statistics of node clients currently in the registry. Removed
// nodes are not retained for accounting; their counters disappear with them.
//
// Experimental: this API may change in a minor release.
func (c *ClusterClient) CSCStats() CSCStats {
	var total CSCStats
	nodes, _ := c.nodes.All()
	for _, node := range nodes {
		s := node.Client.CSCStats()
		total.Hits += s.Hits
		total.Misses += s.Misses
		total.Entries += s.Entries
		total.MemoryUsageBytes += s.MemoryUsageBytes
	}
	return total
}

// CSCRefreshStats sums refresh and invalidation counters of current node clients.
//
// Experimental: this API may change in a minor release.
func (c *ClusterClient) CSCRefreshStats() CSCRefreshStats {
	var total CSCRefreshStats
	nodes, _ := c.nodes.All()
	for _, node := range nodes {
		s := node.Client.CSCRefreshStats()
		total.Enqueued += s.Enqueued
		total.Dropped += s.Dropped
		total.Refreshed += s.Refreshed
		total.RefreshFailed += s.RefreshFailed
		total.DemandFlushes += s.DemandFlushes
		total.Invalidations += s.Invalidations
		total.Deletions += s.Deletions
		total.DeletionsNoop += s.DeletionsNoop
	}
	return total
}

func (c *ClusterClient) autopipelineCSCActive() bool {
	if c.opt.ClientSideCacheConfig != nil {
		return true
	}
	// A custom factory can enable node-local CSC without Cluster cache config.
	// Inspect existing clients only; never discover nodes or retain a second registry.
	if !c.opt.cscCustomFactory {
		return false
	}
	c.nodes.mu.RLock()
	defer c.nodes.mu.RUnlock()
	for _, node := range c.nodes.nodes {
		if node.Client.autopipelineCSCActive() {
			return true
		}
	}
	return false
}
