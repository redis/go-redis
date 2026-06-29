package redis

// cacheStatsReporter is an optional interface a Cache implementation may
// satisfy to expose hit/miss counters. The built-in localCache does; user
// implementations are not required to.
type cacheStatsReporter interface {
	Stats() (hits, misses uint64)
}

// CSCStats returns cumulative (hits, misses) for this client's client-side
// cache, regardless of the configured CSCStrategy:
//
//   - CSCStrategyPerConnection: aggregated across the per-connection caches.
//   - CSCStrategyBroadcast / CSCStrategySharedTracking: from the shared cache,
//     when its implementation exposes stats (the built-in localCache does).
//
// Returns (0,0) when CSC is not configured or stats are unavailable.
func (c *Client) CSCStats() (hits, misses uint64) {
	if c == nil {
		return 0, 0
	}
	if c.baseClient.cscPerConnEnabled() {
		h, m, _ := c.baseClient.perConnStats()
		return h, m
	}
	if c.baseClient.csc == nil {
		return 0, 0
	}
	if r, ok := c.baseClient.csc.(cacheStatsReporter); ok {
		return r.Stats()
	}
	return 0, 0
}
