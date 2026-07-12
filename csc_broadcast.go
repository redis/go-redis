package redis

import (
	"context"
)

// This file implements the hooks consulted by redis.go for the Broadcast
// client-side caching strategy. Broadcast disables CLIENT TRACKING ON on pool
// connections and instead routes ALL invalidation traffic through a single
// dedicated sidecar connection that issues CLIENT TRACKING ON BCAST.
//
// Motivation — the connection-lifecycle staleness window: with per-connection
// client tracking, a pool connection's server-side tracking table is dropped
// when it closes. Any keys it had read remain in the shared cache with no
// future invalidation until evicted by capacity or restamped by another conn.
// Broadcast closes that window: BCAST emits invalidations for every key in the
// DB regardless of which connection observed which key, so pool-conn lifecycle
// has zero effect on invalidation coverage.
//
// The shared Cache (localCache) is reused verbatim. Only the path that
// delivers invalidations to it changes.

// cscShouldTrackOnPoolConn returns false when the client is configured for
// the BCAST sidecar strategy: pool connections must not issue CLIENT
// TRACKING ON, because the sidecar owns the tracking subscription for the
// whole DB.
func cscShouldTrackOnPoolConn(c *baseClient) bool {
	if c == nil || c.opt == nil {
		return true
	}
	return c.opt.ClientSideCacheStrategy != CSCStrategyBroadcast
}

// cscAttachBroadcastSidecarIfNeeded starts the BCAST sidecar if the client has CSC
// enabled under CSCStrategyBroadcast. Returns nil otherwise.
//
// Requires c.csc != nil (i.e. attachCSC has already been called) and
// c.opt.Protocol == 3.
func cscAttachBroadcastSidecarIfNeeded(ctx context.Context, c *baseClient) error {
	if c == nil || c.opt == nil {
		return nil
	}
	if c.csc == nil || c.opt.Protocol != 3 {
		return nil
	}
	if c.opt.ClientSideCacheStrategy != CSCStrategyBroadcast {
		return nil
	}

	s := newBroadcastSidecar(c.opt, c.csc, c.opt.DB)
	if err := s.Start(ctx); err != nil {
		return err
	}
	c.cscSidecar = s
	// Expose readiness to the hot path (processCached bypasses the cache while
	// the sidecar is down) without a per-command lookup.
	c.cscBcastReady = &s.ready
	return nil
}

// cscShutdownBroadcastSidecar tears down a started sidecar (no-op if none).
// Owner-only: clone() doesn't copy cscSidecar, so a derived Close is a no-op.
func cscShutdownBroadcastSidecar(c *baseClient) {
	if c == nil || c.cscSidecar == nil {
		return
	}
	s := c.cscSidecar
	c.cscSidecar = nil
	s.Shutdown()
}
