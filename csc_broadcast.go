package redis

import (
	"context"
	"sync"
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

// broadcastSidecarReg maps a *baseClient to its sidecar. We keep this state
// out of the baseClient struct itself so a client using a different strategy
// carries no layout dependency on Broadcast.
var (
	broadcastSidecarRegMu sync.Mutex
	broadcastSidecarReg   = make(map[*baseClient]*broadcastSidecar)
)

func cscRegisterBroadcastSidecar(c *baseClient, s *broadcastSidecar) {
	broadcastSidecarRegMu.Lock()
	broadcastSidecarReg[c] = s
	broadcastSidecarRegMu.Unlock()
}

func cscDeregisterBroadcastSidecar(c *baseClient) *broadcastSidecar {
	broadcastSidecarRegMu.Lock()
	s := broadcastSidecarReg[c]
	delete(broadcastSidecarReg, c)
	broadcastSidecarRegMu.Unlock()
	return s
}

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
	cscRegisterBroadcastSidecar(c, s)
	return nil
}

// cscShutdownBroadcastSidecar tears down a previously-started sidecar. Safe to call
// when no sidecar is attached.
func cscShutdownBroadcastSidecar(c *baseClient) {
	if c == nil {
		return
	}
	if s := cscDeregisterBroadcastSidecar(c); s != nil {
		s.Shutdown()
	}
}
