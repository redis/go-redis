package pool

import (
	"context"
	"errors"
	"time"
)

// startLifetimeReaper runs reapExpiredIdle every ConnMaxLifetimeReapInterval
// until the pool is closed. It is a no-op unless the interval is set and at
// least one of ConnMaxLifetime or ConnMaxIdleTime is set.
func (p *ConnPool) startLifetimeReaper() {
	if p.cfg.ConnMaxLifetimeReapInterval <= 0 ||
		(p.cfg.ConnMaxLifetime <= 0 && p.cfg.ConnMaxIdleTime <= 0) {
		return
	}
	go func() {
		t := time.NewTicker(p.cfg.ConnMaxLifetimeReapInterval)
		defer t.Stop()
		for {
			select {
			case <-p.closedCh:
				return
			case <-t.C:
				p.reapExpiredIdle(context.Background())
			}
		}
	}()
}

// expiredIdle mirrors the lifetime and idle-time checks in isHealthyConn.
func (p *ConnPool) expiredIdle(cn *Conn, nowNs int64) bool {
	if p.cfg.ConnMaxLifetime > 0 && cn.expiresAt.UnixNano() < nowNs {
		return true
	}
	return p.cfg.ConnMaxIdleTime > 0 && nowNs-cn.UsedAtNs() >= int64(p.cfg.ConnMaxIdleTime)
}

// reapableState matches what popIdle can acquire: IDLE, or CREATED for a conn
// that was pooled before its first use.
func reapableState(s ConnState) bool { return s == StateIdle || s == StateCreated }

// reapExpiredIdle closes idle connections past ConnMaxLifetime or idle for
// ConnMaxIdleTime, and refills the pool toward MinIdleConns (so the floor is
// recycled, not kept stale). Only IDLE or CREATED connections are taken; ones
// owned by a handoff or re-auth worker (UNUSABLE) are left alone. Returns the
// number closed.
func (p *ConnPool) reapExpiredIdle(ctx context.Context) int {
	nowNs := time.Now().UnixNano()

	var expired []*Conn
	p.connsMu.Lock()
	if p.closed() {
		p.connsMu.Unlock()
		return 0
	}
	for _, cn := range p.idleConns {
		if !p.expiredIdle(cn, nowNs) {
			continue
		}
		if sm := cn.GetStateMachine(); sm == nil || !reapableState(sm.GetState()) {
			continue
		}
		expired = append(expired, cn)
	}
	removedConns := expired[:0]
	for _, cn := range expired {
		if removed, _ := p.removeConn(cn); removed {
			removedConns = append(removedConns, cn)
		}
	}
	countCb := p.metrics().ConnectionCount
	p.connsMu.Unlock()

	if len(removedConns) == 0 {
		return 0
	}

	hookManager := p.hookManager.Load()
	for _, cn := range removedConns {
		if hookManager != nil {
			hookManager.ProcessOnRemove(ctx, cn, errors.New(CloseReasonStale))
		}
		p.recordConnectionMetrics(ctx, cn, CloseReasonStale, MetricStateIdle, countCb)
		_ = p.closeConn(cn)
	}
	p.checkMinIdleConns()
	return len(removedConns)
}
