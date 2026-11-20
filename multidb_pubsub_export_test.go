package redis

import (
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/push"
)

// processorFor returns the push processor ProcessPush dispatches cn's
// pushes through: its owner's, nil for an unknown connection or a
// non-RESP3 owner.
func (h *multidbPubSubHooks) processorFor(cn *pool.Conn) push.NotificationProcessor {
	info := h.connInfo(cn)
	if info == nil || info.owner.opt.Protocol != 3 {
		return nil
	}
	return info.owner.pushProcessor
}
