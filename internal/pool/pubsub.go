package pool

import "context"

// PubSubStats contains pub/sub connection pool stats.
//
// Deprecated: pub/sub connections are no longer pooled — they are owned
// by the client's shared pub/sub manager (internal/pubsub) — so these
// stats are always zero. The type and the Stats.PubSubStats field are
// kept for API compatibility.
type PubSubStats struct {
	Created   uint32
	Untracked uint32
	Active    uint32
}

// TrackPubSubConn counts an unpooled pub/sub connection in the
// connection-count metric (as used, isPubSub) under poolName, once.
func TrackPubSubConn(cn *Conn, poolName string) {
	cn.SetPoolName(poolName)
	if !cn.pubsubTracked.CompareAndSwap(false, true) {
		return
	}
	if cb := getMetricConnectionCountCallback(); cb != nil {
		cb(context.Background(), 1, cn, "used", true)
	}
}

// UntrackPubSubConn reverses TrackPubSubConn; safe to call more than
// once, or for a connection never tracked.
func UntrackPubSubConn(cn *Conn) {
	if !cn.pubsubTracked.CompareAndSwap(true, false) {
		return
	}
	if cb := getMetricConnectionCountCallback(); cb != nil {
		cb(context.Background(), -1, cn, "used", true)
	}
}
