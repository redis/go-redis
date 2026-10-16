package pool

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
