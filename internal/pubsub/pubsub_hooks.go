package pubsub

import (
	"context"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

// Hooks are the owner's touchpoints a Manager is built from: the
// manager owns its one connection and reaches the client only through
// these, so it stays unaware of pools, dial hooks, push processors and
// maintenance handoffs alike. Every method must be safe for concurrent
// use; the ones marked optional have a no-op answer (false, "", or
// nothing) an owner without the feature returns.
type Hooks interface {
	// NewConn dials a fresh, initialized pub/sub connection to addr.
	// Called with the manager's lock released; it may block for the
	// dial's duration and honors ctx.
	NewConn(ctx context.Context, addr string) (*pool.Conn, error)
	// CloseConn tears a connection NewConn produced down.
	CloseConn(cn *pool.Conn) error
	// ProcessPush drains the push notifications buffered on cn before
	// the manager reads its next frame (see Manager.listen).
	ProcessPush(ctx context.Context, cn *pool.Conn, rd *proto.Reader) error
	// IsBadConn classifies an error seen on the connection: true means
	// broken-and-replace, false means an error reply on a healthy
	// connection.
	IsBadConn(err error, allowTimeout bool) bool
	// RequestTopologyRefresh (optional, must not block) asks the owner
	// for a topology refresh after a MOVED reply, a server-initiated
	// unsubscribe or a failed reconnect; the cluster client wires it to
	// LazyReload, a standalone client does nothing.
	RequestTopologyRefresh()
	// ShouldReplaceConn (optional, must not block) is the owner's say
	// over when the shared connection has to go: it is consulted after
	// every frame read, and true retires cn now, with a reconnect
	// through ResolveAddress. Returning false always retires only
	// connections the pool marked unusable. It runs WITHOUT the
	// manager's lock — on the hot path, so the common no-retire answer
	// costs no lock — and may overlap ResolveAddress, so it must be
	// lock-free (safe against concurrent state changes of its own) and
	// idempotent: it is asked again on later frames, and a true
	// answered while a replacement is in flight or backing off is
	// dropped, not remembered.
	ShouldReplaceConn(cn *pool.Conn) bool
	// ResolveAddress (optional, must not block) runs before every dial
	// but the first — a reconnect, the restoration of a connection lost
	// to a failed write, or a retry after a dial that failed — with the
	// connection being replaced (the one lost, for a restoration; nil
	// when there is none, as after a first dial that never connected)
	// and the address dialed last; the manager dials what it returns
	// ("" keeps current) and remembers it for later reconnects. It runs
	// under the manager's lock, so its calls serialize with each other
	// but not with ShouldReplaceConn.
	ResolveAddress(prev *pool.Conn, current string) string
}

// ClusterHooks are the cluster client's touchpoints a ClusterManager is
// built from: it owns its connections and reaches the cluster only
// through these. They are the per-connection Hooks with the node's addr
// added where the answer depends on it, plus slot ownership. An empty
// addr names the broadcast connection (any healthy node); every other
// addr is one node. Every method must be safe for concurrent use.
type ClusterHooks interface {
	// NewConn dials a pub/sub connection to addr, or to any healthy node
	// when addr is empty (see Hooks.NewConn).
	NewConn(ctx context.Context, addr string) (*pool.Conn, error)
	// CloseConn tears a connection NewConn produced down.
	CloseConn(cn *pool.Conn) error
	// ProcessPush drains buffered push notifications of a connection to
	// addr (see Hooks.ProcessPush).
	ProcessPush(ctx context.Context, addr string, cn *pool.Conn, rd *proto.Reader) error
	// IsBadConn classifies an error of a connection to addr (see
	// Hooks.IsBadConn).
	IsBadConn(err error, allowTimeout bool, addr string) bool
	// ResolveOwners groups shard channels by the addr of the node that
	// owns their slot. current maps channels to the addr their
	// subscription lives on now (nil for fresh subscriptions): the
	// resolver should keep a channel there while that node still serves
	// its slot, so re-route sweeps move only the channels whose node
	// lost the slot (and a non-deterministic replica picker does not
	// shuffle subscriptions on every reload).
	ResolveOwners(ctx context.Context, channels []string, current map[string]string) (map[string]map[string]struct{}, error)
	// RequestReload (must not block) asks the cluster for a topology
	// refresh; it is also every owned manager's
	// Hooks.RequestTopologyRefresh (MOVED replies, server-initiated
	// unsubscribes, failed dials). The cluster is expected to call
	// ScheduleReroute once the refreshed topology is in place.
	RequestReload()
	// ShouldReplaceConn and ResolveAddress are passed through to every
	// owned node manager (see Hooks.ShouldReplaceConn,
	// Hooks.ResolveAddress). The broadcast manager gets
	// ShouldReplaceConn only (see nodeHooks.ResolveAddress).
	ShouldReplaceConn(cn *pool.Conn) bool
	ResolveAddress(prev *pool.Conn, current string) string
}
