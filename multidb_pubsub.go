package redis

import (
	"context"
	"sync"
	"sync/atomic"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/internal/pubsub"
)

// multidbPubSubHooks are the pubsub.Hooks of a MultiDB client's shared
// pub/sub connection. Every dial resolves the active member, so a
// reconnect lands on the current active database; an active-member
// change bumps gen and asks the manager to recheck, which retires the
// connection dialed under the old member (see ShouldReplaceConn).
type multidbPubSubHooks struct {
	c *multidbCore
	// addr is the address isBadConn classifies errors against (the
	// member whose options the manager was built from).
	addr string

	// staticAllCluster records that the configuration had no standalone
	// member when the manager was created. Only then is "a cluster member
	// is active" a permanent mismatch (ErrClosed); otherwise a later
	// failover to, or AddDatabase of, a standalone member heals it.
	staticAllCluster bool

	// gen counts active-member changes; each connection remembers the gen
	// it was dialed under.
	gen atomic.Uint64
	// conns maps each live connection (*pool.Conn) to its
	// *multidbPubSubConn. Entries are added by NewConn and dropped by
	// CloseConn after the connection is closed.
	conns sync.Map

	// handoffAddrs holds the endpoint a member's connection was moved to
	// by a maintenance handoff (MOVING), keyed by member so a failover
	// never dials another member's endpoint. switched is closed (and
	// replaced) on every active-member change, canceling a dial still in
	// flight to the former active member. Both guarded by mu.
	mu           sync.Mutex
	handoffAddrs map[*Client]string
	switched     chan struct{}
}

// multidbPubSubConn is what the hooks know about one live connection.
type multidbPubSubConn struct {
	owner *Client
	gen   uint64
}

var _ pubsub.Hooks = (*multidbPubSubHooks)(nil)

func newMultidbPubSubHooks(c *multidbCore, addr string) *multidbPubSubHooks {
	return &multidbPubSubHooks{
		c:                c,
		addr:             addr,
		staticAllCluster: !c.hasStandaloneMember(),
		handoffAddrs:     make(map[*Client]string),
		switched:         make(chan struct{}),
	}
}

// activeChanged retires the connection dialed under the previous active
// member and cancels a dial still in flight to it, so a dial stuck on a
// superseded member does not hold the replacement back until its
// timeout. Non-blocking.
func (h *multidbPubSubHooks) activeChanged(m *pubsub.Manager) {
	h.gen.Add(1)
	h.mu.Lock()
	close(h.switched)
	h.switched = make(chan struct{})
	h.mu.Unlock()
	m.RecheckConn()
}

func (h *multidbPubSubHooks) connInfo(cn *pool.Conn) *multidbPubSubConn {
	if v, ok := h.conns.Load(cn); ok {
		return v.(*multidbPubSubConn)
	}
	return nil
}

// NewConn dials the active member. The addr argument is unused: the
// target follows the active member, not the manager's address.
func (h *multidbPubSubHooks) NewConn(ctx context.Context, _ string) (*pool.Conn, error) {
	c := h.c
	if c.closed.Load() {
		return nil, ErrClosed
	}
	// Loaded before the dial: a switch during the dial tags the new
	// connection stale, costing one extra reconnect, never a connection
	// left on a former active member.
	gen := h.gen.Load()
	h.mu.Lock()
	switched := h.switched
	h.mu.Unlock()
	db, _ := c.activeSnapshot()
	if db == nil {
		return nil, ErrTemporarilyNotAvailable
	}
	if db.c == nil {
		// A cluster member is active and cannot serve the subscription.
		if h.staticAllCluster && !c.hasStandaloneMember() {
			return nil, ErrClosed
		}
		return nil, errPubSubRequiresStandalone
	}
	addr := db.c.opt.Addr
	h.mu.Lock()
	if a := h.handoffAddrs[db.c]; a != "" {
		addr = a
	}
	h.mu.Unlock()
	// An active-member change during the dial cancels it; the manager's
	// retry then dials the new active member.
	dialCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-switched:
			cancel()
		case <-dialCtx.Done():
		}
	}()
	cn, err := db.c.pubsubConnect(dialCtx, addr)
	if err != nil {
		return nil, pubSubDialErr(db, err, c.closed.Load())
	}
	// Removal (and Close) marks the member before closing its pub/sub
	// pool, which checks closed only before dialing: re-check after
	// tracking, so either the pool close sees this conn or this sees the
	// mark. A conn to a removed member would otherwise stay open.
	if db.removed.Load() {
		_ = db.c.pubsubDisconnect(cn)
		return nil, pubSubDialErr(db, pool.ErrClosed, c.closed.Load())
	}
	h.conns.Store(cn, &multidbPubSubConn{owner: db.c, gen: gen})
	return cn, nil
}

// CloseConn untracks cn from its owner's pub/sub pool and closes it. The
// entry is forgotten only after the close: a push still being processed
// on cn looks the owner up lock-free, and must not find it gone while the
// connection is alive (see ProcessPush).
func (h *multidbPubSubHooks) CloseConn(cn *pool.Conn) error {
	var err error
	if info := h.connInfo(cn); info != nil {
		err = info.owner.pubsubDisconnect(cn)
	} else {
		err = cn.Close()
	}
	h.conns.Delete(cn)
	return err
}

// ProcessPush dispatches cn's pushes through the member that owns it: its
// handlers (maintenance notifications, for one) must see them, and its
// Protocol decides whether the connection carries pushes at all. A push
// read after CloseConn finished with cn is dropped.
func (h *multidbPubSubHooks) ProcessPush(ctx context.Context, cn *pool.Conn, rd *proto.Reader) error {
	info := h.connInfo(cn)
	if info == nil {
		return nil
	}
	return info.owner.pubsubProcessPush(ctx, cn, rd)
}

func (h *multidbPubSubHooks) IsBadConn(err error, allowTimeout bool) bool {
	return isBadConn(err, allowTimeout, h.addr)
}

// RequestTopologyRefresh is a no-op: members refresh their own topology.
func (*multidbPubSubHooks) RequestTopologyRefresh() {}

// ShouldReplaceConn retires cn on a maintenance handoff and once an
// active-member change made its gen stale. Lock-free and idempotent, as
// the engine requires.
func (h *multidbPubSubHooks) ShouldReplaceConn(cn *pool.Conn) bool {
	info := h.connInfo(cn)
	if info == nil {
		return false
	}
	if info.owner.pubsubShouldReplaceConn(cn) {
		return true
	}
	return info.gen != h.gen.Load()
}

// ResolveAddress remembers a handoff endpoint prev was marked with, for
// the next dial to prev's member (see NewConn). The returned address is
// unused by NewConn.
func (h *multidbPubSubHooks) ResolveAddress(prev *pool.Conn, current string) string {
	if prev == nil || !prev.ShouldHandoff() {
		return current
	}
	info := h.connInfo(prev)
	if ep := prev.GetHandoffEndpoint(); info != nil && ep != "" && ep != internal.RedisNull {
		h.mu.Lock()
		h.handoffAddrs[info.owner] = ep
		h.mu.Unlock()
	}
	return current
}

// pubSubOptions returns the options the shared pub/sub manager is built
// from: the active standalone member's, else the lowest-id standalone
// member's (the subscription can only be served by a standalone), else
// defaults. They are read once: members with different Protocols are a
// documented limitation (the manager's RESP2 gate is fixed at creation).
func (c *multidbCore) pubSubOptions() *Options {
	if db, _ := c.activeSnapshot(); db != nil && db.c != nil {
		return db.c.cloneOpt()
	}
	if sd := c.standaloneForPubSub(); sd != nil {
		return sd.cloneOpt()
	}
	opt := &Options{}
	opt.init()
	return opt
}

// pubSubManagerLocked returns the shared pub/sub manager, creating it on
// first use. On a closed core the manager is born closed, so its handles
// are too. Callers must hold c.pubsubMu.
func (c *multidbCore) pubSubManagerLocked() *pubsub.Manager {
	if c.psManager == nil {
		opt := c.pubSubOptions()
		hooks := newMultidbPubSubHooks(c, opt.Addr)
		cfg := pubSubConfigFromOptions(opt)
		cfg.Recorder = c.recorder()
		c.psManager = pubsub.NewManager(cfg, hooks)
		c.psHooks = hooks
		if c.closed.Load() {
			_ = c.psManager.Close()
		}
	}
	return c.psManager
}

// newPubSub creates a PubSub on the shared manager, whose connection
// follows the active database (see multidbPubSubHooks).
func (c *multidbCore) newPubSub() *PubSub {
	c.pubsubMu.Lock()
	m := c.pubSubManagerLocked()
	c.pubsubMu.Unlock()
	return newPubSub(m)
}

// notifyPubSubs moves the shared pub/sub connection to the new active
// database now, instead of waiting for the next frame. Non-blocking: the
// manager re-dials on its own goroutine and keeps the old connection
// serving until the replacement is subscribed.
func (c *multidbCore) notifyPubSubs(context.Context) {
	c.pubsubMu.Lock()
	m, hooks := c.psManager, c.psHooks
	c.pubsubMu.Unlock()
	if m != nil {
		hooks.activeChanged(m)
	}
}

// closePubSub closes the shared pub/sub manager, ending every PubSub.
func (c *multidbCore) closePubSub() {
	c.pubsubMu.Lock()
	m := c.psManager
	c.pubsubMu.Unlock()
	if m != nil {
		_ = m.Close()
	}
}
