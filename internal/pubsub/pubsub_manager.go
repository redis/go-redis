package pubsub

import (
	"cmp"
	"context"
	"errors"
	"maps"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/hashtag"
	"github.com/redis/go-redis/v9/internal/otel"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

// subState tracks a registered name. A subscribe write makes it Pending
// (the fire-and-forget command awaits the server's reply); the
// confirmation makes it Subscribed; an error reply answering the
// command makes it Rejected. Replies on one connection are ordered and
// never skipped, so a Pending name's confirmation is on its way — or
// the connection is broken, which the read loop and the health check
// detect and the reconnect replay repairs. Pending names are therefore
// never re-sent on the same connection; Rejected ones are, by
// retryRejected.
type subState uint8

const (
	subStatePending subState = iota
	subStateSubscribed
	subStateRejected
)

// subscription is one registered name's registry entry: its fan-out
// handles and its establishment state.
type subscription struct {
	handles map[*handle]struct{}
	state   subState
	// seq is the name's registration order: replays write names in the
	// order they were first registered, so a fresh dial's confirmations
	// arrive in subscription order rather than map order.
	seq uint64
}

// Manager multiplexes every pub/sub subscription of one client over a
// single shared connection: subscribers get a handle each, and the
// manager's read loop fans incoming messages out to the handles
// registered for the message's channel, pattern or shard channel.
type Manager struct {
	cfg Config

	// newConn dials cfg.Addr; the resolver may redirect the manager by
	// having cfg.Addr rewritten (see resolveAddrLocked).
	newConn     func(ctx context.Context, addr string) (*pool.Conn, error)
	closeConn   func(*pool.Conn) error
	processPush func(ctx context.Context, cn *pool.Conn, rd *proto.Reader) error
	// isBadConn: true means broken-and-replace, false means an error
	// reply on a healthy connection.
	isBadConn func(err error, allowTimeout bool) bool
	// requestTopologyRefresh (optional, must not block) asks the owner
	// for a topology refresh; the cluster client wires it to LazyReload.
	requestTopologyRefresh func()
	// resolver (optional) decides where every reconnect dials and when a
	// live connection must be retired; see AddrResolver.
	resolver AddrResolver

	mu sync.RWMutex

	// the ONE connection
	conn *pool.Conn
	// dropped is the connection most recently lost — dropped after a
	// failed write — kept until a replacement is installed so that the
	// restoration, whichever path runs it, can show it to the resolver:
	// a maintenance handoff it had received must still redirect the
	// dial (see resolveAddrLocked). Guarded by mu.
	dropped *pool.Conn
	// dialed records that a dial has been attempted, connected or not:
	// Resolve runs before every dial but the first (see AddrResolver),
	// and a failed first dial leaves no dropped connection to key that
	// off. Guarded by mu.
	dialed bool

	// mu guards the in-memory state only and is never held across the
	// network. io is the I/O slot: a one-slot semaphore held by whoever
	// dials or writes — from appending a command's ledger entry (under
	// mu) through the socket write, and across the dial and handshake of
	// the connection — with mu released for the network parts. It
	// serializes writers so wire order equals ledger order, makes dials
	// single-flight, and keeps a stalled write or a slow dial from
	// blocking anything that takes only mu: Close, the read loop's
	// fan-out, a handle's Subscriptions. It is acquired with the
	// caller's context, so a blocked writer stays cancellable. Lock
	// order: io before mu, never the reverse.
	io chan struct{}

	// Reconnect bookkeeping (guarded by mu). reconnectLog paces the
	// failure lines of an outage to one per LogInterval, the rest
	// counted on the next line.
	reconnectAttempts int
	reconnectLog      internal.Logging
	// nextReconnectAt gates the replacement of a live connection the
	// resolver wants retired (see receive): a redirect to an endpoint
	// that does not dial yet must not cost a blocking dial per frame.
	// Set from the reconnect backoff on failure, cleared on success.
	nextReconnectAt time.Time
	// errorLog paces the line logged for error replies on the shared
	// connection: a persistently rejected subscribe is retried on a
	// cadence and would otherwise log on every retry.
	errorLog internal.Logging

	// The per-kind registries, one subscription entry per name. They
	// always equal the desired state: registration happens at write
	// time, removal is immediate.
	subscribers        map[string]*subscription
	patternSubscribers map[string]*subscription
	shardSubscribers   map[string]*subscription
	// regSeq stamps new registry entries with their order (see
	// subscription.seq). Guarded by mu.
	regSeq uint64

	// handles tracks every live handle, subscribed or not; pong and
	// error-reply fan-out iterate all of them.
	handles map[*handle]struct{}

	once   sync.Once
	pingCh chan struct{}
	// cfgChanged wakes the health checker after a config update
	// (buffered so a signal is never missed).
	cfgChanged chan struct{}
	// wake un-parks the read loop (buffered so a connect racing the
	// loop's idle check is never missed).
	wakeListen chan struct{}
	wakeResync chan struct{}
	done       chan struct{}

	// replyQueue is the reply ledger: every written command in write
	// order, with the reply frames the server still owes it. Pongs are
	// delivered to their entry's handle; an error reply settles the
	// entry of the command it rejected.
	replyQueue []replyWaiter
}

// replyKindPong marks a ledger entry whose reply is pong-shaped (PING,
// CLIENT SETNAME).
const replyKindPong = "pong"

// replyWaiter is one written command's outstanding replies: PING and
// CLIENT SETNAME owe one pong-shaped frame, subscribe-family commands
// one confirmation per name. A rejected command answers with a single
// error frame instead, settling the whole entry.
type replyWaiter struct {
	kind string // replyKindPong, or the subscribe-family command
	// names are the confirmations still owed, one per name written
	// (nil for pong-shaped entries, which owe exactly one frame).
	names []string
	h     *handle // the handle awaiting the reply (nil: nobody awaits it)
	// broadcast marks a replay write (reconnect, pending resync): its
	// confirmations go to every registered owner, not one handle.
	broadcast bool
	// affected maps each name of an orphan unsubscribe to the handles
	// that released it, captured at write time: the registry no longer
	// lists the names, so an error reply has no owners left to look up,
	// and a name re-added meanwhile belongs to an unrelated handle. Per
	// name, because a sharded unsubscribe is one command per slot and
	// the error reply of one slot concerns only the names that command
	// carried (see deliverToReleasersLocked).
	affected map[string][]*handle
}

// NewManager creates a manager that multiplexes all subscriptions over
// one shared connection, dialed lazily on the first Subscribe. cfg must
// arrive with defaults already applied; requestTopologyRefresh and
// resolver may be nil.
func NewManager(
	cfg Config,
	newConn func(ctx context.Context, addr string) (*pool.Conn, error),
	closeConn func(*pool.Conn) error,
	processPush func(ctx context.Context, cn *pool.Conn, rd *proto.Reader) error,
	isBadConn func(err error, allowTimeout bool) bool,
	requestTopologyRefresh func(),
	resolver AddrResolver,
) *Manager {
	return &Manager{
		cfg: cfg,

		newConn:                newConn,
		closeConn:              closeConn,
		processPush:            processPush,
		isBadConn:              isBadConn,
		requestTopologyRefresh: requestTopologyRefresh,
		resolver:               resolver,

		reconnectLog: internal.NewThrottledLogger(cfg.LogInterval, nil),
		errorLog:     internal.NewThrottledLogger(cfg.LogInterval, nil),

		subscribers:        make(map[string]*subscription),
		patternSubscribers: make(map[string]*subscription),
		shardSubscribers:   make(map[string]*subscription),

		handles: make(map[*handle]struct{}),

		io:         make(chan struct{}, 1),
		pingCh:     make(chan struct{}, 1),
		cfgChanged: make(chan struct{}, 1),
		wakeListen: make(chan struct{}, 1),
		wakeResync: make(chan struct{}, 1),
		done:       make(chan struct{}),
	}
}

// acquireIO takes the I/O slot (see Manager.io), giving up on ctx
// cancellation or Close. On success the caller must releaseIO.
func (m *Manager) acquireIO(ctx context.Context) error {
	select {
	case m.io <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-m.done:
		return pool.ErrClosed
	}
}

func (m *Manager) releaseIO() { <-m.io }

// newHandleLocked constructs a handle with its events stream ready.
// Callers must hold m.mu.
func (m *Manager) newHandleLocked() *handle {
	return &handle{
		m:         m,
		channels:  make(map[string]struct{}),
		patterns:  make(map[string]struct{}),
		schannels: make(map[string]struct{}),
		events:    make(chan any, m.cfg.ChanSize),
		done:      make(chan struct{}),
		dropLog:   internal.NewThrottledLogger(m.cfg.LogInterval, nil),
	}
}

// NewHandle returns a handle with no subscriptions; the PubSub API
// wraps one. On a closed manager the handle is born closed.
func (m *Manager) NewHandle() PubSuber {
	m.mu.Lock()
	defer m.mu.Unlock()

	h := m.newHandleLocked()
	select {
	case <-m.done:
		h.closed = true
		close(h.done)
		close(h.events)
	default:
		m.handles[h] = struct{}{}
	}
	return h
}

// ClientSetName names the shared connection via CLIENT SETNAME
func (m *Manager) ClientSetName(ctx context.Context, name string) error {
	return m.clientSetName(ctx, nil, name)
}

func (m *Manager) clientSetName(ctx context.Context, h *handle, name string) error {
	if err := m.acquireIO(ctx); err != nil {
		return err
	}
	defer m.releaseIO()
	if err := ctx.Err(); err != nil {
		return err // see handleSubscribe
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if h != nil && h.closed {
		return pool.ErrClosed
	}

	// A closed manager must not dial: the connection would leak.
	select {
	case <-m.done:
		return pool.ErrClosed
	default:
	}

	if err := m.connectIdempotentLocked(ctx); err != nil && !errors.Is(err, errConnExists) {
		return err
	}
	// The dial ran with the lock released: re-validate before writing.
	select {
	case <-m.done:
		return pool.ErrClosed
	default:
	}
	if h != nil && h.closed {
		return pool.ErrClosed
	}

	// The +OK reply parses as a pong: ledger it for h (nil: consumed
	// silently) before the write, like every command.
	m.replyQueue = append(m.replyQueue, replyWaiter{kind: replyKindPong, h: h})
	return m.writeReleasingLock(socketCtx, m.conn, [][]any{{"client", "setname", name}})
}

// Subscribe subscribes to the given channels and returns a handle that
// receives matching messages.
func (m *Manager) Subscribe(ctx context.Context, channels ...string) (PubSuber, error) {
	return m.handleSubscribe(ctx, nil, "subscribe", channels...)
}

// PSubscribe subscribes to the given patterns and returns a handle that
// receives matching messages.
func (m *Manager) PSubscribe(ctx context.Context, patterns ...string) (PubSuber, error) {
	return m.handleSubscribe(ctx, nil, "psubscribe", patterns...)
}

// SSubscribe subscribes to the given shard channels (a namespace
// separate from regular channels).
func (m *Manager) SSubscribe(ctx context.Context, channels ...string) (PubSuber, error) {
	return m.handleSubscribe(ctx, nil, "ssubscribe", channels...)
}

// handleSubscribe registers the handle (created when h == nil) for the
// given names and writes the subscribe command. Re-subscribing owned
// names is idempotent.
func (m *Manager) handleSubscribe(ctx context.Context, h *handle, redisCommand string, channels ...string) (PubSuber, error) {
	if len(channels) == 0 {
		switch redisCommand {
		case "psubscribe":
			return nil, ErrNoPatterns
		case "ssubscribe":
			return nil, ErrNoShardChannels
		default:
			return nil, ErrNoChannels
		}
	}

	if err := m.acquireIO(ctx); err != nil {
		return nil, err
	}
	defer m.releaseIO()
	// The slot may have gone to an already-cancelled caller (acquireIO's
	// select is not biased): a write on its behalf would fail before any
	// byte is sent and needlessly drop the shared connection for every
	// subscriber, so bow out here, before touching any state.
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	select {
	case <-m.done:
		return nil, pool.ErrClosed
	default:
	}

	// Resolve the target handle before doing any work: a closed handle
	// must not trigger a dial.
	handle := h
	if handle == nil {
		handle = m.newHandleLocked()
		m.handles[handle] = struct{}{}
	} else if handle.closed {
		return nil, pool.ErrClosed
	}

	// Register BEFORE connecting: a fresh dial replays the whole registry
	// — these names included — with the lock released around the write,
	// and the replay's confirmations are broadcast to the names' owners
	// the moment they arrive, so the handle must already be one (an
	// explicit write after the replay could otherwise miss a confirmation
	// that landed in between). Registering first also keeps the intent
	// across a failed dial for the reconnect replay to restore, since
	// callers hand out the PubSub without checking this error.
	switch redisCommand {
	case "subscribe":
		for _, ch := range channels {
			handle.channels[ch] = struct{}{}
			m.registerHandleLocked(m.subscribers, ch, handle)
		}
	case "psubscribe":
		for _, pt := range channels {
			handle.patterns[pt] = struct{}{}
			m.registerHandleLocked(m.patternSubscribers, pt, handle)
		}
	case "ssubscribe":
		for _, ch := range channels {
			handle.schannels[ch] = struct{}{}
			m.registerHandleLocked(m.shardSubscribers, ch, handle)
		}
	}

	err := m.connectIdempotentLocked(ctx)

	// The dial ran with the lock released, so re-validate: the manager
	// may have closed (taking a handle created here, and the registry,
	// down with it), or a caller-held handle may have been closed.
	if err := m.stillOpenLocked(handle); err != nil {
		return nil, err
	}

	switch {
	case err == nil:
		// Fresh connection: the replay wrote every registered name, these
		// included; writing them again would draw duplicate confirmations
		// (duplicate events, two ledger entries).
	case errors.Is(err, errConnExists):
		cmds := m.prepareSubscribeLocked(redisCommand, replyWaiter{h: handle}, channels)
		if err := m.writeReleasingLock(socketCtx, m.conn, cmds); err != nil {
			// The failed write dropped the connection for the reconnect
			// replay. The caller gets no handle, so a handle created by
			// this call must not stay registered: it would be
			// resubscribed by the replay with nobody to drain it, and
			// would keep the manager from ever going idle.
			if h == nil {
				m.rollbackSubscribeLocked(handle, redisCommand, channels)
			}
			return nil, err
		}
		// The write ran with the lock released too: a Close that landed
		// meanwhile tore the handle down (a manager Close, the registry
		// with it), and a closed handle must not be handed out as
		// subscribed.
		if err := m.stillOpenLocked(handle); err != nil {
			return nil, err
		}
	default:
		// The dial failed. A caller-held handle keeps its registration
		// for the reconnect replay; a handle created by this call is
		// returned as nil, so its registration is rolled back instead.
		if h == nil {
			m.rollbackSubscribeLocked(handle, redisCommand, channels)
		} else {
			// Wake the read loop: it may have parked idle before this
			// registration existed, and nothing else retries the dial.
			select {
			case m.wakeListen <- struct{}{}:
			default:
			}
			select {
			case m.wakeResync <- struct{}{}:
			default:
			}
		}
		return nil, err
	}

	return handle, nil
}

// rollbackSubscribeLocked undoes handleSubscribe's registration of the
// given names on a handle nobody holds. Callers must hold m.mu.
func (m *Manager) rollbackSubscribeLocked(handle *handle, redisCommand string, channels []string) {
	registry := m.registryForKind(redisCommand)
	for _, name := range channels {
		sub := registry[name]
		if sub == nil {
			continue
		}
		delete(sub.handles, handle)
		if len(sub.handles) == 0 {
			delete(registry, name)
		}
	}
	delete(m.handles, handle)
}

// stillOpenLocked re-validates, after a network step ran with m.mu
// released, that neither the manager nor the handle was closed
// meanwhile: a Close that landed in between owns the teardown, and a
// closed handle must not be handed out. Callers must hold m.mu.
func (m *Manager) stillOpenLocked(h *handle) error {
	select {
	case <-m.done:
		return pool.ErrClosed
	default:
	}
	if h.closed {
		return pool.ErrClosed
	}
	return nil
}

// Unsubscribe removes channel subscriptions across all handles;
// UNSUBSCRIBE is sent for channels left with no subscriber.
func (m *Manager) Unsubscribe(ctx context.Context, channels ...string) error {
	return m.handleUnsubscribe(ctx, nil, "unsubscribe", channels...)
}

// PUnsubscribe removes pattern subscriptions across all handles;
// PUNSUBSCRIBE is sent for patterns left with no subscriber.
func (m *Manager) PUnsubscribe(ctx context.Context, patterns ...string) error {
	return m.handleUnsubscribe(ctx, nil, "punsubscribe", patterns...)
}

// SUnsubscribe removes shard-channel subscriptions across all handles;
// SUNSUBSCRIBE is sent for shard channels left with no subscriber.
func (m *Manager) SUnsubscribe(ctx context.Context, channels ...string) error {
	return m.handleUnsubscribe(ctx, nil, "sunsubscribe", channels...)
}

// Ping writes a PING on the shared connection, dialing it if needed;
// the pong is consumed by its replyQueue entry (nil: nobody awaits it).
func (m *Manager) Ping(ctx context.Context, payload ...string) error {
	return m.ping(ctx, nil, false, payload...)
}

// ping writes a user-facing PING issued by h (nil: the manager itself),
// which gates the write — a closed handle must not dial — and awaits
// the pong on its events unless silent. See pingLocked.
func (m *Manager) ping(ctx context.Context, h *handle, silent bool, payload ...string) error {
	if err := m.acquireIO(ctx); err != nil {
		return err
	}
	defer m.releaseIO()
	if err := ctx.Err(); err != nil {
		return err // see handleSubscribe
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	if h != nil && h.closed {
		return pool.ErrClosed
	}
	waiter := h
	if silent {
		waiter = nil
	}
	_, err := m.pingLocked(ctx, socketCtx, h, waiter, true, payload...)
	return err
}

// pingLocked writes a PING, returning the connection it went out on;
// the pong is consumed by waiter's ledger entry (nil: nobody awaits
// it), and issuer (nil: none) is re-checked after a dial. ctx bounds
// the dial a user-facing ping may trigger; writeCtx bounds the socket
// write and must be a manager policy (see socketCtx). Callers must
// hold m.io and m.mu. shouldDial controls the no-connection behavior:
// user-facing pings dial the shared connection on demand, while the
// health checker's ping must report errPubSubNoConn instead — its tick
// and this write run in separate critical sections, so the last
// unsubscribe can release the connection in between, and a dialing
// ping would resurrect a connection with no subscribers that nothing
// ever closes (its own later pings would keep the zombie alive).
func (m *Manager) pingLocked(ctx, writeCtx context.Context, issuer, waiter *handle, shouldDial bool, payload ...string) (*pool.Conn, error) {
	args := []any{"ping"}
	if len(payload) == 1 {
		args = append(args, payload[0])
	}

	select {
	case <-m.done:
		return nil, pool.ErrClosed
	default:
	}

	if shouldDial {
		if err := m.connectIdempotentLocked(ctx); err != nil && !errors.Is(err, errConnExists) {
			return nil, err
		}
		// The dial ran with the lock released: re-validate the manager
		// and the issuing handle before writing.
		select {
		case <-m.done:
			return nil, pool.ErrClosed
		default:
		}
		if issuer != nil && issuer.closed {
			return nil, pool.ErrClosed
		}
	} else if m.conn == nil {
		return nil, errPubSubNoConn
	}

	cn := m.conn
	m.replyQueue = append(m.replyQueue, replyWaiter{kind: replyKindPong, h: waiter})
	if err := m.writeReleasingLock(writeCtx, cn, [][]any{args}); err != nil {
		return nil, err
	}
	return cn, nil
}

// Close tears the manager down: read loop, health checker, connection
// and every handle's delivery channel. Subsequent calls return
// pool.ErrClosed.
func (m *Manager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	select {
	case <-m.done:
		return pool.ErrClosed
	default:
	}
	return m.closeNowLocked()
}

// CloseIfIdle closes the manager only when it has no live handles —
// and therefore no subscriptions — reporting whether it is closed
// afterwards.
func (m *Manager) CloseIfIdle() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	select {
	case <-m.done:
		return true
	default:
	}
	if len(m.handles) > 0 || !m.noSubscribersLocked() {
		return false
	}
	_ = m.closeNowLocked()
	return true
}

// closeNowLocked runs the teardown. Callers must hold m.mu with done
// not yet closed. A dial in flight is not waited for: its owner finds
// done closed on return and closes the connection it produced.
func (m *Manager) closeNowLocked() error {
	close(m.done)

	var err error
	if m.conn != nil {
		// Unblocks listen's pending read with an error.
		err = m.closeConn(m.conn)
		m.conn = nil
	}

	for _, h := range slices.Collect(maps.Keys(m.handles)) {
		h.closeLocked()
	}

	m.handles = make(map[*handle]struct{})
	m.subscribers = make(map[string]*subscription)
	m.patternSubscribers = make(map[string]*subscription)
	m.shardSubscribers = make(map[string]*subscription)
	m.replyQueue = nil

	return err
}

func (m *Manager) listen() {
	ctx := context.TODO()

	// Floor the backoff: a dial can fail instantly, and MinRetryBackoff
	// -1 ("never back off") would spin this loop hot.
	minBackoff := m.cfg.MinRetryBackoff
	if minBackoff <= 0 {
		minBackoff = 10 * time.Millisecond
	}
	maxBackoff := max(m.cfg.ReconnectMaxBackoff, minBackoff)

	var errCount int
	for {
		ev, cn, err := m.receiveNext(ctx)
		if err != nil {
			select {
			case <-m.done:
				return
			default:
			}
			// No connection and no subscriptions: park until a
			// Subscribe wakes the loop or the manager closes.
			m.mu.RLock()
			idle := m.conn == nil && m.noSubscribersLocked()
			m.mu.RUnlock()
			if idle {
				select {
				case <-m.wakeListen:
					errCount = 0
					continue
				case <-m.done:
					return
				}
			}
			if errCount > 0 {
				time.Sleep(internal.RetryBackoff(errCount-1, minBackoff, maxBackoff))
			}
			errCount++
			continue
		}
		errCount = 0

		// Confirmations mutate entry state (Pending → Subscribed) and
		// pongs consume their queue entry, so both need the write lock —
		// and both are settled only if their source is still the live
		// connection, verified under the SAME lock: a reconnect landing
		// between the read and this point must not have an old frame
		// mark an unconfirmed name established or consume a replay
		// waiter on the replacement's ledger. Messages fan out
		// read-only, from any source (they are real deliveries).
		switch ev := ev.(type) {
		case *Subscription:
			m.mu.Lock()
			if m.conn == cn {
				m.fanoutSubscriptionLocked(ev)
			}
			m.mu.Unlock()
			continue
		case *Pong:
			m.mu.Lock()
			if m.conn == cn {
				m.fanoutPongLocked(ev)
			}
			m.mu.Unlock()
			continue
		}

		m.mu.RLock()
		switch ev := ev.(type) {
		case *shardMessage:
			m.fanoutShardedMessageLocked(ev.Message)
		case *patternMessage:
			m.fanoutPatternMessageLocked(ev.Message)
		case *Message:
			m.fanoutMessageLocked(ev)
		}
		m.mu.RUnlock()
	}
}

// noSubscribersLocked reports whether no handle is subscribed to
// anything. Callers must hold m.mu (read or write).
func (m *Manager) noSubscribersLocked() bool {
	return len(m.subscribers) == 0 &&
		len(m.patternSubscribers) == 0 &&
		len(m.shardSubscribers) == 0
}

// registryForKind maps a confirmation kind onto its fan-out registry.
func (m *Manager) registryForKind(kind string) map[string]*subscription {
	switch kind {
	case "subscribe", "unsubscribe":
		return m.subscribers
	case "psubscribe", "punsubscribe":
		return m.patternSubscribers
	case "ssubscribe", "sunsubscribe":
		return m.shardSubscribers
	}
	return nil
}

// fanoutSubscriptionLocked routes a subscription confirmation and
// settles the entry's state (a mutation — unlike the message fan-outs,
// callers must hold the WRITE lock). A confirmation settling a targeted
// ledger entry is delivered only to the handle whose write it answers
// (still registered, or nobody); broadcast (replay) and unsolicited
// confirmations go to every registered owner. An unsubscribe
// confirmation that answers none of our writes (unmatched) and still
// finds subscribers is server-initiated (a slot migrated away): the
// reload hook fires so the re-route sweep restores the subscription on
// the new owner. A matched one is the reply to our own unsubscribe —
// finding subscribers again just means a resubscribe raced it.
func (m *Manager) fanoutSubscriptionLocked(sub *Subscription) {
	w, matched := m.consumeConfirmationLocked(sub.Kind, sub.Channel)
	registry := m.registryForKind(sub.Kind)
	if registry == nil {
		return
	}

	entry := registry[sub.Channel]
	if entry != nil {
		if matched && !w.broadcast {
			if _, ok := entry.handles[w.h]; ok {
				w.h.deliverSubscriptionLocked(sub)
			}
		} else { // Broadcast
			for h := range entry.handles {
				h.deliverSubscriptionLocked(sub)
			}
		}
	}

	switch sub.Kind {
	case "unsubscribe", "punsubscribe", "sunsubscribe":
	default:
		if entry != nil {
			entry.state = subStateSubscribed
		}
		return
	}

	if !matched && entry != nil && len(entry.handles) > 0 && m.requestTopologyRefresh != nil {
		m.requestTopologyRefresh()
	}
}

// fanoutPongLocked delivers a pong to the oldest pong waiter: pongs
// arrive in ping-write order, so per-kind FIFO attribution is exact. A
// nil waiter (silent ping, or the writer closed) consumes the pong
// without delivering it. Callers must hold the WRITE lock.
func (m *Manager) fanoutPongLocked(pong *Pong) {
	if w, ok := m.consumePongLocked(); ok && w.h != nil {
		w.h.deliverPongLocked(pong)
	}
}

// consumePongLocked settles one pong-shaped frame against the oldest
// pong entry, returning a copy of it. Callers must hold the WRITE lock.
func (m *Manager) consumePongLocked() (replyWaiter, bool) {
	for i := range m.replyQueue {
		if m.replyQueue[i].kind != replyKindPong {
			continue
		}
		w := m.replyQueue[i]
		// Delete zeroes the vacated tail slot: the backing array must
		// not pin the handle.
		m.replyQueue = slices.Delete(m.replyQueue, i, i+1)
		return w, true
	}
	return replyWaiter{}, false
}

// consumeConfirmationLocked settles one confirmation frame against the
// oldest ledger entry of the same kind still owing that name, returning
// a copy of the entry. Non-matching entries are left alone so an
// unsolicited frame (e.g. a server-initiated sunsubscribe) can't
// consume a foreign entry. Callers must hold the WRITE lock.
func (m *Manager) consumeConfirmationLocked(kind, name string) (replyWaiter, bool) {
	for i := range m.replyQueue {
		e := &m.replyQueue[i]
		if e.kind != kind {
			continue
		}
		j := slices.Index(e.names, name)
		if j < 0 {
			continue
		}
		w := *e
		e.names = slices.Delete(e.names, j, j+1)
		if len(e.names) == 0 {
			m.replyQueue = slices.Delete(m.replyQueue, i, i+1)
		}
		return w, true
	}
	return replyWaiter{}, false
}

// markRejectedLocked flags the names of a subscribe-family command the
// server answered with an error reply: the whole command was refused
// (a single error frame settles the entry), so every name it carried
// that still awaits that reply becomes Rejected for retryRejected to
// re-send. Callers must hold the WRITE lock.
func (m *Manager) markRejectedLocked(w replyWaiter) {
	switch w.kind {
	case "subscribe", "psubscribe", "ssubscribe":
	default:
		return
	}
	registry := m.registryForKind(w.kind)
	for _, name := range w.names {
		if sub := registry[name]; sub != nil && sub.state == subStatePending {
			sub.state = subStateRejected
		}
	}
}

// consumeErrorReplyLocked settles an error reply against the oldest
// ledger entry, returning it (ok = false: the ledger was empty).
func (m *Manager) consumeErrorReplyLocked() (replyWaiter, bool) {
	if len(m.replyQueue) == 0 {
		return replyWaiter{}, false
	}
	w := m.replyQueue[0]
	m.replyQueue = slices.Delete(m.replyQueue, 0, 1)
	return w, true
}

func (m *Manager) fanoutShardedMessageLocked(shardedMsg *Message) {
	if sub := m.shardSubscribers[shardedMsg.Channel]; sub != nil {
		deliverMessageLocked(sub.handles, shardedMsg)
	}
}

func (m *Manager) fanoutMessageLocked(msg *Message) {
	if sub := m.subscribers[msg.Channel]; sub != nil {
		deliverMessageLocked(sub.handles, msg)
	}
}

func (m *Manager) fanoutPatternMessageLocked(msg *Message) {
	if sub := m.patternSubscribers[msg.Pattern]; sub != nil {
		deliverMessageLocked(sub.handles, msg)
	}
}

func deliverMessageLocked(hs map[*handle]struct{}, msg *Message) {
	for h := range hs {
		h.deliverLocked(cloneMessage(msg))
	}
}

func cloneMessage(msg *Message) *Message {
	c := *msg
	if msg.PayloadSlice != nil {
		c.PayloadSlice = slices.Clone(msg.PayloadSlice)
	}
	return &c
}

// deliverToReleasersLocked reports a refused orphan unsubscribe to the
// handles that released the names the command carried (captured at
// write time, see replyWaiter.affected) — a sharded unsubscribe is one
// command per slot, so a MOVED on one slot never reaches the releasers
// of a slot that succeeded. A closed handle no longer listens. Callers
// must hold the WRITE lock.
func (m *Manager) deliverToReleasersLocked(w replyWaiter, err error) {
	delivered := make(map[*handle]struct{})
	for _, name := range w.names {
		for _, h := range w.affected[name] {
			if _, done := delivered[h]; done {
				continue
			}
			delivered[h] = struct{}{}
			h.deliverLocked(err)
		}
	}
}

// fanoutErrorLocked routes an unattributable error reply to every
// handle's events stream.
func (m *Manager) fanoutErrorLocked(err error) {
	for h := range m.handles {
		h.deliverLocked(err)
	}
}

// deliverToOwnersLocked routes the error reply that rejected a
// (re)subscribe command to the handles subscribed to the names it
// carried, each once. Callers must hold the WRITE lock.
func (m *Manager) deliverToOwnersLocked(w replyWaiter, err error) {
	registry := m.registryForKind(w.kind)
	if registry == nil {
		m.fanoutErrorLocked(err)
		return
	}
	delivered := make(map[*handle]struct{})
	for _, name := range w.names {
		sub := registry[name]
		if sub == nil {
			continue
		}
		for h := range sub.handles {
			if _, done := delivered[h]; done {
				continue
			}
			delivered[h] = struct{}{}
			h.deliverLocked(err)
		}
	}
}

// healthCheck probes the shared connection when a HealthCheckInterval
// window passes without inbound traffic (every received frame feeds
// m.pingCh): it writes a PING and then requires a reply. Replies on one
// connection are ordered, so any inbound frame within PingTimeout
// proves the connection alive (the pong is at most one frame behind
// whatever arrives first); none means the server is unresponsive or the
// socket silently dead, and the connection is dropped and re-dialed
// with the subscription replay. A maintenance window relaxes the wait
// the way it relaxes command reads. The checker wakes once per window —
// never per frame, which under steady traffic would spin taking the
// manager lock — and treats a fed pingCh as proof of life for the
// window that just elapsed. Settings are re-snapshot every cycle;
// interval <= 0 parks the loop until a config update revives it.
func (m *Manager) healthCheck() {
	timer := time.NewTimer(time.Minute)
	timer.Stop()
	defer timer.Stop()

	for {
		m.mu.RLock()
		interval := m.cfg.HealthCheckInterval
		pingTimeout := m.cfg.PingTimeout
		m.mu.RUnlock()

		if interval <= 0 {
			select {
			case <-m.cfgChanged:
				continue
			case <-m.done:
				return
			}
		}

		timer.Reset(interval)
		select {
		case <-m.cfgChanged:
			continue
		case <-m.done:
			return
		case <-timer.C:
		}

		select {
		case <-m.pingCh:
			// A frame arrived during the window: alive, nothing to probe.
			continue
		default:
		}

		// A non-positive PingTimeout removes the probe's own deadline (the
		// write still honours WriteTimeout) and, below, the reply
		// requirement; a born-expired context would fail every write and
		// drop a healthy connection on each tick.
		ctx, cancel := timeoutCtx(pingTimeout)
		cn, pingErr := m.healthPing(ctx)
		cancel()

		if errors.Is(pingErr, errPubSubNoConn) || errors.Is(pingErr, errPingNotSent) {
			// No conn (possibly released since the tick started) means
			// nothing to health-check; a probe that never got the I/O
			// slot (a dial or a write held it for the whole window) has
			// no verdict either — that operation reports its own failure.
			continue
		}
		if pingErr != nil {
			// The failed ping already dropped the conn (see pingLocked);
			// nil cn = "restore unless someone already did".
			_ = m.reconnectBounded(context.Background(), nil, pingErr)
			continue
		}

		// The write went out; now the server owes a reply.
		wait := cn.EffectiveReadTimeout(pingTimeout)
		if wait <= 0 {
			continue
		}
		timer.Reset(wait)
		select {
		case <-m.pingCh:
			// Alive.
		case <-timer.C:
			// cn is the connection the PING went out on: a reconnect
			// that already replaced it makes this a no-op.
			_ = m.reconnectBounded(context.Background(), cn, errPingTimeout)
		case <-m.cfgChanged:
		case <-m.done:
			return
		}
	}
}

// healthPing writes the health-check PING without dialing, returning
// the connection it went out on so the reply wait can target it
// (errPubSubNoConn when there is none).
func (m *Manager) healthPing(ctx context.Context) (*pool.Conn, error) {
	if err := m.acquireIO(ctx); err != nil {
		if errors.Is(err, pool.ErrClosed) {
			return nil, err
		}
		return nil, errPingNotSent
	}
	defer m.releaseIO()
	// The slot may have come free just as the probe's deadline expired
	// (acquireIO's select is not biased): a write with a born-expired
	// deadline fails before sending a byte, and writeReleasingLock would
	// drop a healthy connection for every subscriber. Like every writer,
	// check before touching the connection.
	if ctx.Err() != nil {
		return nil, errPingNotSent
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	// The probe's own PingTimeout bounds its write: a manager policy.
	return m.pingLocked(ctx, ctx, nil, nil, false)
}

// retryRejected re-sends, on a fixed cadence, the subscriptions the
// server rejected with an error reply (NOPERM, LOADING, a cluster
// redirect, …) so a rejection that was transient heals without a
// reconnect. It is the only path that re-sends on the same connection:
// an unconfirmed name is never retried (see subState). The loop is
// independent of the health checker (which may be disabled) and
// deliberately consumes no shared wake channels (cfgChanged and
// wakeListen each keep exactly one consumer).
func (m *Manager) retryRejected() {
	m.mu.RLock()
	interval := m.cfg.SubscribeRetryInterval
	m.mu.RUnlock()

	// <= 0 disables the loop; the root package defaults the interval, so
	// a nonpositive value here is a deliberate opt-out.
	if interval <= 0 {
		return
	}

	timer := time.NewTimer(time.Minute)
	timer.Stop()
	defer timer.Stop()

	for {
		// Nothing registered and no connection: park until a dial (or a
		// kept-registration subscribe failure) signals wakeResync, rather
		// than ticking no-ops forever.
		m.mu.RLock()
		idle := m.conn == nil && m.noSubscribersLocked()
		m.mu.RUnlock()
		if idle {
			select {
			case <-m.wakeResync:
			case <-m.done:
				return
			}
			continue
		}

		timer.Reset(interval)
		select {
		case <-timer.C:
			// Re-snapshot: WithChannelPingTimeout applies at runtime.
			m.mu.RLock()
			pingTimeout := m.cfg.PingTimeout
			m.mu.RUnlock()
			m.resubscribeRejected(pingTimeout)
		case <-m.done:
			return
		}
	}
}

// connectIdempotentLocked ensures the shared connection exists: with
// none, it dials — with m.mu RELEASED for the duration of the dial, see
// dialUnlocked — and replays every registered subscription on the fresh
// connection. An existing connection is reported as errConnExists: no
// replay ran, so subscribers must write their commands themselves.
// Holding m.io, the caller is the only dialer: a concurrent connect
// waits for the slot and then finds the connection installed.
//
// Callers must hold m.io and m.mu; they hold both again on return, but
// because m.mu was dropped around the dial they must re-validate
// whatever they inspected before the call (the manager or their handle
// may have closed meanwhile).
func (m *Manager) connectIdempotentLocked(ctx context.Context) error {
	if m.conn != nil {
		return errConnExists
	}

	// A closed manager must not dial: the connection would leak.
	select {
	case <-m.done:
		return pool.ErrClosed
	default:
	}

	// Started on the first dial ATTEMPT, not the first success:
	// registrations kept across a failed dial need the reconnect replay.
	m.once.Do(func() {
		go m.listen()
		go m.healthCheck()
		go m.retryRejected()
	})

	// Every dial but the first — a restoration, or a retry after a first
	// dial that failed — is redirected like a reconnect: the resolver
	// sees the connection lost (which may have received a maintenance
	// handoff naming the endpoint to use), nil when none ever came up —
	// a resolver that rotates endpoints on a dead address needs the call
	// all the same, or every retry would dial the same dead address.
	if m.dialed {
		m.resolveAddrLocked(ctx, m.dropped)
	}
	m.dialed = true

	cn, err := m.dialUnlocked(ctx, m.cfg.Addr)
	if err != nil {
		return err
	}

	// The lock was released during the dial: a Close that landed
	// meanwhile owns the teardown, and the connection it never saw must
	// not outlive it.
	select {
	case <-m.done:
		_ = m.closeConn(cn)
		return pool.ErrClosed
	default:
	}
	m.conn, m.dropped = cn, nil

	// The replay is written on every subscriber's behalf, so the dialing
	// caller's context must not bound it; a failed write drops the
	// connection again.
	if err := m.writeReleasingLock(socketCtx, cn, m.replayLocked()); err != nil {
		return err
	}

	// Reset the outage accounting and un-park the idle read and resync
	// loops (independent non-blocking sends: a coupled or blocking send
	// under m.mu could starve one loop or deadlock).
	m.reconnectAttempts = 0
	m.nextReconnectAt = time.Time{}
	select {
	case m.wakeListen <- struct{}{}:
	default:
	}
	select {
	case m.wakeResync <- struct{}{}:
	default:
	}
	return nil
}

// resolveAddrLocked lets the owner's resolver redirect the dial about
// to happen — and every one after it — e.g. to the endpoint a
// maintenance handoff named (see AddrResolver). prev is the connection
// being replaced: the live one a reconnect retires, or the one lost
// when a restoration dials; nil when there is none. Callers must hold
// m.mu.
func (m *Manager) resolveAddrLocked(ctx context.Context, prev *pool.Conn) {
	if m.resolver == nil {
		return
	}
	if next := m.resolver.Resolve(ctx, prev, m.cfg.Addr); next != "" {
		m.cfg.Addr = next
	}
}

// reconnect closes cn, dials a new connection and replays the registry
// — but only if cn is still the manager's connection (a stale cn means
// someone already reconnected). cn may be nil to restore a lost
// connection. The dial runs with m.mu released (see dialUnlocked);
// holding m.io, this is the only dialer.
func (m *Manager) reconnect(ctx context.Context, cn *pool.Conn, reason error) error {
	if err := m.acquireIO(ctx); err != nil {
		return err
	}
	defer m.releaseIO()

	m.mu.Lock()
	defer m.mu.Unlock()

	// A reconnect that dialed after Close would leak the connection.
	select {
	case <-m.done:
		return pool.ErrClosed
	default:
	}

	// A stale cn means someone already reconnected (or released the
	// connection) while this call waited for the I/O slot.
	if m.conn != cn {
		return nil
	}

	if m.noSubscribersLocked() {
		// Settle waiters even with the conn already gone: a failed write
		// can have dropped it while a ping-only handle still awaits its
		// pong.
		m.dropConnLocked(errPubSubNoConn)
		return errPubSubNoConn
	}

	// The resolver sees the connection being replaced: the live one being
	// retired, or — restoring a lost connection — the one dropped.
	prev := cn
	if prev == nil {
		prev = m.dropped
	}
	m.resolveAddrLocked(ctx, prev)

	m.reconnectAttempts++
	m.dialed = true
	newConn, err := m.dialUnlocked(ctx, m.cfg.Addr)
	if err != nil {
		m.nextReconnectAt = time.Now().Add(m.reconnectBackoffLocked())
		if m.requestTopologyRefresh != nil {
			m.requestTopologyRefresh()
		}
		m.reconnectLog.Printf(ctx,
			"pubsub: reconnect failed (attempt %d, reconnecting due to: %v): %v",
			m.reconnectAttempts, reason, err)
		return err
	}

	// The lock was released during the dial: re-validate before
	// installing. A Close that landed meanwhile owns the teardown, and
	// the connection it never saw must not outlive it; with the last
	// subscriber gone nothing needs the connection. (Writers wait for
	// m.io, so only Close or a release by the handle that unsubscribed
	// before the dial can have changed the state meanwhile.)
	select {
	case <-m.done:
		_ = m.closeConn(newConn)
		return pool.ErrClosed
	default:
	}
	if m.noSubscribersLocked() {
		_ = m.closeConn(newConn)
		m.dropConnLocked(errPubSubNoConn)
		return errPubSubNoConn
	}

	oldConn := m.conn
	m.conn, m.dropped = newConn, nil

	// The old connection's outstanding replies die with the switch;
	// settle them before the replay builds the fresh ledger.
	m.failReplyWaitersLocked(reason)

	if err := m.writeReleasingLock(ctx, newConn, m.replayLocked()); err != nil {
		// The failed replay write dropped newConn; retire the source too.
		if oldConn != nil {
			_ = m.closeConn(oldConn)
		}
		m.nextReconnectAt = time.Now().Add(m.reconnectBackoffLocked())

		if m.requestTopologyRefresh != nil {
			m.requestTopologyRefresh()
		}
		m.reconnectLog.Printf(ctx,
			"pubsub: resubscribe failed (attempt %d, reconnecting due to: %v): %v",
			m.reconnectAttempts, reason, err)
		return err
	}

	// Retire the handoff source only now, with the replacement
	// subscribed: closing it earlier would open a window with neither
	// connection subscribed.
	if oldConn != nil {
		_ = m.closeConn(oldConn)
	}

	internal.Logger.Printf(ctx, "pubsub: reconnected (attempts: %d, due to: %v)", m.reconnectAttempts, reason)
	m.reconnectAttempts = 0
	m.nextReconnectAt = time.Time{}
	return nil
}

// dialUnlocked dials addr with m.mu released, so the dial and handshake
// never stall the operations that take only the state lock. m.io stays
// held: that makes the dial single-flight and keeps every other writer
// — including one that may have nothing to write, such as an
// unsubscribe or a handle Close — waiting for its outcome. Callers must
// hold m.io and m.mu; m.mu is held again on return.
func (m *Manager) dialUnlocked(ctx context.Context, addr string) (*pool.Conn, error) {
	m.mu.Unlock()
	cn, err := m.newConn(ctx, addr)
	m.mu.Lock()
	return cn, err
}

// reconnectBackoffLocked is the pause before the next reconnect attempt,
// from the attempt count: the exponential schedule the read loop also
// applies after a failed receive. Callers must hold m.mu.
func (m *Manager) reconnectBackoffLocked() time.Duration {
	minBackoff := m.cfg.MinRetryBackoff
	if minBackoff <= 0 {
		minBackoff = 10 * time.Millisecond
	}
	maxBackoff := max(m.cfg.ReconnectMaxBackoff, minBackoff)
	return internal.RetryBackoff(max(m.reconnectAttempts-1, 0), minBackoff, maxBackoff)
}

// reconnectBounded is reconnect with ctx bounded by the configured
// ReconnectTimeout.
func (m *Manager) reconnectBounded(ctx context.Context, cn *pool.Conn, reason error) error {
	m.mu.RLock()
	reconnectTimeout := m.cfg.ReconnectTimeout
	m.mu.RUnlock()
	if reconnectTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, reconnectTimeout)
		defer cancel()
	}
	return m.reconnect(ctx, cn, reason)
}

// replayLocked prepares the subscription replay for a freshly installed
// connection: the ledger died with the old one, every registered name
// is Pending again (a fresh connection owes every confirmation), and
// one broadcast command per kind re-subscribes them. Callers must hold
// m.mu and m.io; the returned commands go to writeReleasingLock.
func (m *Manager) replayLocked() [][]any {
	m.replyQueue = nil

	var cmds [][]any
	for _, r := range []struct {
		kind     string
		registry map[string]*subscription
	}{
		{"subscribe", m.subscribers},
		{"psubscribe", m.patternSubscribers},
		{"ssubscribe", m.shardSubscribers},
	} {
		if len(r.registry) == 0 {
			continue
		}
		cmds = append(cmds, m.prepareSubscribeLocked(r.kind, replyWaiter{broadcast: true}, collectAllChannelNames(r.registry))...)
	}
	return cmds
}

// resubscribeRejected re-sends every Rejected subscription — answered
// with an error reply instead of a confirmation — making it Pending
// again until the server answers the retry. The I/O slot wait and each
// write are bounded by pingTimeout, with a budget each: a dead pipe
// must not hold the slot for the full WriteTimeout, and a budget shared
// across the wait could be spent by the time the write starts — a
// socket deadline already in the past fails the write before a byte is
// sent, and writeReleasingLock would then drop a live connection for
// every subscriber. With no connection the reconnect replay owns
// recovery.
func (m *Manager) resubscribeRejected(pingTimeout time.Duration) {
	waitCtx, cancelWait := timeoutCtx(pingTimeout)
	defer cancelWait()
	if err := m.acquireIO(waitCtx); err != nil {
		return
	}
	defer m.releaseIO()

	m.mu.Lock()
	defer m.mu.Unlock()

	for kind, registry := range map[string]map[string]*subscription{
		"subscribe":  m.subscribers,
		"psubscribe": m.patternSubscribers,
		"ssubscribe": m.shardSubscribers,
	} {
		// Re-checked per kind: a failed write drops the connection.
		if m.conn == nil {
			return
		}
		rejected := namesInOrder(registry, func(sub *subscription) bool { return sub.state == subStateRejected })
		if len(rejected) == 0 {
			continue
		}
		// Broadcast: the retry's confirmations belong to every owner,
		// like a replay's.
		cmds := m.prepareSubscribeLocked(kind, replyWaiter{broadcast: true}, rejected)
		// The write's budget starts now, slot and lock in hand.
		writeCtx, cancelWrite := timeoutCtx(pingTimeout)
		err := m.writeReleasingLock(writeCtx, m.conn, cmds)
		cancelWrite()
		if err != nil {
			return
		}
	}
}

// timeoutCtx bounds a fresh background context by d; d <= 0 means no
// bound (and a no-op cancel).
func timeoutCtx(d time.Duration) (context.Context, context.CancelFunc) {
	if d <= 0 {
		return context.Background(), func() {}
	}
	return context.WithTimeout(context.Background(), d)
}

// prepareSubscribeLocked ledgers the (un)subscribe command(s) carrying
// names, attributed as the template w says — to w.h, to every owner
// (w.broadcast), or to the handles that released the names (w.affected,
// for an orphan unsubscribe) — and returns them to be written with
// writeReleasingLock. The server refuses a subscribe
// command as a whole when it denies any of its names, so a name whose
// last outcome was a rejection is written alone: it cannot drag its
// siblings down with it, and the error reply then pins the blame on it
// exactly (see markRejectedLocked). The other names are batched;
// unsubscribes always are. Sharded commands are further split into one
// command per hash slot, in first-appearance order: a cluster server
// rejects slot-spanning SSUBSCRIBE with CROSSSLOT, and confirmations
// must come back in subscription order. A subscribe write owes a
// confirmation, so its names become Pending (a Rejected one is thus not
// re-sent twice by retryRejected). Callers must hold m.mu, and m.io
// through the write.
func (m *Manager) prepareSubscribeLocked(redisCommand string, w replyWaiter, names []string) [][]any {
	if len(names) == 0 {
		return nil
	}

	batch, solo := names, []string(nil)
	switch redisCommand {
	case "subscribe", "psubscribe", "ssubscribe":
		registry := m.registryForKind(redisCommand)
		batch = make([]string, 0, len(names))
		for _, name := range names {
			if sub := registry[name]; sub != nil && sub.state == subStateRejected {
				solo = append(solo, name)
			} else {
				batch = append(batch, name)
			}
		}
	}

	var groups [][]string
	if len(batch) > 0 {
		groups = slotGroups(redisCommand, batch)
	}
	for _, name := range solo {
		groups = append(groups, []string{name})
	}

	cmds := make([][]any, 0, len(groups))
	for _, group := range groups {
		args := make([]any, 0, 1+len(group))
		args = append(args, redisCommand)
		for _, name := range group {
			args = append(args, name)
		}
		cmds = append(cmds, args)
		w.kind, w.names = redisCommand, slices.Clone(group)
		m.replyQueue = append(m.replyQueue, w)
	}

	switch redisCommand {
	case "subscribe", "psubscribe", "ssubscribe":
		registry := m.registryForKind(redisCommand)
		for _, name := range names {
			if sub := registry[name]; sub != nil {
				sub.state = subStatePending
			}
		}
	}
	return cmds
}

// slotGroups splits the names of a sharded command into one group per
// hash slot, in first-appearance order; other commands keep one group.
func slotGroups(redisCommand string, names []string) [][]string {
	switch redisCommand {
	case "ssubscribe", "sunsubscribe":
	default:
		return [][]string{names}
	}
	bySlot := make(map[int][]string)
	var order []int
	for _, name := range names {
		slot := hashtag.Slot(name)
		if _, ok := bySlot[slot]; !ok {
			order = append(order, slot)
		}
		bySlot[slot] = append(bySlot[slot], name)
	}
	groups := make([][]string, 0, len(order))
	for _, slot := range order {
		groups = append(groups, bySlot[slot])
	}
	return groups
}

// socketCtx is the context for writes on the shared connection on a
// subscriber's behalf: deliberately none of the callers'. The write
// deadline is derived from the context, and a failed write drops the
// connection for every subscriber (see writeReleasingLock), so one
// handle's short deadline expiring mid-write would interrupt delivery
// for all of them. Only WriteTimeout bounds these writes; a caller's
// context still governs its wait for the I/O slot and the dial it
// triggers, where the only collateral is delay. The manager's own
// probes pass their PingTimeout context instead.
var socketCtx = context.Background()

// writeReleasingLock writes cmds on cn, in order and fire-and-forget
// (replies arrive on the read loop), with m.mu RELEASED for the
// duration: the commands' ledger entries were appended under the lock
// just before, and m.io — held by the caller — keeps every other writer
// out until the write is done, so wire order equals ledger order. The
// lock is held again on return. A failed write leaves the RESP stream
// desynced, so the connection is dropped (its waiters failed) for the
// reconnect replay — unless a Close meanwhile already took it down.
// ctx bounds the socket write together with WriteTimeout and must be a
// manager policy (socketCtx, a probe's PingTimeout, a reconnect's
// bound), never a caller's. Callers must hold m.io and m.mu.
func (m *Manager) writeReleasingLock(ctx context.Context, cn *pool.Conn, cmds [][]any) error {
	if len(cmds) == 0 {
		return nil
	}
	writeTimeout := m.cfg.WriteTimeout

	m.mu.Unlock()
	var err error
	for _, args := range cmds {
		if err = cn.WithWriter(ctx, writeTimeout, func(wr *proto.Writer) error {
			return wr.WriteArgs(args)
		}); err != nil {
			break
		}
	}
	m.mu.Lock()

	// Only a dialer installs a connection, and dialers need m.io: cn is
	// still the connection unless Close dropped it.
	if err != nil && m.conn == cn {
		m.dropConnLocked(err)
	}
	return err
}

// handleUnsubscribe removes h — or every handle, when h is nil — from
// the given names (all of the handle's names when empty). Removal is
// immediate at write time, with a synthesized confirmation; only names
// left with no subscriber are unsubscribed server-side (a bare
// UNSUBSCRIBE would tear down other handles' subscriptions).
func (m *Manager) handleUnsubscribe(ctx context.Context, h *handle, redisCommand string, names ...string) error {
	if err := m.acquireIO(ctx); err != nil {
		// A closed manager unsubscribed everything already.
		if errors.Is(err, pool.ErrClosed) {
			return nil
		}
		return err
	}
	defer m.releaseIO()
	if err := ctx.Err(); err != nil {
		return err // see handleSubscribe
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	return m.handleUnsubscribeLocked(h, redisCommand, names...)
}

// handleUnsubscribeLocked is handleUnsubscribe's body; it exists so
// handle.Close can detach all three namespaces and mark the handle
// closed while holding the I/O slot throughout, so no subscribe can
// interleave. Callers must hold m.io and m.mu; m.mu is released around
// the server-side unsubscribe write and held again on return.
func (m *Manager) handleUnsubscribeLocked(h *handle, redisCommand string, names ...string) error {
	registry := m.registryForKind(redisCommand)

	var hs map[*handle]struct{}
	if h != nil {
		hs = map[*handle]struct{}{h: {}}
	} else {
		hs = collectAllHandlers(registry)
	}

	// Names whose last owner leaves, in first-appearance order so
	// confirmations come back in unsubscribe order — and, per name, the
	// handles that released it, for an error reply to reach (see
	// replyWaiter.affected).
	var orphanOrder []string
	affected := make(map[string][]*handle)
	for cur := range hs {
		owned := cur.channels
		switch redisCommand {
		case "punsubscribe":
			owned = cur.patterns
		case "sunsubscribe":
			owned = cur.schannels
		}

		// No names given means everything the handle owns.
		curNames := names
		if len(curNames) == 0 {
			curNames = make([]string, 0, len(owned))
			for name := range owned {
				curNames = append(curNames, name)
			}
			// Nothing owned: a bare unsubscribe still gets its reply, as
			// the server answers one with no subscriptions ("unsubscribe
			// nil 0"), so a Receive awaiting it returns.
			if len(curNames) == 0 && h != nil {
				cur.deliverSubscriptionLocked(&Subscription{Kind: redisCommand})
			}
		}

		for _, name := range curNames {
			if _, ok := owned[name]; !ok {
				// Not owned by this handle: nothing to detach or write,
				// but the server would still have answered — synthesize
				// the confirmation so a Receive awaiting it does not hang.
				if h != nil {
					cur.deliverSubscriptionLocked(&Subscription{
						Kind:    redisCommand,
						Channel: name,
						Count:   cur.ownedTotal(),
					})
				}
				continue
			}
			delete(owned, name)
			affected[name] = append(affected[name], cur)
			if sub := registry[name]; sub != nil {
				delete(sub.handles, cur)
				if len(sub.handles) == 0 {
					delete(registry, name)
					orphanOrder = append(orphanOrder, name)
				}
			}
			cur.deliverSubscriptionLocked(&Subscription{
				Kind:    redisCommand,
				Channel: name,
				Count:   cur.ownedTotal(),
			})
		}
	}

	if m.conn != nil && len(orphanOrder) > 0 {
		cmds := m.prepareSubscribeLocked(redisCommand, replyWaiter{affected: affected}, orphanOrder)
		if err := m.writeReleasingLock(socketCtx, m.conn, cmds); err != nil {
			// The failed write dropped the connection; the reconnect
			// replays exactly what is still registered.
			return err
		}
	}

	// Nothing left to listen for: release the connection (closing it
	// unsubscribes everything server-side anyway). Checked even with no
	// unsubscribe written: an empty handle can have dialed the shared
	// conn through Ping or ClientSetName, and its Close must not leave
	// the conn (and the health-check traffic on it) running.
	if m.conn != nil && m.noSubscribersLocked() {
		m.dropConnLocked(errPubSubNoConn)
	}
	return nil
}

// failReplyWaitersLocked settles every outstanding ledger entry when
// the connection is dropped or released: the replies can no longer
// arrive, so pong waiters get an error event instead of waiting
// forever. Callers must hold m.mu.
func (m *Manager) failReplyWaitersLocked(err error) {
	for i := range m.replyQueue {
		if m.replyQueue[i].kind == replyKindPong && m.replyQueue[i].h != nil {
			m.replyQueue[i].h.deliverLocked(err)
		}
	}
	m.replyQueue = nil
}

// dropConnLocked settles the ledger and releases the connection: once
// the conn is gone its outstanding replies can never arrive, and a
// silent drop would strand pong waiters until a later connect wipes the
// ledger. Callers must hold m.mu.
func (m *Manager) dropConnLocked(err error) {
	m.failReplyWaitersLocked(err)
	if m.conn != nil {
		_ = m.closeConn(m.conn)
		m.dropped, m.conn = m.conn, nil
	}
}

// receive reads and parses one frame, returning it together with the
// connection it was read from.
func (m *Manager) receive(ctx context.Context) (any, *pool.Conn, error) {
	// Snapshot the connection: reconnect may replace m.conn while this
	// goroutine is blocked reading.
	m.mu.RLock()
	cn := m.conn
	m.mu.RUnlock()

	if cn == nil {
		_ = m.reconnectBounded(ctx, nil, errPubSubNoConn)
		return nil, nil, errPubSubNoConn
	}

	var reply any
	// Block until a frame arrives (timeout 0): a pub/sub connection is
	// idle by design, so a read deadline would fail healthy connections.
	err := cn.WithReader(ctx, 0, func(rd *proto.Reader) error {
		// Drain buffered push notifications before reading the reply.
		if err := m.processPush(ctx, cn, rd); err != nil {
			internal.Logger.Printf(ctx, "push: conn[%d] error processing pending notifications before reading reply: %v", cn.GetID(), err)
		}
		var err error
		reply, err = rd.ReadReply()
		return err
	})
	if err != nil {
		// allowTimeout is false: the read has no deadline, so a timeout
		// means the conn is broken.
		if m.isBadConn(err, false) {
			_ = m.reconnectBounded(ctx, cn, err)
			return nil, nil, err
		}
		// A RESP error reply leaves the connection healthy. The ledger
		// attributes it: the handle that issued the rejected command
		// receives it alone; a replay or retry command (broadcast)
		// reports to the owners of the names it carried; an orphan
		// unsubscribe reports to the handles that released its names;
		// an entry nobody awaits, or an unmatched reply, fans out to
		// every handle (Receive surfaces it, the Channel pumps skip it).
		// A rejected
		// (re)subscribe's names become Rejected; retryRejected re-sends
		// them one by one.
		m.errorLog.Printf(ctx, "pubsub: error reply on shared connection: %v", err)
		// MOVED: the topology view is stale; the reload-driven sweep
		// moves the registered channels to the right owner. (ASK is a
		// transient one-shot redirect, not a stale view: see
		// isMovedError.)
		if isMovedError(err) && m.requestTopologyRefresh != nil {
			m.requestTopologyRefresh()
		}
		m.mu.Lock()
		if m.conn != cn {
			m.mu.Unlock()
			return nil, nil, nil
		}
		w, ok := m.consumeErrorReplyLocked()
		if ok {
			m.markRejectedLocked(w)
		}
		switch {
		case ok && !w.broadcast && w.h != nil:
			w.h.deliverLocked(err)
		case ok && len(w.affected) > 0:
			// An orphan unsubscribe: its names left the registry when
			// it was written, so the handles that released them were
			// captured instead.
			m.deliverToReleasersLocked(w, err)
		case ok && len(w.names) > 0:
			m.deliverToOwnersLocked(w, err)
		default:
			m.fanoutErrorLocked(err)
		}
		m.mu.Unlock()
		return nil, nil, nil
	}

	// A background operation may have made the connection unusable, or
	// the owner's resolver may want it retired (in the client: a
	// maintenance handoff dispatched by processPush during this very
	// read marked it): reconnect — the resolver picks the address — and
	// replay there. The frame just read is still delivered — the routing
	// in listen drops stateful frames whose source is no longer the live
	// connection, so messages pass through and confirmations or pongs
	// can't settle the replacement's replayed ledger.
	if m.shouldReplace(cn) {
		_ = m.reconnectBounded(ctx, cn, errConnUnusable)
	}

	parsed, err := parsePubSubMessage(reply)
	if err != nil {
		return nil, nil, err
	}

	// Record received-message telemetry.
	switch v := parsed.(type) {
	case *shardMessage:
		otel.RecordPubSubMessage(ctx, cn, "received", v.Channel, true)
	case *patternMessage:
		otel.RecordPubSubMessage(ctx, cn, "received", v.Channel, false)
	case *Message:
		otel.RecordPubSubMessage(ctx, cn, "received", v.Channel, false)
	}
	return parsed, cn, nil
}

// shouldReplace reports whether the connection a frame was just read
// from must be retired now: the pool marked it unusable, or the owner's
// resolver wants it gone. The resolver is consulted under the manager
// lock — the serialization AddrResolver promises against Resolve, which
// a concurrent reconnect (a health-check timeout, say) runs under the
// same lock. A connection already replaced needs nothing. A replacement
// whose dial keeps failing (a redirect to an endpoint not accepting
// connections yet) is retried on the reconnect backoff, not on every
// frame: the frames keep flowing from the connection being retired
// meanwhile.
func (m *Manager) shouldReplace(cn *pool.Conn) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.conn != cn {
		return false
	}
	if cn.IsUsable() && (m.resolver == nil || !m.resolver.ShouldReplace(cn)) {
		return false
	}
	return time.Until(m.nextReconnectAt) <= 0
}

// receiveNext blocks until a frame worth routing arrives (nil for an
// error reply, already fanned out), returning it with its source
// connection. Every frame feeds the health check. Only the listen
// goroutine may call it.
func (m *Manager) receiveNext(ctx context.Context) (any, *pool.Conn, error) {
	reply, cn, err := m.receive(ctx)
	if err != nil {
		return nil, nil, err
	}

	select {
	case m.pingCh <- struct{}{}:
	default:
	}
	return reply, cn, nil
}

// consumerSettings snapshots the settings a consumer-view pump is built
// with; a later config update never affects an already-started pump.
func (m *Manager) consumerSettings() (chanSize int, sendTimeout time.Duration) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.cfg.ChanSize, m.cfg.SendTimeout
}

// The update* methods back the PubSubConfiger surface: manager-wide
// config mutations, applied from the next snapshot on.

func (m *Manager) updatePingTimeout(newTimeout time.Duration) {
	m.mu.Lock()
	m.cfg.PingTimeout = newTimeout
	m.mu.Unlock()

	// A health checker waiting out a reply with the old timeout
	// re-snapshots.
	select {
	case m.cfgChanged <- struct{}{}:
	default:
	}
}

func (m *Manager) updateChannelSize(newSize int) {
	// A non-positive size is meaningless for a buffer: zero would make
	// every later delivery stream unbuffered (and dropped), a negative
	// one would panic in make. Ignore it.
	if newSize <= 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cfg.ChanSize = newSize
}

func (m *Manager) updateSendTimeout(newTimeout time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cfg.SendTimeout = newTimeout
}

func (m *Manager) updateHealthCheckInterval(newInterval time.Duration) {
	m.mu.Lock()
	m.cfg.HealthCheckInterval = newInterval
	m.mu.Unlock()

	select {
	case m.cfgChanged <- struct{}{}:
	default:
	}
}

func (m *Manager) updateReconnectTimeout(newTimeout time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.cfg.ReconnectTimeout = newTimeout
}

// isMovedError reports a MOVED reply: the topology view that routed
// the rejected command is stale. ASK deliberately is not one: it is a
// one-shot redirect for the duration of a slot migration, during which
// CLUSTER SLOTS still names the migrating source — a reload would
// change nothing, and a connection pinned to one node cannot follow a
// one-shot redirect to another. An ASK-rejected name is treated like
// any other refusal: it stays Rejected and retryRejected re-sends it
// on its interval, so the retry that lands after the migration either
// succeeds or earns the MOVED that does drive a reload.
func isMovedError(err error) bool {
	return strings.HasPrefix(err.Error(), "MOVED ")
}

// shardMessage marks a Message delivered via sharded pub/sub, a
// namespace separate from regular channels.
type shardMessage struct{ *Message }

// patternMessage marks a Message delivered via a pattern subscription:
// the frame kind, not the Pattern field, is the routing discriminator
// (the empty pattern is valid).
type patternMessage struct{ *Message }

// registerHandleLocked adds h to the registry entry for name. A new
// entry starts Pending (its write isn't confirmed yet) and takes the
// next registration sequence; an existing one keeps its state and
// place. Callers must hold m.mu.
func (m *Manager) registerHandleLocked(registry map[string]*subscription, name string, h *handle) {
	sub := registry[name]
	if sub == nil {
		m.regSeq++
		sub = &subscription{
			handles: make(map[*handle]struct{}),
			state:   subStatePending,
			seq:     m.regSeq,
		}
		registry[name] = sub
	}
	sub.handles[h] = struct{}{}
}

func collectAllHandlers(registry map[string]*subscription) map[*handle]struct{} {
	all := make(map[*handle]struct{})
	for _, sub := range registry {
		for h := range sub.handles {
			all[h] = struct{}{}
		}
	}
	return all
}

// collectAllChannelNames returns the registry's names in registration
// order (see subscription.seq).
func collectAllChannelNames(registry map[string]*subscription) []string {
	return namesInOrder(registry, func(*subscription) bool { return true })
}

// namesInOrder returns the names whose entry passes keep, in
// registration order (see subscription.seq).
func namesInOrder(registry map[string]*subscription, keep func(*subscription) bool) []string {
	names := make([]string, 0, len(registry))
	for name, sub := range registry {
		if keep(sub) {
			names = append(names, name)
		}
	}
	slices.SortFunc(names, func(a, b string) int {
		return cmp.Compare(registry[a].seq, registry[b].seq)
	})
	return names
}
