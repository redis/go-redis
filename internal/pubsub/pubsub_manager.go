package pubsub

import (
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

// subState tracks a registered name: Pending from the fire-and-forget
// subscribe write until the server's confirmation makes it Subscribed.
type subState uint8

const (
	subStatePending subState = iota
	subStateSubscribed
)

// subscription is one registered name's registry entry: its fan-out
// handles and its establishment state. Names left Pending (rejected or
// lost subscribes) are re-sent by resubscribePending.
type subscription struct {
	handles map[*handle]struct{}
	state   subState
	// sentAt is when the name's (re)subscribe was last written: a
	// Pending name gets one resync interval from its own write before
	// resubscribePending re-sends it.
	sentAt time.Time
}

// Manager multiplexes every pub/sub subscription of one client over a
// single shared connection: subscribers get a handle each, and the
// manager's read loop fans incoming messages out to the handles
// registered for the message's channel, pattern or shard channel.
type Manager struct {
	cfg Config

	// newConn dials cfg.Addr; a maintenance handoff redirects the
	// manager by rewriting cfg.Addr (see reconnect).
	newConn     func(ctx context.Context, addr string) (*pool.Conn, error)
	closeConn   func(*pool.Conn) error
	processPush func(ctx context.Context, cn *pool.Conn, rd *proto.Reader) error
	// isBadConn: true means broken-and-replace, false means an error
	// reply on a healthy connection.
	isBadConn func(err error, allowTimeout bool) bool
	// requestTopologyRefresh (optional, must not block) asks the owner
	// for a topology refresh; the cluster client wires it to LazyReload.
	requestTopologyRefresh func()
	mu                     sync.RWMutex

	// the ONE connection
	conn *pool.Conn

	// Reconnect bookkeeping (guarded by mu).
	reconnectAttempts int
	reconnectLogTime  time.Time

	// The per-kind registries, one subscription entry per name. They
	// always equal the desired state: registration happens at write
	// time, removal is immediate.
	subscribers        map[string]*subscription
	patternSubscribers map[string]*subscription
	shardSubscribers   map[string]*subscription

	// handles tracks every live handle, subscribed or not; pong and
	// error-reply fan-out iterate all of them.
	handles map[*handle]struct{}

	// lastPendingResync throttles resubscribePending (guarded by mu).
	lastPendingResync time.Time

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
}

// NewManager creates a manager that multiplexes all subscriptions over
// one shared connection, dialed lazily on the first Subscribe. cfg must
// arrive with defaults already applied.
func NewManager(
	cfg Config,
	newConn func(ctx context.Context, addr string) (*pool.Conn, error),
	closeConn func(*pool.Conn) error,
	processPush func(ctx context.Context, cn *pool.Conn, rd *proto.Reader) error,
	isBadConn func(err error, allowTimeout bool) bool,
	requestTopologyRefresh func(),
) *Manager {
	return &Manager{
		cfg: cfg,

		newConn:                newConn,
		closeConn:              closeConn,
		processPush:            processPush,
		isBadConn:              isBadConn,
		requestTopologyRefresh: requestTopologyRefresh,

		subscribers:        make(map[string]*subscription),
		patternSubscribers: make(map[string]*subscription),
		shardSubscribers:   make(map[string]*subscription),

		handles: make(map[*handle]struct{}),

		pingCh:     make(chan struct{}, 1),
		cfgChanged: make(chan struct{}, 1),
		wakeListen: make(chan struct{}, 1),
		wakeResync: make(chan struct{}, 1),
		done:       make(chan struct{}),
	}
}

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
	if err := m.writeArgs(ctx, []any{"client", "setname", name}); err != nil {
		// Partial write ⇒ desynced RESP stream: drop the connection for
		// the reconnect replay (see Ping).
		if m.conn != nil {
			_ = m.closeConn(m.conn)
			m.conn = nil
		}
		return err
	}
	m.replyQueue = append(m.replyQueue, replyWaiter{kind: replyKindPong, h: h})
	return nil
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

	// Connect BEFORE registering the new names: a fresh dial replays the
	// registry in map order, while the explicit write below keeps
	// confirmations in subscription order.
	err := m.connectIdempotentLocked(ctx)
	if errors.Is(err, errConnExists) {
		err = nil
	}

	// Register even when the connect failed: the intent must survive for
	// the reconnect replay to restore, since callers hand out the PubSub
	// without checking this error.
	switch redisCommand {
	case "subscribe":
		for _, ch := range channels {
			handle.channels[ch] = struct{}{}
			registerHandle(m.subscribers, ch, handle)
		}
	case "psubscribe":
		for _, pt := range channels {
			handle.patterns[pt] = struct{}{}
			registerHandle(m.patternSubscribers, pt, handle)
		}
	case "ssubscribe":
		for _, ch := range channels {
			handle.schannels[ch] = struct{}{}
			registerHandle(m.shardSubscribers, ch, handle)
		}
	}

	if err == nil {
		if err = m.subscribe(ctx, redisCommand, handle, false, channels...); err != nil {
			// Partial write ⇒ desynced RESP stream: drop the connection
			// so the reconnect replays the registry.
			if m.conn != nil {
				_ = m.closeConn(m.conn)
				m.conn = nil
			}
			// The caller gets no handle, so a handle created by this call
			// must not stay registered: it would be resubscribed by the
			// replay with nobody to drain it, and would keep the manager
			// from ever going idle.
			if h == nil {
				m.rollbackSubscribeLocked(handle, redisCommand, channels)
			}
			return nil, err
		}
	} else {
		// A caller-held handle keeps its registration for the reconnect
		// replay; a handle created by this call is returned as nil, so
		// its registration is rolled back instead.
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
	return m.ping(ctx, nil, true, payload...)
}

// ping writes a PING gated on and awaited by h (nil: nobody awaits the
// pong); see pingLocked.
func (m *Manager) ping(ctx context.Context, h *handle, shouldDial bool, payload ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if h != nil && h.closed {
		return pool.ErrClosed
	}
	return m.pingLocked(ctx, h, shouldDial, payload...)
}

// pingLocked writes a PING; the pong is consumed by waiter's ledger
// entry (nil: nobody awaits it). Callers must hold m.mu and gate on the
// issuing handle's closed state. shouldDial controls the no-connection
// behavior: user-facing pings dial the shared connection on demand,
// while the health checker's ping must report errPubSubNoConn instead —
// its tick and this write run in separate critical sections, so the
// last unsubscribe can release the connection in between, and a dialing
// ping would resurrect a connection with no subscribers that nothing
// ever closes (its own later pings would keep the zombie alive).
func (m *Manager) pingLocked(ctx context.Context, waiter *handle, shouldDial bool, payload ...string) error {
	args := []any{"ping"}
	if len(payload) == 1 {
		args = append(args, payload[0])
	}

	select {
	case <-m.done:
		return pool.ErrClosed
	default:
	}

	if shouldDial {
		if err := m.connectIdempotentLocked(ctx); err != nil && !errors.Is(err, errConnExists) {
			return err
		}
	} else if m.conn == nil {
		return errPubSubNoConn
	}
	if err := m.writeArgs(ctx, args); err != nil {
		// Partial write ⇒ desynced RESP stream: drop the connection so
		// the reconnect replays the registry.
		if m.conn != nil {
			_ = m.closeConn(m.conn)
			m.conn = nil
		}
		return err
	}
	m.replyQueue = append(m.replyQueue, replyWaiter{kind: replyKindPong, h: waiter})
	return nil
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

// CloseIfIdle closes the manager only when it has no subscriptions,
// reporting whether it is closed afterwards. Check and close are atomic
// under the manager lock, so a racing Subscribe either registers first
// or observes pool.ErrClosed and retries with a fresh manager.
func (m *Manager) CloseIfIdle() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	select {
	case <-m.done:
		return true
	default:
	}
	if !m.noSubscribersLocked() {
		return false
	}
	_ = m.closeNowLocked()
	return true
}

// closeNowLocked runs the teardown. Callers must hold m.mu with done
// not yet closed.
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
		ev, err := m.receiveNext(ctx)
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
			// Receive already attempted a reconnect; back off so a
			// persistent failure doesn't spin.
			if errCount > 0 {
				time.Sleep(internal.RetryBackoff(errCount-1, minBackoff, maxBackoff))
			}
			errCount++
			continue
		}
		errCount = 0

		// Confirmations mutate entry state (Pending → Subscribed) and
		// pongs consume their queue entry, so both need the write lock;
		// messages fan out read-only.
		switch ev := ev.(type) {
		case *Subscription:
			m.mu.Lock()
			m.fanoutSubscriptionLocked(ev)
			m.mu.Unlock()
			continue
		case *Pong:
			m.mu.Lock()
			m.fanoutPongLocked(ev)
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

// retireWaitersLocked removes the given names from every outstanding
// entry of the kind: their replies are presumed lost, and the retry
// about to be written owns them from now on — a superseded waiter left
// queued would be consumed by the retry's confirmation and shift the
// attribution of every entry behind it. Callers must hold the WRITE
// lock.
func (m *Manager) retireWaitersLocked(kind string, names []string) {
	for _, name := range names {
		for i := 0; i < len(m.replyQueue); {
			e := &m.replyQueue[i]
			if e.kind == kind {
				if j := slices.Index(e.names, name); j >= 0 {
					e.names = slices.Delete(e.names, j, j+1)
					if len(e.names) == 0 {
						m.replyQueue = slices.Delete(m.replyQueue, i, i+1)
						continue
					}
				}
			}
			i++
		}
	}
}

// consumeErrorReplyLocked settles an error reply: replies arrive in
// write order, so the rejected command is the ledger head and its one
// error frame replaces everything the entry was owed. Callers must hold
// the WRITE lock.
func (m *Manager) consumeErrorReplyLocked() {
	if len(m.replyQueue) > 0 {
		m.replyQueue = slices.Delete(m.replyQueue, 0, 1)
	}
}

func (m *Manager) fanoutShardedMessageLocked(shardedMsg *Message) {
	if sub := m.shardSubscribers[shardedMsg.Channel]; sub != nil {
		for h := range sub.handles {
			h.deliverLocked(shardedMsg)
		}
	}
}

func (m *Manager) fanoutMessageLocked(msg *Message) {
	if sub := m.subscribers[msg.Channel]; sub != nil {
		for h := range sub.handles {
			h.deliverLocked(msg)
		}
	}
}

func (m *Manager) fanoutPatternMessageLocked(msg *Message) {
	if sub := m.patternSubscribers[msg.Pattern]; sub != nil {
		for h := range sub.handles {
			h.deliverLocked(msg)
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

// healthCheck pings the shared connection whenever it has been silent
// for HealthCheckInterval (every received frame feeds m.pingCh, resetting
// the clock), reconnects when the ping fails, and reconciles Pending
// subscriptions. Settings are re-snapshot every cycle; interval <= 0
// parks the loop until a config update revives it.
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
		case <-m.pingCh:
			// A frame arrived — the connection is alive. It may be an
			// error reply rejecting a subscribe: reconcile Pending.
			m.resubscribePending(interval, pingTimeout)
		case <-m.cfgChanged:
		case <-timer.C:
			ctx, cancel := context.WithTimeout(context.Background(), pingTimeout)
			pingErr := m.ping(ctx, nil, false)
			cancel()

			if errors.Is(pingErr, errPubSubNoConn) {
				// No conn (possibly released since the tick started)
				// means nothing to health-check
				continue
			}
			if pingErr != nil {
				// The failed ping already dropped the conn (see ping);
				// nil cn = "restore unless someone already did".
				_ = m.reconnectBounded(context.Background(), nil, pingErr)
			} else {
				m.resubscribePending(interval, pingTimeout)
			}
		case <-m.done:
			return
		}
	}
}

// resubscribe re-sends Pending subscriptions on a fixed cadence,
// independent of the health checker (which may be disabled): a
// subscribe rejected with an error reply on a healthy connection — or
// one whose confirmation is lost without breaking the socket — must not
// stay Pending forever just because periodic PINGs are off.
// resubscribePending no-ops cheaply when there is no connection or
// nothing is Pending, so the idle tick costs nothing; the loop
// deliberately consumes no shared wake channels (cfgChanged and wake
// each keep exactly one consumer).
func (m *Manager) resubscribe() {
	m.mu.RLock()
	interval := m.cfg.PendingResyncFallback
	pingTimeout := m.cfg.PingTimeout
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
			m.resubscribePending(interval, pingTimeout)
		case <-m.done:
			return
		}
	}
}

// connectIdempotentLocked dials the shared connection if there is none,
// replaying every registered subscription. An existing connection is
// reported as errConnExists: no replay ran, so subscribers must write
// their commands themselves. Callers must hold m.mu.
func (m *Manager) connectIdempotentLocked(ctx context.Context) error {
	if m.conn != nil {
		return errConnExists
	}

	// Started on the first dial ATTEMPT, not the first success:
	// registrations kept across a failed dial need the reconnect replay.
	m.once.Do(func() {
		go m.listen()
		go m.healthCheck()
		go m.resubscribe()
	})

	var err error
	m.conn, err = m.newConn(ctx, m.cfg.Addr)
	if err != nil {
		return err
	}

	if err := m.resubscribeLocked(ctx); err != nil {
		_ = m.closeConn(m.conn)
		m.conn = nil
		return err
	}

	// Reset the outage accounting and un-park the idle read and resync
	// loops (independent non-blocking sends: a coupled or blocking send
	// under m.mu could starve one loop or deadlock).
	m.reconnectAttempts = 0
	m.reconnectLogTime = time.Time{}
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

// reconnect closes cn, dials a new connection and replays the registry
// — but only if cn is still the manager's connection (a stale cn means
// someone already reconnected). cn may be nil to restore a lost
// connection.
func (m *Manager) reconnect(ctx context.Context, cn *pool.Conn, reason error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	// A reconnect that dialed after Close would leak the connection.
	select {
	case <-m.done:
		return pool.ErrClosed
	default:
	}

	if m.conn != cn {
		return nil
	}

	// A MOVING maintenance handoff carries the endpoint to move to;
	// redirect this dial and every one after it.
	if cn != nil && cn.ShouldHandoff() {
		if newAddr := cn.GetHandoffEndpoint(); newAddr != "" && newAddr != internal.RedisNull {
			internal.Logger.Printf(ctx, "pubsub: maintenance handoff, redirecting to %s (was %s)", newAddr, m.cfg.Addr)
			m.cfg.Addr = newAddr
		}
	}

	// Nothing to restore: release the connection (nobody is listening,
	// and a handoff-updated cfg.Addr must not be shadowed by a stale
	// conn) and stay disconnected, the next Subscribe redials. m.conn
	// can be nil here: a nil cn passes the identity guard above.
	if m.noSubscribersLocked() {
		if m.conn != nil {
			m.failReplyWaitersLocked(errPubSubNoConn)
			_ = m.closeConn(m.conn)
			m.conn = nil
		}
		return errPubSubNoConn
	}

	m.reconnectAttempts++
	newConn, err := m.newConn(ctx, m.cfg.Addr)
	if err != nil {
		if m.requestTopologyRefresh != nil {
			m.requestTopologyRefresh()
		}
		logThrottled(ctx, &m.reconnectLogTime, m.cfg.LogInterval,
			"pubsub: reconnect failed (attempt %d, reconnecting due to: %v): %v",
			m.reconnectAttempts, reason, err)
		return err
	}

	if m.conn != nil {
		_ = m.closeConn(m.conn)
		m.conn = nil
	}

	m.conn = newConn

	if err := m.resubscribeLocked(ctx); err != nil {
		_ = m.closeConn(m.conn)
		m.conn = nil

		if m.requestTopologyRefresh != nil {
			m.requestTopologyRefresh()
		}
		logThrottled(ctx, &m.reconnectLogTime, m.cfg.LogInterval,
			"pubsub: resubscribe failed (attempt %d, reconnecting due to: %v): %v",
			m.reconnectAttempts, reason, err)
		return err
	}

	internal.Logger.Printf(ctx, "pubsub: reconnected (attempts: %d, due to: %v)", m.reconnectAttempts, reason)
	m.reconnectAttempts = 0
	m.reconnectLogTime = time.Time{}
	return nil
}

// reconnectBounded is reconnect with ctx bounded by the configured
// ReconnectTimeout: reconnect dials while holding m.mu, and an unbounded
// dial would hold the lock hostage — blocking Subscribe, the health
// checker and even Close — for as long as the injected dialer takes.
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

// resubscribeLocked replays every registered subscription on the
// current connection, dropping every entry back to Pending — a fresh
// connection owes every confirmation. Callers must hold m.mu.
func (m *Manager) resubscribeLocked(ctx context.Context) error {
	// The ledger died with the old connection; the replay writes below
	// enqueue fresh entries.
	m.replyQueue = nil

	for _, registry := range []map[string]*subscription{
		m.subscribers, m.patternSubscribers, m.shardSubscribers,
	} {
		for _, sub := range registry {
			sub.state = subStatePending
		}
	}

	if len(m.subscribers) > 0 {
		if err := m.subscribe(ctx, "subscribe", nil, true, collectAllChannelNames(m.subscribers)...); err != nil {
			return err
		}
	}
	if len(m.patternSubscribers) > 0 {
		if err := m.subscribe(ctx, "psubscribe", nil, true, collectAllChannelNames(m.patternSubscribers)...); err != nil {
			return err
		}
	}
	if len(m.shardSubscribers) > 0 {
		if err := m.subscribe(ctx, "ssubscribe", nil, true, collectAllChannelNames(m.shardSubscribers)...); err != nil {
			return err
		}
	}
	return nil
}

// resubscribePending re-sends every Pending subscription — written but
// never confirmed, i.e. rejected or lost. Rejections surface only as
// unattributable error replies on a healthy connection, so the health
// checker calls this on every wakeup, throttled to once per interval.
// Writes are bounded by pingTimeout: they hold m.mu, and a dead pipe
// must not stall the manager for the full WriteTimeout. With no
// connection the reconnect replay owns recovery.
func (m *Manager) resubscribePending(interval, pingTimeout time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.conn == nil || interval <= 0 || time.Since(m.lastPendingResync) < interval {
		return
	}
	// The scan throttle only: per-name grace comes from each entry's
	// sentAt, so unrelated subscribe traffic can't starve a retry.
	m.lastPendingResync = time.Now()

	ctx := context.Background()
	if pingTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, pingTimeout)
		defer cancel()
	}
	for kind, registry := range map[string]map[string]*subscription{
		"subscribe":  m.subscribers,
		"psubscribe": m.patternSubscribers,
		"ssubscribe": m.shardSubscribers,
	} {
		var pending []string
		for name, sub := range registry {
			if sub.state == subStatePending && time.Since(sub.sentAt) >= interval {
				pending = append(pending, name)
			}
		}
		if len(pending) == 0 {
			continue
		}
		// The originals' replies are presumed lost: retire their waiters
		// so the retry's confirmations settle the retry's entry.
		m.retireWaitersLocked(kind, pending)
		if err := m.subscribe(ctx, kind, nil, true, pending...); err != nil {
			// Partial write ⇒ desynced RESP stream: drop the connection
			// so the reconnect replays the registry.
			if m.conn != nil {
				_ = m.closeConn(m.conn)
				m.conn = nil
			}
			return
		}
	}
}

// subscribe writes one (un)subscribe command carrying the given names,
// ledgered for h (or every owner, when broadcast). Sharded commands are
// split into one command per hash slot: a cluster server rejects
// slot-spanning SSUBSCRIBE with CROSSSLOT.
func (m *Manager) subscribe(ctx context.Context, redisCommand string, h *handle, broadcast bool, channels ...string) error {
	switch redisCommand {
	case "ssubscribe", "sunsubscribe":
		// Write the slot groups in first-appearance order so
		// confirmations come back in subscription order.
		bySlot := make(map[int][]string)
		var order []int
		for _, channel := range channels {
			slot := hashtag.Slot(channel)
			if _, ok := bySlot[slot]; !ok {
				order = append(order, slot)
			}
			bySlot[slot] = append(bySlot[slot], channel)
		}
		for _, slot := range order {
			if err := m.writeSubscriptionCmd(ctx, redisCommand, bySlot[slot], h, broadcast); err != nil {
				return err
			}
		}
		return nil
	default:
		return m.writeSubscriptionCmd(ctx, redisCommand, channels, h, broadcast)
	}
}

func (m *Manager) writeSubscriptionCmd(ctx context.Context, redisCommand string, names []string, h *handle, broadcast bool) error {
	args := make([]any, 0, 1+len(names))
	args = append(args, redisCommand)
	for _, name := range names {
		args = append(args, name)
	}
	if err := m.writeArgs(ctx, args); err != nil {
		return err
	}
	m.replyQueue = append(m.replyQueue, replyWaiter{kind: redisCommand, names: slices.Clone(names), h: h, broadcast: broadcast})
	switch redisCommand {
	case "subscribe", "psubscribe", "ssubscribe":
		registry := m.registryForKind(redisCommand)
		now := time.Now()
		for _, name := range names {
			if sub := registry[name]; sub != nil {
				sub.sentAt = now
			}
		}
	}
	return nil
}

// writeArgs writes one command fire-and-forget: replies arrive
// asynchronously on the read loop.
func (m *Manager) writeArgs(ctx context.Context, args []any) error {
	if m.conn == nil {
		return errPubSubNoConn
	}
	return m.conn.WithWriter(ctx, m.cfg.WriteTimeout, func(wr *proto.Writer) error {
		return wr.WriteArgs(args)
	})
}

// handleUnsubscribe removes h — or every handle, when h is nil — from
// the given names (all of the handle's names when empty). Removal is
// immediate at write time, with a synthesized confirmation; only names
// left with no subscriber are unsubscribed server-side (a bare
// UNSUBSCRIBE would tear down other handles' subscriptions).
func (m *Manager) handleUnsubscribe(ctx context.Context, h *handle, redisCommand string, names ...string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.handleUnsubscribeLocked(ctx, h, redisCommand, names...)
}

// handleUnsubscribeLocked is handleUnsubscribe's body; it exists so
// handle.Close can detach all three namespaces and mark the handle
// closed in ONE critical section. Callers must hold m.mu.
func (m *Manager) handleUnsubscribeLocked(ctx context.Context, h *handle, redisCommand string, names ...string) error {
	registry := m.registryForKind(redisCommand)

	var hs map[*handle]struct{}
	if h != nil {
		hs = map[*handle]struct{}{h: {}}
	} else {
		hs = collectAllHandlers(registry)
	}

	// Names whose last owner leaves, in first-appearance order so
	// confirmations come back in unsubscribe order.
	var orphanOrder []string
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
		}

		for _, name := range curNames {
			if _, ok := owned[name]; !ok {
				continue
			}
			delete(owned, name)
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
		if err := m.subscribe(ctx, redisCommand, nil, false, orphanOrder...); err != nil {
			// Partial write ⇒ desynced RESP stream: drop the connection so
			// the reconnect replays exactly what is still registered.
			if m.conn != nil {
				_ = m.closeConn(m.conn)
				m.conn = nil
			}
			return err
		}
	}

	// Nothing left to listen for: release the connection (closing it
	// unsubscribes everything server-side anyway). Checked even with no
	// unsubscribe written: an empty handle can have dialed the shared
	// conn through Ping or ClientSetName, and its Close must not leave
	// the conn (and the health-check traffic on it) running.
	if m.conn != nil && m.noSubscribersLocked() {
		m.failReplyWaitersLocked(errPubSubNoConn)
		_ = m.closeConn(m.conn)
		m.conn = nil
	}
	return nil
}

// failReplyWaitersLocked settles every outstanding ledger entry when
// the connection is deliberately released: the replies can no longer
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

func (m *Manager) receive(ctx context.Context) (any, error) {
	// Snapshot the connection: reconnect may replace m.conn while this
	// goroutine is blocked reading.
	m.mu.RLock()
	cn := m.conn
	m.mu.RUnlock()

	if cn == nil {
		// Restore the connection; the caller's retry loop owns
		// persistence.
		_ = m.reconnectBounded(ctx, nil, errPubSubNoConn)
		return nil, errPubSubNoConn
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
			return nil, err
		}
		// A RESP error reply leaves the connection healthy. It cannot be
		// attributed to one subscriber, so it fans out to all of them
		// (Receive surfaces it, the Channel pumps skip it). A rejected
		// (re)subscribe stays Pending and resubscribePending re-sends it.
		internal.Logger.Printf(ctx, "pubsub: error reply on shared connection: %v", err)
		// MOVED/ASK: the topology view is stale; the reload-driven sweep
		// moves the registered channels to the right owner.
		if isRedirectError(err) && m.requestTopologyRefresh != nil {
			m.requestTopologyRefresh()
		}
		m.mu.Lock()
		// Only the live connection's ledger may be settled.
		if m.conn == cn {
			m.consumeErrorReplyLocked()
		}
		m.fanoutErrorLocked(err)
		m.mu.Unlock()
		return nil, nil
	}

	m.mu.RLock()
	stale := m.conn != cn
	m.mu.RUnlock()
	if stale {
		return nil, nil
	}

	// A MOVING handoff (dispatched by processPush during the read) marked
	// the connection: reconnect redirects to the handoff endpoint and
	// replays there.
	handedOff := cn.ShouldHandoff() || !cn.IsUsable()
	if handedOff {
		_ = m.reconnectBounded(ctx, cn, errConnUnusable)
	}

	parsed, err := parsePubSubMessage(reply)
	if err != nil {
		return nil, err
	}

	if handedOff {
		m.mu.RLock()
		replaced := m.conn != cn
		m.mu.RUnlock()
		if replaced {
			// The frame was read from the replaced connection. Messages
			// are real deliveries and pass through; confirmations and
			// pongs correlate with cn's dead ledger — settling them
			// against the replacement's replayed ledger would mark names
			// Subscribed that the new node never confirmed and shift
			// every waiter behind them.
			switch parsed.(type) {
			case *Subscription, *Pong:
				return nil, nil
			}
		}
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
	return parsed, nil
}

// receiveNext blocks until a frame worth routing arrives (nil for an
// error reply, already fanned out). Every frame feeds the health check.
// Only the listen goroutine may call it.
func (m *Manager) receiveNext(ctx context.Context) (any, error) {
	reply, err := m.receive(ctx)
	if err != nil {
		return nil, err
	}

	select {
	case m.pingCh <- struct{}{}:
	default:
	}
	return reply, nil
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
	defer m.mu.Unlock()
	m.cfg.PingTimeout = newTimeout
}

func (m *Manager) updateChannelSize(newSize int) {
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

// isRedirectError reports a cluster redirect reply (MOVED/ASK): the
// topology view that routed the rejected command is stale.
func isRedirectError(err error) bool {
	msg := err.Error()
	return strings.HasPrefix(msg, "MOVED ") || strings.HasPrefix(msg, "ASK ")
}

// shardMessage marks a Message delivered via sharded pub/sub, a
// namespace separate from regular channels.
type shardMessage struct{ *Message }

// patternMessage marks a Message delivered via a pattern subscription:
// the frame kind, not the Pattern field, is the routing discriminator
// (the empty pattern is valid).
type patternMessage struct{ *Message }

// logThrottled logs at most once per interval (lastLog is owned and
// synchronized by the caller), reporting whether it logged.
func logThrottled(ctx context.Context, lastLog *time.Time, interval time.Duration, format string, args ...any) bool {
	now := time.Now()
	if now.Sub(*lastLog) < interval {
		return false
	}
	*lastLog = now
	internal.Logger.Printf(ctx, format, args...)
	return true
}

// registerHandle adds h to the registry entry for name. A new entry
// starts Pending (its write isn't confirmed yet); an existing one keeps
// its state.
func registerHandle(registry map[string]*subscription, name string, h *handle) {
	sub := registry[name]
	if sub == nil {
		sub = &subscription{
			handles: make(map[*handle]struct{}),
			state:   subStatePending,
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

func collectAllChannelNames(registry map[string]*subscription) []string {
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	return names
}
