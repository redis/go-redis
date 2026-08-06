package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/failuredetector"
	"github.com/redis/go-redis/v9/internal/otel"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/maintnotifications"
	"github.com/redis/go-redis/v9/push"

	imultidb "github.com/redis/go-redis/v9/internal/multidb"
)

// multidbDatabase is one member database: its client, circuit breaker,
// resolved health checks and metadata.
type multidbDatabase struct {
	// exactly one of c / cc is set; client points at the same object through
	// the shared Process/Close surface.
	c  *Client
	cc *ClusterClient

	cb     *imultidb.CircuitBreaker
	weight float64 // guarded by core.dbMu
	fqdn   string
	// idx is the database's current position in core.dbs, maintained under
	// core.dbMu on every membership change. Callbacks read it lock-free to
	// avoid recursive dbMu read-locking (which can deadlock with a queued
	// writer).
	idx atomic.Int32

	checks []MultiDBHealthCheck
	policy MultiDBHealthCheckPolicy

	// removed is set (under core.failoverMu) when the database leaves the
	// membership; a stale probe snapshot must stop recording outcomes and
	// firing callbacks for it.
	removed atomic.Bool

	// cbq delivers user circuit-state callbacks asynchronously (FIFO).
	cbq callbackQueue
}

// callbackQueue runs user callbacks on their own goroutine, in FIFO order.
// Breaker state changes fire deep inside locked sections (failoverMu is held
// during manual selection, pre-failover probes and AddDatabase probes);
// invoking a user callback there would self-deadlock the moment the callback
// touches a control API that takes the same lock.
type callbackQueue struct {
	mu       sync.Mutex
	queue    []func()
	draining bool
}

func (q *callbackQueue) dispatch(fn func()) {
	q.mu.Lock()
	q.queue = append(q.queue, fn)
	if q.draining {
		q.mu.Unlock()
		return
	}
	q.draining = true
	q.mu.Unlock()
	go q.drain()
}

func (q *callbackQueue) drain() {
	for {
		q.mu.Lock()
		if len(q.queue) == 0 {
			q.draining = false
			q.mu.Unlock()
			return
		}
		fn := q.queue[0]
		q.queue = q.queue[1:]
		q.mu.Unlock()
		fn()
	}
}

func (db *multidbDatabase) process(ctx context.Context, cmd Cmder) error {
	if db.cc != nil {
		return db.cc.Process(ctx, cmd)
	}
	return db.c.Process(ctx, cmd)
}

func (db *multidbDatabase) closeClient() error {
	if db.cc != nil {
		return db.cc.Close()
	}
	return db.c.Close()
}

// selectable reports whether the database's circuit permits selecting it,
// WITHOUT reserving a half-open probe slot. IsAllowed consumes one of the
// breaker's bounded half-open requests, so it must only be called right
// before actually executing a command; candidate snapshots, background
// checks and failover re-checks use this instead, or repeated selections
// would exhaust a recovering database's probe budget without ever probing it.
func (db *multidbDatabase) selectable() bool {
	return db.cb.CheckState() != imultidb.CircuitOpen
}

// probe runs the database's health checks under the configured policy,
// bounded by HealthCheckTimeout, and feeds the result into the circuit
// breaker and the OTel recorder.
func (db *multidbDatabase) probe(parent context.Context, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	start := time.Now()
	var healthy bool
	if db.cc != nil {
		healthy = db.policy.ExecuteCluster(ctx, db.checks, db.cc)
	} else {
		healthy = db.policy.Execute(ctx, db.checks, db.c)
	}

	if !healthy && parent.Err() != nil {
		// The caller's own context was canceled or expired mid-probe: the
		// failure says nothing about the database. Record nothing on the
		// breaker — mirroring the command path's neutral outcome — but still
		// report unhealthy so callers do not treat it as a pass.
		return false
	}
	if db.removed.Load() {
		// The database left the membership while this probe was in flight
		// (background snapshot racing RemoveDatabase): its index is stale
		// and its client is closed, so record nothing and fire no callbacks.
		return healthy
	}

	// CheckState first so an Open circuit past its grace period transitions
	// to HalfOpen and can be closed by the success below.
	db.cb.CheckState()
	if healthy {
		// External: probes are not admitted through IsAllowed, so a success
		// must not release a half-open slot a real command probe is holding.
		db.cb.RecordExternalSuccess()
	} else {
		db.cb.RecordFailure()
	}
	otel.RecordMultiDBHealthCheck(ctx, db.fqdn, healthy, time.Since(start))
	return healthy
}

type multidbCore struct {
	opts *MultiDBOptions

	dbMu sync.RWMutex
	dbs  []*multidbDatabase

	active atomic.Int32

	detector MultiDBFailureDetector
	strategy MultiDBFailoverStrategy

	// fallbackInterval is the normalized auto-fallback cadence: always
	// positive so SetAutoFallback(true) works even when the client was
	// constructed with a negative AutoFallbackInterval (which only sets the
	// initial disabled state).
	fallbackInterval time.Duration

	// failoverMu serializes failover attempts and guards the escalation state.
	failoverMu           sync.Mutex
	failoverAttempts     int
	lastFailoverAttempt  time.Time
	autoFallbackDisabled atomic.Bool

	// successSinceFailover breaks the "consecutive failed failover attempts"
	// chain: any successful command sets it, and the next failed failover
	// starts escalating from zero instead of a stale count left over from an
	// earlier, already-recovered outage.
	successSinceFailover atomic.Bool

	pubsubMu sync.Mutex
	pubsubs  map[*PubSub]struct{}

	stopCh chan struct{}
	wg     sync.WaitGroup
	closed atomic.Bool
}

func newMultidbCore(opts *MultiDBOptions) *multidbCore {
	core := &multidbCore{
		opts:     opts,
		detector: opts.FailureDetector,
		strategy: opts.FailoverStrategy,
		pubsubs:  make(map[*PubSub]struct{}),
		stopCh:   make(chan struct{}),
	}
	// A stateful default detector must be per-client: writing it back into
	// the caller's options would share one sliding failure window across
	// every client built from the same MultiDBOptions value.
	if core.detector == nil {
		core.detector = failuredetector.NewCommandFailureDetector(
			failuredetector.DefaultCommandFailureDetectorConfig(),
		)
	}
	core.fallbackInterval = opts.AutoFallbackInterval
	if core.fallbackInterval <= 0 {
		core.fallbackInterval = defaultMultiDBAutoFallback
	}
	core.active.Store(-1)
	core.autoFallbackDisabled.Store(opts.AutoFallbackInterval < 0)
	return core
}

// buildDatabase constructs the underlying client, circuit breaker and
// resolved health checks for one member database.
func (c *multidbCore) buildDatabase(cfg *MultiDBClientConfig) (*multidbDatabase, error) {
	db := &multidbDatabase{
		weight: cfg.Weight,
		fqdn:   cfg.fqdn(),
	}
	if db.weight == 0 {
		db.weight = defaultMultiDBWeight
	}

	switch {
	case cfg.Options != nil:
		opt := *cfg.Options
		disableMaintNotificationsIfUnset(&opt)
		db.c = NewClient(&opt)
	case cfg.FailoverOptions != nil:
		// FailoverOptions does not carry a maintnotifications config (not
		// supported for failover clients), so there is nothing to disable.
		// Private copy, like the standalone and cluster members: the sentinel
		// machinery keeps reading these options on its dial path, and a
		// caller mutating the shared value would change a running member.
		opt := *cfg.FailoverOptions
		db.c = NewFailoverClient(&opt)
	case cfg.ClusterOptions != nil:
		opt := *cfg.ClusterOptions
		if opt.MaintNotificationsConfig == nil {
			opt.MaintNotificationsConfig = &maintnotifications.Config{Mode: maintnotifications.ModeDisabled}
		}
		db.cc = NewClusterClient(&opt)
	}

	db.cb = imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{
		FailureThreshold: c.opts.CircuitBreakerConfig.FailureThreshold,
		SuccessThreshold: c.opts.CircuitBreakerConfig.SuccessThreshold,
		GracePeriod:      c.opts.CircuitBreakerConfig.GracePeriod,
	})

	// Merge semantics: global checks + per-DB checks are additive; a database
	// with no checks from either layer gets a single default PING check.
	db.checks = append(db.checks, c.opts.HealthChecks...)
	db.checks = append(db.checks, cfg.HealthChecks...)
	if len(db.checks) == 0 {
		db.checks = []MultiDBHealthCheck{defaultPingHealthCheck{}}
	}
	db.policy = cfg.HealthCheckPolicy
	if db.policy == nil {
		db.policy = c.opts.HealthCheckPolicy
	}

	stateCallback := c.opts.OnCircuitStateChanged
	dbRef := db
	db.cb.OnStateChange(func(oldState, newState imultidb.CircuitState) {
		otel.RecordMultiDBCircuitStateChange(context.Background(), dbRef.fqdn,
			oldState.String(), newState.String())
		if stateCallback != nil {
			// Deliver asynchronously (FIFO per database): state changes fire
			// under internal locks, and a callback that calls a control API
			// would otherwise self-deadlock.
			idx := int(dbRef.idx.Load())
			from, to := oldState.String(), newState.String()
			dbRef.cbq.dispatch(func() { stateCallback(idx, from, to) })
		}
	})

	return db, nil
}

// initialize runs the initial health checks, enforces the InitialDBState
// policy and selects the initial active database. When ctx carries a deadline
// it blocks and retries every initialHealthCheckRetryDelay; otherwise a
// single pass is performed.
func (c *multidbCore) initialize(ctx context.Context) error {
	required := c.requiredAvailableCount()
	_, hasDeadline := ctx.Deadline()

	probeHealthy := make([]bool, len(c.dbs))
	probeNeutral := make([]bool, len(c.dbs))
	for {
		healthy := 0
		for i, db := range c.dbs {
			probeHealthy[i] = db.probe(ctx, c.opts.HealthCheckTimeout)
			// A probe that failed because the caller's context ended is no
			// verdict on the member (probe recorded nothing either).
			probeNeutral[i] = !probeHealthy[i] && ctx.Err() != nil
			if probeHealthy[i] {
				healthy++
			}
		}
		if healthy >= required {
			break
		}
		if !hasDeadline {
			return fmt.Errorf("%w: %d healthy, %d required", ErrInsufficientHealthyDatabases, healthy, required)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: %d healthy, %d required", ErrInsufficientHealthyDatabases, healthy, required)
		case <-time.After(initialHealthCheckRetryDelay):
		}
	}

	// Reconcile breaker state with the final probe pass, which is the
	// definitive startup signal:
	//  - a member that just passed its probe gets a fresh (closed) breaker,
	//    even if earlier retries of a blocking init opened it — otherwise a
	//    recovered member could not be selected until the grace period ends;
	//  - a member that failed its probe gets its breaker opened, so automatic
	//    failover cannot switch to a database already known to be down before
	//    the background checks have had a chance to open it organically.
	for i, db := range c.dbs {
		if probeNeutral[i] {
			// Never actually probed (the caller's context ended first):
			// leave the breaker untouched — the background checks will
			// establish the member's real state.
			continue
		}
		if probeHealthy[i] {
			if db.cb.CheckState() != imultidb.CircuitClosed {
				db.cb.Reset()
			}
			continue
		}
		for f := 0; f < db.cb.Config().FailureThreshold; f++ {
			db.cb.RecordFailure()
		}
	}

	// Select the highest-weight database among those that passed the final
	// probe pass. Circuit state alone is not enough here: with the default
	// FailureThreshold a database that failed its only startup probe would
	// otherwise still look selectable.
	cands := make([]MultiDBDatabaseState, 0, len(c.dbs))
	for i, db := range c.dbs {
		cands = append(cands, MultiDBDatabaseState{
			Index:   i,
			Weight:  db.weight,
			Allowed: probeHealthy[i],
		})
	}
	best := c.strategy.Select(cands)
	if best < 0 {
		return fmt.Errorf("%w: no selectable database", ErrInsufficientHealthyDatabases)
	}
	c.active.Store(int32(best))
	return nil
}

func (c *multidbCore) requiredAvailableCount() int {
	n := len(c.dbs)
	switch c.opts.InitialDBState {
	case InitialDBStateAllAvailable:
		return n
	case InitialDBStateOneAvailable:
		return 1
	default: // majority
		return n/2 + 1
	}
}

// candidates snapshots every database except `exclude` for the failover
// strategy.
func (c *multidbCore) candidates(exclude int) []MultiDBDatabaseState {
	c.dbMu.RLock()
	defer c.dbMu.RUnlock()
	out := make([]MultiDBDatabaseState, 0, len(c.dbs))
	for i, db := range c.dbs {
		if i == exclude {
			continue
		}
		out = append(out, MultiDBDatabaseState{
			Index:   i,
			Weight:  db.weight,
			Allowed: db.selectable(),
		})
	}
	return out
}

func (c *multidbCore) activeIndex() int { return int(c.active.Load()) }

// memberCount returns the current number of member databases.
func (c *multidbCore) memberCount() int {
	c.dbMu.RLock()
	defer c.dbMu.RUnlock()
	return len(c.dbs)
}

// activeSnapshot returns the active database, or nil when none is selected or
// the index is stale after a removal. The index is loaded under dbMu so it is
// coherent with the slice: RemoveDatabase shifts both under the write lock,
// and reading the index first could otherwise resolve to a shifted neighbor.
func (c *multidbCore) activeSnapshot() (*multidbDatabase, int) {
	c.dbMu.RLock()
	defer c.dbMu.RUnlock()
	idx := int(c.active.Load())
	if idx < 0 || idx >= len(c.dbs) {
		return nil, idx
	}
	return c.dbs[idx], idx
}

func (c *multidbCore) dbAt(index int) *multidbDatabase {
	c.dbMu.RLock()
	defer c.dbMu.RUnlock()
	if index < 0 || index >= len(c.dbs) {
		return nil
	}
	return c.dbs[index]
}

// process is the command hot path: an attempt loop bounded by CommandRetries
// that snapshots the active database, checks its circuit breaker, executes
// the command, and records the outcome on both the breaker and the aggregate
// failure detector. Failures can trigger failover before the next attempt.
func (c *multidbCore) process(ctx context.Context, cmd Cmder) error {
	if c.closed.Load() {
		cmd.SetErr(ErrClosed)
		return ErrClosed
	}
	attempts := c.opts.CommandRetries + 1
	// Blocking commands (BLPOP, XREAD BLOCK, WAIT, ...) carry their own read
	// timeout; a local read deadline on those must not be retried because
	// replaying can duplicate blocking side effects — same rule as *Client.
	retryTimeout := cmd.readTimeout() == nil
	var lastErr error

	attempt := 0
	// Bound consecutive gate rejections: when every selectable member is
	// half-open with a full probe budget, failover keeps handing back
	// members the gate then rejects — without a bound this ping-pongs the
	// active index in a busy loop instead of surfacing unavailability.
	gateRejections := 0
	maxGateRejections := c.memberCount() + 1

	for attempt < attempts {
		if err := ctx.Err(); err != nil {
			cmd.SetErr(err)
			return err
		}

		// Detector before IsAllowed: IsAllowed reserves a bounded half-open
		// probe slot, and a tripped detector routes to failover without
		// executing anything — the reservation would leak and eventually
		// starve the recovering active's probe budget.
		db, idx := c.activeSnapshot()
		if db == nil || c.detector.ShouldFailover() || !db.cb.IsAllowed() {
			gateRejections++
			if gateRejections > maxGateRejections {
				cmd.SetErr(ErrTemporarilyNotAvailable)
				return ErrTemporarilyNotAvailable
			}
			if err := c.tryFailover(ctx, idx); err != nil {
				cmd.SetErr(err)
				return err
			}
			// Re-enter the gate on the newly selected database: its breaker
			// may be half-open and IsAllowed above is what reserves the
			// probe slot. Re-gating does not consume a retry attempt.
			continue
		}
		gateRejections = 0

		if attempt > 0 {
			// Clear the previous attempt's error so a successful retry does
			// not leave the command in a stale error state.
			cmd.SetErr(nil)
		}
		attempt++
		err := db.process(ctx, cmd)
		switch classifyOutcome(err, retryTimeout) {
		case outcomeSuccess:
			db.cb.RecordSuccess()
			c.detector.RecordSuccess()
			c.successSinceFailover.Store(true)
			return err
		case outcomeNeutral:
			// Not a database-health signal: return to the caller without
			// recording a failure or failing over. Give back the half-open
			// probe slot that IsAllowed may have reserved above — recording
			// nothing would otherwise leak it.
			db.cb.ReleaseHalfOpen()
			return err
		case outcomeFailure:
			db.cb.RecordFailure()
			c.detector.RecordFailure(err)
			lastErr = err
			if cmd.NoRetry() {
				// Commands that stream into caller-owned writers/buffers
				// must never be replayed after a partial read: the failure
				// is recorded, but the error goes straight to the caller.
				return err
			}
		}
	}
	return lastErr
}

// isRedisReplyError reports whether err is an error reply from the server
// rather than a transport-level failure.
func isRedisReplyError(err error) bool {
	var redisErr proto.RedisError
	return errors.As(err, &redisErr)
}

// outcomeKind classifies a command outcome for breaker/detector recording.
type outcomeKind int

const (
	// outcomeSuccess proves the database served the request (including
	// definitive error replies like WRONGTYPE or redis.Nil).
	outcomeSuccess outcomeKind = iota
	// outcomeFailure is an availability signal: transport-level failures and
	// retryable server replies (LOADING, READONLY, CLUSTERDOWN, ...).
	outcomeFailure
	// outcomeNeutral is not a database-health signal at all: client-side
	// errors (context cancellation, deterministic local rejections) and
	// locally synthesized Redis errors such as ErrCrossSlot.
	outcomeNeutral
)

// classifyOutcome decides how a command outcome feeds the circuit breaker
// and the failure detector. Order matters: retryable server replies (LOADING,
// READONLY, ...) are availability failures even though they are RedisErrors,
// and locally synthesized RedisErrors (ErrCrossSlot) must not count as proof
// of a healthy server because no round trip happened. retryTimeout mirrors
// the *Client rule: false for commands with their own read timeout, whose
// local deadlines must not be treated as retryable failures.
func classifyOutcome(err error, retryTimeout bool) outcomeKind {
	switch {
	case err == nil:
		return outcomeSuccess
	case errors.Is(err, pool.ErrPoolTimeout) || errors.Is(err, pool.ErrPoolExhausted):
		// Local pool saturation: the command never reached the database, so
		// this is capacity pressure on the client, not a health signal —
		// the same classification the failure detector applies.
		return outcomeNeutral
	case shouldRetry(err, retryTimeout):
		return outcomeFailure
	case errors.Is(err, ErrCrossSlot):
		return outcomeNeutral
	case isRedisReplyError(err):
		return outcomeSuccess
	default:
		return outcomeNeutral
	}
}

const (
	failoverReasonAutomatic = "automatic"
	failoverReasonManual    = "manual"
	failoverReasonFallback  = "fallback"
)

// tryFailover selects a new active database and switches to it. It is the
// single automatic-failover path shared by the command hot path and the
// background loop; escalation state is shared so the two paths cannot
// double-count.
func (c *multidbCore) tryFailover(ctx context.Context, from int) error {
	c.failoverMu.Lock()
	// announce runs AFTER the unlock: user callbacks may call control APIs
	// that take failoverMu themselves.
	var announce func()
	defer func() {
		c.failoverMu.Unlock()
		if announce != nil {
			announce()
		}
	}()

	// Re-check under the lock: a concurrent failover may already have fixed
	// the active database.
	if db, idx := c.activeSnapshot(); db != nil && idx != from && db.selectable() {
		return nil
	}

	start := time.Now()
	cands := c.candidates(from)
	for {
		best := c.strategy.Select(cands)
		if best < 0 {
			// No alternate candidate. If the active database itself is fully
			// available again (health checks closed its breaker after the
			// failure burst that tripped the detector), stay on it and clear
			// the tripped state: escalating here would strand the client in
			// *NotAvailable forever, because no command can succeed to reset
			// the detector. Closed only — a half-open breaker must keep being
			// escalated or the gate would spin on exhausted probe slots.
			if db := c.dbAt(from); db != nil && db.cb.CheckState() == imultidb.CircuitClosed {
				c.failoverAttempts = 0
				c.detector.Reset()
				return nil
			}
			return c.recordFailedFailoverLocked()
		}
		if c.opts.ProbeTargetBeforeFailover {
			if db := c.dbAt(best); db != nil && !db.probe(ctx, c.opts.HealthCheckTimeout) {
				if err := ctx.Err(); err != nil {
					// The caller's context ended mid-probe: the candidate was
					// never proven unhealthy. Surface the context error
					// without dropping the candidate or charging a failover
					// attempt — short command deadlines must not escalate
					// toward ErrPermanentlyNotAvailable.
					return err
				}
				// The probe recorded the failure on the candidate's breaker;
				// drop it from this round and re-select.
				cands = removeCandidate(cands, best)
				continue
			}
		}
		c.failoverAttempts = 0
		announce = c.switchActive(ctx, from, best, failoverReasonAutomatic, time.Since(start))
		c.detector.Reset()
		return nil
	}
}

func removeCandidate(cands []MultiDBDatabaseState, index int) []MultiDBDatabaseState {
	out := cands[:0]
	for _, cand := range cands {
		if cand.Index != index {
			out = append(out, cand)
		}
	}
	return out
}

// recordFailedFailoverLocked implements the escalation chain: attempts are
// rate-limited by FailoverAttemptDelay (a burst within the window counts as
// one attempt), ErrTemporarilyNotAvailable is returned until
// MaxFailoverAttempts consecutive attempts have failed, then the terminal
// ErrPermanentlyNotAvailable. failoverMu must be held.
func (c *multidbCore) recordFailedFailoverLocked() error {
	if c.successSinceFailover.Swap(false) {
		// Successful traffic since the last failed attempt: the chain of
		// consecutive failures is broken, so a fresh outage escalates from
		// zero instead of a stale count.
		c.failoverAttempts = 0
	}
	now := time.Now()
	if c.lastFailoverAttempt.IsZero() || now.Sub(c.lastFailoverAttempt) >= c.opts.FailoverAttemptDelay {
		c.failoverAttempts++
		c.lastFailoverAttempt = now
	}
	if c.failoverAttempts >= c.opts.MaxFailoverAttempts {
		return ErrPermanentlyNotAvailable
	}
	return ErrTemporarilyNotAvailable
}

// switchActive is the single transition point for every active-index change:
// automatic failover, auto-fallback and manual selection all funnel through
// it, so callbacks, metrics and PubSub notifications fire exactly once per
// real change. It returns a non-nil announce closure when the switch
// happened; callers MUST invoke it AFTER releasing failoverMu — user
// callbacks may re-enter control APIs (SetActiveIndex, RemoveDatabase, ...)
// that take the same lock, and invoking them under it would self-deadlock.
func (c *multidbCore) switchActive(ctx context.Context, from, to int, reason string, took time.Duration) (announce func()) {
	if !c.active.CompareAndSwap(int32(from), int32(to)) {
		return nil
	}

	fromFQDN, toFQDN := "", ""
	if db := c.dbAt(from); db != nil {
		fromFQDN = db.fqdn
	}
	if db := c.dbAt(to); db != nil {
		toFQDN = db.fqdn
	}

	internal.Logger.Printf(ctx, "multidb: active database changed %d (%s) -> %d (%s), reason=%s",
		from, fromFQDN, to, toFQDN, reason)

	return func() {
		otel.RecordMultiDBActiveDatabaseChange(ctx, fromFQDN, toFQDN)
		if reason == failoverReasonAutomatic || reason == failoverReasonManual {
			otel.RecordMultiDBFailover(ctx, fromFQDN, toFQDN, reason, took)
			if c.opts.OnFailover != nil {
				c.opts.OnFailover(ctx, from, to)
			}
		}
		if c.opts.OnActiveDatabaseChanged != nil {
			c.opts.OnActiveDatabaseChanged(from, to)
		}
		c.notifyPubSubs(ctx)
	}
}

// setActiveIndex implements manual failover. With probe=true it is the safe
// probe-then-switch path (SetActiveIndex); with probe=false it is the
// unconditional operator override (ForceActiveIndex).
func (c *multidbCore) setActiveIndex(ctx context.Context, index int, probe bool) error {
	// An already-done context must not reach the probe below: a probe that
	// fails only because the caller's context ended would otherwise record
	// breaker failures against a perfectly healthy target.
	if err := ctx.Err(); err != nil {
		return err
	}
	// Hold failoverMu across the whole operation — lookup, probe and switch.
	// Membership changes (RemoveDatabase) also serialize on it, so the probed
	// database cannot be removed and the slice cannot shift between the probe
	// and the switch. The probe is bounded by HealthCheckTimeout.
	c.failoverMu.Lock()
	var announce func()
	defer func() {
		c.failoverMu.Unlock()
		if announce != nil {
			announce()
		}
	}()

	db := c.dbAt(index)
	if db == nil {
		return fmt.Errorf("redis: multidb: database index %d out of range", index)
	}
	start := time.Now()
	if probe {
		if !db.probe(ctx, c.opts.HealthCheckTimeout) {
			if err := ctx.Err(); err != nil {
				// The context ended mid-probe: report the caller's error,
				// not a verdict about the target's health.
				return err
			}
			return ErrTargetUnhealthy
		}
		if err := ctx.Err(); err != nil {
			// The probe passed, but the caller's context died while it ran:
			// a canceled control operation must not switch the active state.
			return err
		}
	}
	// The operator explicitly selected this database — either a fresh probe
	// just passed, or ForceActiveIndex is an unconditional override. Reset
	// its breaker in both cases so a still-open circuit does not immediately
	// fail the switch away; a genuinely dead forced target re-opens it
	// organically on the next failures.
	db.cb.Reset()
	// A fresh detector window as well: after an explicit operator selection
	// a previously tripped detector must not immediately fail away — also
	// when the selected database is already the active one.
	c.detector.Reset()
	from := int(c.active.Load())
	if from == index {
		return nil
	}
	c.failoverAttempts = 0
	if announce = c.switchActive(ctx, from, index, failoverReasonManual, time.Since(start)); announce == nil {
		return errors.New("redis: multidb: active database changed concurrently, retry")
	}
	return nil
}

func (c *multidbCore) addDatabase(ctx context.Context, cfg MultiDBClientConfig) (int, error) {
	if err := cfg.validate(); err != nil {
		return -1, err
	}
	if c.closed.Load() {
		return -1, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return -1, err
	}
	db, err := c.buildDatabase(&cfg)
	if err != nil {
		return -1, err
	}
	// Serialize with the other membership paths (RemoveDatabase, manual
	// failover), which also hold failoverMu: the new member's index is then
	// stable before the initial probe runs, so breaker state-change
	// callbacks fired by the probe report the real index instead of 0.
	c.failoverMu.Lock()
	defer c.failoverMu.Unlock()

	c.dbMu.RLock()
	idx := len(c.dbs)
	c.dbMu.RUnlock()
	db.idx.Store(int32(idx))

	// The initial probe result never blocks membership; it only seeds the
	// circuit breaker (and is skipped entirely with SkipInitialHealthCheck).
	// A failed probe opens the breaker outright — one recorded failure would
	// leave the member selectable under the default threshold, letting
	// failover pick a database already known to be down.
	if !cfg.SkipInitialHealthCheck {
		healthy := db.probe(ctx, c.opts.HealthCheckTimeout)
		if err := ctx.Err(); err != nil {
			// The caller's context ended while the probe ran (whatever the
			// verdict): a canceled control operation must not mutate the
			// membership.
			_ = db.closeClient()
			return -1, err
		}
		if !healthy {
			for f := 0; f < db.cb.Config().FailureThreshold; f++ {
				db.cb.RecordFailure()
			}
		}
	}

	c.dbMu.Lock()
	if c.closed.Load() {
		// Close ran while the member was being prepared: closeAll has
		// already drained c.dbs, so appending here would leak the client
		// (nothing would ever close it).
		c.dbMu.Unlock()
		_ = db.closeClient()
		return -1, ErrClosed
	}
	c.dbs = append(c.dbs, db)
	c.dbMu.Unlock()
	return idx, nil
}

func (c *multidbCore) removeDatabase(ctx context.Context, index int) error {
	// Hold failoverMu so the active-index adjustment below cannot race a
	// concurrent switchActive (all active transitions happen under it).
	c.failoverMu.Lock()
	defer c.failoverMu.Unlock()
	c.dbMu.Lock()
	if index < 0 || index >= len(c.dbs) {
		c.dbMu.Unlock()
		return fmt.Errorf("redis: multidb: database index %d out of range", index)
	}
	if int(c.active.Load()) == index {
		c.dbMu.Unlock()
		return errors.New("redis: multidb: cannot remove the active database")
	}
	db := c.dbs[index]
	// Mark before the client is closed: a background probe holding a stale
	// snapshot must stop recording outcomes for this member.
	db.removed.Store(true)
	c.dbs = append(c.dbs[:index], c.dbs[index+1:]...)
	for i := index; i < len(c.dbs); i++ {
		c.dbs[i].idx.Store(int32(i))
	}
	// Keep the active index pointing at the same database after the slice
	// shifted.
	if active := int(c.active.Load()); active > index {
		c.active.Store(int32(active - 1))
	}
	c.dbMu.Unlock()

	return db.closeClient()
}

func (c *multidbCore) setWeight(index int, weight float64) error {
	c.dbMu.Lock()
	defer c.dbMu.Unlock()
	if index < 0 || index >= len(c.dbs) {
		return fmt.Errorf("redis: multidb: database index %d out of range", index)
	}
	c.dbs[index].weight = weight
	return nil
}

func (c *multidbCore) setAutoFallback(enabled bool) {
	c.autoFallbackDisabled.Store(!enabled)
}

func (c *multidbCore) addDatabaseHook(index int, hook Hook) error {
	db := c.dbAt(index)
	if db == nil {
		return fmt.Errorf("redis: multidb: database index %d out of range", index)
	}
	if db.cc != nil {
		db.cc.AddHook(hook)
	} else {
		db.c.AddHook(hook)
	}
	return nil
}

// startBackgroundLoop starts the loop that keeps every database's circuit
// breaker fed by health checks, fails the active database over when its
// breaker no longer allows traffic (so idle and PubSub-only workloads still
// fail over), and performs auto-fallback to a recovered higher-weight
// database.
func (c *multidbCore) startBackgroundLoop() {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()
		ticker := time.NewTicker(c.opts.HealthCheckInterval)
		defer ticker.Stop()

		var lastFallbackCheck time.Time
		for {
			select {
			case <-c.stopCh:
				return
			case <-ticker.C:
			}

			ctx := context.Background()
			c.runHealthChecksOnce(ctx)

			// Background-driven failover: the active index must move even
			// with no command traffic.
			if db, idx := c.activeSnapshot(); db != nil && !db.selectable() {
				_ = c.tryFailover(ctx, idx)
			}

			if !c.autoFallbackDisabled.Load() &&
				time.Since(lastFallbackCheck) >= c.fallbackInterval {
				lastFallbackCheck = time.Now()
				c.tryFallbackToPrimary(ctx)
			}
		}
	}()
}

func (c *multidbCore) runHealthChecksOnce(ctx context.Context) {
	c.dbMu.RLock()
	dbs := make([]*multidbDatabase, len(c.dbs))
	copy(dbs, c.dbs)
	c.dbMu.RUnlock()

	for _, db := range dbs {
		select {
		case <-c.stopCh:
			return
		default:
		}
		db.probe(ctx, c.opts.HealthCheckTimeout)
	}
}

// tryFallbackToPrimary switches back to a strictly-higher-weight database
// whose circuit is closed again. Selection and switch happen under
// failoverMu so a concurrent RemoveDatabase (which also holds it) cannot
// remove the selected member or shift the slice in between.
func (c *multidbCore) tryFallbackToPrimary(ctx context.Context) {
	c.failoverMu.Lock()
	var announce func()
	defer func() {
		c.failoverMu.Unlock()
		if announce != nil {
			announce()
		}
	}()

	active, idx := c.activeSnapshot()
	if active == nil {
		return
	}

	c.dbMu.RLock()
	activeWeight := active.weight
	best, bestWeight := -1, activeWeight
	for i, db := range c.dbs {
		if i == idx {
			continue
		}
		if db.weight > bestWeight && db.cb.CheckState() == imultidb.CircuitClosed {
			best, bestWeight = i, db.weight
		}
	}
	c.dbMu.RUnlock()

	if best < 0 {
		return
	}
	if announce = c.switchActive(ctx, idx, best, failoverReasonFallback, 0); announce != nil {
		// The detector window still holds outcomes recorded against the old
		// active; left tripped, the very next command would immediately fail
		// away from the just-recovered primary. Clear it, as the automatic
		// and manual failover paths do.
		c.detector.Reset()
	}
}

// hasStandaloneMember reports whether any member database is served by a
// standalone (or sentinel) client, i.e. whether PubSub can ever be dialed.
func (c *multidbCore) hasStandaloneMember() bool {
	c.dbMu.RLock()
	defer c.dbMu.RUnlock()
	for _, db := range c.dbs {
		if db.c != nil {
			return true
		}
	}
	return false
}

// newPubSub creates a PubSub whose connections always target the currently
// active database: every (re-)dial resolves the active snapshot, and
// notifyPubSubs forces a re-dial on every active-database change.
func (c *multidbCore) newPubSub() *PubSub {
	// Connections may be dialed against different members over the PubSub's
	// lifetime; remember each connection's owner so closeConn can untrack it
	// on the right member's pool.
	var ownersMu sync.Mutex
	owners := make(map[*pool.Conn]*Client)

	pubsub := &PubSub{
		newConn: func(ctx context.Context, _ string, channels []string) (*pool.Conn, error) {
			if c.closed.Load() {
				// ErrClosed (== pool.ErrClosed) is the only error the PubSub
				// channel loop treats as terminal; anything else would make
				// a post-close subscription retry forever.
				return nil, ErrClosed
			}
			db, _ := c.activeSnapshot()
			if db == nil {
				return nil, ErrTemporarilyNotAvailable
			}
			if db.c == nil {
				// A cluster member is active. Without any standalone member
				// in the configuration the subscription can never be served:
				// return the terminal ErrClosed so Channel loops exit. In
				// mixed configurations the error is transient — a later
				// failover/fallback to a standalone member lets the re-dial
				// succeed.
				if !c.hasStandaloneMember() {
					return nil, ErrClosed
				}
				return nil, errors.New("redis: multidb: PubSub requires a standalone or sentinel active database")
			}
			cn, err := db.c.pubSubPool.NewConn(ctx, db.c.opt.Network, db.c.opt.Addr, channels)
			if err != nil {
				return nil, err
			}
			if err := db.c.initConn(ctx, cn); err != nil {
				_ = cn.Close()
				return nil, err
			}
			db.c.pubSubPool.TrackConn(cn)
			ownersMu.Lock()
			owners[cn] = db.c
			ownersMu.Unlock()
			return cn, nil
		},
		closeConn: func(cn *pool.Conn) error {
			ownersMu.Lock()
			owner := owners[cn]
			delete(owners, cn)
			ownersMu.Unlock()
			if owner != nil {
				owner.pubSubPool.UntrackConn(cn)
			}
			return cn.Close()
		},
	}

	// opt must always be non-nil (PubSub reads it unconditionally). Clone the
	// active standalone member's options rather than sharing the pointer: the
	// owning client mutates its own opt (e.g. maintnotifications handoffs).
	//
	// These fields are captured ONCE, from the member active at Subscribe
	// time, and deliberately not refreshed on failover: PubSub reads opt
	// outside its mutex (e.g. the RESP3 push gate on the receive path), so
	// swapping it at re-dial time would be a data race. Each connection is
	// still initialized through the owning member's own initConn/handshake;
	// only PubSub-level knobs (write timeout on subscribe frames, the
	// Protocol gate for push processing) keep the creation-time values —
	// configure members with matching Protocol when mixing them under one
	// MultiDB client.
	if db, _ := c.activeSnapshot(); db != nil && db.c != nil {
		optCopy := *db.c.opt
		pubsub.opt = &optCopy
		pubsub.pushProcessor = db.c.pushProcessor
	} else {
		pubsub.opt = &Options{}
		pubsub.pushProcessor = push.NewVoidProcessor()
	}
	pubsub.onClose = func() { c.removePubSub(pubsub) }
	pubsub.init()

	c.pubsubMu.Lock()
	c.pubsubs[pubsub] = struct{}{}
	c.pubsubMu.Unlock()
	return pubsub
}

// removePubSub deregisters a subscription closed by the caller so the
// registry does not grow unbounded and notifyPubSubs stops touching it.
func (c *multidbCore) removePubSub(ps *PubSub) {
	c.pubsubMu.Lock()
	delete(c.pubsubs, ps)
	c.pubsubMu.Unlock()
}

// notifyPubSubs forces every registered subscription to re-dial so it lands
// on the new active database immediately instead of waiting for the next
// read error.
func (c *multidbCore) notifyPubSubs(ctx context.Context) {
	c.pubsubMu.Lock()
	subs := make([]*PubSub, 0, len(c.pubsubs))
	for ps := range c.pubsubs {
		subs = append(subs, ps)
	}
	c.pubsubMu.Unlock()
	if len(subs) == 0 {
		return
	}

	// Reconnect dials and resubscribes synchronously; running it inline
	// would bill every subscription's recovery to whichever command
	// happened to trigger the failover. Detach: each reconnect resolves the
	// active member at dial time, so a late notification still lands on the
	// current active. The triggering command's context must not cancel
	// PubSub recovery, only its values are kept.
	ctx = context.WithoutCancel(ctx)
	go func() {
		for _, ps := range subs {
			ps.Reconnect(ctx, errors.New("multidb: active database changed"))
		}
	}()
}

func (c *multidbCore) close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(c.stopCh)
	c.wg.Wait()

	// Snapshot and clear the registry first, close outside the lock:
	// PubSub.Close fires onClose (which calls removePubSub → pubsubMu), so
	// closing under pubsubMu would self-deadlock.
	c.pubsubMu.Lock()
	subs := make([]*PubSub, 0, len(c.pubsubs))
	for ps := range c.pubsubs {
		subs = append(subs, ps)
	}
	c.pubsubs = map[*PubSub]struct{}{}
	c.pubsubMu.Unlock()
	for _, ps := range subs {
		_ = ps.Close()
	}

	return c.closeAll()
}

func (c *multidbCore) closeAll() error {
	c.dbMu.Lock()
	defer c.dbMu.Unlock()
	var firstErr error
	for _, db := range c.dbs {
		if err := db.closeClient(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	c.dbs = nil
	return firstErr
}

// defaultPingHealthCheck is the built-in health check used when a database
// has no checks configured: a plain PING (universally supported, works
// against OSS replicas and clusters). Richer checks live in the multidb
// package.
type defaultPingHealthCheck struct{}

func (defaultPingHealthCheck) CheckHealth(ctx context.Context, client *Client) (bool, error) {
	if err := client.Ping(ctx).Err(); err != nil {
		return false, err
	}
	return true, nil
}

func (defaultPingHealthCheck) CheckClusterHealth(ctx context.Context, client *ClusterClient) (bool, error) {
	var shards atomic.Int32
	err := client.ForEachShard(ctx, func(ctx context.Context, shard *Client) error {
		shards.Add(1)
		return shard.Ping(ctx).Err()
	})
	if err != nil {
		return false, err
	}
	if shards.Load() == 0 {
		// An empty topology pinged nothing: that is not proof of health.
		return false, errors.New("redis: multidb: cluster reported no shards to health-check")
	}
	return true, nil
}

// defaultMultiDBPolicy is the built-in policy: every check must pass, each
// evaluated once. Probe/delay-aware policies live in the multidb package.
type defaultMultiDBPolicy struct{}

// runCheckSafely evaluates one health check, treating a panic in a custom
// check as unhealthy: probes run during initialization and on the background
// goroutine, where an escaped panic would crash the process.
func runCheckSafely(ctx context.Context, run func() (bool, error)) (healthy bool) {
	defer func() {
		if r := recover(); r != nil {
			internal.Logger.Printf(ctx, "multidb: health check panicked: %v", r)
			healthy = false
		}
	}()
	ok, _ := run()
	return ok
}

func (defaultMultiDBPolicy) Execute(ctx context.Context, checks []MultiDBHealthCheck, client *Client) bool {
	for _, hc := range checks {
		if !runCheckSafely(ctx, func() (bool, error) { return hc.CheckHealth(ctx, client) }) {
			return false
		}
	}
	return true
}

func (defaultMultiDBPolicy) ExecuteCluster(ctx context.Context, checks []MultiDBHealthCheck, client *ClusterClient) bool {
	for _, hc := range checks {
		if !runCheckSafely(ctx, func() (bool, error) { return hc.CheckClusterHealth(ctx, client) }) {
			return false
		}
	}
	return true
}
