package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9/internal"
	cbq "github.com/redis/go-redis/v9/internal/callbackqueue"
	"github.com/redis/go-redis/v9/internal/failuredetector"
	"github.com/redis/go-redis/v9/internal/otel"
	"github.com/redis/go-redis/v9/internal/pool"
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

	// cbq delivers user circuit-state callbacks asynchronously (FIFO). Breaker
	// state changes fire deep inside locked sections (failoverMu is held
	// during manual selection, pre-failover probes and AddDatabase probes);
	// invoking a user callback there would self-deadlock the moment the
	// callback touches a control API that takes the same lock.
	cbq cbq.CallbackQueue
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

// failbackOnlyHealthCheck marks health checks whose verdict may only gate
// routing traffic TO a member (candidate probes, auto-fallback, initial
// selection) and must never evict the current active — the lag-aware REST
// check is the canonical case: replication lag on the member already
// serving traffic is not an availability signal, and failover rides on
// traffic signals instead. Satisfied structurally (multidb.LagAwareHealthCheck
// and custom checks alike).
type failbackOnlyHealthCheck interface {
	FailbackOnly() bool
}

// probe runs the database's health checks under the configured policy,
// bounded by HealthCheckTimeout, and feeds the result into the circuit
// breaker and the OTel recorder.
func (db *multidbDatabase) probe(parent context.Context, timeout time.Duration) bool {
	return db.probeWith(parent, timeout, db.checks)
}

// nonFailbackChecks returns the checks whose verdict may evict the member —
// everything except fail-back-only checks (e.g. the lag-aware REST check),
// which gate routing TO a member but never the one already serving traffic.
func (db *multidbDatabase) nonFailbackChecks() []MultiDBHealthCheck {
	filtered := make([]MultiDBHealthCheck, 0, len(db.checks))
	for _, check := range db.checks {
		if fb, ok := check.(failbackOnlyHealthCheck); ok && fb.FailbackOnly() {
			continue
		}
		filtered = append(filtered, check)
	}
	return filtered
}

// probeAsActive probes the CURRENTLY ACTIVE member: fail-back-only checks
// are excluded, so a lag breach cannot open the active's breaker and evict
// the member traffic is flowing through. With no applicable checks left the
// probe records nothing — a vacuously healthy pass would pump external
// successes into a breaker that real traffic failures opened.
func (db *multidbDatabase) probeAsActive(parent context.Context, timeout time.Duration) {
	filtered := db.nonFailbackChecks()
	if len(filtered) == 0 {
		return
	}
	db.probeWith(parent, timeout, filtered)
}

// probeExcludingFailbackOnly runs the member's non-fail-back-only checks and
// reports whether it is healthy, used to gate a re-selection of the current
// active: a lag breach must not fail selecting the member already serving
// traffic. With only fail-back-only checks configured, nothing gates the
// active, so it is reported healthy without recording an outcome.
func (db *multidbDatabase) probeExcludingFailbackOnly(parent context.Context, timeout time.Duration) bool {
	filtered := db.nonFailbackChecks()
	if len(filtered) == 0 {
		return true
	}
	return db.probeWith(parent, timeout, filtered)
}

func (db *multidbDatabase) probeWith(parent context.Context, timeout time.Duration, checks []MultiDBHealthCheck) bool {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()

	start := time.Now()
	var healthy bool
	if db.cc != nil {
		healthy = db.policy.ExecuteCluster(ctx, checks, db.cc)
	} else {
		healthy = db.policy.Execute(ctx, checks, db.c)
	}

	if parent.Err() != nil {
		// The caller's own context was canceled or expired mid-probe: every
		// caller discards the verdict, so the breaker must not act on it
		// either — a late healthy result could otherwise close an open
		// circuit for an operation that returns context.Canceled.
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

	// cbq runs the OnFailover / OnActiveDatabaseChanged callbacks on its own
	// goroutine (FIFO). They fire from switchActive's announce closure, which
	// runs on the background loop; running them inline there would deadlock
	// if a callback called Close() (close() waits on that goroutine via
	// wg.Wait). Like the per-db cbq, close() does not wait on this queue.
	cbq cbq.CallbackQueue
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
	case cfg.ClusterOptions != nil:
		opt := *cfg.ClusterOptions
		// The cluster client keeps the options pointer and reads Addrs on
		// topology reloads: a private slice keeps a caller mutating its
		// seed list from changing a running member.
		opt.Addrs = append([]string(nil), cfg.ClusterOptions.Addrs...)
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
		if stateCallback != nil && !dbRef.removed.Load() {
			// Deliver asynchronously (FIFO per database): state changes fire
			// under internal locks, and a callback that calls a control API
			// would otherwise self-deadlock. The removed flag and the index
			// are (re-)read at DELIVERY time: a removal that lands while the
			// callback is queued would otherwise surface a stale index that
			// now points at a different member. A removal racing the final
			// reads keeps an unavoidable instant-wide window — carrying the
			// member identity (fqdn) in the callback is the follow-up API
			// that closes it entirely.
			from, to := oldState.String(), newState.String()
			dbRef.cbq.Dispatch(func() {
				if dbRef.removed.Load() {
					return
				}
				stateCallback(int(dbRef.idx.Load()), from, to)
			})
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
	for {
		healthy := 0
		for i, db := range c.dbs {
			probeHealthy[i] = db.probe(ctx, c.opts.HealthCheckTimeout)
			if probeHealthy[i] {
				healthy++
			}
		}
		if err := ctx.Err(); err != nil {
			// A probe may report healthy even after the constructor's
			// context expired (checks that notice cancellation late): a
			// canceled construction must not return a live client.
			return err
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
	// The context check above guarantees the loop only breaks with a live
	// context, so every probe verdict here is real (a canceled pass returns
	// before this reconciliation).
	for i, db := range c.dbs {
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
	best := c.selectCandidate(cands)
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

// isHImportCmd matches the rejected HIMPORT family both by the typed marker
// interface and by command name, so raw Do/NewCmd submissions are covered.
func isHImportCmd(cmd Cmder) bool {
	if _, ok := cmd.(interface{ himportCmd() }); ok {
		return true
	}
	return cmd.Name() == "himport"
}

// selectCandidate runs the failover strategy with panic recovery: strategies
// are user code and also run on the library-owned background loop, where an
// escaped panic would crash the process. A panicking strategy selects
// nothing (logged), which surfaces as temporary unavailability.
func (c *multidbCore) selectCandidate(cands []MultiDBDatabaseState) (best int) {
	defer func() {
		if r := recover(); r != nil {
			internal.Logger.Printf(context.Background(), "multidb: failover strategy panicked: %v", r)
			best = -1
		}
	}()
	return c.strategy.Select(cands)
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
	if isHImportCmd(cmd) {
		// The typed HImport* methods are overridden to reject the family,
		// but a hand-built HImport*Cmd — or a raw Do(ctx, "himport", ...) —
		// through Process would bypass them and register a fieldset on a
		// single member: the exact failover hazard the rejection prevents.
		cmd.SetErr(errMultiDBHImport)
		return errMultiDBHImport
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
		if c.closed.Load() {
			// Close landed mid-retry: report the terminal state instead of
			// escalating through the drained membership.
			cmd.SetErr(ErrClosed)
			return ErrClosed
		}
		if err := ctx.Err(); err != nil {
			cmd.SetErr(err)
			return err
		}

		// Detector before the breaker admission: a half-open admission
		// reserves a bounded probe slot, and a tripped detector routes to
		// failover without executing anything — the reservation would leak
		// and eventually starve the recovering active's probe budget.
		db, idx := c.activeSnapshot()
		admitted, reserved := false, false
		if db != nil && !c.detector.ShouldFailover() {
			admitted, reserved = db.cb.Allow()
		}
		if !admitted {
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
			// may be half-open and the Allow above is what reserves the
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
		if err != nil && ctx.Err() != nil {
			// The caller's own context ended (deadline/cancel) while the
			// command ran — typically cutting a dial short, which surfaces as
			// a *net.OpError{Op:"dial"} the classifier would otherwise count
			// as a database failure. That is a client-side signal, not a
			// health verdict: recording it would let short caller deadlines
			// open the breaker and drive false failover. Give back any
			// reserved half-open slot (as the neutral branch does) and return
			// without recording an outcome. A genuine unreachable-endpoint
			// dial, where ctx is still alive, still reaches classifyOutcome.
			if reserved {
				db.cb.ReleaseHalfOpen()
			}
			return err
		}
		switch classifyOutcome(err, retryTimeout) {
		case outcomeSuccess:
			// Settle by reservation: a closed-state admission holds no
			// half-open slot, and a command that outlives a later open ->
			// half-open transition must not free the slot a real recovery
			// probe is holding — its success still counts toward closing.
			if reserved {
				db.cb.RecordSuccess()
			} else {
				db.cb.RecordExternalSuccess()
			}
			c.detector.RecordSuccess()
			c.successSinceFailover.Store(true)
			return err
		case outcomeNeutral:
			// Not a database-health signal: return to the caller without
			// recording a failure or failing over. Give back the half-open
			// probe slot the admission reserved — recording nothing would
			// otherwise leak it.
			if reserved {
				db.cb.ReleaseHalfOpen()
			}
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
// rather than a transport-level failure. It matches the Error marker
// interface, not the concrete proto.RedisError string: the reader parses
// recognized reply prefixes into typed structs (*proto.AuthError,
// *proto.MovedError, ...) that only share the marker.
func isRedisReplyError(err error) bool {
	var redisErr Error
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
	case errors.Is(err, errMultiDBHImport):
		// Locally synthesized rejection (a RedisError only so batch
		// machinery treats it per-command): no round trip happened, so it
		// must not count as proof of a healthy server. Defensive — the
		// rejection paths all skip outcome recording anyway.
		return outcomeNeutral
	case shouldRetry(err, retryTimeout):
		// Includes broken transport of every flavor: shouldRetry matches
		// any error carrying a Timeout() method (all net.OpErrors — resets,
		// broken pipes) via isTimeoutError, not only io.EOF, so a hard
		// member crash mid-read is recorded and drives failover.
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
	// Re-check after the lock wait: a command whose context expired while
	// queued behind another failover must not still switch the active
	// database (without pre-failover probes there is no later check) — and
	// a client closed while this call queued must not mutate anything.
	if c.closed.Load() {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	// Re-check under the lock: a concurrent failover may already have fixed
	// the active database.
	if db, idx := c.activeSnapshot(); db != nil && idx != from && db.selectable() {
		return nil
	}

	start := time.Now()
	cands := c.candidates(from)
	for {
		best := c.selectCandidate(cands)
		if best < 0 {
			// No alternate candidate. If the CURRENT active database is fully
			// available again (health checks closed its breaker after the
			// failure burst that tripped the detector), stay on it and clear
			// the tripped state: escalating here would strand the client in
			// *NotAvailable forever, because no command can succeed to reset
			// the detector. Closed only — a half-open breaker must keep being
			// escalated or the gate would spin on exhausted probe slots. The
			// live index is used, not the caller's snapshot: `from` can be
			// stale after a concurrent switch, and the verdict must be about
			// the database traffic actually lands on.
			if db, _ := c.activeSnapshot(); db != nil && db.cb.CheckState() == imultidb.CircuitClosed {
				c.resetFailoverEscalationLocked()
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
			if err := ctx.Err(); err != nil {
				// The probe passed, but the caller's context died while it
				// ran: a canceled attempt must not switch the active state.
				return err
			}
		}
		if c.closed.Load() {
			// Same close-during-probe race as setActiveIndex: close() takes no
			// lock, so a ProbeTargetBeforeFailover probe can outlast it. Do not
			// switch the active state on an already-closed client.
			return ErrClosed
		}
		if announce = c.switchActive(ctx, from, best, failoverReasonAutomatic, time.Since(start)); announce == nil {
			// The CAS lost against a concurrent switch (the caller's `from`
			// went stale): nothing changed, so nothing may be reset — the
			// caller re-enters its gate against the new active. Escalation
			// state stays untouched too; the concurrent switch's own path
			// already maintains it.
			return nil
		}
		c.resetFailoverEscalationLocked()
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

// resetFailoverEscalationLocked starts a fresh escalation chain: the next
// failed failover counts from zero. Both the counter and the timestamp must
// clear — leaving lastFailoverAttempt set would make recordFailedFailoverLocked's
// FailoverAttemptDelay gate fold the new chain's first failure into the old
// burst and skip the increment. failoverMu must be held.
func (c *multidbCore) resetFailoverEscalationLocked() {
	c.failoverAttempts = 0
	c.lastFailoverAttempt = time.Time{}
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
		c.resetFailoverEscalationLocked()
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

// switchActive is the single point for every active-index change: automatic
// failover, auto-fallback and manual selection all funnel through it, so
// callbacks, metrics and PubSub fire once per real change. It returns a
// non-nil announce closure when the switch happened; callers MUST invoke it
// AFTER releasing failoverMu. Metrics and the PubSub nudge run synchronously;
// the user callbacks go through cbq.
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

	// Callbacks run later on cbq, so detach from the caller ctx (a manual
	// SetActiveIndex ctx dies on return). Keep trace/values, drop cancel.
	cbCtx := context.WithoutCancel(ctx)

	// Enqueue the user callbacks HERE, under failoverMu (every caller holds it
	// across this CAS), so their queue order matches the switch order. Doing it
	// in the announce closure instead — which runs after the caller unlocks —
	// lets two back-to-back switches (A->B then B->C) enqueue B->C before A->B.
	// Dispatch only appends and never runs user code, so holding the lock here
	// is safe; the callbacks still execute off-lock on the cbq goroutine, which
	// RunSafely-wraps each and lets a callback re-enter control APIs. closed is
	// re-read at delivery so a callback never fires on an already-closed client.
	if reason == failoverReasonAutomatic || reason == failoverReasonManual {
		if c.opts.OnFailover != nil {
			c.cbq.Dispatch(func() {
				if c.closed.Load() {
					return
				}
				c.opts.OnFailover(cbCtx, from, to)
			})
		}
	}
	if c.opts.OnActiveDatabaseChanged != nil {
		c.cbq.Dispatch(func() {
			if c.closed.Load() {
				return
			}
			c.opts.OnActiveDatabaseChanged(from, to)
		})
	}

	// The announce closure runs AFTER the caller releases failoverMu: it only
	// does off-lock work (metrics, PubSub re-dial), no ordering-sensitive
	// callback dispatch.
	return func() {
		otel.RecordMultiDBActiveDatabaseChange(ctx, fromFQDN, toFQDN)
		if reason == failoverReasonAutomatic || reason == failoverReasonManual {
			otel.RecordMultiDBFailover(ctx, fromFQDN, toFQDN, reason, took)
		}
		c.notifyPubSubs(ctx)
	}
}

// setActiveIndex implements manual failover. With probe=true it is the safe
// probe-then-switch path (SetActiveIndex); with probe=false it is the
// unconditional operator override (ForceActiveIndex).
func (c *multidbCore) setActiveIndex(ctx context.Context, index int, probe bool) error {
	if c.closed.Load() {
		// Consistent with the command paths: the drained membership would
		// otherwise surface as a misleading out-of-range error.
		return ErrClosed
	}
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
	// Re-check after the lock wait: the client may have closed and the
	// operator's context may have expired while this call queued behind
	// another failover/membership operation, and the probe-less force path
	// has no later chance to notice.
	if c.closed.Load() {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	db := c.dbAt(index)
	if db == nil {
		return fmt.Errorf("redis: multidb: database index %d out of range", index)
	}
	start := time.Now()
	if probe {
		// Re-selecting the CURRENT active must not be gated by fail-back-only
		// checks (e.g. lag): those decide routing TO a member, never the one
		// already serving traffic, so a lag breach must not fail the operation
		// or record a breaker failure against the active. Switching to a
		// DIFFERENT member runs the full check set (fail-back-to gating).
		healthy := false
		if index == int(c.active.Load()) {
			healthy = db.probeExcludingFailbackOnly(ctx, c.opts.HealthCheckTimeout)
		} else {
			healthy = db.probe(ctx, c.opts.HealthCheckTimeout)
		}
		if !healthy {
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
	if c.closed.Load() {
		// close() takes no lock, so it can have drained the membership while
		// the probe above ran (up to HealthCheckTimeout). Re-check before
		// mutating breaker/detector/active state or reporting success on an
		// already-closed client.
		return ErrClosed
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
	// The operator explicitly selected a healthy member: the chain of failed
	// failover attempts is over — also when no index change happens, or a
	// later unrelated outage would escalate from the stale count.
	c.resetFailoverEscalationLocked()
	from := int(c.active.Load())
	if from == index {
		return nil
	}
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
	// Re-check after the lock wait: with SkipInitialHealthCheck there is no
	// probe, so this is the last chance to notice that the caller's context
	// expired while the call queued behind another control operation.
	if err := ctx.Err(); err != nil {
		db.removed.Store(true)
		_ = db.closeClient()
		return -1, err
	}

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
			// membership. Mark it removed first: the probe may already have
			// queued circuit callbacks for an index that was never added.
			db.removed.Store(true)
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
		db.removed.Store(true)
		_ = db.closeClient()
		return -1, ErrClosed
	}
	c.dbs = append(c.dbs, db)
	c.dbMu.Unlock()
	return idx, nil
}

func (c *multidbCore) removeDatabase(ctx context.Context, index int) error {
	if c.closed.Load() {
		// Consistent with the other control paths: the drained membership
		// would otherwise surface as a misleading out-of-range error.
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	// Hold failoverMu so the active-index adjustment below cannot race a
	// concurrent switchActive (all active transitions happen under it).
	c.failoverMu.Lock()
	defer c.failoverMu.Unlock()
	// Re-check after the lock wait: a client closed or an operator request
	// whose context died while queued must not still delete a member.
	if c.closed.Load() {
		return ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return err
	}
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
		// Re-read the active each iteration, not once before the loop: a
		// concurrent failover during the (up to HealthCheckTimeout per member)
		// pass would otherwise leave the freshly-selected active probed with
		// fail-back-only checks (e.g. lag) that could open its breaker and
		// evict it right after selection.
		active, _ := c.activeSnapshot()
		if db == active {
			// The active is probed without fail-back-only checks (e.g. the
			// lag-aware REST check): their verdicts gate routing traffic TO
			// a member, never evicting the one traffic flows through.
			db.probeAsActive(ctx, c.opts.HealthCheckTimeout)
			continue
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
		// and manual failover paths do — and end the failed-failover chain:
		// a successful fallback IS a recovery, and a later unrelated outage
		// must escalate from a clean slate.
		c.resetFailoverEscalationLocked()
		c.detector.Reset()
	}
}

// hasStandaloneMember reports whether any member database is served by a
// standalone client, i.e. whether PubSub can ever be dialed.
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
				return nil, errors.New("redis: multidb: PubSub requires a standalone active database")
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

	// Reconnect dials and resubscribes synchronously; running it inline would
	// bill every subscription's recovery to whichever command triggered the
	// failover. Detach from the caller's context — its cancellation must not
	// abort recovery — but bind cancellation to client shutdown so close() can
	// interrupt an in-flight dial instead of blocking on a subscription's lock
	// (PubSub.Reconnect holds it across the dial). Reconnect each subscription
	// on its own goroutine so one slow dial cannot stall the others; each
	// resolves the active member at dial time, so a late reconnect still lands
	// on the current active.
	rctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	reason := errors.New("multidb: active database changed")
	var wg sync.WaitGroup
	for _, ps := range subs {
		wg.Add(1)
		go func(ps *PubSub) {
			defer wg.Done()
			ps.Reconnect(rctx, reason)
		}(ps)
	}
	go func() {
		defer cancel()
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-c.stopCh: // client closing: unblock any in-flight reconnect dial
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
	var masters atomic.Int32
	// Ping every master. Fail on the first error.
	//
	// We do this by hand so the check does not depend on cluster policy
	// routing. It still matches PING's Redis policy (all_shards +
	// all_succeeded: hit one node per shard, all must pass), but a plain
	// client.Ping() would rely on that policy.
	err := client.ForEachMaster(ctx, func(ctx context.Context, master *Client) error {
		masters.Add(1)
		return master.Ping(ctx).Err()
	})
	if err != nil {
		return false, err
	}
	if masters.Load() == 0 {
		// An empty topology pinged nothing: that is not proof of health.
		return false, errors.New("redis: multidb: cluster reported no masters to health-check")
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
