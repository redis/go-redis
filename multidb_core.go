package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9/internal"
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

// probe runs the database's health checks under the configured policy,
// bounded by HealthCheckTimeout, and feeds the result into the circuit
// breaker and the OTel recorder.
func (db *multidbDatabase) probe(ctx context.Context, timeout time.Duration) bool {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	start := time.Now()
	var healthy bool
	if db.cc != nil {
		healthy = db.policy.ExecuteCluster(ctx, db.checks, db.cc)
	} else {
		healthy = db.policy.Execute(ctx, db.checks, db.c)
	}

	// CheckState first so an Open circuit past its grace period transitions
	// to HalfOpen and can be closed by the success below.
	db.cb.CheckState()
	if healthy {
		db.cb.RecordSuccess()
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

	// failoverMu serializes failover attempts and guards the escalation state.
	failoverMu           sync.Mutex
	failoverAttempts     int
	lastFailoverAttempt  time.Time
	autoFallbackDisabled atomic.Bool

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
		db.c = NewFailoverClient(cfg.FailoverOptions)
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
			stateCallback(int(dbRef.idx.Load()), oldState.String(), newState.String())
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

	for {
		healthy := 0
		for _, db := range c.dbs {
			if db.probe(ctx, c.opts.HealthCheckTimeout) {
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

	// Select the highest-weight database whose circuit allows traffic.
	best := c.strategy.Select(c.candidates(-1))
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
			Allowed: db.cb.IsAllowed(),
		})
	}
	return out
}

func (c *multidbCore) activeIndex() int { return int(c.active.Load()) }

// activeSnapshot returns the active database, or nil when none is selected or
// the index is stale after a removal.
func (c *multidbCore) activeSnapshot() (*multidbDatabase, int) {
	idx := int(c.active.Load())
	c.dbMu.RLock()
	defer c.dbMu.RUnlock()
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
	attempts := c.opts.CommandRetries + 1
	var lastErr error

	for attempt := 0; attempt < attempts; attempt++ {
		if err := ctx.Err(); err != nil {
			cmd.SetErr(err)
			return err
		}

		db, idx := c.activeSnapshot()
		if db == nil || !db.cb.IsAllowed() || c.detector.ShouldFailover() {
			if err := c.tryFailover(ctx, idx); err != nil {
				cmd.SetErr(err)
				return err
			}
			db, _ = c.activeSnapshot()
			if db == nil {
				continue
			}
		}

		if attempt > 0 {
			// Clear the previous attempt's error so a successful retry does
			// not leave the command in a stale error state.
			cmd.SetErr(nil)
		}
		err := db.process(ctx, cmd)
		if err == nil || isRedisReplyError(err) {
			// A server reply — including error replies like WRONGTYPE and
			// redis.Nil — proves the database is reachable and healthy.
			db.cb.RecordSuccess()
			c.detector.RecordSuccess()
			return err
		}

		db.cb.RecordFailure()
		c.detector.RecordFailure(err)
		lastErr = err
	}
	return lastErr
}

// isRedisReplyError reports whether err is an error reply from the server
// rather than a transport-level failure.
func isRedisReplyError(err error) bool {
	var redisErr proto.RedisError
	return errors.As(err, &redisErr)
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
	defer c.failoverMu.Unlock()

	// Re-check under the lock: a concurrent failover may already have fixed
	// the active database.
	if db, idx := c.activeSnapshot(); db != nil && idx != from && db.cb.IsAllowed() {
		return nil
	}

	start := time.Now()
	cands := c.candidates(from)
	for {
		best := c.strategy.Select(cands)
		if best < 0 {
			return c.recordFailedFailoverLocked()
		}
		if c.opts.ProbeTargetBeforeFailover {
			if db := c.dbAt(best); db != nil && !db.probe(ctx, c.opts.HealthCheckTimeout) {
				// The probe recorded the failure on the candidate's breaker;
				// drop it from this round and re-select.
				cands = removeCandidate(cands, best)
				continue
			}
		}
		c.failoverAttempts = 0
		c.switchActive(ctx, from, best, failoverReasonAutomatic, time.Since(start))
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
// real change.
func (c *multidbCore) switchActive(ctx context.Context, from, to int, reason string, took time.Duration) bool {
	if !c.active.CompareAndSwap(int32(from), int32(to)) {
		return false
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
	return true
}

// setActiveIndex implements manual failover. With probe=true it is the safe
// probe-then-switch path (SetActiveIndex); with probe=false it is the
// unconditional operator override (ForceActiveIndex).
func (c *multidbCore) setActiveIndex(ctx context.Context, index int, probe bool) error {
	db := c.dbAt(index)
	if db == nil {
		return fmt.Errorf("redis: multidb: database index %d out of range", index)
	}
	start := time.Now()
	if probe {
		if !db.probe(ctx, c.opts.HealthCheckTimeout) {
			return ErrTargetUnhealthy
		}
	}
	// The active index only moves under failoverMu (automatic failover,
	// fallback, membership changes and manual selection all hold it), so the
	// snapshot below cannot go stale before switchActive runs.
	c.failoverMu.Lock()
	defer c.failoverMu.Unlock()
	from := int(c.active.Load())
	if from == index {
		return nil
	}
	c.failoverAttempts = 0
	if !c.switchActive(ctx, from, index, failoverReasonManual, time.Since(start)) {
		return errors.New("redis: multidb: active database changed concurrently, retry")
	}
	// A fresh window on the new database, same as automatic failover.
	c.detector.Reset()
	return nil
}

func (c *multidbCore) addDatabase(ctx context.Context, cfg MultiDBClientConfig) (int, error) {
	if err := cfg.validate(); err != nil {
		return -1, err
	}
	db, err := c.buildDatabase(&cfg)
	if err != nil {
		return -1, err
	}
	// The initial probe result never blocks membership; it only seeds the
	// circuit breaker (and is skipped entirely with SkipInitialHealthCheck).
	if !cfg.SkipInitialHealthCheck {
		db.probe(ctx, c.opts.HealthCheckTimeout)
	}
	c.dbMu.Lock()
	c.dbs = append(c.dbs, db)
	idx := len(c.dbs) - 1
	db.idx.Store(int32(idx))
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
			if db, idx := c.activeSnapshot(); db != nil && !db.cb.IsAllowed() {
				_ = c.tryFailover(ctx, idx)
			}

			if c.opts.AutoFallbackInterval > 0 && !c.autoFallbackDisabled.Load() &&
				time.Since(lastFallbackCheck) >= c.opts.AutoFallbackInterval {
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
// whose circuit is closed again.
func (c *multidbCore) tryFallbackToPrimary(ctx context.Context) {
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
	c.failoverMu.Lock()
	c.switchActive(ctx, idx, best, failoverReasonFallback, 0)
	c.failoverMu.Unlock()
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
			db, _ := c.activeSnapshot()
			if db == nil {
				return nil, ErrTemporarilyNotAvailable
			}
			if db.c == nil {
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

	for _, ps := range subs {
		ps.Reconnect(ctx, errors.New("multidb: active database changed"))
	}
}

func (c *multidbCore) close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	close(c.stopCh)
	c.wg.Wait()

	c.pubsubMu.Lock()
	for ps := range c.pubsubs {
		_ = ps.Close()
	}
	c.pubsubs = map[*PubSub]struct{}{}
	c.pubsubMu.Unlock()

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
	err := client.ForEachShard(ctx, func(ctx context.Context, shard *Client) error {
		return shard.Ping(ctx).Err()
	})
	if err != nil {
		return false, err
	}
	return true, nil
}

// defaultMultiDBPolicy is the built-in policy: every check must pass, each
// evaluated once. Probe/delay-aware policies live in the multidb package.
type defaultMultiDBPolicy struct{}

func (defaultMultiDBPolicy) Execute(ctx context.Context, checks []MultiDBHealthCheck, client *Client) bool {
	for _, hc := range checks {
		if ok, _ := hc.CheckHealth(ctx, client); !ok {
			return false
		}
	}
	return true
}

func (defaultMultiDBPolicy) ExecuteCluster(ctx context.Context, checks []MultiDBHealthCheck, client *ClusterClient) bool {
	for _, hc := range checks {
		if ok, _ := hc.CheckClusterHealth(ctx, client); !ok {
			return false
		}
	}
	return true
}
