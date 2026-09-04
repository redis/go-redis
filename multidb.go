package redis

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"sync"
	"time"

	imultidb "github.com/redis/go-redis/v9/internal/multidb"
	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// InitialDBState controls how many databases must be healthy for
// NewMultiDBClient to succeed.
type InitialDBState string

const (
	// InitialDBStateAllAvailable requires every configured database to be
	// healthy on initialization.
	InitialDBStateAllAvailable InitialDBState = "all_available"
	// InitialDBStateMajorityAvailable requires a strict majority of the
	// configured databases to be healthy on initialization. This is the default.
	InitialDBStateMajorityAvailable InitialDBState = "majority_available"
	// InitialDBStateOneAvailable requires at least one configured database to
	// be healthy on initialization.
	InitialDBStateOneAvailable InitialDBState = "one_available"
)

var (
	// ErrTemporarilyNotAvailable is returned when no healthy database can be
	// selected right now; the client keeps attempting failover on subsequent
	// commands. It is also returned when the member a command or batch was routed
	// to was removed mid-flight (a concurrent RemoveDatabase or manual switch): the
	// client is still open and the next attempt lands on the live active, so the
	// caller should retry rather than treat it as terminal.
	ErrTemporarilyNotAvailable = errors.New("redis: multidb: no healthy database available (temporarily)")
	// ErrPermanentlyNotAvailable is returned once MaxFailoverAttempts
	// consecutive failover attempts have failed; the application should stop
	// using this client.
	ErrPermanentlyNotAvailable = errors.New("redis: multidb: no healthy database available (permanently)")
	// ErrInsufficientHealthyDatabases is returned by NewMultiDBClient when the
	// InitialDBState policy is not satisfied.
	ErrInsufficientHealthyDatabases = errors.New("redis: multidb: insufficient healthy databases on initialization")
	// ErrTargetUnhealthy is returned by SetActiveDatabase when the requested
	// target database fails its health probe. Use ForceActiveDatabase to switch
	// unconditionally.
	ErrTargetUnhealthy = errors.New("redis: multidb: target database is unhealthy")
	// ErrDatabaseNotFound is returned by the control API when no member has the
	// given id (never added, or already removed). Member ids are stable and
	// never reused, so a removed member's id stays invalid forever.
	ErrDatabaseNotFound = errors.New("redis: multidb: no database with the given id")
)

// MultiDBFailureDetector aggregates command outcomes and decides when the
// active database is unhealthy enough to trigger failover.
// The default implementation is a sliding-window detector that trips when at
// least a minimum number of failures AND a minimum failure rate are observed
// within the detection window.
//
// A detector is called from inside the command and failover paths and must
// not call the control operations (see MultiDBCtrl).
type MultiDBFailureDetector interface {
	RecordSuccess()
	RecordFailure(err error)
	ShouldFailover() bool
	Reset()
}

// MultiDBHealthCheckPolicy evaluates a set of health checks against a
// database. Implementations with probe/delay semantics live in the multidb
// package (HealthyAllPolicy, HealthyMajorityPolicy, HealthyAnyPolicy) and
// satisfy this interface structurally.
type MultiDBHealthCheckPolicy interface {
	Execute(ctx context.Context, checks []MultiDBHealthCheck, client *Client) bool
	ExecuteCluster(ctx context.Context, checks []MultiDBHealthCheck, client *ClusterClient) bool
}

// MultiDBDatabaseState is a snapshot of one database handed to a failover
// strategy.
type MultiDBDatabaseState struct {
	// ID is the database's stable member id (the value AddDatabase returned and
	// ActiveDatabaseID reports), not a position. It is what Select must return.
	ID      int
	Weight  float64
	Allowed bool // circuit breaker allows traffic
}

// MultiDBFailoverStrategy selects the failover target from candidate
// databases. Return the chosen database's id (MultiDBDatabaseState.ID), or -1
// when no candidate is acceptable.
//
// A candidate whose Allowed is false is never seated: returning its id drops
// that candidate and Select is asked again with the remaining ones, until it
// returns an allowed id or -1. An id that is not among the offered candidates
// ends the selection with no target.
//
// Select runs under MultiDB's failover lock and must not call the control
// operations (see MultiDBCtrl).
type MultiDBFailoverStrategy interface {
	Select(candidates []MultiDBDatabaseState) int
}

// WeightBasedFailoverStrategy selects the allowed candidate with the highest
// weight. It is the default strategy.
type WeightBasedFailoverStrategy struct{}

func (WeightBasedFailoverStrategy) Select(candidates []MultiDBDatabaseState) int {
	best := -1
	bestWeight := 0.0
	for _, c := range candidates {
		if !c.Allowed {
			continue
		}
		if best == -1 || c.Weight > bestWeight {
			best = c.ID
			bestWeight = c.Weight
		}
	}
	return best
}

// MultiDBCircuitBreakerConfig configures the per-database circuit breaker.
type MultiDBCircuitBreakerConfig struct {
	// FailureThreshold is the number of consecutive failures before the
	// circuit opens. Default: 5.
	FailureThreshold int
	// SuccessThreshold is the number of successes in half-open state before
	// the circuit closes again. Default: 2.
	SuccessThreshold int
	// GracePeriod is the cooldown between the circuit opening and the first
	// half-open probe, giving the database time to self-heal. Default: 60s.
	GracePeriod time.Duration
}

// MultiDBClientConfig describes one member database. Exactly one of Options
// or ClusterOptions must be set.
type MultiDBClientConfig struct {
	// Options configures a standalone client for this database.
	Options *Options
	// ClusterOptions configures a cluster client.
	ClusterOptions *ClusterOptions

	// Weight is the database's selection priority; the failover strategy
	// prefers higher weights. Default: 1.0.
	Weight float64

	// HealthChecks run against this database in addition to the global
	// MultiDBOptions.HealthChecks (merge semantics: global + per-DB are
	// additive). A database that ends up with no checks from either layer is
	// given a single default PING check. A database whose checks are all
	// fail-back-only (see FailbackOnly on the lag-aware check) still runs the
	// default PING as a liveness floor while it is the active, so a dead
	// endpoint is detected even though the fail-back-only checks alone never
	// evict the active.
	HealthChecks []MultiDBHealthCheck
	// HealthCheckPolicy overrides MultiDBOptions.HealthCheckPolicy for this
	// database.
	HealthCheckPolicy MultiDBHealthCheckPolicy

	// SkipInitialHealthCheck makes AddDatabase register the database as a
	// member regardless of its first health-check result. It is only honored
	// for databases added at runtime, never during client initialization.
	SkipInitialHealthCheck bool
}

// MultiDBOptions configures MultiDBClient.
type MultiDBOptions struct {
	// Clients configures the member databases. At least one is required.
	Clients []MultiDBClientConfig

	// HealthCheckInterval is the cadence of the background health checks.
	// Default: 5s.
	HealthCheckInterval time.Duration
	// HealthCheckTimeout bounds one health-check pass against one database
	// via the probe context. Note that with the default PING check the
	// member client applies its own dial/read/write timeouts to the socket
	// unless ContextTimeoutEnabled is set on the member options — enable it
	// there when the probe deadline must also cut socket waits short.
	// Default: 3s; clamped to HealthCheckInterval/2 when >= interval.
	HealthCheckTimeout time.Duration
	// HealthChecks run against every database (see MultiDBClientConfig.HealthChecks
	// for the merge semantics).
	HealthChecks []MultiDBHealthCheck
	// HealthCheckPolicy is the default policy for evaluating a database's
	// health checks. Default: every check must pass.
	HealthCheckPolicy MultiDBHealthCheckPolicy

	// FailureDetector aggregates command outcomes; when it trips, traffic is
	// moved even before the active breaker fully opens. Default: a
	// sliding-window detector (1000 failures AND 10% failure rate within 2s).
	//
	// A non-nil detector is used as-is and is stateful: do NOT share one
	// options value carrying a custom detector across multiple clients, or
	// outcomes recorded by one client will trip failover on the others.
	// Leave nil to get an independent default detector per client.
	FailureDetector MultiDBFailureDetector
	// CircuitBreakerConfig configures every database's circuit breaker.
	CircuitBreakerConfig *MultiDBCircuitBreakerConfig
	// FailoverStrategy selects failover targets. Default:
	// WeightBasedFailoverStrategy.
	FailoverStrategy MultiDBFailoverStrategy

	// CommandRetries is the number of additional attempts a failed command
	// gets (against the newly selected database after a failover).
	// -1 or 0 means the default of 2; use CommandRetriesNone for no retries.
	CommandRetries int

	// AutoFallbackInterval is how often the client checks whether a
	// strictly-higher-weight database has recovered and switches back to it.
	// 0 means the default of 120s; a negative value disables auto-fallback.
	AutoFallbackInterval time.Duration

	// InitialDBState is the number of healthy databases required for
	// NewMultiDBClient to succeed. Default: InitialDBStateMajorityAvailable.
	// When the context passed to NewMultiDBClient carries a deadline,
	// initialization blocks and retries until the policy is satisfied or the
	// deadline expires; without a deadline a single health-check pass is
	// performed.
	InitialDBState InitialDBState

	// MaxFailoverAttempts is the number of consecutive failed failover
	// attempts after which ErrPermanentlyNotAvailable is returned.
	// Default: 10.
	MaxFailoverAttempts int
	// FailoverAttemptDelay is the minimum delay between failed failover
	// attempts; a burst of failures within the window counts as one attempt.
	// Default: 12s.
	FailoverAttemptDelay time.Duration

	// ProbeTargetBeforeFailover makes automatic failover run a synchronous
	// health-check pass against the selected candidate before switching.
	// Default: false (trust the circuit state maintained by the background
	// health checks).
	ProbeTargetBeforeFailover bool

	// OnFailover is called after an automatic or manual failover switched the
	// active database from `from` to `to`, both stable member ids (see
	// AddDatabase). Delivered asynchronously (FIFO); do not rely on it running
	// synchronously with the failover. The ids stay valid even if a concurrent
	// RemoveDatabase drops another member. The ctx keeps the triggering
	// operation's trace/values but not its cancellation.
	OnFailover func(ctx context.Context, from, to int)
	// OnActiveDatabaseChanged is called on every active-database change,
	// including auto-fallback. `from` and `to` are stable member ids. Delivered
	// asynchronously (FIFO); do not rely on it running synchronously with the
	// change.
	OnActiveDatabaseChanged func(from, to int)
	// OnCircuitStateChanged is called when any database's circuit breaker
	// changes state ("closed", "open", "half-open"). dbID is the database's
	// stable member id. Delivered asynchronously (FIFO per database); do not
	// rely on it running synchronously with the transition.
	OnCircuitStateChanged func(dbID int, from, to string)
}

// CommandRetriesNone disables command retries.
const CommandRetriesNone = -2

const (
	defaultMultiDBHealthCheckInterval = 5 * time.Second
	defaultMultiDBHealthCheckTimeout  = 3 * time.Second
	defaultMultiDBCommandRetries      = 2
	defaultMultiDBAutoFallback        = 120 * time.Second
	defaultMultiDBMaxFailoverAttempts = 10
	defaultMultiDBFailoverDelay       = 12 * time.Second
	defaultMultiDBWeight              = 1.0

	initialHealthCheckRetryDelay = 500 * time.Millisecond
)

func (opt *MultiDBOptions) init() error {
	if len(opt.Clients) == 0 {
		return errors.New("redis: multidb: at least one database must be configured")
	}
	if opt.HealthCheckInterval <= 0 {
		opt.HealthCheckInterval = defaultMultiDBHealthCheckInterval
	}
	if opt.HealthCheckTimeout <= 0 {
		opt.HealthCheckTimeout = defaultMultiDBHealthCheckTimeout
	}
	if opt.HealthCheckTimeout >= opt.HealthCheckInterval {
		opt.HealthCheckTimeout = opt.HealthCheckInterval / 2
	}
	switch {
	case opt.CommandRetries == CommandRetriesNone:
		opt.CommandRetries = 0
	case opt.CommandRetries <= 0:
		opt.CommandRetries = defaultMultiDBCommandRetries
	}
	if opt.AutoFallbackInterval == 0 {
		opt.AutoFallbackInterval = defaultMultiDBAutoFallback
	}
	if opt.InitialDBState == "" {
		opt.InitialDBState = InitialDBStateMajorityAvailable
	}
	switch opt.InitialDBState {
	case InitialDBStateAllAvailable, InitialDBStateMajorityAvailable, InitialDBStateOneAvailable:
	default:
		return fmt.Errorf("redis: multidb: invalid InitialDBState %q", opt.InitialDBState)
	}
	if opt.MaxFailoverAttempts <= 0 {
		opt.MaxFailoverAttempts = defaultMultiDBMaxFailoverAttempts
	}
	if opt.FailoverAttemptDelay <= 0 {
		opt.FailoverAttemptDelay = defaultMultiDBFailoverDelay
	}
	// Note: the default FailureDetector is deliberately NOT filled in here.
	// It is stateful, and writing it back into the caller's options would
	// share one sliding failure window across every client built from the
	// same MultiDBOptions value; newMultidbCore allocates it per client.
	if opt.FailoverStrategy == nil {
		opt.FailoverStrategy = WeightBasedFailoverStrategy{}
	}
	if opt.HealthCheckPolicy == nil {
		opt.HealthCheckPolicy = defaultMultiDBPolicy{}
	}
	if opt.CircuitBreakerConfig == nil {
		def := imultidb.DefaultCircuitBreakerConfig()
		opt.CircuitBreakerConfig = &MultiDBCircuitBreakerConfig{
			FailureThreshold: def.FailureThreshold,
			SuccessThreshold: def.SuccessThreshold,
			GracePeriod:      def.GracePeriod,
		}
	}
	return nil
}

func (cfg *MultiDBClientConfig) validate() error {
	set := 0
	if cfg.Options != nil {
		set++
	}
	if cfg.ClusterOptions != nil {
		set++
	}
	if set != 1 {
		return fmt.Errorf("redis: multidb: exactly one of Options or ClusterOptions must be set per database (got %d)", set)
	}
	if math.IsNaN(cfg.Weight) {
		// A NaN weight makes every ordered comparison false, so the failover
		// strategy and auto-fallback can neither prefer nor reject the member on
		// priority — selection degenerates to iteration order. Reject it here
		// (and in SetWeight) instead of storing a value that poisons selection.
		return errors.New("redis: multidb: database Weight must not be NaN")
	}
	if cfg.ClusterOptions != nil && (cfg.ClusterOptions.RouteByLatency || cfg.ClusterOptions.RouteRandomly || cfg.ClusterOptions.ReadOnly) {
		// The cluster health checks (built-in PING and lag-aware) probe masters
		// only. Routing reads to replicas would let a member report healthy
		// while the replicas actually serving traffic are down or lagging, so
		// failover never fires. Reject rather than silently health-check the
		// wrong nodes.
		return errors.New("redis: multidb: RouteByLatency/RouteRandomly/ReadOnly are not supported for cluster member databases (health checks probe masters only)")
	}
	return nil
}

// fqdn derives the host-only identifier used for callbacks and metrics:
// the option's host for standalone and cluster databases.
func hostOnly(addr string) string {
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return addr
}

func (cfg *MultiDBClientConfig) fqdn() string {
	switch {
	case cfg.Options != nil:
		return hostOnly(cfg.Options.Addr)
	case cfg.ClusterOptions != nil && len(cfg.ClusterOptions.Addrs) > 0:
		return hostOnly(cfg.ClusterOptions.Addrs[0])
	}
	return ""
}

// MultiDBCtrl is the operational control surface of MultiDBClient: active
// database selection, runtime membership changes and health management. It is
// implemented by *MultiDBClient.
//
// Databases are addressed by a stable id: AddDatabase returns one, and it keeps
// naming the same database until that database is removed. Ids are never
// reused, so a handle held by the caller (or delivered to a callback) never
// silently points at a different member. An id that names no current member
// yields ErrDatabaseNotFound.
//
// Control operations (every method here, and Close) are for application code.
// Do not call them from code that MultiDB runs on your behalf: hooks installed
// with AddHook or AddDatabaseHook, health checks and health-check policies,
// failover strategies and failure detectors. That code runs inside MultiDB's
// command, probe and failover paths, in places under MultiDB's own locks or
// while an admission is held, and a control call from there can deadlock or
// corrupt the failover accounting. The asynchronous callbacks (OnFailover,
// OnActiveDatabaseChanged, OnCircuitStateChanged) are the exception: they run
// on their own queue and may call control operations.
type MultiDBCtrl interface {
	// ActiveDatabaseID returns the stable id of the currently active database,
	// or -1 when none is selected.
	ActiveDatabaseID() int
	// SetActiveDatabase switches to the database with the given id after a fresh
	// health probe of the target; it refuses with ErrTargetUnhealthy when the
	// probe fails, or ErrDatabaseNotFound for an unknown id.
	SetActiveDatabase(ctx context.Context, id int) error
	// ForceActiveDatabase switches to the database with the given id
	// unconditionally (operator override); it returns ErrDatabaseNotFound for an
	// unknown id.
	ForceActiveDatabase(ctx context.Context, id int) error
	// AddDatabase adds a member database at runtime and returns its stable id.
	AddDatabase(ctx context.Context, cfg MultiDBClientConfig) (int, error)
	// RemoveDatabase removes the database with the given id. The active database
	// cannot be removed. Returns ErrDatabaseNotFound for an unknown id.
	RemoveDatabase(ctx context.Context, id int) error
	// SetWeight changes the weight of the database with the given id. Returns
	// ErrDatabaseNotFound for an unknown id.
	SetWeight(id int, weight float64) error
	// SetAutoFallback enables or disables automatic fallback at runtime.
	SetAutoFallback(enabled bool)
}

var _ MultiDBCtrl = (*MultiDBClient)(nil)

// MultiDBClient routes all commands to a single active database out of N
// configured member databases (standalone or cluster) and
// transparently fails over between them based on circuit-breaker state,
// health checks and a failover strategy. It exposes the full Redis command
// surface and is a drop-in replacement for *Client in application code; the
// operational surface is available via MultiDBCtrl.
//
// See the Active-Active client design for details.
type MultiDBClient struct {
	cmdable
	hooksMixin

	core *multidbCore

	// Cached autopipeliner instances, mirroring *Client (see Client.AutoPipeline).
	autopipelinerMu     *sync.Mutex
	autopipeliner       *AutoPipeliner
	asyncAutopipeliner  *AutoPipeliner
	autopipelinerClosed bool
	// builtAutopipeliners holds every instance built for this client, including
	// ones no longer cached. An instance leaves the cache as soon as its
	// shutdown CAS is taken, which is before its drain finishes, so a caller
	// that closes one and asks for another would otherwise leave the first
	// draining with nothing left holding a reference — and Close must wait for
	// that drain before core.close() tears down the member pools it writes to.
	// Drained entries are dropped as new ones are added, so recreating in a
	// loop does not accumulate. Guarded by autopipelinerMu.
	builtAutopipeliners []*AutoPipeliner
	// closeOnce serializes concurrent Close calls so a second caller cannot
	// race ahead to core.close() while the first is still draining the
	// autopipeliner; closeErr carries that single close result to every caller.
	closeOnce sync.Once
	closeErr  error
}

// NewMultiDBClient creates a MultiDBClient for the configured member
// databases. It runs initial health checks and fails with
// ErrInsufficientHealthyDatabases when the InitialDBState policy is not
// satisfied; when ctx carries a deadline the initialization blocks and
// retries until the deadline.
func NewMultiDBClient(ctx context.Context, opts *MultiDBOptions) (*MultiDBClient, error) {
	if opts == nil {
		return nil, errors.New("redis: multidb: nil options")
	}
	// Work on a private copy: init() normalizes fields in place, and writing
	// them back into the caller's value would leak into other clients built
	// from the same options (e.g. CommandRetriesNone normalizes to 0, which
	// a second construction would then treat as "unset" and default to 2 —
	// re-enabling retries for a running client that reads the shared struct).
	private := *opts
	private.Clients = append([]MultiDBClientConfig(nil), opts.Clients...)
	// AddDatabase merges the global checks into later members: a private
	// slice keeps runtime-added members consistent with the initial ones
	// even when the caller mutates its slice after construction.
	private.HealthChecks = append([]MultiDBHealthCheck(nil), opts.HealthChecks...)
	if opts.CircuitBreakerConfig != nil {
		// Nested mutable state: AddDatabase builds later members from this
		// config, and a caller mutating the shared pointer after
		// construction would give runtime-added members different breaker
		// thresholds than the initial ones.
		cbc := *opts.CircuitBreakerConfig
		private.CircuitBreakerConfig = &cbc
	}
	opts = &private
	if err := opts.init(); err != nil {
		return nil, err
	}

	core := newMultidbCore(opts)
	for i := range opts.Clients {
		cfg := &opts.Clients[i]
		if err := cfg.validate(); err != nil {
			_ = core.closeAll()
			return nil, err
		}
		db, err := core.buildDatabase(cfg)
		if err != nil {
			_ = core.closeAll()
			return nil, err
		}
		core.dbs[db.id] = db
	}

	if err := core.initialize(ctx); err != nil {
		_ = core.closeAll()
		return nil, err
	}
	core.startBackgroundLoop()

	c := &MultiDBClient{core: core, autopipelinerMu: new(sync.Mutex)}
	c.cmdable = c.Process
	c.initHooks(hooks{
		process:    core.process,
		pipeline:   core.processPipeline,
		txPipeline: core.processTxPipeline,
	})
	return c, nil
}

// Process routes the command to the active database, feeding the circuit
// breaker and failure detector, and retrying against the newly selected
// database after a failover. MultiDB-level hooks (AddHook) wrap this path.
func (c *MultiDBClient) Process(ctx context.Context, cmd Cmder) error {
	err := c.processHook(ctx, cmd)
	cmd.SetErr(err)
	return err
}

// Close stops the autopipeliners, the background loop, and every underlying
// client.
//
// Do not call Close from a goroutine this client owns. Close waits for the
// background loop to exit and for any autopipeliner drain to finish, and
// either one can be the goroutine that called Close:
//
//   - From a health check or a health-check policy, Close blocks forever: those
//     run on the background loop, and Close waits for that loop to exit.
//   - From a member hook invoked inside an autopipeliner batch, Close blocks
//     until that drain reaches its internal backstop, then reports a timeout:
//     the drain is waiting for the batch whose hook is calling Close.
//
// Calling Close from OnFailover, OnActiveDatabaseChanged or
// OnCircuitStateChanged is fine: those callbacks run on a queue Close does not
// wait for. Close is a control operation like the MultiDBCtrl methods, so the
// rule there applies: do not call it from hooks, health checks, policies,
// strategies or detectors.
func (c *MultiDBClient) Close() error {
	// Serialize concurrent Close calls. Without this a second caller can
	// observe the autopipeliner pointers already cleared, skip the drain loop,
	// and call core.close() while the first caller's AutoPipeliner.Close is
	// still flushing queued batches — tearing down the member clients under the
	// drain and failing those writes with ErrClosed. Once blocks the second
	// caller until the single ordered drain-then-close completes; both return
	// the same error.
	c.closeOnce.Do(func() {
		c.autopipelinerMu.Lock()
		// Every instance ever built, not just the two cached pointers: an
		// instance is evicted from the cache as soon as its shutdown CAS is
		// taken, so one closed by its own caller can still be draining with
		// no cached reference left (see builtAutopipeliners).
		aps := c.builtAutopipeliners
		c.builtAutopipeliners = nil
		c.autopipeliner, c.asyncAutopipeliner = nil, nil
		c.autopipelinerClosed = true
		c.autopipelinerMu.Unlock()
		var firstErr error
		for _, p := range aps {
			if p == nil {
				continue
			}
			// Close can return immediately without draining: it loses its
			// internal CAS when something else (e.g. a batch hook calling Close
			// on its own executor goroutine) already claimed the shutdown.
			// WaitClosed blocks until that drain actually finishes and reports
			// its real result either way, so core.close() below never tears
			// down a member pool a drain is still writing to.
			//
			// The wait is unconditional on purpose. Skipping it when this
			// Close lost the CAS is what caused the teardown-under-drain bug,
			// and there is no way to tell "another goroutine is draining"
			// (wait) from "the drain is waiting for my own goroutine" (do not
			// wait) without goroutine identity. Bounding the wait does not
			// help either: a legitimate drain may run to the backstop. The
			// reentrant case is documented on Close instead.
			_ = p.Close()
			if err := p.WaitClosed(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
		if err := c.core.close(); err != nil && firstErr == nil {
			firstErr = err
		}
		c.closeErr = firstErr
	})
	return c.closeErr
}

// ActiveDatabaseID implements MultiDBCtrl.
func (c *MultiDBClient) ActiveDatabaseID() int { return c.core.activeDatabaseID() }

// SetActiveDatabase implements MultiDBCtrl.
func (c *MultiDBClient) SetActiveDatabase(ctx context.Context, id int) error {
	return c.core.setActiveDatabase(ctx, id, true)
}

// ForceActiveDatabase implements MultiDBCtrl.
func (c *MultiDBClient) ForceActiveDatabase(ctx context.Context, id int) error {
	return c.core.setActiveDatabase(ctx, id, false)
}

// AddDatabase implements MultiDBCtrl. A cluster member may be added while
// autopipeliners are live: commands that cannot ride a pipeline on it are kept
// out of merged batches from then on (see canPipeline on this type).
//
// "From then on" is the limit: a command accepted into a batch before the
// member was published is not re-checked at flush, and neither is a batch
// retried onto the new member after a failover. If such a batch carries a
// command the cluster cannot pipeline, the cluster member fails the whole
// batch with its routing error (retryable; the callers retry) or, for a
// command with special routing, runs it on the shard the cluster picks. The
// window is the flush interval after the add. To avoid it, add cluster
// members before enabling autopipelining, or close and re-create the
// autopipeliners around the add.
func (c *MultiDBClient) AddDatabase(ctx context.Context, cfg MultiDBClientConfig) (int, error) {
	// Reject any add once Close has begun. Close sets autopipelinerClosed under
	// autopipelinerMu before it drains the autopipeliners and calls
	// core.close(); a member published in that window would be torn down by the
	// completing Close, handing the caller an id for a dead member. Checked
	// under the mutex so it cannot race the flag write, and released before
	// core.addDatabase: that call runs the member's initial health probe
	// synchronously, and a custom check calling AutoPipeline() would otherwise
	// deadlock on the non-reentrant mutex.
	c.autopipelinerMu.Lock()
	if c.autopipelinerClosed {
		c.autopipelinerMu.Unlock()
		return -1, ErrClosed
	}
	c.autopipelinerMu.Unlock()
	return c.core.addDatabase(ctx, cfg)
}

// RemoveDatabase implements MultiDBCtrl.
func (c *MultiDBClient) RemoveDatabase(ctx context.Context, id int) error {
	return c.core.removeDatabase(ctx, id)
}

// SetWeight implements MultiDBCtrl.
func (c *MultiDBClient) SetWeight(id int, weight float64) error {
	return c.core.setWeight(id, weight)
}

// SetAutoFallback implements MultiDBCtrl.
func (c *MultiDBClient) SetAutoFallback(enabled bool) {
	c.core.setAutoFallback(enabled)
}

// AddHook adds a hook to the MultiDB-level chain that wraps Process, Pipeline
// and TxPipeline — the paths this client runs directly.
//
// A DialHook added here never fires: a MultiDBClient does not dial, it
// delegates every connection to its member databases. To instrument member
// dialing (or any other per-connection behaviour), install the hook on the
// member with AddDatabaseHook instead.
//
// A hook must not call the control operations (the MultiDBCtrl methods, or
// Close): see MultiDBCtrl. It runs inside the command path, and a control
// call from there can deadlock or corrupt the failover accounting.
func (c *MultiDBClient) AddHook(hook Hook) {
	c.hooksMixin.AddHook(hook)
}

// AddDatabaseHook installs a hook on the underlying client of the database with
// the given id. Unlike AddHook it wraps the member connection directly, so it
// is the way to instrument member dialing and per-connection behaviour.
//
// A member hook must not call the control operations (the MultiDBCtrl
// methods, or Close): see MultiDBCtrl. It runs inside the member's command
// path while MultiDB holds an admission for that command, and in the
// autopipeliner's batch dispatch; a control call from there can deadlock or
// corrupt the failover accounting.
//
// A member hook must pass the context it receives — or one derived from it —
// to next. MultiDB carries per-batch execution tracking in context values:
// the pipeline paths attach it before entering the member's hook chain and
// the wire path reads it back to mark commands as executed. A hook that
// substitutes an unrelated context (context.Background() to detach
// cancellation, a freshly built deadline context) drops that value, so a batch
// that executed successfully is indistinguishable from one a hook served
// locally and records no success for the circuit breaker or failure detector.
// The effect is under-recording only — slower recovery of a half-open member,
// failures not reset — never a phantom success or a replay.
//
// For a cluster member this installs the hook on the *ClusterClient's own
// process and pipeline chains only. It does not reach the per-node *Client
// instances the cluster dials internally, so a DialHook never runs for a
// cluster member's connections. There is currently no MultiDB API to reach
// those per-node clients.
func (c *MultiDBClient) AddDatabaseHook(id int, hook Hook) error {
	return c.core.addDatabaseHook(id, hook)
}

// Subscribe creates a PubSub subscription that follows the active database:
// it dials whichever database is active and re-dials when the active database
// changes. The active database must be standalone; cluster databases are not
// supported for MultiDB PubSub yet.
func (c *MultiDBClient) Subscribe(ctx context.Context, channels ...string) *PubSub {
	pubsub := c.core.newPubSub()
	if len(channels) > 0 {
		_ = pubsub.Subscribe(ctx, channels...)
	}
	return pubsub
}

// PSubscribe is like Subscribe but for patterns.
func (c *MultiDBClient) PSubscribe(ctx context.Context, patterns ...string) *PubSub {
	pubsub := c.core.newPubSub()
	if len(patterns) > 0 {
		_ = pubsub.PSubscribe(ctx, patterns...)
	}
	return pubsub
}

// DBSize delegates to the active member so a cluster member keeps its
// fan-out semantics (summing across masters) instead of counting a single
// arbitrary shard.
//
// Like Watch, the cluster delegation runs on the member client directly:
// MultiDB-level hooks (AddHook) do not wrap it — the member's own hooks
// apply, so install hooks via AddDatabaseHook when this traffic must be
// instrumented. Against a non-cluster member the command takes the regular
// hook-wrapped path. The same holds for ScriptLoad, ScriptFlush and
// ScriptExists.
//
// A fan-out failure here is deliberately NOT recorded on the circuit breaker
// or failure detector and does NOT trigger failover: these are cluster
// control commands, not health-representative traffic, and a fan-out error is
// often a partial/single-shard condition for which moving the whole member
// off is the wrong response (e.g. re-loading a script on a different member).
// Member health is driven by the data path, which does feed the accounting.
// The active is still admitted like any command, though: one whose breaker
// is open, or whose failure detector has tripped, is failed over BEFORE the
// command is sent (see activeClusterAfterGate).
func (c *MultiDBClient) DBSize(ctx context.Context) *IntCmd {
	if c.core.closed.Load() {
		cmd := NewIntCmd(ctx, "dbsize")
		cmd.SetErr(ErrClosed)
		return cmd
	}
	if db := c.activeClusterAfterGate(ctx); db != nil {
		res := db.cc.DBSize(ctx)
		rewriteRemovedMemberErr(db, res, c.core.closed.Load())
		return res
	}
	return c.cmdable.DBSize(ctx)
}

// activeAfterGate returns the active member after the admission the data
// path applies to every command: an active whose breaker is open, or whose
// failure detector has tripped, is failed over first, so the caller does not
// bind work to a member already known to be unhealthy. No half-open probe
// slot is reserved and no outcome is recorded: the callers (cluster control
// commands, sharded subscriptions) are not health-representative traffic.
// Returns nil when there is no active member.
func (c *MultiDBClient) activeAfterGate(ctx context.Context) *multidbDatabase {
	db, idx := c.core.activeSnapshot()
	if db != nil && (!db.selectable() || c.core.shouldFailoverSafely()) {
		// A failed failover (no selectable candidate) keeps the current
		// active: the operation then fails against it, as any command would.
		if err := c.core.tryFailover(ctx, idx); err == nil {
			db, _ = c.core.activeSnapshot()
		}
	}
	return db
}

// activeClusterAfterGate is activeAfterGate restricted to a cluster member:
// it returns nil when the active is not one, and the caller then takes the
// regular hook-wrapped path (see DBSize).
func (c *MultiDBClient) activeClusterAfterGate(ctx context.Context) *multidbDatabase {
	if db := c.activeAfterGate(ctx); db != nil && db.cc != nil {
		return db
	}
	return nil
}

// ScriptLoad delegates to the active member so a cluster member loads the
// script on every shard — a single-shard load would make later EVALSHA calls
// fail with NOSCRIPT on the other shards.
func (c *MultiDBClient) ScriptLoad(ctx context.Context, script string) *StringCmd {
	if c.core.closed.Load() {
		cmd := NewStringCmd(ctx, "script", "load", script)
		cmd.SetErr(ErrClosed)
		return cmd
	}
	if db := c.activeClusterAfterGate(ctx); db != nil {
		res := db.cc.ScriptLoad(ctx, script)
		rewriteRemovedMemberErr(db, res, c.core.closed.Load())
		return res
	}
	return c.cmdable.ScriptLoad(ctx, script)
}

// ScriptFlush delegates to the active member (cluster members flush every
// shard).
func (c *MultiDBClient) ScriptFlush(ctx context.Context) *StatusCmd {
	if c.core.closed.Load() {
		cmd := NewStatusCmd(ctx, "script", "flush")
		cmd.SetErr(ErrClosed)
		return cmd
	}
	if db := c.activeClusterAfterGate(ctx); db != nil {
		res := db.cc.ScriptFlush(ctx)
		rewriteRemovedMemberErr(db, res, c.core.closed.Load())
		return res
	}
	return c.cmdable.ScriptFlush(ctx)
}

// ScriptExists delegates to the active member (cluster members AND the
// per-shard answers so a script only counts as present when every shard has
// it).
func (c *MultiDBClient) ScriptExists(ctx context.Context, hashes ...string) *BoolSliceCmd {
	if c.core.closed.Load() {
		args := make([]interface{}, 2, 2+len(hashes))
		args[0], args[1] = "script", "exists"
		for _, h := range hashes {
			args = append(args, h)
		}
		cmd := NewBoolSliceCmd(ctx, args...)
		cmd.SetErr(ErrClosed)
		return cmd
	}
	if db := c.activeClusterAfterGate(ctx); db != nil {
		res := db.cc.ScriptExists(ctx, hashes...)
		rewriteRemovedMemberErr(db, res, c.core.closed.Load())
		return res
	}
	return c.cmdable.ScriptExists(ctx, hashes...)
}

// rewriteRemovedMemberErr handles a cluster control command delegated to a
// snapshotted member that a concurrent control op removed mid-call: the
// member's closed client returns the terminal ErrClosed even though the
// MultiDBClient is still open. Rewrite it to the retryable
// ErrTemporarilyNotAvailable, as the data path does, so the caller retries and
// the next snapshot lands on the live active.
// closed is the MultiDBClient's own closed flag, read AFTER the delegated call:
// Close marks every member removed, so without it a control command in flight
// across Close would report the retryable error for a client that has shut
// down. The command, pipeline, transaction and Watch paths keep ErrClosed the
// same way.
func rewriteRemovedMemberErr(db *multidbDatabase, cmd Cmder, closed bool) {
	if closed {
		return
	}
	if db.removed.Load() && errors.Is(cmd.rawErr(), ErrClosed) {
		cmd.SetErr(ErrTemporarilyNotAvailable)
	}
}

// errMultiDBHImport rejects the HIMPORT command family: fieldset
// registrations live in each member client's own registry, so a fieldset
// prepared on the active member is silently missing on the member a failover
// switches to. Until registrations fan out across members (tracked as
// follow-up work in the design doc), rejecting loudly beats half-working.
// A RedisError, not a plain error: the autopipeliner's sequential dispatch
// treats a non-Redis error from one sub-batch as a fatal abort for the groups
// behind it, and a rejected HIMPORT must fail only itself — never unrelated
// commands that happened to be queued after it.
var errMultiDBHImport = proto.RedisError("MULTIDB HIMPORT commands are not supported with MultiDBClient yet (fieldset registrations are per member and would be lost on failover)")

// errPubSubRequiresStandalone is the retryable PubSub dial error returned when a
// cluster member is active but the subscription needs a standalone one. It is
// NOT the terminal ErrClosed, so the PubSub channel loop keeps retrying until a
// standalone member becomes active (a later failover/fallback, or an
// AddDatabase). Only a config that was all-cluster at PubSub creation and still
// has no standalone member gets the terminal ErrClosed instead.
var errPubSubRequiresStandalone = errors.New("redis: multidb: PubSub requires a standalone active database")

// HImportPrepare is not supported on MultiDBClient; see errMultiDBHImport.
func (c *MultiDBClient) HImportPrepare(ctx context.Context, fieldsetName string, fields ...string) *StatusCmd {
	cmd := NewHImportPrepareCmd(ctx, fieldsetName, fields...)
	cmd.SetErr(errMultiDBHImport)
	return &cmd.StatusCmd
}

// HImportSet is not supported on MultiDBClient; see errMultiDBHImport.
func (c *MultiDBClient) HImportSet(ctx context.Context, key, fieldsetName string, values ...interface{}) *StatusCmd {
	cmd := NewHImportSetCmd(ctx, key, fieldsetName, values...)
	cmd.SetErr(errMultiDBHImport)
	return &cmd.StatusCmd
}

// HImportDiscard is not supported on MultiDBClient; see errMultiDBHImport.
func (c *MultiDBClient) HImportDiscard(ctx context.Context, fieldsetName string) *IntCmd {
	cmd := NewHImportDiscardCmd(ctx, fieldsetName)
	cmd.SetErr(errMultiDBHImport)
	return &cmd.IntCmd
}

// HImportDiscardAll is not supported on MultiDBClient; see errMultiDBHImport.
func (c *MultiDBClient) HImportDiscardAll(ctx context.Context) *IntCmd {
	cmd := NewHImportDiscardAllCmd(ctx)
	cmd.SetErr(errMultiDBHImport)
	return &cmd.IntCmd
}

// disableMaintNotificationsIfUnset turns Smart Client Handoff (maintenance
// notifications) off for a MultiDB member database unless the user configured
// it explicitly: MultiDB owns endpoint transitions itself.
func disableMaintNotificationsIfUnset(opt *Options) {
	if opt.MaintNotificationsConfig == nil {
		opt.MaintNotificationsConfig = &maintnotifications.Config{Mode: maintnotifications.ModeDisabled}
	}
}
