package redis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"time"

	imultidb "github.com/redis/go-redis/v9/internal/multidb"
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
	// commands.
	ErrTemporarilyNotAvailable = errors.New("redis: multidb: no healthy database available (temporarily)")
	// ErrPermanentlyNotAvailable is returned once MaxFailoverAttempts
	// consecutive failover attempts have failed; the application should stop
	// using this client.
	ErrPermanentlyNotAvailable = errors.New("redis: multidb: no healthy database available (permanently)")
	// ErrInsufficientHealthyDatabases is returned by NewMultiDBClient when the
	// InitialDBState policy is not satisfied.
	ErrInsufficientHealthyDatabases = errors.New("redis: multidb: insufficient healthy databases on initialization")
	// ErrTargetUnhealthy is returned by SetActiveIndex when the requested
	// target database fails its health probe. Use ForceActiveIndex to switch
	// unconditionally.
	ErrTargetUnhealthy = errors.New("redis: multidb: target database is unhealthy")
)

// MultiDBFailureDetector aggregates command outcomes and decides when the
// active database is unhealthy enough to trigger failover.
// The default implementation is a sliding-window detector that trips when at
// least a minimum number of failures AND a minimum failure rate are observed
// within the detection window.
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
	Index   int
	Weight  float64
	Allowed bool // circuit breaker allows traffic
}

// MultiDBFailoverStrategy selects the failover target from candidate
// databases. Return the chosen index, or -1 when no candidate is acceptable.
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
			best = c.Index
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

// MultiDBClientConfig describes one member database. Exactly one of Options,
// FailoverOptions or ClusterOptions must be set.
type MultiDBClientConfig struct {
	// Options configures a standalone client for this database.
	Options *Options
	// FailoverOptions configures a Sentinel-backed failover client.
	FailoverOptions *FailoverOptions
	// ClusterOptions configures a cluster client.
	ClusterOptions *ClusterOptions

	// Weight is the database's selection priority; the failover strategy
	// prefers higher weights. Default: 1.0.
	Weight float64

	// HealthChecks run against this database in addition to the global
	// MultiDBOptions.HealthChecks (merge semantics: global + per-DB are
	// additive). A database that ends up with no checks from either layer is
	// given a single default PING check.
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
	// active database from index `from` to index `to`.
	OnFailover func(ctx context.Context, from, to int)
	// OnActiveDatabaseChanged is called on every active-database change,
	// including auto-fallback.
	OnActiveDatabaseChanged func(from, to int)
	// OnCircuitStateChanged is called when any database's circuit breaker
	// changes state ("closed", "open", "half-open"). It is delivered
	// asynchronously (FIFO per database), so it is safe to call control APIs
	// (SetActiveIndex, RemoveDatabase, ...) from the callback; do not rely
	// on it running synchronously with the transition that triggered it.
	OnCircuitStateChanged func(dbIndex int, from, to string)
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
	if cfg.FailoverOptions != nil {
		set++
	}
	if cfg.ClusterOptions != nil {
		set++
	}
	if set != 1 {
		return fmt.Errorf("redis: multidb: exactly one of Options, FailoverOptions or ClusterOptions must be set per database (got %d)", set)
	}
	if cfg.FailoverOptions != nil && (cfg.FailoverOptions.RouteByLatency || cfg.FailoverOptions.RouteRandomly) {
		// NewFailoverClient panics for these options (they require the
		// failover cluster client); reject them here so a bad member config
		// surfaces as a constructor error instead of a crash.
		return errors.New("redis: multidb: RouteByLatency/RouteRandomly are not supported for sentinel member databases")
	}
	return nil
}

// fqdn derives the host-only identifier used for callbacks and metrics:
// the option's host for standalone/cluster databases, or the Sentinel master
// name for failover databases.
func (cfg *MultiDBClientConfig) fqdn() string {
	hostOnly := func(addr string) string {
		if host, _, err := net.SplitHostPort(addr); err == nil {
			return host
		}
		return addr
	}
	switch {
	case cfg.Options != nil:
		return hostOnly(cfg.Options.Addr)
	case cfg.FailoverOptions != nil:
		return cfg.FailoverOptions.MasterName
	case cfg.ClusterOptions != nil && len(cfg.ClusterOptions.Addrs) > 0:
		return hostOnly(cfg.ClusterOptions.Addrs[0])
	}
	return ""
}

// MultiDBCtrl is the operational control surface of MultiDBClient: active
// database selection, runtime membership changes and health management. It is
// implemented by *MultiDBClient.
type MultiDBCtrl interface {
	// ActiveIndex returns the index of the currently active database.
	ActiveIndex() int
	// SetActiveIndex switches to the database at index after a fresh health
	// probe of the target; it refuses with ErrTargetUnhealthy when the probe
	// fails.
	SetActiveIndex(ctx context.Context, index int) error
	// ForceActiveIndex switches to the database at index unconditionally
	// (operator override; only the index range is validated).
	ForceActiveIndex(ctx context.Context, index int) error
	// AddDatabase adds a member database at runtime and returns its index.
	AddDatabase(ctx context.Context, cfg MultiDBClientConfig) (int, error)
	// RemoveDatabase removes the database at index. The active database
	// cannot be removed.
	RemoveDatabase(ctx context.Context, index int) error
	// SetWeight changes the weight of the database at index.
	SetWeight(index int, weight float64) error
	// SetAutoFallback enables or disables automatic fallback at runtime.
	SetAutoFallback(enabled bool)
}

var _ MultiDBCtrl = (*MultiDBClient)(nil)

// MultiDBClient routes all commands to a single active database out of N
// configured member databases (standalone, Sentinel or cluster) and
// transparently fails over between them based on circuit-breaker state,
// health checks and a failover strategy. It exposes the full Redis command
// surface and is a drop-in replacement for *Client in application code; the
// operational surface is available via MultiDBCtrl.
//
// See the Active-Active client design for details.
type MultiDBClient struct {
	cmdable
	core *multidbCore
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
		db.idx.Store(int32(len(core.dbs)))
		core.dbs = append(core.dbs, db)
	}

	if err := core.initialize(ctx); err != nil {
		_ = core.closeAll()
		return nil, err
	}
	core.startBackgroundLoop()

	c := &MultiDBClient{core: core}
	c.cmdable = c.Process
	return c, nil
}

// Process routes the command to the active database, feeding the circuit
// breaker and failure detector, and retrying against the newly selected
// database after a failover.
func (c *MultiDBClient) Process(ctx context.Context, cmd Cmder) error {
	return c.core.process(ctx, cmd)
}

// Close stops the background loop and closes every underlying client.
func (c *MultiDBClient) Close() error {
	return c.core.close()
}

// ActiveIndex implements MultiDBCtrl.
func (c *MultiDBClient) ActiveIndex() int { return c.core.activeIndex() }

// SetActiveIndex implements MultiDBCtrl.
func (c *MultiDBClient) SetActiveIndex(ctx context.Context, index int) error {
	return c.core.setActiveIndex(ctx, index, true)
}

// ForceActiveIndex implements MultiDBCtrl.
func (c *MultiDBClient) ForceActiveIndex(ctx context.Context, index int) error {
	return c.core.setActiveIndex(ctx, index, false)
}

// AddDatabase implements MultiDBCtrl.
func (c *MultiDBClient) AddDatabase(ctx context.Context, cfg MultiDBClientConfig) (int, error) {
	return c.core.addDatabase(ctx, cfg)
}

// RemoveDatabase implements MultiDBCtrl.
func (c *MultiDBClient) RemoveDatabase(ctx context.Context, index int) error {
	return c.core.removeDatabase(ctx, index)
}

// SetWeight implements MultiDBCtrl.
func (c *MultiDBClient) SetWeight(index int, weight float64) error {
	return c.core.setWeight(index, weight)
}

// SetAutoFallback implements MultiDBCtrl.
func (c *MultiDBClient) SetAutoFallback(enabled bool) {
	c.core.setAutoFallback(enabled)
}

// AddDatabaseHook installs a hook on the underlying client of the database at
// index. It is mainly useful for testing and instrumentation.
func (c *MultiDBClient) AddDatabaseHook(index int, hook Hook) error {
	return c.core.addDatabaseHook(index, hook)
}

// Subscribe creates a PubSub subscription that follows the active database:
// it dials whichever database is active and re-dials when the active database
// changes. The active database must be standalone or Sentinel-backed;
// cluster databases are not supported for MultiDB PubSub yet.
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
func (c *MultiDBClient) DBSize(ctx context.Context) *IntCmd {
	if db, _ := c.core.activeSnapshot(); db != nil && db.cc != nil {
		return db.cc.DBSize(ctx)
	}
	return c.cmdable.DBSize(ctx)
}

// ScriptLoad delegates to the active member so a cluster member loads the
// script on every shard — a single-shard load would make later EVALSHA calls
// fail with NOSCRIPT on the other shards.
func (c *MultiDBClient) ScriptLoad(ctx context.Context, script string) *StringCmd {
	if db, _ := c.core.activeSnapshot(); db != nil && db.cc != nil {
		return db.cc.ScriptLoad(ctx, script)
	}
	return c.cmdable.ScriptLoad(ctx, script)
}

// ScriptFlush delegates to the active member (cluster members flush every
// shard).
func (c *MultiDBClient) ScriptFlush(ctx context.Context) *StatusCmd {
	if db, _ := c.core.activeSnapshot(); db != nil && db.cc != nil {
		return db.cc.ScriptFlush(ctx)
	}
	return c.cmdable.ScriptFlush(ctx)
}

// ScriptExists delegates to the active member (cluster members AND the
// per-shard answers so a script only counts as present when every shard has
// it).
func (c *MultiDBClient) ScriptExists(ctx context.Context, hashes ...string) *BoolSliceCmd {
	if db, _ := c.core.activeSnapshot(); db != nil && db.cc != nil {
		return db.cc.ScriptExists(ctx, hashes...)
	}
	return c.cmdable.ScriptExists(ctx, hashes...)
}

// errMultiDBHImport rejects the HIMPORT command family: fieldset
// registrations live in each member client's own registry, so a fieldset
// prepared on the active member is silently missing on the member a failover
// switches to. Until registrations fan out across members (tracked as
// follow-up work in the design doc), rejecting loudly beats half-working.
var errMultiDBHImport = errors.New("redis: multidb: HIMPORT commands are not supported with MultiDBClient yet (fieldset registrations are per member and would be lost on failover)")

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
