# Per-client OpenTelemetry recorders

Design for letting each go-redis client instance record its metrics through its
own `redis.OTelRecorder`, with per-client custom attributes layered over the
process-wide configuration. The same interface serves the global recorder and
the per-client one, and an application may supply its own implementation at
either level.

## Motivation

Today metrics are a process-wide concern. An application calls
`redisotel.GetObservabilityInstance().Init(cfg)` once (or `redis.SetOTelRecorder`
with its own recorder), and every client in the process reports through that
single recorder. `Config.WithCustomAttributes` attaches extra attributes, but to
every client alike.

Applications that run several clients in one process (one per tenant, per
role such as "cache" vs "sessions", per region, per Active-Active member) need
to tell their metric series apart by something other than `server.address`.
They need:

- a recorder **per client instance**, of the same `redis.OTelRecorder` type the
  global one has, passable through **any** client configuration (`Options`,
  `ClusterOptions`, `FailoverOptions`, `RingOptions`, `UniversalOptions`,
  `MultiDBOptions`);
- the freedom to pass **their own** `OTelRecorder` implementation as the global
  or as a per-client recorder;
- per-client configuration in the **same builder style** as the global one, so
  the existing `redisotel.Config` and its `WithCustomAttributes` are the
  per-client configuration too;
- a fixed **attribute precedence**: built-in semantic-convention attributes <
  attributes configured on the `Config` passed to `Init` < attributes configured
  on the client's `Config`.

## Goals and non-goals

Goals

- Per-client granularity for **every** metric family: command and pipeline
  durations, pool connection count / pending requests / create and wait time /
  close / relaxed timeout / handoff, client errors, maintenance notifications,
  Pub/Sub messages, stream lag, MultiDB failover and health-check metrics.
- One recorder interface and one recorder implementation in `redisotel` for both
  levels. No second type to learn.
- Precedence encoded once, with the "last duplicate key wins" rule that
  `WithCustomAttributes` already documents.
- No change for applications that only use the global `Init` or
  `SetOTelRecorder`. No change for third parties that implement
  `redis.OTelRecorder`.
- No per-call allocation on the full-duplex success path, where every command
  is recorded on one reader goroutine.
- UpDown counters stay balanced across `Init`, `Shutdown` and client `Close`.

Non-goals

- A registry of clients, or any post-construction "instrument this client" API.
- Changing `Init`, `Shutdown`, `Config`, `SetOTelRecorder` or the recorder
  interfaces.

## Usage

```go
inst := redisotel.GetObservabilityInstance()

// Process-wide, unchanged. Its custom attributes form the middle layer for
// every client, including clients with their own recorder.
if err := inst.Init(redisotel.NewConfig().
    WithEnabled(true).
    WithMeterProvider(provider).
    WithCustomAttributes(attribute.String("deployment.environment", "prod")),
); err != nil {
    log.Fatal(err)
}
defer inst.Shutdown()

// Per client: the same Config builder, handed to NewRecorder instead of Init.
acmeRec, err := inst.NewRecorder(redisotel.NewConfig().
    WithCustomAttributes(attribute.String("tenant", "acme")))
if err != nil {
    log.Fatal(err)
}
acme := redis.NewClient(&redis.Options{Addr: "redis-acme:6379", OTelRecorder: acmeRec})

globexRec, _ := inst.NewRecorder(redisotel.NewConfig().
    WithCustomAttributes(attribute.String("tenant", "globex")))
globex := redis.NewClient(&redis.Options{Addr: "redis-globex:6379", OTelRecorder: globexRec})

legacy := redis.NewClient(&redis.Options{Addr: "redis-legacy:6379"}) // global recorder only
```

Resulting series:

```
db.client.connection.count{tenant="acme",   deployment.environment="prod", db.client.connection.pool.name="redis-acme:6379_1a2b…"}
db.client.connection.count{tenant="globex", deployment.environment="prod", db.client.connection.pool.name="redis-globex:6379_9f8e…"}
db.client.connection.count{                 deployment.environment="prod", db.client.connection.pool.name="redis-legacy:6379_…"}
```

The field travels with whatever a configuration builds:

```go
// Cluster: every node client, including nodes created later on a topology
// reload, and the cluster's own MOVED/ASK error and pipeline metrics.
cc := redis.NewClusterClient(&redis.ClusterOptions{
    Addrs:        []string{"c1:7000", "c2:7000"},
    OTelRecorder: acmeRec,
    // A custom node factory keeps working: opt already carries OTelRecorder.
    NewClient: func(opt *redis.Options) *redis.Client { return redis.NewClient(opt) },
})

// Universal: Simple(), Cluster() and Failover() all carry the field, so a
// sentinel setup covers the sentinel connections too.
uc := redis.NewUniversalClient(&redis.UniversalOptions{
    Addrs: []string{"sentinel-1:26379"}, MasterName: "mymaster",
    OTelRecorder: acmeRec,
})

// MultiDB: the MultiDB client records its failover / health-check / circuit
// metrics through its own field; a member whose options leave OTelRecorder nil
// inherits it, a member may also set its own.
mdb, err := redis.NewMultiDBClient(ctx, &redis.MultiDBOptions{
    OTelRecorder: acmeRec,
    Clients: []redis.MultiDBClientConfig{
        {Options: &redis.Options{Addr: "eu.acme.example:6379"}},                 // inherits acmeRec
        {Options: &redis.Options{Addr: "us.acme.example:6379", OTelRecorder: acmeUSRec}},
    },
})
```

A recorder is any `redis.OTelRecorder`, so an application's own implementation
works at both levels and receives every event of the client it is attached to,
pool-level ones included, whether or not a global recorder is installed:

```go
type tenantMetrics struct{ tenant string /* ... */ }
// implements redis.OTelRecorder, optionally OTelConnectionCounter, OTelMultiDBRecorder, OTelPoolRegistrar

redis.SetOTelRecorder(&tenantMetrics{tenant: "default"})                              // global
rdb := redis.NewClient(&redis.Options{Addr: "x:6379", OTelRecorder: &tenantMetrics{tenant: "acme"}}) // per client
```

One recorder may serve many clients. A per-client recorder needs no `Close`;
it records for as long as a client uses it, and `Shutdown` of the instance
disables the recorders it issued.

## Public API

### Root package `redis`

| Symbol | Purpose |
|---|---|
| `Options.OTelRecorder OTelRecorder` | The per-client recorder. The same field, with the same semantics, on `ClusterOptions`, `RingOptions`, `FailoverOptions`, `UniversalOptions` and `MultiDBOptions`. `nil` uses the process-wide recorder set with `SetOTelRecorder`. |

That is the whole root surface: one field concept, no new types or functions.
`OTelRecorder`, the optional capability interfaces (`OTelConnectionCounter`,
`OTelMultiDBRecorder`, `OTelPoolRegistrar`) and `SetOTelRecorder` are unchanged,
and the capabilities are detected for a per-client recorder exactly as for the
global one.

The field is read once when the client is built. Clients made from a copy of
the same options (cluster node clients, `WithTimeout` clones) share the
recorder, which is the intended granularity: one recorder per *configured*
client. Installing or shutting down the global recorder later does not touch a
client that has its own. Inside the package the resolved recorder is read
through a nil-safe `recorder()` accessor on each client type, so a value built
without its constructor (as many tests do) records to the global recorder.

A recorder may optionally implement `Enabled() bool`. The client uses it to skip
metric work whose inputs are expensive (timing a command, reading a result on
the async autopipeline face). A recorder without the method is always enabled.

### Package `redisotel` (extra/redisotel-native)

| Symbol | Purpose |
|---|---|
| `func (o *ObservabilityInstance) NewRecorder(cfg *Config) (redis.OTelRecorder, error)` | Builds a recorder for one or more clients from the same `Config` builder `Init` takes. |

`NewRecorder` returns the same recorder implementation `Init` installs
globally, configured by `cfg`:

- It requires `Init` to have run (and to have been enabled); otherwise it
  returns an error. The recorder is disabled when the instance is `Shutdown`.
- `cfg.MeterProvider == nil` means the provider the instance was initialized
  with (falling back to the OTel global as `Init` does), so clients share the
  process-wide provider by default. Instruments with identical definitions are
  deduplicated by the OTel SDK, so a per-client recorder on the shared provider
  feeds the same metric streams and differs only by attributes.
- `cfg.MetricGroups`, `IncludeCommands`/`ExcludeCommands`, the cardinality
  switches, `RecordNilErrors` and the histogram settings apply to that
  recorder's clients.
- `cfg.Enabled` is ignored: whether metrics are on is the instance's decision.
  (`NewConfig()` defaults to disabled, and honoring it here would silently mute
  clients whose author forgot `WithEnabled(true)`.)
- Its custom attributes are the `Init` config's followed by `cfg`'s, which is
  the attribute precedence below.

`Init`, `Shutdown`, `Config` and `NewConfig()` are unchanged.

### Attribute precedence

| Rank (low → high) | Source |
|---|---|
| 1 | built-in semantic-convention attributes (`db.system.name`, `server.address`, `db.client.connection.pool.name`, …) |
| 2 | `CustomAttributes` of the `Config` passed to `Init` |
| 3 | `CustomAttributes` of the `Config` passed to `NewRecorder` |

For a duplicate key the later layer wins. A client without `OTelRecorder` gets
layers 1 and 2; a client with a recorder from `NewRecorder` gets 1, 2 and 3. The
order is fixed when `NewRecorder` runs: it concatenates the two custom-attribute
lists in that order and the recorder appends them after the built-in attributes
on every instrument, the cached operation-duration set included. No merge code
exists beyond that one concatenation.

## Architecture

Metrics flow through three layers, all of which are process-global today:

1. `internal/pool` fires pool-level events through package-level callback
   variables.
2. `internal/otel` holds the global `Recorder` and the package functions the
   root package calls.
3. `redisotel-native` implements the recorder as a singleton.

The design makes the first two layers per-client with one small mechanism each
and keeps the existing global path as the `nil` case of that mechanism. The
third layer needs no new type: the existing recorder is constructed once more
per client.

### Root: one field, resolved once into two views

When a client is built, `Options.OTelRecorder` is resolved **once** into:

- `baseClient.rec` — an internal `otel.Recorder` the client's own code records
  through (command and pipeline durations, errors, Pub/Sub, streams). It is
  never nil: with no per-client recorder it is a `scopedGlobal` value whose
  methods forward to the process-wide recorder at call time. This value is the
  **only** place "fall back to the global recorder" is written.
- `pool.Options.Metrics` — a `*pool.MetricCallbacks` table built from the same
  adapter, handed to the client's three pools (main, Pub/Sub, pipeline). `nil`
  means "follow the global table".

Both views wrap the same `otelRecorderAdapter` that `SetOTelRecorder` already
uses, so the optional capability interfaces are detected the same way. Clients
that own no pools (`ClusterClient`, `Ring`, the MultiDB core) keep only the
recorder view; their node/shard/member clients build their own pools from the
options the field was copied into.

The package-level `otel.RecordX`, `otel.Enabled` and `otel.Get*Callback`
functions are deleted. Every root site records through `<client>.rec`. Deleting
the globals is deliberate: a call site that is missed fails to compile instead
of silently reporting to the process-wide recorder.

### Pool: an immutable per-pool callback table

`internal/pool` replaces its eleven callback variables, their `RWMutex` and
getters with:

- `Options.Metrics *MetricCallbacks` — the owning client's table, set at
  construction and never mutated;
- `Conn.metrics` — the pool's table, stamped on every connection beside the
  existing `poolName`;
- one global `atomic.Pointer[MetricCallbacks]` for pools built without a table;
- one resolution rule: `p.metrics()` inside the pool, `pool.MetricsOf(cn)` for
  code that holds a connection but no client (`maintnotifications`).

Because a pool's table never changes, every `+1` and `-1` a connection records
reaches the same recorder, and nothing has to be re-pointed on `Close`. Pools
built with their own table are **not** part of the live-pool set that
`SetAllMetricCallbacks` recounts when the global recorder is installed or
cleared: a global install or shutdown never touches them, so the UpDown
invariant holds for them by construction and the recount keeps today's shape
for the pools that follow the global table. `SetAllMetricCallbacks` keeps its
signature. The lock order (`livePoolsMu`, then every `connsMu`, then every
`trackMu`) is unchanged.

### `internal/otel`: a recorder per client

- `Enabled(r Recorder) bool` is a package function, not an interface method:
  false for the noop recorder (or a view of a global that is noop), else the
  recorder's own optional `Enabled()`. A function keeps the interface as it
  is, so a test or partial recorder that embeds `NoopRecorder()` to fill in
  the methods it does not care about is not silently switched off.
- `Scoped(r) Recorder` returns `r`, or the `scopedGlobal` fallback.
- `PoolCallbacks(r) *pool.MetricCallbacks` builds a table from method values,
  so every callback captures `r`. The global installation uses the same
  function, so the `-1`-reaches-the-right-recorder guarantee the UpDown
  callbacks already had now holds uniformly.
- `RegisterPools`/`UnregisterPools` take the client's recorder, so a client
  whose own recorder implements `OTelPoolRegistrar` registers its pools there.

### `redisotel-native`: the same recorder, constructed per client

`Init` already turns a `Config` into a `metricsRecorder` (`configToInternal` +
`createRecorder`) and installs it with `redis.SetOTelRecorder`. `NewRecorder`
reuses that path and returns the recorder instead of installing it. The only
additions:

- the meter comes from `cfg.MeterProvider`, else from the provider `Init` used;
- `customAttributes` is `Init`'s list followed by `cfg`'s. The `Init` layer is
  the global recorder's own copy of the attributes, not the caller-owned
  `Config`, so mutating that `Config` after `Init` changes nothing;
- when the provider is the one `Init` used (nil, or the same value passed
  explicitly; providers of a non-comparable dynamic type count as distinct),
  the recorder shares the global recorder's asynchronous recording queue (the
  operation-duration histogram is the same deduplicated instrument), so no
  extra goroutine is started. A recorder on another provider gets its own
  queue, which the instance tracks and stops in `Shutdown`;
- every recorder of one `Init` (the global one and those `NewRecorder`
  issued) shares a stop flag that `Shutdown` sets; `Enabled()` reports it and
  every record method returns early on it, except the two UpDown counters
  (`connection.count`, `pending_requests`), which keep flowing so a client
  closed after `Shutdown` still brings its counts back to zero. Recorders
  issued before a `Shutdown` stay disabled after a later `Init`; the
  application builds new ones.

Custom attributes are appended after the built-in attributes by one helper that
every instrument goes through, including the cached success-path
operation-duration set, so the full-duplex reader still records a prebuilt
`attribute.Set` with no per-call allocation. Each recorder owns its own
attribute cache.

### Propagation across client types

The field is copied wherever options are converted, following the precedent of
`MaintNotificationsConfig`: `UniversalOptions.Simple/Cluster/Failover`,
`ClusterOptions.clientOptions` (so every node, including ones created on a
topology reload and ones built by a custom `NewClient` factory),
`RingOptions.clientOptions`, `FailoverOptions.clientOptions/sentinelOptions/
clusterOptions`. MultiDB members inherit `MultiDBOptions.OTelRecorder` when
their own options leave it nil. A reflection test per conversion guards the
hand-synced lists.

### Coverage of every metric family

| Family | Where it is recorded | Per-client source |
|---|---|---|
| operation duration | `processStartingAt`, full-duplex reader | `c.rec` |
| pipeline duration | pipeline paths of `baseClient`, `ClusterClient`, autopipeliner | `c.rec` |
| client errors, handshake failures | root sites with a client in hand | `c.rec` |
| MOVED/ASK redirects | `ClusterClient` method | the cluster's `rec` (never the global via a nil conn) |
| connection count, pending requests, create/wait time, closed | inside the pool | `p.metrics()` |
| relaxed timeout, handoff, maintenance notifications | `maintnotifications` (conn only) | `pool.MetricsOf(cn)` |
| Pub/Sub received | `PubSub` receive loop | the owning client's `rec` on `PubSub` |
| Pub/Sub sent, stream lag | **moved** from the `cmdable` wrappers to `baseClient.recordCommand` | `c.rec` |
| MultiDB failover, active-db change, circuit, health check | MultiDB core and members | the MultiDB `rec` |

Pub/Sub "sent" and XREADGROUP stream lag were recorded from the command
wrappers, which cannot see the client (and counted a pipelined PUBLISH at queue
time, so a discarded pipeline still counted). They move to after the reply is
read (`recordCommandSamples`): `recordCommand` calls it on the single-command
path and the full-duplex reader, and the two pipeline read loops
(`pipelineReadCmdsWithFlags`, `pipelineReadCmdsInView`) call it for each
command whose reply was read without error, which covers Pipeline, TxPipeline,
cluster pipelines and the half-duplex autopipeliner. The client and the
serving connection are known there, so the samples also carry `server.address`.

## Testing

- **Precedence** (`redisotel-native`): one instrument recorded through a
  `NewRecorder` recorder whose key collides with a built-in and with an `Init`
  attribute; assert the client value wins, the `Init` value wins over the
  built-in, and non-colliding keys from all three layers are present, on a
  histogram, a counter and a MultiDB instrument.
- **Isolation**: two recorders with different attributes on the same provider
  produce two series per instrument and never share a cached attribute set.
- **Lifecycle**: `NewRecorder` before `Init` returns an error; a recorder
  records after `Init`, and after `Shutdown` records nothing but UpDown
  deltas; `cfg.Enabled` has no effect; a nil `cfg.MeterProvider` uses the
  `Init` provider; a recorder on another provider gets its own queue, which
  `Shutdown` stops; the `Init` attribute layer is a snapshot taken at `Init`.
- **UpDown balance** (`internal/pool`): a pool built with its own table keeps
  its `connection.count` across install and uninstall of the global table and
  returns to zero on `Close`; a pool following the global moves its count from
  the old recorder to the new one, as today. Run with `-race`.
- **Propagation** (root): reflection tests that each options conversion copies
  `OTelRecorder`; a cluster node created after construction carries the
  cluster's recorder; a MultiDB member inherits and may override.
- **Routing**: with two clients each configured with a counting fake
  `OTelRecorder`, commands, errors, Pub/Sub messages and pool events of one
  client never reach the other's recorder, and a third client without the field
  reaches only the global one.
- **Hot path**: the full-duplex benchmark with metrics enabled does not regress
  (a per-client recorder is one field read; the global path is unchanged).
- Existing suites: `go test -race ./internal/pool/ ./internal/otel/`,
  `extra/redisotel-native`, and the Ginkgo suite against the docker stack
  (`make test.ci`).

## Challenges and trade-offs

- **Pool-level metrics have no client in hand.** Solved by stamping the table
  on the pool and on each connection at creation, so the connection itself
  knows where to report. The alternative, looking the client up by
  `cn.PoolName()` in a registry, would have added a lookup on the hot path, a
  registration lifecycle users must get right around `Close`, and no path for
  conn-less events (cluster redirects, MultiDB, Pub/Sub sent).
- **UpDown counters across `Init`/`Shutdown`.** Per-client pools are bound to
  one recorder for life and excluded from the global recount, so their balance
  needs no mechanism at all; the global recount keeps today's shape.
- **Breadth of change.** Replacing the global getters touches roughly sixty
  call sites in `internal/pool`, root and `maintnotifications`, plus eight
  hand-synced option conversions. Deleting the globals turns every missed site
  into a compile error; reflection tests guard the conversions. The pool and
  `internal/otel` refactor is behaviour-neutral and can land as its own PR
  before the public field.
- **One recorder type for both levels.** Reusing `metricsRecorder` means a
  per-client `Config` is a full config. Two consequences are documented rather
  than engineered around: `cfg.Enabled` is ignored, and histogram settings that
  differ from the `Init` config on a shared provider hit the SDK's
  duplicate-instrument rule (first registration wins, with a warning).
- **`Init` first.** `NewRecorder` needs the instance's provider and attribute
  layer, so it errors before `Init`. This matches the order every program
  already follows and keeps the global layer a fixed snapshot: re-reading it at
  record time would let an UpDown `-1` land on a different series than its `+1`.
- **Hot path.** Custom attributes are baked into the cached attribute set once
  per key; a per-client recorder is a field read on the client, cheaper than
  today's global callback lookup.
- **Behavior change.** Pub/Sub "sent" and stream-lag samples are recorded
  after the reply is read, once per successful command on every execution
  path, and gain server attributes. This is the smallest change that gives
  every family per-client attribution.

## Alternatives considered

- **A separate per-client `redisotel.Recorder` type** layering attributes on
  the global recorder's instruments at record time: no ordering vs `Init` and
  no instruments of its own, but a second exported type with its own builder,
  a root hook to read the installed global recorder, a scoped attribute cache,
  and a recount bracket that has to run the recorder swap inside it. Rejected
  for the surface and machinery it adds over simply constructing the existing
  recorder once more.
- **Registry keyed by pool name inside redisotel** (`RegisterClient(rdb, attrs)`):
  root unchanged, but no third-party per-client recorder is possible, the
  string coupling is invisible from `redis.go`, and events with a nil connection
  have no key.
- **Attributes in root** (`Options.OTelAttributes map[string]string` exposed via
  `ConnInfo`): leaks a telemetry vocabulary into root types that know nothing
  about attributes, and still gives third parties no per-client path.
- **Post-construction instrumentation** (`redisotel.InstrumentMetrics(rdb, cfg)`,
  like the tracing sibling's hooks): requires swapping callbacks on live pools,
  loses events before attach, and races lazily created cluster nodes and MultiDB
  members. It also contradicts "pass the recorder via any client
  configuration".

## Open questions

- Should `NewRecorder` reject a `cfg` whose histogram settings differ from the
  `Init` config while the provider is shared, instead of relying on the SDK's
  warning?
- Should a recorder issued before `Shutdown` re-enable itself after a later
  `Init` on the same provider, or is "build new recorders after re-Init" the
  right rule?

## Rollout

1. `redisotel-native`: custom attributes applied through one helper on every
   instrument, including the cached operation-duration set.
2. `internal/pool`: per-pool table, single dispatch rule, exclusion of
   per-client pools from the global recount, balance test.
3. `internal/otel`: per-client recorder, `PoolCallbacks`, globals removed.
4. Root: the `OTelRecorder` field and conversions, resolution at construction,
   call sites routed through the client's recorder, `recordCommand`,
   propagation and routing tests.
5. `redisotel-native`: `NewRecorder` and its tests.
6. Documentation: `example/otel-metrics` README and example, `.claude/specs/pool.md`
   note on per-pool tables, `AGENTS.md` pointer.
