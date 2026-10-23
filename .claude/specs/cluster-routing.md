# Cluster routing

This is an architectural reference for how `ClusterClient` decides which node to send a command to and how it merges results from multi-shard fan-outs. Read this before changing `osscluster.go`, `osscluster_router.go`, `command_metadata.go`, `routing_metadata.go`, `command_policy_resolver.go`, or anything under `internal/routing/` and `internal/hashtag/`.

One immutable command-metadata view supplies key positions and read-only status. The configured resolver selects execution policies: the default preserves existing behavior, while `NewCommandMetadataResolver` or `NewDynamicResolver` explicitly enables metadata fan-out. One routing decision carries these choices through node selection, execution, and aggregation. Most bugs come from mixing metadata generations, extracting only some keys, picking the wrong policy, or aggregating responses incorrectly.

---

## Slot computation

Redis Cluster has 16384 slots. A key's slot is `CRC16(hashtag(key)) % 16384` where `hashtag(key)` returns the substring inside the first balanced `{...}` if present, otherwise the whole key (`internal/hashtag/hashtag.go`). A proven empty key hashes deterministically to slot 0. Keyless commands use the distinct slot `-1` sentinel and select a node through the keyless routing path; do not treat an actual empty key as keyless.

Two non-obvious edge cases:

- **Wire-faithful keys** (#3049): generic arguments can be strings or byte slices. Routing uses `routingArgText`, which reproduces the argument text Redis receives and rejects unsupported argument types instead of guessing. If you add a generic-args path, use the same conversion.
- **Commands whose key is *not* the first arg, or which take the slot directly** — key positions normally come from the resolved `COMMAND` key specs. `COUNTKEYSINSLOT` is the canonical exception: its argument is the slot integer, not a key (#3327), so the slot path handles it explicitly. Do not describe a literal slot as a key just to make generic extraction accept it.

Cluster keylessness is proved by the resolved metadata record, not by `keylessCommands`. A proven keyless command gets slot `-1`, which tells the router to use the configured `ShardPicker` (round-robin / random / static). A missing record preserves raw module and proxy commands through the legacy first-key fallback, including an explicit `SetFirstKeyPos` hint. Hinted keys must still have a reproducible wire encoding; unsupported keys return a routing error rather than selecting a random node or inheriting a transaction slot. A tombstoned or malformed known record still returns a routing error before dispatch. `DisableRoutingPolicies` and Ring retain their independent legacy routing paths.

For ordinary single-slot routing, the router derives the first key from a resolved record without building a full key slice. Ordinary keyed AutoPipeline and pipeline commands can share an immutable decision owned by the captured metadata view, with the invocation-specific slot held on the command. Sharing requires an exact resolved-decision match and never bypasses resolver callbacks, validation, or retry mapping. Resolved key specifications (snapshot, live, or overrides) take precedence over `SetFirstKeyPos`; that hint applies when metadata is unavailable. One usable key spec remains sufficient when a sibling spec is incomplete or unfamiliar because Redis Cluster requires all real keys to share a slot. Limited range specs can likewise prove their first key (`XREAD` uses `limit=2`, so keys occupy the first half after `STREAMS`). `ReqMultiShard` is stricter: `routingResolveKeyPlan` must enumerate every key and the complete key-associated argument groups. Range and `numkeys` layouts are supported, including keyword-based starts. Per-shard commands preserve the common prefix and suffix and rewrite the `numkeys` argument where needed. Unconditional cross-slot `MSETEX` is split this way; conditional `NX`/`XX` forms fail with `ErrCrossSlot` before dispatch because independent shard decisions could apply only part of the write. Multiple, incomplete, unsupported, malformed, or non-wire-faithful key layouts never authorize fan-out with a partial key list.

---

## Shared command metadata

`commandMetadataView` is the single immutable source used to derive Cluster routing and CSC eligibility. It contains normalized `CommandInfo` records, parent/subcommand relationships, deliberately shadowed container parents, separately derived routing and CSC tables, a live/static source bit, and a unique generation. The public `CommandInfo.CommandPolicy` field remains populated for compatibility, but Cluster routing derives its internal policy directly from the record's flags, tips, and key specs.

Records resolve in this order:

1. Application override (`CommandMetadataConfig.Overrides`).
2. Built-in correction.
3. Live `COMMAND` record.
4. Generated `command_info_snapshot.go` record.

Overrides are normalized case-insensitively; subcommands use Redis's `parent|child` names. A non-nil override replaces lower layers, while a nil override marks the command unknown. Built-in corrections produce a complete shared record for narrow, verified server-metadata gaps: most add a negative eligibility tip, while a correction may also remove a false flag or replace incorrect key positions. A named malformed live record is a nil tombstone: it blocks both snapshot fallback and built-in correction, so corrupt metadata cannot resurrect a command through a lower layer. Bare container records are shadowed when resolved child records exist, preventing a parent such as `xinfo` from hiding `xinfo|stream`.

The `COMMAND` parser treats records independently. Unknown, well-typed flags, tips, map fields, and trailing record fields are accepted and drained; an extended record retains its validated known prefix. Explicit null elements in flag, ACL, tip, and key-spec flag collections are skipped as absent tokens; a null collection or a wrong-typed non-null element remains malformed. Recoverable malformed fields, numeric overflow, or unsupported key-spec structures make only the named record unusable, preserving valid parents and sibling subcommands. An entry whose name cannot be decoded is omitted because it cannot safely shadow anything. Framing errors, truncation, failed draining, or excessive nesting fail the whole refresh because reply alignment is no longer trustworthy.

View construction derives two consumer-specific tables from the same resolved records:

- Cluster routing derives keyless/keyed status, exact key specs, the `readonly` flag, and request/response policies. Six- and seven-field legacy records inherit only the snapshot's routing tips, retaining live flags and key positions. Modern records supply their own tips, including an explicitly empty set. An unmatched keyword key spec remains unresolved, except for the known keyless `JSON.DEBUG HELP` invocation; `JSON.DEBUG MEMORY` still requires a key.
- CSC applies its stricter eligibility rules to the same records, with no CSC-only provenance clamp after resolution. Only an identified Redis server at version 8.10 or newer supplies the complete live exclusion signals. Older Redis versions, other server implementations, and unidentified servers retain the compatibility corrections: known gaps add missing `script_runner` or `dont_cache` signals, and live-only records receive `dont_cache`. An identified Redis 8.10-or-newer record with an inconsistent legacy-only key shape also receives `dont_cache`. Corrections are resolved into the shared record, so CSC and routing consume the same normalized metadata.

The default `CommandMetadataStatic` mode uses the checked-in snapshot plus overrides and performs no metadata network request or background refresh unless resolution reaches an explicitly installed dynamic resolver. `CommandMetadataPreferLive` starts from that static view and atomically upgrades after a successful live fetch; an optional jittered `RefreshInterval` covers later server or module changes. A failed fetch leaves routing on the current safe view. Metadata configuration alone does not opt into new execution policies. Installing `NewDynamicResolver` explicitly enables them and performs a synchronous live attempt only if resolution reaches that resolver, retaining the current view on failure. Activation also enables reconnect/topology identity verification and the configured refresh interval in Static mode. With no interval, in-place module changes need a reconnect or topology observation. Each metadata probe has a bounded internal context, and callers waiting for another refresh can cancel independently.

Commands missing from the snapshot retain legacy key-position fallback, not an implicit `COMMAND` request. Configure `CommandMetadataPreferLive` or `CommandMetadataConfig.Overrides` when such commands need authoritative key positions or read-only flags. Static mode cannot infer those flags for an unknown command.

A live refresh compares the fetched records, legacy provenance, server name and version, and fingerprint with the last successfully published input. Identical input reuses the current live view; changed input or a retired view triggers a full rebuild. Both paths validate the server identity and epoch under the publication lock. The store retains one immutable raw fetch result for this comparison, without changing the command hot path.

`SLOWLOG GET` merges typed and generic shard replies by descending timestamp, then applies the requested count globally (10 by default, -1 for all). Equal timestamps preserve per-node order; IDs are node-local and cannot order entries across nodes.

Each command captures one view and carries one `clusterRoutingDecision`, including its natural slot, through slot selection, replica selection, fan-out, and aggregation. A regular user pipeline resolves custom policies once, prepares live metadata only if a command reaches a dynamic metadata resolver, captures one view for the batch, and reuses it for mapping and every retry. AutoPipeline pins a decision per admitted command because one merged batch can contain commands admitted on opposite sides of a refresh. The decision pointer is owned by the command, consumed with take-and-clear, and cleared on completion or cancellation; there is no global map keyed by `Cmder`, so custom non-comparable commands are supported.

---

## MOVED and ASK redirection

`MOVED <slot> <addr>` means "this slot lives somewhere else now, permanently" — the client follows the redirect and schedules a topology reload. `ASK <slot> <addr>` means "this slot is *currently* migrating; just for this command, send `ASKING` then retry against `<addr>`". The client also schedules a lazy topology refresh after `ASK`, but the redirected retry remains a one-shot `ASKING` operation. Detection is via typed errors (`error.go`, `IsMovedError`/`IsAskError`) which return the address as well as a bool.

Two design decisions worth knowing:

- **No backoff after MOVED/ASK** (#3048). Redirection is informational; the new node is ready to serve. Adding exponential backoff between the redirect and the retry just adds latency for no benefit. If you're tempted to add a delay, you're probably solving a different problem (e.g. a thundering herd during failover) and should solve it elsewhere.
- **`MASTERDOWN` is retriable** (#3164). It means a primary stepped down and a replica is being promoted; the right behaviour is to wait briefly and retry (with the existing topology), not to fail. `dial tcp` errors are now also retriable as redirects (#3786) so we don't lose connectivity during DNS flips.

When `MOVED` points at the same address we just used, that's a stale DNS cache, not a real redirect. The pool/dialer treats this as a bad connection and removes it (#3219 made `clusterNodes.Addrs()` return a copy so this race no longer corrupts the address list under the GC).

---

## Command policies (#3422)

`internal/routing/policy.go` defines the policy taxonomy. A command has a `RequestPolicy` (where it goes) and a `ResponsePolicy` (how multiple responses are combined).

### Request policies

| Policy | Meaning |
|---|---|
| `ReqDefault` | Single shard. Slot from first key (or the configured ShardPicker for keyless commands). |
| `ReqAllNodes` | Broadcast to all masters **and** replicas. (`DEBUG OBJECT`, etc.) |
| `ReqAllShards` | Broadcast to all masters only. (`DBSIZE`, `KEYS`.) |
| `ReqMultiShard` | Group keys by slot, fan out per slot. (`MGET`, `MSET`, `DEL` with cross-shard keys.) |
| `ReqSpecial` | Command-specific handler; `FT.CURSOR` retains native cursor IDs and its existing routing. |

### Response policies

| Policy | Meaning |
|---|---|
| `RespDefaultKeyless` | For fan-out, collect successful target replies into a slice and fail if a target errored. |
| `RespDefaultHashSlot` | Reassemble in original key order (for `MGET` etc.). |
| `RespAllSucceeded` | Fail the whole call if any shard errored. |
| `RespOneSucceeded` | Succeed if any shard succeeded. |
| `RespAggSum` / `RespAggMin` / `RespAggMax` | Numeric aggregates. |
| `RespAggLogicalAnd` / `RespAggLogicalOr` | Boolean aggregates. |
| `RespSpecial` | Command-specific aggregation that requires an explicitly supported handler. |

Explicit pipelines and transactions retain metadata key selection but suppress automatically derived execution policies. Each command executes on its selected node; commands such as `PING`, `DBSIZE`, and `SCRIPT LOAD` are not rejected merely because their metadata describes fan-out. Explicit custom policies retain their existing batch restrictions. AutoPipeline validates routing once before dispatch, including commands independently requiring diversion. Ordered AsyncAutoPipeline rejects actual fan-out and explicit custom multi-shard policies requiring direct execution; same-slot metadata commands remain batchable. The blocking face already waits per call, and unordered half-duplex mode explicitly permits diversion. Existing direct helpers (`DBSize`, script-management methods, and `Do`) and supported special routing retain their separate execution path: await earlier async results before a dependent direct call, then await that call before later dependent commands. An explicit pipeline preserves order for commands on one node, without metadata fan-out.

`command_policy_resolver.go` preserves the public resolver and `SetFallbackResolver` APIs. The configured resolver chain is walked in order and the first non-nil policy wins. Custom resolver functions run once. Metadata-backed resolvers derive policy from the captured view, and a dynamic metadata fallback performs its live attempt only if earlier resolvers delegate to it; a successful custom resolver therefore causes no fallback network request. Key selection still uses the immutable view paired with the selected policy, so policy and keys cannot come from different refresh generations.

`NewDefaultCommandPolicyResolver` retains the existing Search policy table. Core commands use metadata for keys and read-only status without automatically adopting new fan-out policies. The typed `DBSize`, `ScriptLoad`, `ScriptFlush`, and `ScriptExists` helpers retain their existing cluster-wide behavior with the default resolver. Install `c.SetCommandInfoResolver(redis.NewCommandMetadataResolver())` to opt into full policies from the client's shared metadata view, or install `c.NewDynamicResolver()` for an explicit live attempt. A standalone `NewCommandMetadataResolver()` uses the shipped snapshot. `DisableRoutingPolicies` bypasses policy resolution and preserves the legacy slot/keyless path, fan-out helpers, and transaction behavior; configured metadata does not trigger a background fetch in this mode.

`ReqSpecial` and `RespSpecial` require matching, tested handlers when selected for execution. The existing `FT.CURSOR READ`/`DEL` handler routes by native cursor ID and honors redirects. This does not establish affinity with the node that created the cursor; cursor ownership, batch support, and reply parsing are deferred to a separate change. READ is non-retryable after ambiguous transport failure because it advances cursor state. The `RANDOMKEY` response handler ignores empty shards and selects one successful shard contribution. An explicitly selected unsupported special policy returns a routing error before dispatch; `RespSpecial` never falls through to "first successful response." Resolver fallbacks retain their ordering, so a successful explicit custom fallback can still replace an unsupported metadata policy.

Known keyless `SCAN`, `BLESS SCAN`, `INFO`, and diagnostic forms of `MEMORY`, `LATENCY`, `FUNCTION STATS`, and `HOTKEYS` retain node-local policies. Replies and cursors pass through unchanged; scanning or inspecting the whole cluster requires `ForEachMaster`. This interpretation changes only the derived routing policy, not shared records or CSC eligibility. It does not authorize arbitrary special policies, changed key layouts, or special aggregation for an explicit fan-out override.

---

## Aggregators (`internal/routing/aggregator.go`)

The aggregator is the runtime side of `ResponsePolicy`. Two families:

- **Keyless aggregators** — collect responses without per-key bookkeeping. `DefaultKeylessAggregator`, `AllSucceededAggregator`, `AggSumAggregator`, etc.
- **Keyed aggregators** — preserve key order. `DefaultKeyedAggregator` is the one you want for collection replies such as `MGET`: each shard returns results for its subset of keys, and the aggregator stitches them back in the user's original order. Scalar commands such as `MSET` and `DEL` instead use their declared all-succeeded and sum policies.

Sum/min/max preserve integer-only replies as `int64` without conversion through `float64`; integer overflow returns an error. This result type is visible to raw opted-in `Do` calls. If any shard supplies a floating-point reply, the aggregate is `float64`.

When you add a new fan-out command, the typical workflow is:

1. Pick the right `RequestPolicy` (almost always `ReqMultiShard` for multi-key, `ReqAllShards` for global state).
2. Pick the right `ResponsePolicy`. If none fit, implement and explicitly register a command-specific special handler; metadata alone is not enough.
3. For `ReqMultiShard`, prove that the key spec produces one complete, splittable key plan and that command-specific prefixes, grouped arguments, suffixes, and `numkeys` values survive the split.
4. Test it with **at least 3 shards** and **keys distributed across all of them**, including the empty case (no keys land on a particular shard).

The `add aggregator case for new command` style commit shows up regularly because step 2 is easy to forget. Pair it with `add-command` in your mental checklist; the `add-command` skill points at this doc for a reason.

---

## Shard picker

For keyless commands and selection among replicas, `ShardPicker` (`internal/routing/shard_picker.go`) selects a node. Three implementations: round-robin (default), random, static. Keyed writes still use the slot's master; read routing can use the picker among that slot's replicas.

The dispatch function `cmdNodeWithShardPicker` uses slot `-1` as the sentinel for "use the picker." Don't pass `-1` accidentally for a real command — that's a routing bug waiting to happen.

---

## Replica routing

For keyed reads, `ReadOnly`, `RouteRandomly`, and `RouteByLatency` select eligible nodes:

- `ReadOnly = false` (default): always go to master.
- `ReadOnly = true`, no other knob: select a healthy replica, falling back to the master when no replica is available.
- `RouteRandomly = true`: pick a random healthy master or replica for read-only commands; this enables `ReadOnly`.
- `RouteByLatency = true`: pick the lowest-latency healthy master or replica; this enables `ReadOnly`. `RouteByLatencyTolerance` distributes reads among nodes within the configured tolerance.

Default direct keyless commands still select masters. Default subcommand read routing retains the parent command's readonly flag; explicit metadata policies use the resolved child flag (for example, `MEMORY USAGE`). Metadata policies also permit eligible keyless reads on replicas. Explicit readonly pipelines retain their existing master/replica selection. `ReplicaOnly` is a Sentinel failover option, not a `ClusterOptions` field.

Two history points:

- **Latency throttling** (#2795): `RouteByLatency` would otherwise ping every replica before every command. Latencies are now measured at most once per `minLatencyMeasurementInterval` (10s, defined in `osscluster.go`).
- **`slotClosestNode` fail-safe** (#3043): if all replicas are unhealthy, fall back to the lowest-latency *failing* replica rather than a random one; if all pings have timed out, fall back to a random replica. This was a real bug that produced "no nodes available" errors during partial outages.
- **PubSub on replicas** (#3480): `ClusterClient.Subscribe` can target replica nodes, with the connection running in `READONLY` mode.
- **`ReplicaOnly` + `NewFailoverClusterClient`** (#3482): the option used to be silently dropped on failover-cluster construction. Watch for this when extending failover.

---

## Topology reload

`clusterState` carries online `Masters`/`Slaves`, every node declared by `CLUSTER SHARDS`, node health, and slot→node mappings. It is stored under `clusterStateHolder` (an `atomic.Value`) so reads are lock-free. Reload triggers include:

- `MOVED` errors (canonical case).
- A `CLUSTER SHARDS` owner reported as `fail` or `loading` when routing needs it.
- Periodic refresh.
- `SMIGRATED` notifications from the maintenance-notifications subsystem (it can target a partial reload by passing the affected slot ranges).

Foreground retries can await a topology reload before their next attempt. Concurrent foreground and background calls share one in-flight load. The caller starting that load owns its context; other callers can cancel their own wait without canceling the shared load. If the initiating caller cancels its load, live waiters share a new attempt instead of inheriting that cancellation. This coalesces retry storms without replacing an awaited refresh with a background-only request.

`loadState` tries known online addresses first, retaining other seeds as fallbacks. It prefers `CLUSTER SHARDS` and preserves zero-slot shards as fan-out targets. A null or empty node endpoint reuses the host of the connection that returned the reply; `?` is unknown and never aliases that origin. Unsupported or denied `SHARDS`, and unusable replies including missing or unknown health values, fall back to `CLUSTER SLOTS`. Connection failures move to another node without trying `SLOTS` over the same failed connection. A configured `ClusterSlots` callback remains authoritative. If all addresses fail, `activeAddrs` is cleared so DNS gets re-resolved on the next attempt.

Slot membership retains declared replicas across health changes so health-only reloads preserve metadata identity. Only `online` nodes are ordinary routing candidates; keyless and unknown-slot fallbacks prefer nodes without temporary failing or loading markers, but can use a topology-online node when every candidate carries such a marker. `ForEachSlave` and `ForEachShard` enumerate online nodes. `ForEachMaster` includes all declared masters so legacy cluster-wide helpers cannot silently return a partial aggregate. `PoolStats` includes every declared node, including unhealthy ones. Fan-out requires every current master to be online and rejects an unhealthy master before dispatch, reloading within its retry budget. `all_nodes` excludes unavailable replicas. Once dispatch starts, target errors cannot replay successful siblings. `SLOTS` and custom topologies use their complete supplied node sets without assuming unavailable health information. Stale node generations are collected one minute after a reload.

The `ClusterClient` parent owns exactly one command-metadata store; per-node clients do not start metadata workers. Successful node connection handshakes report their `HELLO` identity to that parent, so reconnecting after a server upgrade or module change refreshes metadata even when addresses and topology stay unchanged and periodic refresh is disabled. When `HELLO` is rejected, reconnects schedule background identity verification through the existing `INFO server` / `MODULE LIST` fallback, including when a live view already exists. Internal metadata fetch connections suppress these notifications to avoid recursive refreshes. A materially changed routing topology or newly online membership invalidates a live view before topology publication and schedules a replacement in `PreferLive` mode or after a dynamic resolver has requested live metadata; losing an online member or changing input order alone does not. The fetch captures the topology generation, selects a node from that generation, and sends `HELLO` and `COMMAND` through one pipeline on one physical connection. If the server rejects `HELLO` but accepts `COMMAND`, the fetch retries with `INFO server`, `MODULE LIST`, and `COMMAND` in one pipeline; version and module identity are required, and the original connection's records are not mixed with the fallback connection's identity. The internal metadata commands bypass application process/pipeline hooks and CSC, while normal dialing, initialization, authentication, `OnConnect`, and reauthentication still run.

Before publishing a first live view (including after topology invalidation), the fetch verifies the version/module identity of every online sibling, including replicas. It fetches the full command table from only the selected node; sibling checks reuse the identity fetch and legacy fallback under the same bounded refresh context. A different or unverifiable sibling identity leaves the static view in place. Newly online nodes retire the live view before topology publication so recovery cannot expose an unverified server. Losing an online node does not by itself retire an established live view. Later topology reloads retry verification while the view remains static. Dynamic resolution waits for at most the first completed verification attempt, including one already owned by the background worker. A failed or canceled owner hands recovery to the worker; subsequent commands immediately use the conservative view. Invalidation does not reopen foreground waiting. Background cooldown survives the automatic retry cap and new command-triggered requests, allowing recovery without a request storm.

The parsed `HELLO` server name and version are kept separate from the server-identity fingerprint. CSC treats live exclusion metadata as authoritative only when the server name is `redis` and its version is at least 8.10; a compatible version string alone is insufficient. The fingerprint and topology generation prevent metadata fetched across an upgrade, heterogeneous-node transition, or topology reload from being published for the wrong server. A node handshake with a different identity schedules verification even when a live view exists; it does not replace the shared identity. If one node has a different identity during a rolling change, the fetch tries sibling nodes before retiring the current live view. The fetch rechecks topology after reading the reply, publication rechecks the store identity, and a concurrent topology reload clears that identity and retires any old view. Routing uses the static view while a replacement is fetched.

The `IsClusterMode` option (#3255) flips routing behaviour for ElastiCache cluster mode, which emulates `CLUSTER SLOTS` but with cluster-mode semantics layered on a different topology. The flag exists because we can't always detect cluster mode reliably from the server response alone.

---

## Cross-slot, transactions, and pipelines

`ClusterClient` enforces these:

- **Fan-out arguments**: raw/streaming commands, unsupported argument types, and non-repeatable binary marshalers are rejected before broadcast or multi-slot dispatch. Numeric key counts must retain Redis-compatible syntax before rewriting, and key-group bounds are checked without overflowing. A multi-shard policy whose complete keys share one slot sends the original command once and accepts these values. For fan-out, callers can marshal values into bytes before submitting the command; a shared stateful marshaler need not support concurrent calls.

- **`Watch`/`MULTI`/`EXEC` (TxPipeline)**: all keys must hash to the same slot. A complete key plan detects cross-slot transactions before dispatch. Incomplete metadata with a proven first key (for example, `SORT`) routes by that key and leaves remaining key validation to Redis, preserving existing valid transactions. Metadata-derived fan-out is suppressed: keyless commands run on the transaction's selected node, without a PING-only exception table. Explicit custom policies retain their transaction restrictions because one transaction cannot span connections.
- **Stream transactions**: the recognized `XREAD`/`XREADGROUP` layout parses options to locate the actual `STREAMS` delimiter, then checks every key in the first half of the remaining arguments. This supports same-slot transactions despite the incomplete metadata flag; it does not authorize splitting keys and IDs across shards.
- **`Pipeline`**: commands may target different slots and are grouped by node for parallel dispatch. Metadata supplies keys and read-only status, but does not split or broadcast an individual command in this explicit batch. A cross-slot `MGET`, for example, reaches one node and receives the server's `CROSSSLOT` error rather than preventing an unrelated `GET` from running. Explicit custom fan-out/special policies retain their existing admission restrictions. Pipelines are not atomic.
- **AutoPipeline**: preserves direct-command routing. Actual fan-out requires the blocking face or unordered half-duplex mode; ordered async admission rejects it before dispatch. Explicit custom multi-shard policies requiring direct execution have the same restriction. Ordinary commands and same-slot metadata multi-key commands remain eligible for the node batch. Inherited direct helpers and special routing retain their ordering caveats. Admission decisions stay pinned through flush and retries.
- **`Eval`/`EvalSha`**: routed by the **first key in `KEYS`**. Scripts that read multiple keys must declare them all in `KEYS` and all of them must hash to the same slot. If your script touches a key it didn't declare, you'll get cluster-routing errors at runtime.
- **`FT.CURSOR READ`/`DEL`**: native cursor IDs retain their existing special routing. Pipelines allow the registered cursor commands and execute them on the selected node, without guaranteeing cursor affinity. Unsupported explicit special policies remain rejected, and transaction admission is unchanged. Use the originating node client when server-local cursor ownership is required. READ is not retried after an ambiguous transport failure.

---

## History worth knowing

Recurring shape of cluster bugs:

- **Wrong slot computation**: byte-slice keys (#3049), commands with non-key first arg (`COUNTKEYSINSLOT` #3327), and historically missing keyless markers (`WAIT` #3615, `TIME` #3722). Those hand-maintained metadata gaps are why Cluster routing now derives keyedness and key positions from the shared record view.
- **Race in node accounting**: `clusterNodes.Addrs()` returning a shared slice (#3219).
- **Replica routing fallbacks**: `slotClosestNode` not handling all-failing case (#3043).
- **Treating recoverable failures as fatal**: `MASTERDOWN` (#3164), `dial tcp` errors during failover (#3786).
- **Unnecessary delays on recoverable redirects**: backoff after MOVED/ASK (#3048).
- **Failover variants forgetting options**: `ReplicaOnly` (#3482), `MaintNotificationsConfig` (#3600).

When you add a feature or fix a bug here, add a test that:

1. Spins a 6-node cluster (the test scaffolding in `main_test.go` already provides `clusterScenario` with 16600–16605).
2. Uses keys that you've verified land on different shards (use `CLUSTER KEYSLOT` if unsure).
3. Exercises both the happy path and the redirect path.

Metadata changes also need unit coverage for malformed-record isolation and fail-closed extraction, plus Cluster coverage showing that policy, key selection, pinned routing decisions, replicas, fan-out, aggregation, pipelines, and retries do not mix generations. Live-refresh tests must cover server-identity and topology changes; multi-shard tests must exercise range and `numkeys` layouts, byte-slice keys, grouped key arguments, and preserved suffixes.

The cluster integration tests are slow but catch real protocol issues — the unit-only tests don't.
