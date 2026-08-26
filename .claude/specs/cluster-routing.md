# Cluster routing

This is an architectural reference for how `ClusterClient` decides which node to send a command to and how it merges results from multi-shard fan-outs. Read this before changing `osscluster.go`, `osscluster_router.go`, `command_metadata.go`, `routing_metadata.go`, `command_policy_resolver.go`, or anything under `internal/routing/` and `internal/hashtag/`.

The mental model: one immutable command-metadata view produces a routing decision containing the key plan, slot, read-only status, request policy, and response policy. That decision executes against one or more nodes, and fan-out responses are collapsed by the selected aggregator. Most bugs come from mixing metadata generations, extracting only some keys, picking the wrong policy, or aggregating responses incorrectly.

---

## Slot computation

Redis Cluster has 16384 slots. A key's slot is `CRC16(hashtag(key)) % 16384` where `hashtag(key)` returns the substring inside the first balanced `{...}` if present, otherwise the whole key (`internal/hashtag/hashtag.go`). A proven empty key hashes deterministically to slot 0. Keyless commands use the distinct slot `-1` sentinel and select a node through the keyless routing path; do not treat an actual empty key as keyless.

Two non-obvious edge cases:

- **Wire-faithful keys** (#3049): generic arguments can be strings or byte slices. Routing uses `routingArgText`, which reproduces the argument text Redis receives and rejects unsupported argument types instead of guessing. If you add a generic-args path, use the same conversion.
- **Commands whose key is *not* the first arg, or which take the slot directly** — key positions normally come from the resolved `COMMAND` key specs. `COUNTKEYSINSLOT` is the canonical exception: its argument is the slot integer, not a key (#3327), so the slot path handles it explicitly. Do not describe a literal slot as a key just to make generic extraction accept it.

Cluster keylessness is proved by the resolved metadata record, not by `keylessCommands`. A proven keyless command gets slot `-1`, which tells the router to use the configured `ShardPicker` (round-robin / random / static). A missing record preserves raw module and proxy commands through the legacy first-key fallback, including an explicit `SetFirstKeyPos` hint. A tombstoned or malformed known record still returns a routing error before dispatch. `DisableRoutingPolicies` and Ring retain their independent legacy routing paths.

For ordinary single-slot routing, the router derives the first key from a resolved record without building a full key slice; a constructor hint never overrides server metadata. One usable key spec remains sufficient when a sibling spec is incomplete or unfamiliar because Redis Cluster requires all real keys to share a slot. Limited range specs can likewise prove their first key (`XREAD` uses `limit=2`, so keys occupy the first half after `STREAMS`). `ReqMultiShard` is stricter: `routingResolveKeyPlan` must enumerate every key and the complete key-associated argument groups. Range and `numkeys` layouts are supported, including keyword-based starts. Per-shard commands preserve the common prefix and suffix and rewrite the `numkeys` argument where needed. Unconditional cross-slot `MSETEX` is split this way; conditional `NX`/`XX` forms fail with `ErrCrossSlot` before dispatch because independent shard decisions could apply only part of the write. Multiple, incomplete, unsupported, malformed, or non-wire-faithful key layouts never authorize fan-out with a partial key list.

---

## Shared command metadata

`commandMetadataView` is the single immutable source used to derive Cluster routing and CSC eligibility. It contains normalized `CommandInfo` records, parent/subcommand relationships, deliberately shadowed container parents, separately derived routing and CSC tables, a live/static source bit, and a unique generation. The public `CommandInfo.CommandPolicy` field remains populated for compatibility, but Cluster routing derives its internal policy directly from the record's flags, tips, and key specs.

Records resolve in this order:

1. Application override (`CommandMetadataConfig.Overrides`).
2. Built-in correction.
3. Live `COMMAND` record.
4. Generated `command_info_snapshot.go` record.

Overrides are normalized case-insensitively; subcommands use Redis's `parent|child` names. A non-nil override replaces lower layers, while a nil override marks the command unknown. Built-in corrections produce a complete shared record for narrow, verified server-metadata gaps: most add a negative eligibility tip, while a correction may also remove a false flag or replace incorrect key positions. A named malformed live record is a nil tombstone: it blocks both snapshot fallback and built-in correction, so corrupt metadata cannot resurrect a command through a lower layer. Bare container records are shadowed when resolved child records exist, preventing a parent such as `xinfo` from hiding `xinfo|stream`.

The `COMMAND` parser treats records independently. Unknown, well-typed flags, tips, and map fields are accepted and drained. Recoverable malformed fields, numeric overflow, or unsupported key-spec structures make only the named record unusable, preserving valid parents and sibling subcommands. An entry whose name cannot be decoded is omitted because it cannot safely shadow anything. Framing errors, truncation, failed draining, or excessive nesting fail the whole refresh because reply alignment is no longer trustworthy.

View construction derives two consumer-specific tables from the same resolved records:

- Cluster routing derives keyless/keyed status, exact key specs, the `readonly` flag, and request/response policies. Valid metadata from older Redis versions can be used for routing.
- CSC applies its stricter eligibility rules to the same records, with no CSC-only provenance clamp after resolution. Compatibility gaps are represented as fields on the one shared effective record: for pre-8.10 metadata, known gaps add only the missing `script_runner` flag or `dont_cache` tip; live-only records from those older servers receive `dont_cache`; and a Redis 8.10-or-newer record with an inconsistent legacy-only key shape receives `dont_cache`. Command-specific corrections are resolved into that same record, so CSC and routing always consume identical normalized metadata even when their algorithms use different fields.

The default `CommandMetadataStatic` mode uses the checked-in snapshot plus overrides and performs no metadata network request or background refresh. `CommandMetadataPreferLive` starts from that static view and atomically upgrades after a successful live fetch; an optional jittered `RefreshInterval` covers later server or module changes. A failed fetch leaves routing on the current safe view. `NewDynamicResolver` performs a synchronous live attempt only if resolution reaches that resolver, then uses the current static view on failure so the application command is not interrupted.

Each command captures one view and carries one `clusterRoutingDecision`, including its natural slot, through slot selection, replica selection, fan-out, and aggregation. A regular user pipeline resolves custom policies once, prepares live metadata only if a command reaches a dynamic metadata resolver, captures one view for the batch, and reuses it for mapping and every retry. AutoPipeline instead pins a decision per admitted command because one merged batch can contain commands admitted on opposite sides of a refresh; flush and retry consume those admission-time decisions rather than recomputing their slots.

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
| `ReqAllShards` | Broadcast to all masters only. (`DBSIZE`, `KEYS`, `SCAN` orchestration.) |
| `ReqMultiShard` | Group keys by slot, fan out per slot. (`MGET`, `MSET`, `DEL` with cross-shard keys.) |
| `ReqSpecial` | Custom resolver; e.g. `FT.CURSOR` is sticky-routed by cursor ID. |

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

`ReqAllNodes`, `ReqAllShards`, and `ReqSpecial` cannot sit inside a regular pipeline because they need routing outside one node batch. A metadata-derived `ReqMultiShard` invocation is admitted only when its complete key plan proves that every key hashes to the same slot and that slot agrees with the command's actual dispatch slot; cross-slot calls and custom multi-shard policies are rejected before dispatch.

`command_policy_resolver.go` preserves the public resolver and `SetFallbackResolver` APIs. The configured resolver chain is walked in order and the first non-nil policy wins. Custom resolver functions run once. Metadata-backed resolvers derive policy from the captured view, and a dynamic metadata fallback performs its live attempt only if earlier resolvers delegate to it; a successful custom resolver therefore causes no fallback network request. Key selection still uses the immutable view paired with the selected policy, so policy and keys cannot come from different refresh generations.

The default resolver derives all ordinary policies from the checked-in metadata snapshot. `DisableRoutingPolicies` bypasses metadata resolution and preserves the legacy slot/keyless path, fan-out helpers, and transaction behavior; configured metadata does not trigger a background fetch in this mode.

`ReqSpecial` and `RespSpecial` are capability declarations, not permission to use a generic fallback. A command must have a matching, tested handler in the internal special-policy registry. The checked-in request handler for `FT.CURSOR READ`/`DEL` routes by cursor ID, and the checked-in `RANDOMKEY` response handler ignores empty shards and selects one successful shard contribution. A live-only or checked-in special policy without an implemented handler returns an explicit routing error before dispatch; in particular, `RespSpecial` must never fall through to "first successful response."

---

## Aggregators (`internal/routing/aggregator.go`)

The aggregator is the runtime side of `ResponsePolicy`. Two families:

- **Keyless aggregators** — collect responses without per-key bookkeeping. `DefaultKeylessAggregator`, `AllSucceededAggregator`, `AggSumAggregator`, etc.
- **Keyed aggregators** — preserve key order. `DefaultKeyedAggregator` is the one you want for collection replies such as `MGET`: each shard returns results for its subset of keys, and the aggregator stitches them back in the user's original order. Scalar commands such as `MSET` and `DEL` instead use their declared all-succeeded and sum policies.

When you add a new fan-out command, the typical workflow is:

1. Pick the right `RequestPolicy` (almost always `ReqMultiShard` for multi-key, `ReqAllShards` for global state).
2. Pick the right `ResponsePolicy`. If none fit, implement and explicitly register a command-specific special handler; metadata alone is not enough.
3. For `ReqMultiShard`, prove that the key spec produces one complete, splittable key plan and that command-specific prefixes, grouped arguments, suffixes, and `numkeys` values survive the split.
4. Test it with **at least 3 shards** and **keys distributed across all of them**, including the empty case (no keys land on a particular shard).

The `add aggregator case for new command` style commit shows up regularly because step 2 is easy to forget. Pair it with `add-command` in your mental checklist; the `add-command` skill points at this doc for a reason.

---

## Shard picker

For keyless commands and tie-breaking among replicas, `ShardPicker` (`internal/routing/shard_picker.go`) selects a node. Three implementations: round-robin (default), random, static. The picker is configured on `ClusterOptions.ShardPicker` and does *not* affect commands that have a slot.

The dispatch function `cmdNodeWithShardPicker` uses slot `-1` as the sentinel for "use the picker." Don't pass `-1` accidentally for a real command — that's a routing bug waiting to happen.

---

## Replica routing

`ReadOnly`, `RouteRandomly`, `RouteByLatency`, `ReplicaOnly` are the four read-routing knobs. Their interaction is:

- `ReadOnly = false` (default): always go to master.
- `ReadOnly = true`, no other knob: go to master, but allow replicas as fallback. Reads never go to replicas in this mode unless the master is down.
- `RouteRandomly = true`: pick a random non-failing replica for read-only commands.
- `RouteByLatency = true`: pick the lowest-latency healthy replica.
- `ReplicaOnly = true`: only ever talk to replicas.

Two history points:

- **Latency throttling** (#2795): `RouteByLatency` would otherwise ping every replica before every command. Latencies are now measured at most once per `minLatencyMeasurementInterval` (10s, defined in `osscluster.go`).
- **`slotClosestNode` fail-safe** (#3043): if all replicas are unhealthy, fall back to the lowest-latency *failing* replica rather than a random one; if all pings have timed out, fall back to a random replica. This was a real bug that produced "no nodes available" errors during partial outages.
- **PubSub on replicas** (#3480): `ClusterClient.Subscribe` can target replica nodes, with the connection running in `READONLY` mode.
- **`ReplicaOnly` + `NewFailoverClusterClient`** (#3482): the option used to be silently dropped on failover-cluster construction. Watch for this when extending failover.

---

## Topology reload

`clusterState` carries online `Masters`/`Slaves`, every node declared by `CLUSTER SHARDS`, node health, and slot→node mappings. It is stored under `clusterStateHolder` (an `atomic.Value`) so reads are lock-free. Reloads run in the background, triggered by:

- `MOVED` errors (canonical case).
- A `CLUSTER SHARDS` owner reported as `fail` or `loading` when routing needs it.
- Periodic refresh.
- `SMIGRATED` notifications from the maintenance-notifications subsystem (it can target a partial reload by passing the affected slot ranges).

`loadState` tries every known address in random order until one returns a usable topology. It prefers `CLUSTER SHARDS` and preserves zero-slot shards as fanout targets. A null or empty node endpoint reuses the host of the connection that returned the reply; `?` is unknown and never aliases that origin. Only `online` nodes are routing candidates. A fanout that promises all nodes or all shards fails before dispatch if a `CLUSTER SHARDS` target is `failed` or `loading`, rather than silently executing a subset. Redis versions before 7.0, deployments that deny `CLUSTER SHARDS`, and configured `ClusterSlots` callbacks fan out over their available topology for backward compatibility. `DisableRoutingPolicies` retains the legacy caller-owned behavior. If all addresses fail, `activeAddrs` is cleared so DNS gets re-resolved on the next attempt. Stale node generations get GC'd one minute after a reload (so old node addresses don't keep accumulating).

The `ClusterClient` parent owns exactly one command-metadata store; per-node clients do not start metadata workers. A materially changed routing topology invalidates a live view and schedules a replacement in `PreferLive` mode; transient health changes and input ordering do not. The fetch captures the topology generation, selects a node from that generation, and sends `HELLO` and `COMMAND` through one pipeline on one physical connection. The internal metadata commands bypass application process/pipeline hooks and CSC, while normal dialing, initialization, authentication, `OnConnect`, and reauthentication still run.

The parsed `HELLO` server version is kept separate from the server-identity fingerprint (version plus modules). Version controls CSC compatibility only; the fingerprint and topology generation prevent metadata fetched across an upgrade, heterogeneous-node transition, or topology reload from being published for the wrong server. If one node has a different identity during a rolling change, the fetch tries sibling nodes before retiring the current live view. The fetch rechecks topology after reading the reply, publication rechecks the store identity, and a concurrent topology reload clears that identity and retires any old view. Routing uses the static view while a replacement is fetched.

The `IsClusterMode` option (#3255) flips routing behaviour for ElastiCache cluster mode, which emulates `CLUSTER SLOTS` but with cluster-mode semantics layered on a different topology. The flag exists because we can't always detect cluster mode reliably from the server response alone.

---

## Cross-slot, transactions, and pipelines

`ClusterClient` enforces these:

- **`Watch`/`MULTI`/`EXEC` (TxPipeline)**: all keys must hash to the same slot. Cross-slot returns `ErrCrossSlot` immediately and sets the error on every queued command. Fan-out and special request policies are rejected because one Redis transaction cannot span connections. `PING` is the narrow compatibility exception: inside a transaction it is connection-local and runs on the transaction's selected node.
- **`Pipeline`**: ordinary single-shard commands may target different slots. They are grouped by node and dispatched in parallel, so a regular pipeline can succeed where `TxPipeline` would fail. All-node/all-shard fan-out, special routing, cross-slot multi-shard invocations, and custom multi-shard policies are rejected before any command is dispatched. A metadata-derived multi-shard invocation whose complete keys all share its dispatch slot can use the ordinary one-node pipeline path. AutoPipeline applies the same admission rule and diverts supported `ReqSpecial` commands to their standalone path instead of merging them.
- **`Eval`/`EvalSha`**: routed by the **first key in `KEYS`**. Scripts that read multiple keys must declare them all in `KEYS` and all of them must hash to the same slot. If your script touches a key it didn't declare, you'll get cluster-routing errors at runtime.
- **`FT.CURSOR READ`/`DEL`**: sticky routing by cursor ID via the registered `ReqSpecial` handler, so pagination stays on the node that owns the cursor. Other special policies require their own explicit handler.

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
