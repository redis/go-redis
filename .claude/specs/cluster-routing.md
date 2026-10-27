# Cluster routing

This is an architectural reference for how `ClusterClient` decides which node to send a command to and how it merges results from multi-shard fan-outs. Read this before changing `osscluster.go`, `osscluster_router.go`, `command_policy_resolver.go`, or anything under `internal/routing/` and `internal/hashtag/`.

The mental model: a command becomes a `(slot, policy, aggregator)` triple, executes against one or more nodes, and its responses are collapsed by the aggregator. Most bugs come from getting the slot wrong, picking the wrong policy, or aggregating responses incorrectly.

---

## Slot computation

Redis Cluster has 16384 slots. A key's slot is `CRC16(hashtag(key)) % 16384` where `hashtag(key)` returns the substring inside the first balanced `{...}` if present, otherwise the whole key (`internal/hashtag/hashtag.go`). Empty keys map to a random slot to avoid deterministic concentration.

Two non-obvious edge cases:

- **Byte-slice keys** (#3049): the cluster client used to cast args via `fmt.Sprintf("%s", v)` which doesn't handle `[]byte` consistently. `stringArg` in `command.go` now type-switches to handle `[]byte` keys passed through `client.Do(ctx, "GET", []byte("k"))`. If you add a generic-args path, route it through `stringArg`.
- **Commands whose key is *not* the first arg, or which take the slot directly** — `COUNTKEYSINSLOT` is the canonical example: its argument is the slot integer, not a key (#3327). When you add such a command, register the correct key-spec in `command.go` or its slot will be computed from the wrong arg and the routing will land on the wrong shard.

`keylessCommands` in `command.go` is the override list for commands with no key at all (`PING`, `TIME`, `CONFIG`, `WAIT`, …). Routing them by "first arg" would either error or pick a random shard; instead, the router uses the configured `ShardPicker` (round-robin / random / static). Adding `WAIT` (#3615) and `TIME` (#3722) here fixed real bugs where pipelines and txpipelines mis-routed.

---

## MOVED and ASK redirection

`MOVED <slot> <addr>` means "this slot lives somewhere else now, permanently" — the client follows the redirect and triggers a topology reload (`loadState`). `ASK <slot> <addr>` means "this slot is *currently* migrating; just for this command, send `ASKING` then retry against `<addr>`" — no topology reload, only a one-shot redirect. Detection is via typed errors (`error.go`, `IsMovedError`/`IsAskError`) which return the address as well as a bool.

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
| `RespDefaultKeyless` | Return the first non-error result. |
| `RespDefaultHashSlot` | Reassemble in original key order (for `MGET` etc.). |
| `RespAllSucceeded` | Fail the whole call if any shard errored. |
| `RespOneSucceeded` | Succeed if any shard succeeded. |
| `RespAggSum` / `RespAggMin` / `RespAggMax` | Numeric aggregates. |
| `RespAggLogicalAnd` / `RespAggLogicalOr` | Boolean aggregates. |
| `RespSpecial` | Custom — e.g. `ZUNION` with `COUNT` (#3802). |

`CommandPolicy.CanBeUsedInPipeline()` returns `false` for `ReqAllNodes`, `ReqAllShards`, and `ReqMultiShard` — these can't sit inside a regular pipeline because they fan out across multiple connections. If you mark a new command with one of these request policies, expect pipeline tests to need updates.

`command_policy_resolver.go` resolves policies for a given command. Resolution order: explicit policy in the resolver → server-supplied tips (RESP3 `command info`) → defaults. There's a fallback chain you can extend with `SetFallbackResolver` (used by some test scaffolding and module-specific resolvers).

---

## Aggregators (`internal/routing/aggregator.go`)

The aggregator is the runtime side of `ResponsePolicy`. Two families:

- **Keyless aggregators** — collect responses without per-key bookkeeping. `DefaultKeylessAggregator`, `AllSucceededAggregator`, `AggSumAggregator`, etc.
- **Keyed aggregators** — preserve key order. `DefaultKeyedAggregator` is the one you want for `MGET`, `MSET`, `DEL`: each shard returns results for its subset of keys, and the aggregator stitches them back in the user's original order. Reordering bugs here surface as "MGET returned values in the wrong slots."

When you add a new fan-out command, the typical workflow is:

1. Pick the right `RequestPolicy` (almost always `ReqMultiShard` for multi-key, `ReqAllShards` for global state).
2. Pick the right `ResponsePolicy`. If none fit, register a `RespSpecial` aggregator.
3. Test it with **at least 3 shards** and **keys distributed across all of them**, including the empty case (no keys land on a particular shard).

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

`clusterState` carries `Masters`, `Slaves`, and slot→node mappings, built from `CLUSTER SLOTS`. It's stored under `clusterStateHolder` (an `atomic.Value`) so reads are lock-free. Reloads run in the background, triggered by:

- `MOVED` errors (canonical case).
- Periodic refresh.
- `SMIGRATED` notifications from the maintenance-notifications subsystem (it can target a partial reload by passing the affected slot ranges).

`loadState` tries every known address in random order until one returns a usable topology. If all fail, `activeAddrs` is cleared so DNS gets re-resolved on the next attempt. Stale node generations get GC'd one minute after a reload (so old node addresses don't keep accumulating).

The `IsClusterMode` option (#3255) flips routing behaviour for ElastiCache cluster mode, which emulates `CLUSTER SLOTS` but with cluster-mode semantics layered on a different topology. The flag exists because we can't always detect cluster mode reliably from the server response alone.

---

## Cross-slot, transactions, and pipelines

`ClusterClient` enforces these:

- **`Watch`/`MULTI`/`EXEC` (TxPipeline)**: all keys must hash to the same slot. Cross-slot returns `ErrCrossSlot` immediately and sets the error on every queued command. This is a Redis Cluster protocol limit, not a client choice.
- **`Pipeline`**: no restriction. Commands are grouped by slot and dispatched in parallel; each shard sees only the commands targeting it. This is why `Pipeline` can succeed where `TxPipeline` would fail.
- **`Eval`/`EvalSha`**: routed by the **first key in `KEYS`**. Scripts that read multiple keys must declare them all in `KEYS` and all of them must hash to the same slot. If your script touches a key it didn't declare, you'll get cluster-routing errors at runtime.
- **`FT.CURSOR` and similar paginated commands**: sticky routing by cursor ID via `ReqSpecial`, so pagination stays on the node that owns the cursor.

---

## History worth knowing

Recurring shape of cluster bugs:

- **Wrong slot computation**: byte-slice keys (#3049), commands with non-key first arg (`COUNTKEYSINSLOT` #3327), missing keyless markers (`WAIT` #3615, `TIME` #3722).
- **Race in node accounting**: `clusterNodes.Addrs()` returning a shared slice (#3219).
- **Replica routing fallbacks**: `slotClosestNode` not handling all-failing case (#3043).
- **Treating recoverable failures as fatal**: `MASTERDOWN` (#3164), `dial tcp` errors during failover (#3786).
- **Unnecessary delays on recoverable redirects**: backoff after MOVED/ASK (#3048).
- **Failover variants forgetting options**: `ReplicaOnly` (#3482), `MaintNotificationsConfig` (#3600).

When you add a feature or fix a bug here, add a test that:

1. Spins a 6-node cluster (the test scaffolding in `main_test.go` already provides `clusterScenario` with 16600–16605).
2. Uses keys that you've verified land on different shards (use `CLUSTER KEYSLOT` if unsure).
3. Exercises both the happy path and the redirect path.

The cluster integration tests are slow but catch real protocol issues — the unit-only tests don't.
