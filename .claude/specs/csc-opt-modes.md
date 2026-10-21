# CSC tracking modes: Stage A

Baseline: master `856a2f0acd7e9a11267657b1368e7f3fe2bf016f`.
PR #4093 was open at `bb87339aca920dbc6860cef755f62294ad8d60c7`
when implementation began. No code from that branch is included.

Normative specification:
https://redislabs.atlassian.net/wiki/spaces/CAE/pages/6605438982/Client-Side+Caching+OPTIN+OPTOUT+Introduction+and+Requirements

Supporting design and eligibility:
https://redislabs.atlassian.net/wiki/spaces/CAE/pages/6622019594/Client-Side+Caching+OPTIN+OPTOUT+Research+and+Proposed+Client+Implementations
https://redislabs.atlassian.net/wiki/spaces/CAE/pages/6212812803/8.10+-+CSC+capabilities+alignment+Command+Eligibility+HLD

## API and contract

`CacheConfig` (also `ClientSideCacheConfig`) adds `TrackingMode`, `Cacheable`
and `Strict`. Empty mode means plain. Unknown modes fail command execution
before acquiring a connection. Policy is snapshotted at client construction;
configuration must not be changed after construction. Explicit `ClientSideCache`
replaces storage, while these policy fields still apply.

Eligibility and execution capability gate intent. Intent precedence is mark,
predicate, mode default. The predicate applies in plain too. It receives a
canonical command identifier and an owned slice of actual Redis keys, outside
locks; calls may be concurrent. The slice may be retained but not mutated.
Callbacks follow the repository's no-panic/no-client-control contract.

`Client.WithCache(bool)` and `ClusterClient.WithCache(bool)` return a reusable
synchronous `CacheView`. It owns no pools, workers or Close. `redis.WithCache`
also accepts a `UniversalClient`. The typed command methods reuse `cmdable`;
Pipeline, TxPipeline, Watch and AutoPipeline constructors are not exposed.
The view's Process/Do clean their mark even if a hook terminates execution.
Commands must not be executed concurrently; sequential reuse starts fresh.

`AutoPipeliner.WithCache` is explicitly synchronous and diverts through the
existing outside-pipeline admission/lifecycle gate. Marks never depend on
whether a batch happened to contain one command. Async completion is joined
before the view returns. Existing unmarked batching remains uncached, except
for the existing cache-aware solo execution path.

## Five review units

1. **A1 policy and carrier:** `csc_policy.go`, `command.go`, policy fields in
   `local_cache.go`, immutable option snapshots. Tests: `csc_policy_test.go`.
2. **A2 protocol/foreground:** `csc_protocol.go`, `redis.go`,
   `csc_integration.go`. Tests: `csc_protocol_test.go`.
3. **A3 miss coalescer:** `csc_miss_coalesce.go`,
   `csc_miss_coalesce_modes.go`. Tests: `csc_policy_coalesce_test.go`, plus the
   coalesced variants of control/cancellation/handshake tests in A2.
4. **A4 refresh:** `csc_refresh_on_invalidate.go`. Tests:
   `csc_policy_refresh_test.go`.
5. **A5 current Cluster/capability boundaries:** `osscluster.go`,
   `osscluster_router.go`, `autopipeline.go`, `sentinel.go` policy snapshot.
   Tests: `csc_policy_cluster_test.go` and `csc_policy_live_test.go`.

The feature is integrated atomically: all three publishing paths support the
protocol before modes are exposed. Unit grouping is for review, not permission
to release a partially wired OPTIN implementation.

## Physical execution and publication

- Foreground owns a connection in `_process`/`withConn`. Prefix and data are
  written by one WithWriter callback, after any injected setup commands.
- The miss coalescer snapshots the prefix and data into the same owned wire
  buffer before queue admission. Its sole reader consumes ACK and data for each
  request. Prefix bytes count toward serialization and batch budgets.
- Refresh owns a main-pool connection and emits a prefix for every OPTIN target.
  Chunk request and expected reply budgets include prefix/ACK overhead.
- Each reader consumes pushes at the existing blocking checkpoints. No new
  speculative checkpoint or reader ownership model is introduced.
- A complete scalar Redis error from CACHING logs through internal.Logger and
  prevents publication, while the read reply still reaches the caller. Read
  errors keep their identity and retry behavior. Unexpected ACK types/status,
  push/framing/transport failures retire the connection/session.
- A failed pair write retires the connection even if a user encoder returns an
  error typed as a Redis error. A buffered prefix must never reach a later call.
- Existing metadata fingerprint, reservation token, owner connection and init
  generation publication checks remain in force. ACK success adds permission;
  it does not replace these checks.
- Only LocalCache defines token zero as declined admission. Such a known
  refusal skips YES / uses NO when applicable. Custom Cache tokens stay opaque.

Strict validates a positive mark before dispatch and again after connection
initialization, before the data write. Admission refusal, ASK, rejected absent
Cluster replies, or coverage lost after send are not late misuse errors.

Bypass precedes Get/Reserve/demand signaling. It neither waits for an active
fetch nor modifies a prior entry. Predicate decisions are not authorization.

## Stage A Cluster boundary

Single-target execution supports the policy, including a one-slot multi-shard
request policy. Such a plan executes the original command without an unnecessary
child/aggregation boundary, keeping predicate intent and redirect errors intact.
Its redirected attempt uses the outer loop's explicit destination instead of
re-resolving a stale slot owner. These changes apply only to the new policy;
untouched plain routing remains as before. MOVED under new policy forces a
destination network fetch, retires that command's old destination cache entry/reservation, re-pairs
and permits safe node-local publication. ASK uses the existing ASKING/read unit
without lookup, CACHING or storage. Untouched plain calls preserve legacy
redirect behavior. Refresh does not independently follow redirects.

Multi-target plans with a new mode, predicate or mark execute uncached. A
positive mark warns once at the parent; strict rejects before child dispatch.
Children inherit a forced bypass marker and get NO under actual OPTOUT when
trackability is known. Plain without a predicate/mark preserves existing
behavior. No temporary cluster metadata owner or alternate planner is added.

Custom node factories must preserve supplied configuration. Observable mode or
predicate-presence omissions force uncached execution; a positive strict mark
fails before data dispatch. Actual tracking is checked after acquisition.
Deliberately replacing one non-nil predicate with another is a factory contract
violation: callback identity/equivalence is not probed or inferred. Supporting
arbitrary conflicting per-node application policies is deferred to Stage B.
Go functions cannot be compared for equivalence; checking a code pointer would
both miss different closure state and reject valid wrappers. The simpler check
detects omitted configuration and validates actual connection tracking. A factory
deliberately replacing the supplied application policy is outside this contract.

Existing limits remain: RESP3, DB 0, fixed credentials, built-in push processor;
refresh/coalescing require LocalCache. Explicit pipelines/transactions/scripts,
BCAST and full-duplex public batches are uncached. NewFailoverClusterClient is
supported. NewFailoverClient/Sentinel discovery remain uncached; a strict
positive mark on the former still rejects without a data write.

## Stage B after the actual #4093 merge

- Record Stage A's base SHA and validation results; update this branch against the
  actual merged master in a separate integration step. Do not apply a patch
  prepared against the current PR head without inspecting the final diff.
- Replay Stage A tests before expanding capabilities, including plain defaults.
- Align the immutable view used for routing with node CSC eligibility,
  dependency extraction, entry keys and current-generation publication checks.
- Separate access to parent metadata from ownership of its shutdown. Node Close
  must not stop a parent-owned store. Do not merely alias cmdMeta pointers.
- Carry original invocation intent once through child Clone/setArgs. Keep it
  separate from consumed/shared routing decisions; propagate children without
  rerunning a keys-sensitive predicate on subgroups.
- Adapt MOVED/ASK pairing to every new subgroup retry loop without replaying
  completed siblings or increasing retry budgets.
- Compose cache validation with routeCommand and its async/full-duplex
  admission, rejection, cancellation and cleanup paths. Test sequential reuse.
- Remove the multi-target guard only after cross-slot tests cover original
  predicate input, independent fragment publication, missing replies, redirects,
  metadata changes and node removal. No distributed cache commit/rollback.
- Retest Enterprise through the same code path. Shard-add/same-proxy slot move
  reconnect requirements are inherited operational limits, not fixed by modes.

## Validation

Use the standard testing skill/Makefile for the full Redis image matrix. Focused
commands (root module):

```sh
go test -run '^(TestCSC|TestClusterCSC|TestAutopipeline.*CSC|TestFDPipelineRejectsCSCStateCommands)' -count=1 -timeout=180s .
go test -race -run '^TestCSCPolicy' -count=1 -timeout=180s .
go test -race -run '^(TestCSC|TestClusterCSC|TestAutopipeline.*CSC|TestFDPipelineRejectsCSCStateCommands)' -count=1 -timeout=180s .
go test -run '^$' -bench '^(BenchmarkClientCSCHit|BenchmarkClusterCSCMiss|BenchmarkCSCPolicyHit)$' -benchmem -benchtime=500ms -count=6 .
go build ./...
go vet ./...
```

The live test uses the repository's owned six-process redis-server fixture,
including invalidation, refresh, concurrent reads, MOVED and legacy ASK migration.
It never connects to an external application database. Validate Redis 8.x and
Enterprise in the normal CI environments; a local Redis 7.4 run is not evidence
for the full supported-version matrix or atomic slot migration (Redis 8.4+).

### Recorded results, 2026-10-21

Environment: Go 1.26, macOS arm64 / Apple M4 Max, Redis 7.4.4. The full focused
regression selection also used an owned temporary standalone Redis process;
`REDIS_PORT` and `REDIS_ADDR` pointed only at that process, with
`REDIS_VERSION=7.4`. The test runner stopped it on completion. All Go commands
used `GOCACHE=/private/tmp/csc-opt-go-cache` because the default cache directory
was unavailable in the sandbox.

- Full focused regression without `-race`: 589 passing test/subtest events,
  6 skipped, no failures (41.989 s).
- Full focused regression with `-race`: 589 passing test/subtest events,
  6 skipped, no failures (45.245 s).
- Policy tests including the real Cluster with `-race`: passed (7.901 s).
- `go build ./...`, `go vet ./...`, root-package custom vet, formatter checks
  and `git diff --check`: passed.
- Full `go vet -vettool /private/tmp/csc-customvet ./...` reports ten missing
  SetVal methods in the unchanged `internal/routing/aggregator.go`. The same
  ten diagnostics were reproduced on the untouched base SHA. The actual
  Makefile custom-vet invocation targets the root package, which passes.
- The installed staticcheck executable was built with Go 1.24.4 and cannot
  analyze this Go 1.26 module; staticcheck was not completed.
- The Docker/Redis 8.x/Enterprise image matrix was not run. Local execution
  does not replace those CI environments.

The six skips are the existing atomic-slot-migration test (needs Redis 8.4+),
three module reads (TimeSeries, probabilistic and query engine modules), and
two second-key invalidation cases requiring JSON / TimeSeries. They are not
reported as passing. The new Stage A tests did not skip.

Final benchmarks ran serially, with no other test/build job active, using six
500 ms samples per case. Values below are sample medians, without a production
latency or statistical significance claim:

| Benchmark | Base | Stage A | Allocation result |
| --- | ---: | ---: | --- |
| Existing plain Client hit | 191.2 ns | 202.0 ns (+5.6%) | 224 B, 3 allocs in both |
| Existing plain Cluster miss | 27.267 us | 27.469 us (+0.7%) | 1623–1624 B, 31 allocs in both |
| Policy hit, identical config, predicate off/on | 244.5 ns (off) | 302.6 ns (on) | 224 B / 3 allocs to 240 B / 4 allocs |

The policy comparison uses a separate, identical-config control because the
existing Client benchmark uses a different cache capacity. The predicate costs
about 58 ns and one owned key-slice allocation for this one-key hit. Plain hits
have no new allocation, but do have measurable CPU overhead. Loopback miss
timings vary with scheduling; the small miss delta is not conclusive. CACHING
and the data command share one writer flush; there is no separate control
round trip. Hits send neither command.

All implementation and test changes remain unstaged/untracked on
`support-csc-opt-in-opt-out`; no add, commit or push was performed.
