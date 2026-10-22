# CSC tracking modes

Stage A started on master `856a2f0acd7e9a11267657b1368e7f3fe2bf016f`, before
PR #4093. Stage B integrates the actual merge, master
`48d65e5641b7ceaf83dce2019fbb3c26e7e6a74b` (2026-10-21). The final PR head
was `a9fb728d9f5a9ae39a805ae9390167ff2740cec6`; the earlier research head was
not used as the integration target.

The existing Stage A commit was rebased from `6a576212` to `3a5955cc`.
`backup/csc-stage-a-6a576212` preserves the original local history. Stage B
changes remain unstaged on `support-csc-opt-in-opt-out`; nothing was pushed.

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

`AutoPipeliner.WithCache` is explicitly synchronous. Blocking and unordered
faces divert through normal submission/routing validation and the existing
outside-pipeline admission/lifecycle gate. Ordered async marks remain in the
ordered stream, uncached; a positive mark warns or fails at submission in strict
mode. The forced bypass survives normal-path fallback and retries, and excludes
the cache-aware solo shortcut. Marks never depend on batch size. Completion is
joined before clearing invocation state. Existing unmarked batching is unchanged.

Typed DBSize, script-management and HIMPORT fieldset helpers delegate to the
underlying client, preserving its aggregation, fan-out and registry semantics.
They reject strict positive marks before side effects. AutoPipeline helper calls
join its existing diversion gate synchronously, so Close and queue limits still
cover them. No new pool, worker or resource owner is introduced.

Cold Conn/EphemeralConn and Watch wrappers copy the parent's immutable policy
when borrowing its pool. Their reads remain uncached, but every connection they
initialize must use the parent's tracking mode before returning to that pool.
Ordinary uncached reads on their tracked OPTOUT sockets also receive NO;
public pipelines and transactions retain their existing uncached contract.
Trackability accepts known wire key encodings independently of the narrower
cache-key eligibility gate, without invoking user marshalers.

## Test organization

The tracking-mode API, intent rules, protocol pairing and borrowed-connection
contracts share `csc_policy_test.go`, including their fake-server fixture.
Cluster routing, fan-out, metadata ownership, native helpers and owned live
Cluster fixtures are in `csc_cluster_test.go`. AutoPipeline ordering, admission,
context replacement and completion tests are in `autopipeline_internal_test.go`.
Coalescer and refresh mode coverage lives alongside the existing tests in
`csc_miss_coalesce_modes_test.go` and `csc_refresh_on_invalidate_test.go`.
Test names and CI selectors are unchanged by this consolidation.

## Original Stage A review units

1. **A1 policy and carrier:** `csc_policy.go`, `command.go`, policy fields in
   `local_cache.go`, immutable option snapshots. Tests: `csc_policy_test.go`.
2. **A2 protocol/foreground:** `csc_protocol.go`, `redis.go`,
   `csc_integration.go`. Tests: `csc_policy_test.go`.
3. **A3 miss coalescer:** `csc_miss_coalesce.go`,
   `csc_miss_coalesce_modes.go`. Tests: `csc_miss_coalesce_modes_test.go`, plus the
   coalesced variants of control/cancellation/handshake tests in A2.
4. **A4 refresh:** `csc_refresh_on_invalidate.go`. Tests:
   `csc_refresh_on_invalidate_test.go`.
5. **A5 current Cluster/capability boundaries:** `osscluster.go`,
   `osscluster_router.go`, `autopipeline.go`, `sentinel.go` policy snapshot.
   Tests: `csc_cluster_test.go` and `autopipeline_internal_test.go`.

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

## Cluster execution and metadata

Single-target execution supports the policy, including a one-slot multi-shard
request policy. Such a plan executes the original command without an unnecessary
child/aggregation boundary, keeping predicate intent and redirect errors intact.
Its redirected attempt uses the active retry loop's explicit destination instead of
re-resolving a stale slot owner. These changes apply only to the new policy;
untouched plain routing remains as before. MOVED under new policy forces a
destination network fetch, retires that command's old destination cache entry/reservation, re-pairs
and permits safe node-local publication. ASK uses the existing ASKING/read unit
without lookup, CACHING or storage. Untouched plain calls preserve legacy
redirect behavior. Refresh does not independently follow redirects.

Metadata-based multi-target execution supports the modes, marks and predicate.
Enable routing policies explicitly with `SetCommandInfoResolver`, as required by
#4093; caching configuration does not itself enable fan-out. Resolve eligibility
and intent once on the complete original invocation. Children clone this intent
and the immutable routing metadata view; retry loops retain them until completion.
Intent is never stored in a shared/consumed routing decision. Each fragment
independently reserves, fetches and publishes under its destination's tracking
coverage. A rejected reply, Redis error or redirect in one fragment neither
rolls back nor replays a successful sibling. Fan-out reads are not atomic across
shards, as in the router itself.

Nodes borrow read access to the parent-owned metadata store. They do not start
metadata workers and node Close never stops that store. Routing, dependency
extraction and cache keys use the same captured view, while publication compares
against the current parent view. A metadata change during a fetch may therefore
return data without publishing it. Background refresh uses current metadata and
does not retain the original predicate or Cmder.

Known unsupported original invocations fail before dispatch with a strict
positive mark. Connection/tracking capability is checked separately on each
actual target before its data write; this is not an all-shard handshake or
transaction. Independent siblings may already have completed if another target
fails acquisition/initialization. Late reply rejection or retired coverage is
an ordinary uncached result, including under strict.

Ordered AsyncAutoPipeline rejects cross-slot fan-out at submission, including
marked views. Blocking and unordered half-duplex faces permit the normal CSC
execution path. Marked views remain synchronous; they join deferred completion.
Rejection, full queue, Close and completion clear routing pins and invocation
state. Full-duplex public batches are not given caching support.

Custom node factories must preserve supplied configuration. Observable mode,
predicate-presence or parent-metadata-source omissions force uncached execution;
a positive strict mark fails before data dispatch. Actual tracking is checked after acquisition.
Deliberately replacing one non-nil predicate with another is a factory contract
violation: callback identity/equivalence is not probed or inferred. Arbitrary
conflicting per-node application policies are outside this factory contract.
Go functions cannot be compared for equivalence; checking a code pointer would
both miss different closure state and reject valid wrappers. The simpler check
detects omitted configuration and validates actual connection tracking. A factory
deliberately replacing the supplied application policy is outside this contract.

Existing limits remain: RESP3, DB 0, fixed credentials, built-in push processor;
refresh/coalescing require LocalCache. Explicit pipelines/transactions/scripts,
BCAST and full-duplex public batches are uncached. NewFailoverClusterClient is
supported. NewFailoverClient/Sentinel discovery remain uncached; a strict
positive mark on the former still rejects without a data write.

## Stage B review units

1. **Shared metadata access/ownership:** private source forwarding through
   ClusterOptions/Options, NewClient and clones; node CSC reads the parent's
   current view without owning shutdown. Tests: metadata ownership, overrides,
   node creation and custom-factory omission in `csc_cluster_test.go`.
2. **Original invocation policy:** `cscPrepareInvocation`, shared eligibility
   and intent resolution, immutable view/intent copied by Clone. Tests: predicate
   counts/inputs, byte keys, ineligible suffix commands and pinned generations.
3. **Fragment execution:** remove the Stage A multi-target gate; use #4093's
   existing per-subgroup retries with MOVED/ASK handling. No alternate planner
   or new retry budget. Tests: independent publication, absent/error replies,
   redirects and bypass in all modes, including the coalescer.
4. **AutoPipeline composition:** marked views use submit/routeCommand before
   diversion; submission determines ordered-face support. Tests cover all faces,
   sequential reuse, queue-full and Close cleanup, plus context-replacing hooks.
5. **Integration/CI:** real three-master fan-out, live metadata, invalidation,
   refresh without predicate replay, concurrent reads, owner disconnect, MOVED
   and ASK. CI runs both policy live fixtures alongside existing Cluster CSC
   fixtures inside the matching Redis image.

The post-#4093 checklist is implemented. Remaining limits are eligibility and
execution constraints stated above, not a blanket fan-out prohibition. This
change does not add arbitrary aggregators, cache public pipelines/transactions,
or implement a distributed commit/rollback. Enterprise/proxy slot moves retain
the existing reconnect requirements; they are not fixed by tracking modes.

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

### Historical Stage A results, 2026-10-21 (before #4093)

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

These figures describe Stage A on its original base. Stage B measurements
and validation below compare against the rebased Stage A commit instead.


### Stage B results, 2026-10-21 (merged #4093)

Comparison base: rebased Stage A `3a5955cc`, parent master `48d65e56`.
Environment: Go 1.26, Apple M4 Max / macOS arm64. Owned local fixtures use
Redis 7.4.4. Docker runs use the existing `redislabs/client-libs-test:8.10.0`
image and an amd64 static test binary, following the CI action. Containers have
no external network; Redis processes and databases are private to each run.

- Broad root regression selection without race: 1,084 passing test/subtest
  events, 12 skips, no failures (113.422 s).
- Broad root regression selection with race: 1,080 passing test/subtest events,
  12 skips, no failures (120.123 s). Subsequent additions for factory metadata
  omission and fan-out metadata replacement passed with race; the full policy
  selection including them and updated AutoPipeline reuse tests passed (13.446 s).
- Internal routing, RESP reader/writer and push package tests passed with race.
- Redis 8.10 live Cluster selection: 36 passing test/subtest events, no skips.
  This includes real three-master fan-out in every mode, coalescing, refresh,
  original predicate input/counts, concurrent reads, tracking-owner disconnect,
  MOVED, ASK, failover and the existing atomic-slot-migration fixture.
- Redis 8.10 with JSON, TimeSeries, Bloom and Search modules: 10 passing
  test/subtest events, no skips. Live metadata E2E and all previously skipped
  module eligibility/invalidation cases passed.
- `go build ./...`, root `go vet` and root custom vet passed in the active repo.
  `go vet ./...` and full custom vet also passed in an exact copy of tracked
  working-tree sources plus new feature tests. The active repo contains an
  ignored, pre-existing `bench/` harness with an undeclared HdrHistogram test
  dependency; it blocks `go vet ./...` there. It was left untouched.
- Formatter checks and `git diff --check` passed. The complete Compose image
  matrix, Redis Enterprise/proxy environment and unrelated Ginkgo integration
  suites were not run. These focused runs do not replace that matrix.

The local 7.4 skips comprise live-authoritative-metadata E2E (needs 8.10), atomic
slot migration (8.4+), five module cases, and five maintenance notification
migration cases. The first seven were exercised successfully in the owned
8.10 runs above; the maintenance-notification environment was not provided.

Reproduce the broad selection with an owned standalone Redis via REDIS_ADDR and
REDIS_PORT, and `REDIS_VERSION` matching that server (add `-race` for the race run):

```sh
go test -run '^(TestCSC|TestCluster|TestCommandMetadata|TestAutopipeline.*CSC|TestFDPipelineRejectsCSCStateCommands|TestAPCluster|TestAsyncAutoPipeline|TestAutoPipeline|TestFDShards|TestTruth)' -count=1 -timeout=300s .
go test -race -run '^TestCSCPolicy' -count=1 -timeout=120s .
go test -race ./internal/routing ./internal/proto ./push -count=1 -timeout=120s
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go test -c -o /tmp/csc-cluster.test .
docker run --rm --platform linux/amd64 --network none -e GOREDIS_CSC_LIVE=1 -e REDIS_VERSION=8.10 --mount type=bind,src=/tmp/csc-cluster.test,dst=/csc-cluster.test,readonly --entrypoint /csc-cluster.test redislabs/client-libs-test:8.10.0 -test.run '^(TestClusterCSCLive|TestCSCPolicy(Fanout)?Live)' -test.v -test.timeout=3m
```

Benchmarks ran serially with no concurrent test/build process, six 500 ms samples
per case. Baseline sources were exported from `3a5955cc`, without changing the
active branch. These are sample medians, not production throughput guarantees.

| Benchmark | Before Stage B | After Stage B | Allocations before/after |
| --- | ---: | ---: | --- |
| Plain Client hit | 212.4 ns | 210.5 ns | 240 B, 3 / 240 B, 3 |
| Plain routed Cluster hit | 282.3 ns | 295.5 ns | 240 B, 3 / 240 B, 3 |
| Plain Cluster miss | 27.523 us | 27.140 us | 1640 B, 31 / 1638–1640 B, 31 |
| Policy hit, predicate off | 258.5 ns | 256.9 ns | 240 B, 3 / 240 B, 3 |
| Policy hit, predicate on | 317.2 ns | 318.8 ns | 256 B, 4 / 256 B, 4 |

Cluster hit CPU cost rose about 4.7% while pinning/cleaning the invocation view;
there is no extra heap object on a hit. The predicate adds one owned key-slice
allocation, about 62 ns in this one-key sample. Miss changes are within the
limits of a loopback timing comparison. Protocol pairing remains one writer
flush with no separate CACHING round trip; wire-contract tests assert this.

No Stage B feature commit, staging or push was performed. Rebase replayed only
the already-existing Stage A commit; its original is retained by the backup ref.

### Review corrections, 2026-10-22

Corrections are unstaged on `support-csc-opt-in-opt-out`, HEAD
`421cf5c711d2896b38ecf0f33126beee317b1619`; the PR merge-base remains
`48d65e5641b7ceaf83dce2019fbb3c26e7e6a74b`. No Git history or index changes
were made for this correction pass.

The four verified regressions have behavioral tests: ordered marked reads,
Cluster typed-helper semantics, cold borrowed-connection handshakes, and known
wire-key forms in OPTOUT. The tests failed before their respective fixes.
Additional review covered normal-path retry without losing bypass, helpers'
Close/queue admission, and NO on ordinary borrowed Conn reads. Removing the
helper admission gate in an isolated copy made its regression test fail.
Sticky acquisition still deliberately revokes old connection-owned entries;
these corrections do not weaken that existing drainer-coverage protection.

Validation on Go 1.26 / macOS arm64 with owned Redis 7.4.4 fixtures:

- Broad regression selection above: 1,140 passing test/subtest events and
  12 skips, both normal (117.410 s) and race (123.155 s), no failures/races.
- `go test -race ./internal/... ./push/... ./maintnotifications -count=1
  -timeout=180s`: passed.
- Redis 8.10 CI live selection: 45 passing test/subtest events, no skips.
  Includes the new cold Conn/Watch and ordered half/full-duplex scenarios.
- Redis 8.10 with modules: 10 passing events, no skips. Covers the local
  metadata/module skips; atomic slot migration is covered by the live selection.
- `go vet ./...` and `go vet -vettool /private/tmp/csc-customvet ./...` passed
  in an isolated snapshot of tracked files plus all working changes (excluding
  the unrelated ignored bench harness described above).
- Five maintenance-notification integration scenarios remain skipped without
  their server environment. Enterprise/proxy and the complete Compose/Ginkgo
  matrix were not run. A skip is not counted as a pass.

Performance used six alternating 500 ms samples per side, GOMAXPROCS=4, Apple
M4 Max, with no concurrent test/build work. Medians in ns/op:

| Scenario | PR base | Before fixes | After fixes | After B/op; allocs/op |
| --- | ---: | ---: | ---: | --- |
| CSC off GET | 23592 | 23541 | 23287.5 | 232; 8 |
| Plain Client hit | 199.05 | 209.75 | 210.00 | 240; 3 |
| Plain Cluster hit | 269.80 | 289.60 | 290.05 | 240; 3 |
| Plain Client miss | 26296 | 26319 | 26285 | 1105; 24 |
| Async batch of 32 | 73213 | 72893.5 | 73309.5 | 7188; 177 |
| Reused marked view hit | n/a | 240.10 | 239.35 | 240; 3 |
| Predicate off/on | n/a | 248.75 / 310.30 | 249.05 / 309.80 | 240 / 256; 3 / 4 |

The corrections added no allocations in these measured hot paths. The feature's
previously reported 16-byte GET size increase and plain-hit CPU overhead versus
the PR base remain. Small timing differences overlap sample ranges; these local
loopback measurements do not establish a production improvement or prove zero
regression. View creation and rare administrative helper calls are outside the
hot-hit measurements. Protocol pairing still uses one writer flush.
